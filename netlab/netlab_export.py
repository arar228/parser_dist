import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# ОТКЛЮЧАЕМ ПРОКСИ для решения проблем с подключением
proxy_vars = ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY']
for var in proxy_vars:
    if var in os.environ:
        del os.environ[var]
os.environ['no_proxy'] = '*'
os.environ['NO_PROXY'] = '*'

import requests
import defusedxml.ElementTree as ET
from typing import List, Dict, Optional
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
from core.logger import get_logger
import threading
# Убираем неиспользуемые импорты - уведомления теперь в main.py
# from core.telegram_notify import notify_export_progress, notify_export_complete, notify_export_error, notify_export_start
# from core.upsert import upsert_products
# from core.db_engine import AsyncSessionLocal
from sqlalchemy import text
import datetime

# ВАЖНО: Изменена логика получения остатков и цен
# Теперь Netlab получает остатки с 3 СКЛАДОВ (суммирует) + отдельно количество в транзите
# Склады для суммирования: Курская, Калужская, Лобненская
# Количество в транзите: отдельное поле 'transit'
# ЦЕНЫ: ТОЛЬКО категория F для стратегических партнеров (не категория N)
# Это обеспечивает точную картину наличия товаров и правильные цены для партнеров

NETLAB_USERNAME = "YOUR_NETLAB_USERNAME"  # Замените на актуальный
NETLAB_PASSWORD = "YOUR_NETLAB_PASSWORD"      # Замените на актуальный
NETLAB_AUTH_URL = "http://services.netlab.ru/rest/authentication/token.json"
NETLAB_GOODS_INFO_URL_TEMPLATE = "http://services.netlab.ru/rest/catalogsZip/goodsByPartnumber/{part_number}.xml?oauth_token={token}"
NETLAB_GOODS_BY_UID_URL_TEMPLATE = "http://services.netlab.ru/rest/catalogsZip/goodsByUid/{goods_id}.xml?oauth_token={token}"
NETLAB_NEW_GOODS_URL = "http://services.netlab.ru/rest/catalogsZip/newGoods.xml?oauth_token={token}"
NETLAB_DELETED_GOODS_URL = "http://services.netlab.ru/rest/catalogsZip/versions/2/{catalog_name}/{category_id}.xml?oauth_token={token}&showDeleted=1"


def get_netlab_token(username: str = NETLAB_USERNAME, password: str = NETLAB_PASSWORD, log: bool = False) -> Optional[str]:
    params = {"username": username, "password": password}
    try:
        resp = requests.get(NETLAB_AUTH_URL, params=params, timeout=30)
        resp.raise_for_status()
        text = resp.text
        if log:
            print("[Netlab] Ответ на токен:", repr(text[:200]))  # Логируем только если явно указано
        if text.startswith('{} && '):
            text = text[6:].lstrip()
        else:
            json_start = text.find('{')
            if json_start > 0:
                text = text[json_start:]
        try:
            data = json.loads(text)
        except Exception as e:
            print("[Netlab] Ошибка парсинга JSON:", e)
            print("[Netlab] Текст для парсинга:", repr(text))
            return None
        token = (
            data.get("tokenResponse", {})
            .get("data", {})
            .get("token")
        )
        return token
    except Exception as e:
        print(f"[Netlab] Ошибка получения токена: {e}")
        return None


def parse_product_xml(xml_text: str) -> Optional[Dict]:
    try:
        root = ET.fromstring(xml_text)
        ns = {'df': 'http://ws.web.netlab.com/'}
        status_code = root.findtext('.//df:status/df:code', namespaces=ns) or root.findtext('.//status/code')
        if status_code != '200':
            return None
        data_node = root.find('.//df:data', namespaces=ns) or root.find('.//data')
        if data_node is None:
            return None
        props = {}
        for prop in data_node.findall('.//df:properties/df:property', namespaces=ns) + data_node.findall('.//properties/property'):
            name = prop.findtext('df:name', namespaces=ns) or prop.findtext('name')
            value = prop.findtext('df:value', namespaces=ns) or prop.findtext('value')
            if name:
                props[name.strip().lower()] = value
        # Получаем остатки только с 3 складов (суммируем)
        stock_val = 0.0
        warehouse_keys = [
            'количество на курской',
            'количество на калужской', 
            'количество на лобненской'
        ]
        
        for key in warehouse_keys:
            try:
                val = props.get(key, 0)
                if val and str(val).strip() not in ['', '***', 'null', 'none']:
                    stock_val += float(val)
            except (ValueError, TypeError):
                continue
        
        # Получаем количество в транзите отдельно
        transit_val = 0.0
        try:
            transit_raw = props.get('количество в транзите', 0)
            if transit_raw and str(transit_raw).strip() not in ['', '***', 'null', 'none']:
                transit_val = float(transit_raw)
        except (ValueError, TypeError):
            pass
        
        result = {
            'id': data_node.findtext('df:id', namespaces=ns) or data_node.findtext('id'),
            'part_number': props.get('pn') or props.get('partnumber') or props.get('p/n'),
            'name': props.get('название') or props.get('наименование') or props.get('name'),
            'brand': props.get('производитель') or props.get('бренд') or props.get('brand'),
            'stock': stock_val,
            'transit': transit_val,  # Количество в транзите
            'price_usd': float(
                props.get('цена по категории f') or 0  # ТОЛЬКО категория F для стратегических партнеров
            ),
        }
        # Временный тестовый вывод
        try:
            print(f"[TEST][Netlab] OK: id={result['id']} part_number={result['part_number']} price_usd={result['price_usd']} stock={result['stock']}")
        except Exception as e:
            print(f"[TEST][Netlab] ERROR: {e} | result={result}")
        return result
    except Exception as e:
        print(f"[Netlab] Ошибка парсинга XML: {e}")
        return None


# --- Ограничение частоты запросов к Netlab ---
RATE_LIMIT_LOCK = threading.Lock()
LAST_REQUEST_TIME = [0.0]
RATE_LIMIT_DELAY = 0.02  # 20 мс

def rate_limited(func):
    def wrapper(*args, **kwargs):
        with RATE_LIMIT_LOCK:
            now = time.time()
            elapsed = now - LAST_REQUEST_TIME[0]
            if elapsed < RATE_LIMIT_DELAY:
                time.sleep(RATE_LIMIT_DELAY - elapsed)
            result = func(*args, **kwargs)
            LAST_REQUEST_TIME[0] = time.time()
            return result
    return wrapper


@rate_limited
def get_product_info(part_number: str, token: str) -> Optional[Dict]:
    url = NETLAB_GOODS_INFO_URL_TEMPLATE.format(part_number=part_number, token=token)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return parse_product_xml(resp.text)
    except Exception as e:
        print(f"[Netlab] Ошибка получения товара {part_number}: {e}")
        return None


@rate_limited
def get_product_info_by_uid(goods_id: str, token: str) -> Optional[Dict]:
    url = NETLAB_GOODS_BY_UID_URL_TEMPLATE.format(goods_id=goods_id, token=token)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return parse_product_xml(resp.text)
    except Exception as e:
        print(f"[Netlab] Ошибка получения товара по id {goods_id}: {e}")
        return None


def get_netlab_products(part_numbers: List[str], token: Optional[str] = None) -> List[Dict]:
    if token is None:
        token = get_netlab_token()
    if not token:
        print("[Netlab] Не удалось получить токен, выгрузка невозможна.")
        return []
    products = []
    for pn in part_numbers:
        info = get_product_info(pn, token)
        if info:
            products.append(info)
    return products


def get_netlab_products_by_ids(goods_ids: List[str], token: Optional[str] = None) -> List[Dict]:
    if token is None:
        token = get_netlab_token()
    if not token:
        print("[Netlab] Не удалось получить токен, выгрузка невозможна.")
        return []
    products = []
    for gid in goods_ids:
        info = get_product_info_by_uid(gid, token)
        if info:
            products.append(info)
    return products


def get_netlab_prices_batches(goods_ids: List[str], batch_size: int = 100, token: Optional[str] = None) -> List[Dict]:
    """Инкрементальное обновление цен/остатков по списку goods_id (batched, параллельно)."""
    if token is None:
        token = get_netlab_token()
    if not token:
        print("[Netlab] Не удалось получить токен, обновление невозможно.")
        return []
    batches = [goods_ids[i:i+batch_size] for i in range(0, len(goods_ids), batch_size)]
    results = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(get_netlab_products_by_ids, batch, token) for batch in batches]
        for i, future in enumerate(as_completed(futures), 1):
            batch_products = future.result()
            results.extend(batch_products)
            get_logger('Netlab').info(f"[Netlab] Обработан батч {i}/{len(batches)} (товаров: {len(batch_products)})")
    return results


def get_netlab_new_goods(token: Optional[str] = None) -> List[Dict]:
    if token is None:
        token = get_netlab_token()
    if not token:
        print("[Netlab] Не удалось получить токен, выгрузка невозможна.")
        return []
    url = NETLAB_NEW_GOODS_URL.format(token=token)
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        ns = {'df': 'http://ws.web.netlab.com/'}
        goods = []
        for item in root.findall('.//df:goods', namespaces=ns) + root.findall('.//goods'):
            props = {}
            for prop in item.findall('.//df:property', namespaces=ns) + item.findall('.//property'):
                name = prop.findtext('df:name', namespaces=ns) or prop.findtext('name')
                value = prop.findtext('df:value', namespaces=ns) or prop.findtext('value')
                if name:
                    props[name.strip().lower()] = value
            # Получаем остатки только с 3 складов (суммируем)
            stock_val = 0.0
            warehouse_keys = [
                'количество на курской',
                'количество на калужской', 
                'количество на лобненской'
            ]
            
            for key in warehouse_keys:
                try:
                    val = props.get(key, 0)
                    if val and str(val).strip() not in ['', '***', 'null', 'none']:
                        stock_val += float(val)
                except (ValueError, TypeError):
                    continue
            
            # Получаем количество в транзите отдельно
            transit_val = 0.0
            try:
                transit_raw = props.get('количество в транзите', 0)
                if transit_raw and str(transit_raw).strip() not in ['', '***', 'null', 'none']:
                    transit_val = float(transit_raw)
            except (ValueError, TypeError):
                pass
            
            goods.append({
                'id': item.findtext('df:id', namespaces=ns) or item.findtext('id'),
                'part_number': props.get('pn') or props.get('partnumber') or props.get('p/n'),
                'name': props.get('название') or props.get('наименование') or props.get('name'),
                'brand': props.get('производитель') or props.get('бренд') or props.get('brand'),
                'stock': stock_val,
                'transit': transit_val,  # Количество в транзите
                'price_rub': float(
                    props.get('цена по категории f') or 0  # ТОЛЬКО категория F для стратегических партнеров
                ),
                'price_usd': 0.0,
            })
        return goods
    except Exception as e:
        print(f"[Netlab] Ошибка получения новых товаров: {e}")
        return []


def get_netlab_deleted_goods(catalog_name: str, category_id: str, token: Optional[str] = None) -> List[Dict]:
    if token is None:
        token = get_netlab_token()
    if not token:
        print("[Netlab] Не удалось получить токен, выгрузка невозможна.")
        return []
    url = NETLAB_DELETED_GOODS_URL.format(catalog_name=catalog_name, category_id=category_id, token=token)
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        ns = {'df': 'http://ws.web.netlab.com/'}
        goods = []
        for item in root.findall('.//df:goods', namespaces=ns) + root.findall('.//goods'):
            props = {}
            for prop in item.findall('.//df:property', namespaces=ns) + item.findall('.//property'):
                name = prop.findtext('df:name', namespaces=ns) or prop.findtext('name')
                value = prop.findtext('df:value', namespaces=ns) or prop.findtext('value')
                if name:
                    props[name.strip().lower()] = value
            # Получаем остатки только с 3 складов (суммируем)
            stock_val = 0.0
            warehouse_keys = [
                'количество на курской',
                'количество на калужской', 
                'количество на лобненской'
            ]
            
            for key in warehouse_keys:
                try:
                    val = props.get(key, 0)
                    if val and str(val).strip() not in ['', '***', 'null', 'none']:
                        stock_val += float(val)
                except (ValueError, TypeError):
                    continue
            
            # Получаем количество в транзите отдельно
            transit_val = 0.0
            try:
                transit_raw = props.get('количество в транзите', 0)
                if transit_raw and str(transit_raw).strip() not in ['', '***', 'null', 'none']:
                    transit_val = float(transit_raw)
            except (ValueError, TypeError):
                pass
            
            goods.append({
                'id': item.findtext('df:id', namespaces=ns) or item.findtext('id'),
                'part_number': props.get('pn') or props.get('partnumber') or props.get('p/n'),
                'name': props.get('название') or props.get('наименование') or props.get('name'),
                'brand': props.get('производитель') or props.get('бренд') or props.get('brand'),
                'stock': stock_val,
                'transit': transit_val,  # Количество в транзите
                'price_rub': float(props.get('цена по категории f') or 0),  # ТОЛЬКО категория F для стратегических партнеров
                'price_usd': 0.0,
            })
        return goods
    except Exception as e:
        print(f"[Netlab] Ошибка получения удалённых товаров: {e}")
        return []


# --- Получение курса доллара для безналичных расчётов из Netlab ---
def get_usd_rate_from_netlab(token=None):
    import requests
    if token is None:
        token = get_netlab_token()
    if not token:
        print("[Netlab] Не удалось получить токен для курса USD.")
        return None
    url = f"http://services.netlab.ru/rest/catalogsZip/info.json?oauth_token={token}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        text = resp.text
        # Удаляем префикс '{} && ' если есть
        if text.startswith('{} && '):
            text = text[6:].lstrip()
        else:
            json_start = text.find('{')
            if json_start > 0:
                text = text[json_start:]
        data = json.loads(text)
        items = data.get('entityListResponse', {}).get('data', {}).get('items', [])
        if not items:
            print("[Netlab] Не удалось получить курс USD: пустой ответ.")
            return None
        usd_rate = items[0]['properties'].get('usdRateNonCash')
        if usd_rate is None:
            print("[Netlab] Не найден usdRateNonCash в ответе.")
            return None
        return float(usd_rate)
    except Exception as e:
        print(f"[Netlab] Ошибка получения курса USD: {e}")
        return None


# --- ЛОГИРОВАНИЕ ---
logger = get_logger('Netlab')

async def get_all_netlab_products() -> List[Dict]:
    start_time = time.time()
    logger.info("[Netlab][START] Выгрузка начата")
    
    # Уведомления о начале и завершении отправляются в main.py
    # Здесь только сбор данных без upsert и уведомлений
    
    token = get_netlab_token()
    if not token:
        error_msg = "Не удалось получить токен, выгрузка невозможна."
        logger.error(f"[Netlab] {error_msg}")
        # Убираем notify_export_error отсюда - это будет в main.py
        return []
    
    usd_rate = get_usd_rate_from_netlab(token)
    if not usd_rate:
        error_msg = "Не удалось получить курс USD, выгрузка невозможна."
        logger.error(f"[Netlab] {error_msg}")
        # Убираем notify_export_error отсюда - это будет в main.py
        return []
    
    logger.info(f"[Netlab] Курс USD (безнал): {usd_rate}")
    base_url = "http://services.netlab.ru/rest/catalogsZip"
    catalogs_url = f"{base_url}/list.xml?oauth_token={token}"
    try:
        resp = requests.get(catalogs_url, timeout=60)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        ns = {'df': 'http://ws.web.netlab.com/'}
        catalogs = [c.findtext('df:name', namespaces=ns) or c.findtext('name')
                    for c in root.findall('.//df:catalog', namespaces=ns) + root.findall('.//catalog')]
        logger.info(f"[Netlab] Каталоги: {catalogs}")
    except Exception as e:
        logger.error(f"[Netlab] Ошибка получения списка каталогов: {e}")
        return []
    all_products = []
    for catalog in catalogs:
        cat_url = f"{base_url}/{catalog}.xml?oauth_token={token}"
        try:
            resp = requests.get(cat_url, timeout=60)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            ns = {'df': 'http://ws.web.netlab.com/'}
            categories = []
            for c in root.findall('.//df:category', namespaces=ns) + root.findall('.//category'):
                leaf = c.findtext('df:leaf', namespaces=ns) or c.findtext('leaf')
                if str(leaf).lower() in ('true', 'истина', '1'):
                    cat_id = c.findtext('df:id', namespaces=ns) or c.findtext('id')
                    categories.append(cat_id)
            logger.info(f"[Netlab][CATALOG:{catalog}] Листовых категорий: {len(categories)}")
        except Exception as e:
            logger.error(f"[Netlab] Ошибка получения категорий каталога {catalog}: {e}")
            continue
        for cat_id in categories:
            goods_url = f"{base_url}/{catalog}/{cat_id}.xml?oauth_token={token}"
            try:
                resp = requests.get(goods_url, timeout=120)
                resp.raise_for_status()
                root = ET.fromstring(resp.text)
                ns = {'df': 'http://ws.web.netlab.com/'}
                count_before = len(all_products)
                for item in root.findall('.//df:goods', namespaces=ns) + root.findall('.//goods'):
                    props = {}
                    for prop in item.findall('.//df:property', namespaces=ns) + item.findall('.//property'):
                        name = prop.findtext('df:name', namespaces=ns) or prop.findtext('name')
                        value = prop.findtext('df:value', namespaces=ns) or prop.findtext('value')
                        if name:
                            props[name.strip().lower()] = value
                    price_usd = float(
                        props.get('цена по категории f') or 0  # ТОЛЬКО категория F для стратегических партнеров
                    )
                    price_rub = round(price_usd * usd_rate, 2)
                    
                    # Получаем остатки только с 3 складов (суммируем)
                    stock_val = 0.0
                    
                    # Склады для суммирования
                    warehouse_keys = [
                        'количество на курской',
                        'количество на калужской', 
                        'количество на лобненской'
                    ]
                    
                    for key in warehouse_keys:
                        try:
                            val = props.get(key, 0)
                            if val and str(val).strip() not in ['', '***', 'null', 'none']:
                                stock_val += float(val)
                        except (ValueError, TypeError):
                            continue
                    
                    # Получаем количество в транзите отдельно
                    transit_val = 0.0
                    try:
                        transit_raw = props.get('количество в транзите', 0)
                        if transit_raw and str(transit_raw).strip() not in ['', '***', 'null', 'none']:
                            transit_val = float(transit_raw)
                    except (ValueError, TypeError):
                        pass
                    
                    # Оставляем stock как число для правильной работы с базой данных
                    if stock_val.is_integer():
                        stock = int(stock_val)
                    else:
                        stock = float(stock_val)
                    # Очищаем партномер от пробелов
                    clean_part_num = str(props.get('pn') or props.get('partnumber') or props.get('p/n') or '').strip()
                    
                    # Детальное логирование для отладки
                    if not clean_part_num or clean_part_num.strip() == '':
                        logger.warning(f"[Netlab][DEBUG] Пропущен товар с пустым партномером: ID={item.findtext('df:id', namespaces=ns)}, props={props}")
                        continue
                    
                    # Проверяем на специальные значения
                    if clean_part_num.lower() in ['null', 'none', 'undefined', '']:
                        logger.warning(f"[Netlab][DEBUG] Пропущен товар с невалидным партномером '{clean_part_num}': ID={item.findtext('df:id', namespaces=ns)}")
                        continue
                    
                    # Проверяем длину
                    if len(clean_part_num.strip()) == 0:
                        logger.warning(f"[Netlab][DEBUG] Пропущен товар с партномером из пробелов: ID={item.findtext('df:id', namespaces=ns)}")
                        continue
                    
                    # Если все проверки пройдены - добавляем товар
                    product = {
                        'id': item.findtext('df:id', namespaces=ns) or item.findtext('id'),
                        'article': clean_part_num,  # article = part_number для Netlab
                        'part_number': clean_part_num,
                        'name': props.get('название') or props.get('наименование') or props.get('name'),
                        'brand': props.get('производитель') or props.get('бренд') or props.get('brand'),
                        'category_code': cat_id,  # Код категории
                        'stock': stock,
                        'transit': transit_val,  # Количество в транзите (дополнительное поле)
                        'price_usd': price_usd,
                        'price_rub': price_rub,
                        'package_volume': None,  # Netlab не предоставляет объем упаковки
                        'package_weight': None,  # Netlab не предоставляет вес упаковки
                        'tech_specs': json.dumps({
                            'transit': transit_val,
                            'catalog': catalog,
                            'category_id': cat_id,
                            'netlab_id': item.findtext('df:id', namespaces=ns) or item.findtext('id')
                        }, ensure_ascii=False),
                        'transit_date': None,  # Netlab не предоставляет дату транзита
                        'distributor': 'Netlab',
                        'is_active': True
                    }
                    all_products.append(product)
                count_after = len(all_products)
                logger.info(f"[Netlab][CATALOG:{catalog}][CATEGORY:{cat_id}] Товаров добавлено: {count_after - count_before}, всего: {count_after}")
                if count_after - count_before < 10:
                    logger.warning(f"[Netlab][CATALOG:{catalog}][CATEGORY:{cat_id}] Мало товаров: {count_after - count_before}")
                
            except Exception as e:
                logger.error(f"[Netlab] Ошибка получения товаров категории {cat_id} каталога {catalog}: {e}")
                continue
    logger.info(f"[Netlab][ITEMS] Всего товаров выгружено: {len(all_products)}")
    if len(all_products) < 1000:
        logger.error(f"[ALERT][Netlab] Резкое уменьшение товаров: {len(all_products)}")
    
    # Убираем дублирующиеся партномеры
    unique_products = {}
    duplicates_removed = 0
    for product in all_products:
        part_number = product.get('part_number', '').strip()
        if part_number:
            if part_number not in unique_products:
                unique_products[part_number] = product
            else:
                duplicates_removed += 1
                logger.warning(f"[Netlab][DEBUG] Дублирующийся партномер пропущен: {part_number}")
    
    all_products = list(unique_products.values())
    logger.info(f"[Netlab][DEDUP] Убрано дублирующихся товаров: {duplicates_removed}")
    logger.info(f"[Netlab][DEDUP] Уникальных товаров для вставки: {len(all_products)}")
    
    # Товары собраны, возвращаем их в main.py для дальнейшей обработки
    logger.info(f"[Netlab][COLLECTED] Товары собраны для передачи в main.py")
    
    # Убираем всю логику upsert отсюда - она будет в main.py
    logger.info("[Netlab][END] Сбор данных завершен")
    logger.info(f"[Netlab][RETURN] Возвращаем {len(all_products)} товаров в main.py")
    return all_products


if __name__ == "__main__":
    asyncio.run(get_all_netlab_products()) 