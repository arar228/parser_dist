import asyncio
import logging
import concurrent.futures
import datetime
import os
import time
import tempfile
from functools import wraps
import importlib
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
# Удален импорт PostgreSQL-специфичного модуля для совместимости с MySQL

# ОТКЛЮЧАЕМ ПРОКСИ для решения проблем с подключением
import requests
# Отключаем прокси в переменных окружения
proxy_vars = ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY']
for var in proxy_vars:
    if var in os.environ:
        del os.environ[var]
os.environ['no_proxy'] = '*'
os.environ['NO_PROXY'] = '*'
# Отключаем прокси для requests по умолчанию
requests.adapters.DEFAULT_RETRIES = 3

# Импортируем конфигурацию
import config

from core.db_engine import AsyncSessionLocal
from core.db_models import Product
from merlion.merlion_downloader_fixed import get_merlion_products

# Очищаем историю уведомлений при запуске для предотвращения дублирования
from core.telegram_notify import clear_notification_history
clear_notification_history()
logging.info("[Main] История уведомлений очищена при запуске")

from netlab.netlab_export import (
    get_netlab_products,
    get_netlab_prices_batches,
    get_netlab_new_goods,
    get_netlab_deleted_goods,
    get_all_netlab_products
)
from treolan.treolan_export import get_treolan_products
from vvp.vvp_export import get_vvp_products
from marvel.marvel_export import get_marvel_products, can_update_marvel
from ocs.ocs_export import main as ocs_main
from ocs.ocs_sale_export import main as ocs_sale_main
from core.upsert import upsert_products
from core.logger import get_logger, get_distributor_logger

def is_night_mode():
    """Проверяет, находится ли система в ночном режиме (19:00-7:00 МСК)"""
    try:
        # Пытаемся использовать zoneinfo для корректной обработки часовых поясов (Python 3.9+)
        import zoneinfo
        moscow_tz = zoneinfo.ZoneInfo("Europe/Moscow")
    except (ImportError, AttributeError):
        # Fallback для старых версий Python или отсутствия zoneinfo
        # В зимнее время МСК = UTC+3, в летнее время МСК = UTC+4
        # Используем UTC+3 как базовое значение
        moscow_tz = datetime.timezone(datetime.timedelta(hours=3))
    
    moscow_time = datetime.datetime.now(moscow_tz)
    current_hour = moscow_time.hour
    
    # Ночной режим: с 19:00 до 7:00 МСК
    return current_hour >= 19 or current_hour < 7

def can_start_update():
    """Проверяет, можно ли запускать обновление"""
    if is_night_mode():
        logging.info(f"[NIGHT MODE] Ночной режим активен (19:00-7:00 МСК). Обновление отложено до 7:00 МСК.")
        return False
    return True

TREOLAN_MIN_INTERVAL = 60 * 60  # 60 минут (каждый час)
TREOLAN_LAST_UPDATE_FILE = "static/last_update_treolan.txt"

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Сброс всех обработчиков у корневого логгера
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        # logging.FileHandler("main.log"),  # если нужен файл
    ]
)

MERLION_MIN_INTERVAL = 4 * 60 * 60  # 4 часа (рабочие часы 7:00-19:00 МСК)
MERLION_LAST_UPDATE_FILE = "static/last_update_merlion.txt"
MERLION_KNOWN_IDS_FILE = os.path.join("static", "known_merlion_ids.json")

# ТАЙМЕРЫ ОБНОВЛЕНИЯ ДИСТРИБЬЮТОРОВ:
# - Merlion: каждые 4 часа (рабочие часы)
# - Netlab, Treolan, VVP, Marvel, OCS SALE, OCS: каждые 60 минут
# - Главный цикл: проверяет каждые 60 минут
# - Порядок обновления: Merlion → Netlab → Treolan → VVP → Marvel → OCS SALE → OCS

UPDATE_INTERVAL_SECONDS = 60 * 60  # 1 час (60 минут) - для проверки дистрибьюторов

REQUIRED_KEYS = {'id', 'name', 'price'}

NETLAB_MIN_INTERVAL = 60 * 60  # 60 минут (каждый час)
NETLAB_LAST_UPDATE_FILE = "static/last_update_netlab.txt"

VVP_MIN_INTERVAL = 60 * 60  # 1 час
VVP_LAST_UPDATE_FILE = "static/last_update_vvp.txt"

OCS_MIN_INTERVAL = 60 * 60  # 1 час
OCS_LAST_UPDATE_FILE = "static/last_update_ocs.txt"

OCS_SALE_MIN_INTERVAL = 60 * 60  # 1 час
OCS_SALE_LAST_UPDATE_FILE = "static/last_update_ocs_sale.txt"

MARVEL_LAST_UPDATE_FILE = "static/last_update_marvel.txt"

def send_alert(distributor, message):
    logger = get_logger(distributor)
    logger.error(f"[ALERT][{distributor}] {message}")
    # Здесь можно добавить отправку email или Telegram-оповещения

def distributor_task(distributor_name):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            logger = get_logger(distributor_name)
            start = time.time()
            logger.info(f"[{distributor_name}][START] Выгрузка начата")
            try:
                result = await func(*args, **kwargs)
                count = len(result) if isinstance(result, list) else 'N/A'
                logger.info(f"[{distributor_name}][SUCCESS] Выгружено {count} товаров за {time.time()-start:.1f} сек")
                logger.info(f"[{distributor_name}][END] Выгрузка завершена")
                return result
            except Exception as e:
                logger.error(f"[{distributor_name}][ERROR] {str(e)}")
                send_alert(distributor_name, str(e))
                logger.info(f"[{distributor_name}][END] Выгрузка завершена с ошибкой")
                return []
        return wrapper
    return decorator

@distributor_task('Merlion')
async def fetch_merlion():
    return await get_merlion_products()



@distributor_task('Netlab')
async def fetch_netlab():
    # Здесь должен быть список part_numbers для полной выгрузки, либо реализуйте аналог get_all_netlab_products()
    # Пока что просто пример: []
    return await get_all_netlab_products()

@distributor_task('VVP')
async def fetch_vvp():
    return await get_vvp_products()

@distributor_task('Marvel')
async def fetch_marvel():
    return await get_marvel_products()

def validate_products(products, distributor):
    logger = get_logger(distributor)
    example_logged = False
    for p in products:
        if not example_logged:
            logger.info(f"[{distributor}][EXAMPLE] Пример товара: {p}")
            example_logged = True
        if not REQUIRED_KEYS.issubset(p):
            logger.error(f"[{distributor}][ERROR] Некорректная структура товара: {p}")
            send_alert(distributor, f"Некорректная структура товара: {p}")
            raise ValueError(f"Некорректная структура товара: {p}")

def check_integrity(new_count, prev_count, distributor):
    if prev_count and new_count < prev_count * 0.7:
        msg = f"Резкое уменьшение товаров: {prev_count} → {new_count}"
        logging.warning(f"[{distributor}][ANOMALY] {msg}")
        send_alert(distributor, msg)


# --- Новая функция: первичная полная выгрузка Netlab ---
async def full_netlab_load():
    # Уведомление отправляется из wrapper'а в update_all()
    async with AsyncSessionLocal() as db:
        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            products = await get_all_netlab_products()
            if not products or not any(p.get('name') and p.get('part_number') for p in products):
                logging.error("[Netlab] Не удалось получить валидные товары при первичной выгрузке! Старая база не тронута.")
                return
            # Удаление старых товаров Netlab теперь происходит в upsert_products
            # await db.execute(text("DELETE FROM products WHERE distributor = 'Netlab'"))
            # await db.commit()
            # Вставка товаров теперь происходит в netlab_export.py через upsert_products
            # Убираем дублирующий код вставки
            shown = 0
            for p in products:
                # Фильтрация и логирование пустых товаров
                if not p.get('name') or not p.get('part_number'):
                    print(f"[Netlab][WARNING] Пропущен товар без имени или партномера: {p}")
                    continue
                if shown < 3:
                    print(f"[Netlab][DEBUG] Пример товара для вставки: {p}")
                    shown += 1
            
            logging.info(f"[Netlab] Первичная выгрузка завершена. Товаров получено: {len(products)}")
            logging.info("[Netlab] Вставка в базу данных происходит в netlab_export.py")
            
    # Записываем дату обновления

# --- Новая функция: инкрементальное обновление цен и остатков Netlab ---
async def update_netlab_prices():
    async with AsyncSessionLocal() as db:
        result = await db.execute(text("SELECT article FROM products WHERE distributor = 'Netlab' AND is_active = TRUE"))
        rows = result.fetchall()
        all_ids = [r[0] for r in rows if r[0]]
        if not all_ids:
            logging.warning("[Netlab] Нет товаров для обновления цен/остатков.")
            return
            
        # Используем ThreadPoolExecutor для синхронной функции
        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            products = await loop.run_in_executor(executor, lambda: get_netlab_prices_batches(all_ids))
        
        updated_count = 0
        for p in products:
            if p.get('distributor') == 'Netlab':
                price_rub = 0
                price_usd = convert_price_rub(p.get('price_usd'))
                stock = convert_stock(p.get('stock'))
            else:
                price_rub = p.get('price_rub')
                price_usd = p.get('price_usd')
                stock = p.get('stock')
            await db.execute(text("""
                UPDATE products SET price_rub = :price_rub, price_usd = :price_usd, stock = :stock WHERE article = :id AND distributor = :distributor
                """),
            {"price_rub": price_rub, "price_usd": price_usd, "stock": stock, "id": p.get('id'), "distributor": p.get('distributor', 'Netlab')}
            )
            updated_count += 1
        await db.commit()
        logging.info(f"[Netlab] Обновлено цен/остатков: {updated_count}")

async def deactivate_netlab_deleted_goods(db):
    # Получаем все категории из Netlab (как в get_all_netlab_products)
    from netlab.netlab_export import get_netlab_token
    import defusedxml.ElementTree as ET
    token = get_netlab_token()
    if not token:
        print("[Netlab] Не удалось получить токен для поиска удалённых товаров.")
        return
    base_url = "http://services.netlab.ru/rest/catalogsZip"
    catalogs_url = f"{base_url}/list.xml?oauth_token={token}"
    try:
        import requests
        resp = requests.get(catalogs_url, timeout=60)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        ns = {'df': 'http://ws.web.netlab.com/'}
        catalogs = [c.findtext('df:name', namespaces=ns) or c.findtext('name')
                    for c in root.findall('.//df:catalog', namespaces=ns) + root.findall('.//catalog')]
    except Exception as e:
        print(f"[Netlab] Ошибка получения списка каталогов: {e}")
        return
    deleted_ids = set()
    from netlab.netlab_export import get_netlab_deleted_goods
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
                    if catalog and cat_id:
                        try:
                            deleted_goods = get_netlab_deleted_goods(catalog, cat_id, token)
                            for g in deleted_goods:
                                if g.get('id'):
                                    deleted_ids.add(g['id'])
                        except Exception as e:
                            print(f"[Netlab] Ошибка при получении удалённых товаров для каталога {catalog}, категории {cat_id}: {e}")
        except Exception as e:
            print(f"[Netlab] Ошибка получения категорий каталога {catalog}: {e}")
    if deleted_ids:
        await db.execute(text("""
                UPDATE products SET is_active = FALSE WHERE distributor = 'Netlab' AND article = ANY(:ids)
            """),
            {"ids": list(deleted_ids)}
        )
        await db.commit()
        print(f"[Netlab] Деактивировано товаров: {len(deleted_ids)}")
    else:
        print("[Netlab] Нет удалённых товаров для деактивации.")

def can_update(last_update_file, min_interval):
    try:
        with open(last_update_file, "r", encoding="utf-8") as f:
            last_update = f.read().strip()
            last_update_dt = datetime.datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S")
            now = datetime.datetime.now()
            if (now - last_update_dt).total_seconds() < min_interval:
                return False
    except Exception:
        return True
    return True

# --- OCS SALE update wrapper для CLI ---
async def ocs_sale_update_wrapper():
    ocs_sale_can_update = can_update(OCS_SALE_LAST_UPDATE_FILE, OCS_SALE_MIN_INTERVAL)
    if ocs_sale_can_update:
        try:
            await ocs_sale_main()  # ocs_sale_main() уже отправляет уведомления и записывает дату
            logging.info("[OCS SALE] Обновление завершено.")
        except Exception as e:
            logging.error(f"[OCS SALE][ERROR] {e}")
            send_alert("OCS SALE", str(e))
    else:
        logging.warning("[OCS SALE][SKIP] Пропуск обновления: слишком частый вызов.")

# --- OCS update wrapper для CLI ---
async def ocs_update_wrapper():
    ocs_can_update = can_update(OCS_LAST_UPDATE_FILE, OCS_MIN_INTERVAL)
    if ocs_can_update:
        try:
            await ocs_main()  # ocs_main() уже отправляет уведомления и записывает дату
            logging.info("[OCS] Обновление завершено.")
        except Exception as e:
            logging.error(f"[OCS][ERROR] {e}")
            send_alert("OCS", str(e))
    else:
        logging.warning("[OCS][SKIP] Пропуск обновления: слишком частый вызов.")

def map_merlion_product(p):
    return {
        "id": p.get("article"),
        "name": p.get("name"),
        "price": p.get("price_rub"),
        "brand": p.get("brand"),
        "stock": p.get("stock"),
        # Добавьте другие поля по необходимости
    }

# --- Конвертер для price_rub ---
def convert_price_rub(val):
    try:
        f = float(val)
        if f == 0.0:
            return 0
        if f.is_integer():
            return int(f)
        return f
    except Exception:
        return 0

# --- Конвертер для stock ---
def convert_stock(val):
    try:
        f = float(val)
        if f == 0.0:
            return 0
        if f.is_integer():
            return int(f)
        return f
    except Exception:
        return 0

async def update_all():
    logging.info(f"[Main][DEBUG] === НАЧАЛО update_all() ===")
    logging.info(f"[Main][DEBUG] Время: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Очищаем историю уведомлений перед каждой выгрузкой
    from core.telegram_notify import clear_notification_history
    clear_notification_history()
    logging.info("[Main] История уведомлений очищена перед выгрузкой")
    
    async with AsyncSessionLocal() as db:
        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            prev_counts = {}
            for dist, file in [("Merlion", MERLION_KNOWN_IDS_FILE)]:
                if os.path.exists(file):
                    try:
                        import json
                        with open(file, "r", encoding="utf-8") as f:
                            prev_counts[dist] = len(json.load(f))
                    except Exception:
                        prev_counts[dist] = None
                else:
                    prev_counts[dist] = None

            merlion_can_update = can_update(MERLION_LAST_UPDATE_FILE, MERLION_MIN_INTERVAL)
            netlab_can_update = can_update(NETLAB_LAST_UPDATE_FILE, NETLAB_MIN_INTERVAL)
            treolan_can_update = can_update(TREOLAN_LAST_UPDATE_FILE, TREOLAN_MIN_INTERVAL)
            vvp_can_update = can_update(VVP_LAST_UPDATE_FILE, VVP_MIN_INTERVAL)
            marvel_can_update = can_update_marvel()
            ocs_can_update = can_update(OCS_LAST_UPDATE_FILE, OCS_MIN_INTERVAL)
            ocs_sale_can_update = can_update(OCS_SALE_LAST_UPDATE_FILE, OCS_SALE_MIN_INTERVAL)
            
            logging.info(f"[Main][DEBUG] Статус обновлений:")
            logging.info(f"[Main][DEBUG] - Merlion: {merlion_can_update}")
            logging.info(f"[Main][DEBUG] - Netlab: {netlab_can_update}")
            logging.info(f"[Main][DEBUG] - Treolan: {treolan_can_update}")
            logging.info(f"[Main][DEBUG] - VVP: {vvp_can_update}")
            logging.info(f"[Main][DEBUG] - Marvel: {marvel_can_update}")
            logging.info(f"[Main][DEBUG] - OCS: {ocs_can_update}")
            logging.info(f"[Main][DEBUG] - OCS SALE: {ocs_sale_can_update}")

            # --- Последовательный запуск с паузой ---
            # Merlion: сбор данных и upsert в одном блоке (ПЕРВЫМ!)
            if merlion_can_update:
                try:
                    logging.info("[Merlion] Запуск экспорта Merlion...")
                    from merlion.merlion_downloader_fixed import get_merlion_products
                    from core.telegram_notify import notify_export_start, notify_export_complete
                    import time
                    
                    # Отправляем уведомление о начале
                    await notify_export_start("Merlion")
                    
                    # Получаем товары
                    start_time = time.time()
                    products = await get_merlion_products()
                    # УБИРАЕМ: map_merlion_product - он убивает part_number!
                    # if products and not all(('id' in p and 'price' in p) for p in products):
                    #     products = [map_merlion_product(p) for p in products]
                    
                    if products:
                        # Очистка и upsert происходят внутри upsert_products
                        logging.info("[Merlion] Начинаем upsert товаров Merlion...")
                        
                        logging.info(f"[Merlion][EXAMPLE] Пример товара: {products[0]}")
                        validate_products(products, "Merlion")
                        if prev_counts.get('Merlion') is not None:
                            check_integrity(len(products), prev_counts.get('Merlion'), "Merlion")
                        
                        # Выполняем upsert
                        stats = await upsert_products(products, "Merlion", db)
                        end_time = time.time()
                        duration_minutes = (end_time - start_time) / 60
                        
                        logging.info(f"[Merlion][STATS] Добавлено: {stats['inserted']}, Обновлено: {stats['updated']}, Деактивировано: {stats['deleted']}")
                        logging.info(f"[Merlion][END] Выгрузка завершена. Всего обработано: {len(products)}")
                        
                        # Записываем дату обновления
                        import json
                        with open(MERLION_KNOWN_IDS_FILE, "w", encoding="utf-8") as f:
                            json.dump([p.get('id') for p in products if p.get('id')], f, ensure_ascii=False)
                        with open(MERLION_LAST_UPDATE_FILE, "w", encoding="utf-8") as f:
                            f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        
                        # Отправляем уведомление о завершении
                        logging.info(f"[Merlion][DEBUG] === ВЫЗОВ notify_export_complete ===")
                        logging.info(f"[Merlion][DEBUG] Место вызова: update_all() в main.py")
                        logging.info(f"[Merlion][DEBUG] Параметры:")
                        logging.info(f"[Merlion][DEBUG] - distributor: 'Merlion'")
                        logging.info(f"[Merlion][DEBUG] - total_items: {len(products)}")
                        logging.info(f"[Merlion][DEBUG] - inserted: {stats['inserted']}")
                        logging.info(f"[Merlion][DEBUG] - updated: {stats['updated']}")
                        logging.info(f"[Merlion][DEBUG] - errors: 0")
                        logging.info(f"[Merlion][DEBUG] - duration_minutes: {duration_minutes}")
                        logging.info(f"[Merlion][DEBUG] =================================")
                        
                        await notify_export_complete(
                            "Merlion",
                            len(products),
                            stats['inserted'],
                            stats['updated'],
                            0,  # ошибок нет, передаем 0
                            duration_minutes
                        )
                        
                    else:
                        logging.error("[Merlion] Не удалось получить товары!")
                        
                except Exception as e:
                    logging.error(f"[Merlion][ERROR] Ошибка при экспорте: {e}")
                    send_alert("Merlion", f"Ошибка при экспорте: {e}")
                
                await asyncio.sleep(30)
            else:
                logging.info("[Merlion] Обновление не требуется")

            # Netlab: сбор данных и upsert в одном блоке
            if netlab_can_update:
                try:
                    logging.info("[Netlab] Запуск экспорта Netlab...")
                    from netlab.netlab_export import get_all_netlab_products
                    from core.telegram_notify import notify_export_start, notify_export_complete, notify_export_error
                    import time
                    
                    # Засекаем время начала ВСЕГО процесса (включая уведомления)
                    start_time = time.time()
                    start_datetime = datetime.datetime.now()
                    logging.info(f"[Netlab] Время начала процесса: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # Отправляем уведомление о начале
                    await notify_export_start("Netlab")
                    notify_start_time = time.time()
                    logging.info(f"[Netlab] Уведомление о начале отправлено за {notify_start_time - start_time:.2f} сек")
                    
                    # Получаем товары
                    products = await get_all_netlab_products()
                    data_collection_time = time.time()
                    logging.info(f"[Netlab] Данные собраны за {data_collection_time - notify_start_time:.2f} сек")
                    
                    if products:
                        # Очищаем старые товары Netlab перед полной загрузкой
                        logging.info("[Netlab] Очищаем базу от старых товаров Netlab...")
                        await db.execute(text("DELETE FROM products WHERE distributor = 'Netlab'"))
                        await db.commit()
                        db_clean_time = time.time()
                        logging.info(f"[Netlab] База очищена за {db_clean_time - data_collection_time:.2f} сек")
                        
                        logging.info(f"[Netlab][EXAMPLE] Пример товара: {products[0]}")
                        
                        # Выполняем upsert
                        stats = await upsert_products(products, "Netlab", db)
                        upsert_time = time.time()
                        logging.info(f"[Netlab] Upsert выполнен за {upsert_time - db_clean_time:.2f} сек")
                        
                        end_time = time.time()
                        end_datetime = datetime.datetime.now()
                        duration_minutes = (end_time - start_time) / 60
                        duration_seconds = end_time - start_time
                        
                        logging.info(f"[Netlab] Общее время процесса: {duration_seconds:.2f} сек ({duration_minutes:.3f} мин)")
                        logging.info(f"[Netlab] Время завершения: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
                        
                        logging.info(f"[Netlab][STATS] Добавлено: {stats['inserted']}, Обновлено: {stats['updated']}, Деактивировано: {stats['deleted']}")
                        logging.info(f"[Netlab][END] Выгрузка завершена. Всего обработано: {len(products)}")
                        
                        # Записываем дату обновления
                        with open(NETLAB_LAST_UPDATE_FILE, "w", encoding="utf-8") as f:
                            f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        
                        # Отправляем уведомление о завершении
                        await notify_export_complete(
                            "Netlab",
                            len(products),
                            stats['inserted'],
                            stats['updated'],
                            0,  # ошибок нет, передаем 0
                            duration_minutes
                        )
                        
                    else:
                        error_msg = "Не удалось получить товары!"
                        logging.error(f"[Netlab] {error_msg}")
                        await notify_export_error("Netlab", error_msg)
                        
                except Exception as e:
                    logging.error(f"[Netlab][CRITICAL] Ошибка при экспорте: {e}")
                    await notify_export_error("Netlab", f"Ошибка при экспорте: {e}")
                await asyncio.sleep(30)

            if treolan_can_update:
                await update_treolan_only()
                await asyncio.sleep(30)

            if vvp_can_update:
                async def vvp_update_wrapper():
                    from core.telegram_notify import notify_export_start
                    await notify_export_start("VVP")
                    vvp_logger = get_distributor_logger('vvp')
                    vvp_logger.info("[VVP][START] Выгрузка начата")
                    await update_vvp_only(vvp_logger)
                    with open(VVP_LAST_UPDATE_FILE, "w", encoding="utf-8") as f:
                        f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    vvp_logger.info("[VVP][END] Выгрузка завершена.")
                    logging.info("[VVP][END] Выгрузка завершена.")
                    return []
                await vvp_update_wrapper()
                await asyncio.sleep(30)

            # Marvel: сбор данных и upsert в одном блоке
            if marvel_can_update:
                try:
                    logging.info(f"[Marvel][DEBUG] === НАЧАЛО ОБРАБОТКИ MARVEL ===")
                    logging.info(f"[Marvel][DEBUG] Время: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    logging.info(f"[Marvel][DEBUG] marvel_can_update: {marvel_can_update}")
                    
                    logging.info("[Marvel] Запуск экспорта Marvel...")
                    from marvel.marvel_export import get_marvel_products
                    from core.telegram_notify import notify_export_start, notify_export_complete
                    import time
                    
                    # Отправляем уведомление о начале
                    logging.info(f"[Marvel][DEBUG] Вызываем notify_export_start('Marvel')")
                    await notify_export_start("Marvel")
                    logging.info(f"[Marvel][DEBUG] notify_export_start('Marvel') завершен")
                    
                    # Получаем товары
                    start_time = time.time()
                    logging.info(f"[Marvel][DEBUG] Вызываем get_marvel_products()")
                    products = await get_marvel_products()
                    logging.info(f"[Marvel][DEBUG] get_marvel_products() вернул {len(products) if products else 0} товаров")
                    
                    if products:
                        # Очищаем старые товары Marvel перед полной загрузкой
                        logging.info("[Marvel] Очищаем базу от старых товаров Marvel...")
                        await db.execute(text("DELETE FROM products WHERE distributor = 'Marvel'"))
                        await db.commit()
                        logging.info("[Marvel] База очищена от старых товаров Marvel")
                        
                        # Валидация для Marvel (используем правильные ключи)
                        marvel_required_keys = {'article', 'name', 'price_rub'}
                        for p in products[:3]:  # Проверяем первые 3 товара
                            if not marvel_required_keys.issubset(p.keys()):
                                missing = marvel_required_keys - p.keys()
                                logging.warning(f"[Marvel][WARNING] Отсутствуют поля: {missing}")
                                break
                        
                        # Выполняем upsert
                        logging.info(f"[Marvel][DEBUG] Вызываем upsert_products для {len(products)} товаров")
                        stats = await upsert_products(products, "Marvel", db)
                        logging.info(f"[Marvel][DEBUG] upsert_products завершен: {stats}")
                        
                        end_time = time.time()
                        duration_minutes = (end_time - start_time) / 60
                        
                        logging.info(f"[Marvel][STATS] Добавлено: {stats['inserted']}, Обновлено: {stats['updated']}, Деактивировано: {stats['deleted']}")
                        logging.info(f"[Marvel][END] Выгрузка завершена. Всего обработано: {len(products)}")
                        
                        # Записываем дату обновления
                        with open(MARVEL_LAST_UPDATE_FILE, "w", encoding="utf-8") as f:
                            f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        
                        # Отправляем уведомление о завершении
                        logging.info(f"[Marvel][DEBUG] Вызываем notify_export_complete('Marvel', ...)")
                        await notify_export_complete(
                            "Marvel",
                            len(products),
                            stats['inserted'],
                            stats['updated'],
                            0,  # ошибок нет, передаем 0
                            duration_minutes
                        )
                        logging.info(f"[Marvel][DEBUG] notify_export_complete('Marvel', ...) завершен")
                        
                    else:
                        logging.error("[Marvel] Не удалось получить товары!")
                        
                    logging.info(f"[Marvel][DEBUG] === КОНЕЦ ОБРАБОТКИ MARVEL ===")
                        
                except Exception as e:
                    logging.error(f"[Marvel][ERROR] Ошибка при экспорте: {e}")
                    send_alert("Marvel", f"Ошибка при экспорте: {e}")
                
                await asyncio.sleep(30)
            else:
                logging.info("[Marvel] Обновление не требуется")

            # OCS SALE: обновление акционных товаров
            if ocs_sale_can_update:
                await ocs_sale_update_wrapper()
                await asyncio.sleep(30)

            # OCS: обновление всех товаров (ПОСЛЕДНИМ!)
            if ocs_can_update:
                await ocs_update_wrapper()
                await asyncio.sleep(30)

            logging.info("Все задачи обновления завершены")
    logging.info(f"[Main][DEBUG] === КОНЕЦ update_all() ===")

# Функция update_marvel_only удалена - теперь Marvel обрабатывается в update_all()

async def update_netlab_only(netlab_logger=None):
    """CLI функция для обновления только Netlab - использует ту же логику что и update_all"""
    from netlab.netlab_export import get_all_netlab_products
    from core.telegram_notify import notify_export_start, notify_export_complete, notify_export_error
    from core.upsert import upsert_products
    import time
    import datetime
    from sqlalchemy import text

    start_time = time.time()
    start_datetime = datetime.datetime.now()
    logging.info(f"[Netlab] Начинаем обновление Netlab в {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Отправляем уведомление о начале
        await notify_export_start("Netlab")
        notify_start_time = time.time()
        logging.info("[Netlab] Сообщение о старте отправлено успешно")
        logging.info(f"[Netlab] Уведомление о начале отправлено за {notify_start_time - start_time:.2f} сек")
        
        async with AsyncSessionLocal() as db:
            try:
                # Очищаем старые товары Netlab
                logging.info("[Netlab] Очищаем базу от старых товаров Netlab...")
                await db.execute(text("DELETE FROM products WHERE distributor = 'Netlab'"))
                await db.commit()
                db_clean_time = time.time()
                logging.info(f"[Netlab] База очищена за {db_clean_time - notify_start_time:.2f} сек")
                
                # Получаем товары
                logging.info("[Netlab] Начинаем получение товаров из API...")
                products = await get_all_netlab_products()
                data_collection_time = time.time()
                logging.info(f"[Netlab] API вернул товары: {len(products) if products else 0}")
                logging.info(f"[Netlab] Данные собраны за {data_collection_time - db_clean_time:.2f} сек")
                
                if not products:
                    error_msg = "Не удалось получить товары!"
                    logging.error(f"[Netlab] {error_msg}")
                    await notify_export_error("Netlab", error_msg)
                    return
                    
                logging.info(f"[Netlab] Получено товаров: {len(products)}")
                logging.info(f"[Netlab] Пример первого товара: {products[0] if products else 'Нет товаров'}")
                
                # Вставляем товары в базу
                logging.info(f"[Netlab] Начинаем upsert для {len(products)} товаров...")
                result_stats = await upsert_products(products, 'Netlab', db)
                upsert_time = time.time()
                logging.info(f"[Netlab] Upsert завершен: {result_stats}")
                logging.info(f"[Netlab] Upsert выполнен за {upsert_time - data_collection_time:.2f} сек")
                
                end_time = time.time()
                end_datetime = datetime.datetime.now()
                duration_minutes = (end_time - start_time) / 60
                duration_seconds = end_time - start_time
                
                logging.info(f"[Netlab] Общее время процесса: {duration_seconds:.2f} сек ({duration_minutes:.3f} мин)")
                logging.info(f"[Netlab] Время завершения: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Отправляем уведомление о завершении
                logging.info(f"[Netlab][DEBUG] === ВЫЗОВ notify_export_complete (CLI) ===")
                logging.info(f"[Netlab][DEBUG] Место вызова: update_netlab_only() в main.py")
                logging.info(f"[Netlab][DEBUG] Параметры:")
                logging.info(f"[Netlab][DEBUG] - distributor: 'Netlab'")
                logging.info(f"[Netlab][DEBUG] - total_items: {len(products)}")
                logging.info(f"[Netlab][DEBUG] - inserted: {result_stats['inserted']}")
                logging.info(f"[Netlab][DEBUG] - updated: {result_stats['updated']}")
                logging.info(f"[Netlab][DEBUG] - errors: 0")
                logging.info(f"[Netlab][DEBUG] - duration_minutes: {duration_minutes}")
                logging.info(f"[Netlab][DEBUG] ========================================")
                
                await notify_export_complete("Netlab", len(products), result_stats['inserted'], result_stats['updated'], 0, duration_minutes)
                logging.info("[Netlab] Сообщение о завершении отправлено успешно")
                
                # Записываем дату обновления
                with open(NETLAB_LAST_UPDATE_FILE, "w", encoding="utf-8") as f:
                    f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    
                logging.info(f"[Netlab][END] Выгрузка завершена. Всего обработано: {len(products)} за {duration_seconds:.1f} сек ({duration_minutes:.3f} мин)")
                logging.info(f"[Netlab][STATS] Добавлено: {result_stats['inserted']}, Обновлено: {result_stats['updated']}, Деактивировано: {result_stats['deleted']}")
                
            except Exception as e:
                error_msg = f"Ошибка при работе с базой данных: {e}"
                logging.error(f"[Netlab][ERROR] {error_msg}")
                await notify_export_error("Netlab", error_msg)
                raise
                
    except Exception as e:
        error_msg = f"Ошибка при отправке уведомления: {e}"
        logging.error(f"[Netlab][ERROR] {error_msg}")
        # Не отправляем notify_export_error здесь, чтобы избежать дублирования

async def update_vvp_only(vvp_logger=None):
    from vvp.vvp_export import upsert_vvp_products
    from core.telegram_notify import notify_export_complete, notify_export_error
    import time
    
    logging.info("[VVP][START] Выгрузка начата")
    # Уведомление о старте уже отправлено в main.py, убираем дублирование
    start_time = time.time()
    async with AsyncSessionLocal() as db:
        await db.execute(text("DELETE FROM products WHERE distributor = 'VVP'"))
        await db.commit()
        
        # Получаем товары напрямую, без ThreadPoolExecutor
        products = await get_vvp_products()
        
        if not products:
            error_msg = "Не удалось получить товары!"
            logging.error(f"[VVP] {error_msg}")
            logging.info("[VVP][END] Выгрузка завершена. Всего обработано: 0")
            await notify_export_error("VVP", error_msg)
            return
            
        for p in products:
            stock_val = p.get('stock')
            try:
                if stock_val is None or stock_val == '' or stock_val == 'None':
                    p['stock'] = 0
                else:
                    p['stock'] = float(stock_val) if isinstance(stock_val, str) else stock_val
            except (ValueError, TypeError):
                p['stock'] = 0
        logging.info(f"[VVP] Пример товара: {products[0]}")
        try:
            await upsert_vvp_products(products, db)
            end_time = time.time()
            duration_minutes = (end_time - start_time) / 60
            try:
                await notify_export_complete("VVP", len(products), len(products), 0, 0, duration_minutes)
                logging.info("[VVP] Сообщение о завершении отправлено успешно")
            except Exception as e:
                logging.error(f"[VVP][ERROR] Ошибка при отправке уведомления о завершении: {e}")
            logging.info(f"[VVP][END] Выгрузка завершена. Всего обработано: {len(products)} за {end_time - start_time:.1f} сек ({duration_minutes:.1f} мин)")
        except Exception as e:
            error_msg = f"Ошибка при вставке в БД: {e}"
            logging.error(f"[VVP][ERROR] {error_msg}")
            await notify_export_error("VVP", error_msg)
            raise

async def update_treolan_only():
    from treolan.treolan_export import get_treolan_products
    from core.upsert import upsert_products
    from core.telegram_notify import notify_export_start, notify_export_complete
    import datetime
    import logging
    
    logging.info("[Treolan][START] Выгрузка начата")
    try:
        await notify_export_start("Treolan")
        logging.info("[Treolan] Сообщение о старте отправлено успешно")
    except Exception as e:
        logging.error(f"[Treolan][ERROR] Ошибка при отправке уведомления о старте: {e}")
    
    async with AsyncSessionLocal() as db:
        try:
            products = await get_treolan_products()
            logging.info(f"[Treolan] Получено товаров: {len(products) if products else 0}")
        except Exception as e:
            logging.error(f"[Treolan][ERROR] Ошибка при получении товаров: {e}")
            return
        if not products:
            logging.error("[Treolan] Не удалось получить товары!")
            logging.info("[Treolan][END] Выгрузка завершена. Всего обработано: 0")
            return
        for p in products:
            stock_val = p.get('stock')
            try:
                if stock_val is None or stock_val == '' or stock_val == 'None':
                    p['stock'] = 0
                else:
                    p['stock'] = float(stock_val) if isinstance(stock_val, str) else stock_val
            except (ValueError, TypeError):
                p['stock'] = 0
        logging.info(f"[Treolan] Пример товара: {products[0]}")
        try:
            result_stats = await upsert_products(products, 'Treolan', db)
            logging.info(f"[Treolan][STATS] Добавлено: {result_stats['inserted']}, Обновлено: {result_stats['updated']}, Деактивировано: {result_stats['deleted']}")
            logging.info(f"[Treolan][END] Выгрузка завершена. Всего обработано: {len(products)}")
            try:
                await notify_export_complete(
                    "Treolan", 
                    len(products), 
                    result_stats['inserted'], 
                    result_stats['updated'], 
                    0,  # ошибок нет, передаем 0
                    2.3
                )
                logging.info("[Treolan] Сообщение о завершении отправлено успешно")
                with open(TREOLAN_LAST_UPDATE_FILE, "w", encoding="utf-8") as f:
                    f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            except Exception as e:
                logging.error(f"[Treolan][ERROR] Ошибка при отправке уведомления о завершении: {e}")
        except Exception as e:
            error_msg = f"Ошибка при вставке в БД: {e}"
            logging.error(f"[Treolan][ERROR] {error_msg}")
            raise

async def main_loop():
    # Создаем файл блокировки для предотвращения одновременного запуска
    lock_file_path = os.path.join(tempfile.gettempdir(), 'parser_dist_main.lock')
    lock_file = None
    
    try:
        # Пытаемся создать файл блокировки
        lock_file = open(lock_file_path, 'w')
        
        # Для Windows используем другой подход
        if os.name == 'nt':  # Windows
            import msvcrt
            try:
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            except IOError:
                logging.error("Другой экземпляр парсера уже запущен!")
                return
        else:  # Unix/Linux
            import fcntl
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                logging.error("Другой экземпляр парсера уже запущен!")
                return
        
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        
        logging.info("Парсер успешно запущен с блокировкой")
        
        while True:
            # Проверяем ночной режим
            if not can_start_update():
                # Вычисляем время до 7:00 МСК следующего дня с учетом часовых поясов
                try:
                    import zoneinfo
                    moscow_tz = zoneinfo.ZoneInfo("Europe/Moscow")
                except (ImportError, AttributeError):
                    moscow_tz = datetime.timezone(datetime.timedelta(hours=3))
                
                now = datetime.datetime.now(moscow_tz)
                
                if now.hour >= 19:
                    # Уже вечер, ждем до 7:00 следующего дня
                    next_run = now.replace(hour=7, minute=0, second=0, microsecond=0) + datetime.timedelta(days=1)
                else:
                    # Ночь, ждем до 7:00 сегодня
                    next_run = now.replace(hour=7, minute=0, second=0, microsecond=0)
                
                wait_seconds = (next_run - now).total_seconds()
                wait_hours = wait_seconds // 3600
                wait_minutes = (wait_seconds % 3600) // 60
                
                logging.info(f"[NIGHT MODE] Ожидание до {next_run.strftime('%H:%M МСК')} (через {int(wait_hours)}ч {int(wait_minutes)}м)")
                await asyncio.sleep(wait_seconds)
                continue
            
            logging.info("=== Запуск обновления базы ===")
            # Показываем текущее время МСК
            try:
                import zoneinfo
                moscow_tz = zoneinfo.ZoneInfo("Europe/Moscow")
            except (ImportError, AttributeError):
                moscow_tz = datetime.timezone(datetime.timedelta(hours=3))
            
            moscow_time = datetime.datetime.now(moscow_tz)
            logging.info(f"[TIME] Текущее время: {moscow_time.strftime('%H:%M:%S МСК')}")
            logging.info(f"[Main][DEBUG] Вызываем update_all() в {moscow_time.strftime('%H:%M:%S МСК')}")
            
            try:
                await update_all()
                logging.info(f"[Main][DEBUG] update_all() завершен успешно")
            except Exception as e:
                logging.error(f"Критическая ошибка в update_all(): {e}")
                # Продолжаем работу после ошибки
            
            logging.info(f"Ожидание {UPDATE_INTERVAL_SECONDS // 60} минут до следующего обновления...")
            await asyncio.sleep(UPDATE_INTERVAL_SECONDS)
            
    except Exception as e:
        logging.error(f"Критическая ошибка в main_loop(): {e}")
    finally:
        # Освобождаем блокировку
        if lock_file:
            try:
                lock_file.close()
                os.remove(lock_file_path)
                logging.info("Блокировка снята")
            except Exception as e:
                logging.warning(f"Не удалось удалить файл блокировки: {e}")

# --- Основной запуск ---
if __name__ == "__main__":
    import sys
    print(f"[DEBUG] Запуск main.py с аргументами: {sys.argv}")
    if len(sys.argv) > 1 and sys.argv[1] == "debug_netlab_sample":
        pass # Removed debug_netlab_sample
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        print(f"[DEBUG] Обработка команды: {cmd}")
        if cmd == 'update_vvp':
            asyncio.run(update_vvp_only())
            print("[VVP] Выгрузка завершена.")
            logging.shutdown()
            exit(0)
        if cmd == 'update_merlion':
            print("[Merlion] Команда update_merlion удалена - используйте основной режим")
            logging.shutdown()
            exit(0)
        if cmd == 'update_ocs':
            asyncio.run(ocs_update_wrapper())
            print("[OCS] Выгрузка завершена.")
            logging.shutdown()
            exit(0)
        if cmd == 'update_ocs_sale':
            asyncio.run(ocs_sale_update_wrapper())
            print("[OCS SALE] Выгрузка завершена.")
            logging.shutdown()
            exit(0)
        if cmd == 'update_marvel':
            print("[Marvel] Команда update_marvel удалена - используйте основной режим")
            logging.shutdown()
            exit(0)
        if cmd == 'update_netlab':
            asyncio.run(update_netlab_only())
            print("[Netlab] Выгрузка завершена.")
            logging.shutdown()
            exit(0)
        if cmd == 'update_treolan':
            print("[Treolan] Запуск обновления Treolan...")
            asyncio.run(update_treolan_only())
            print("[Treolan] Выгрузка завершена.")
            logging.shutdown()
            exit(0)

    elif len(sys.argv) > 1 and sys.argv[1] == "full_netlab":
        asyncio.run(full_netlab_load())
        logging.shutdown()
    elif len(sys.argv) > 1 and sys.argv[1] == "update_netlab_prices":
        asyncio.run(update_netlab_prices())
        logging.shutdown()

    elif len(sys.argv) > 1 and sys.argv[1] == "update_ocs":
        asyncio.run(ocs_main())
        logging.shutdown()
    else:
        print(f"[DEBUG] Неизвестная команда или запуск основного режима. Аргументы: {sys.argv}")
        asyncio.run(main_loop())
        logging.shutdown()

# --- CLI-блок для ручного запуска только Netlab или всех дистрибьюторов ---
# if __name__ == '__main__':
#     import sys, asyncio
#     if len(sys.argv) > 1 and sys.argv[1].lower() == 'netlab':
#         print("[CLI] Запуск только полной выгрузки Netlab...")
#         asyncio.run(full_netlab_load())
#     else:
#         print("[CLI] Запуск полной выгрузки всех дистрибьюторов...")
#         asyncio.run(main_loop())