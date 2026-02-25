import asyncio
import datetime
import json
import logging
import os
import time
import requests
from typing import Dict, List, Optional
import httpx
from sqlalchemy import insert, text
from sqlalchemy.ext.asyncio import AsyncSession

# Импортируем наши модули
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.db_engine import AsyncSessionLocal
from core.upsert import upsert_products
from core.db_models import Product
from core.telegram_notify import notify_export_start, notify_export_complete, notify_export_error

# Настройка логирования
logger = logging.getLogger("ocs_sale_export")
logger.setLevel(logging.INFO)

# Создаем простой обработчик для файла
os.makedirs("logs", exist_ok=True)
file_handler = logging.FileHandler("logs/update_distributors_ocs_sale_export.log", encoding="utf-8")
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
logger.addHandler(file_handler)

# Глобально отключаем логирование SQLAlchemy и HTTP для чистоты вывода
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.dialects.postgresql').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

# Отключаем логирование драйверов PostgreSQL
logging.getLogger('asyncpg').setLevel(logging.WARNING)
logging.getLogger('postgresql').setLevel(logging.WARNING)
logging.getLogger('psycopg2').setLevel(logging.WARNING)

# Устанавливаем переменные окружения для тишины
os.environ['ASYNC_PG_QUIET'] = '1'
os.environ['PGQUIET'] = '1'

# Дополнительно отключаем SQL-параметры в asyncpg
os.environ['ASYNC_PG_QUIET_PARAMS'] = '1'

# Конфигурация
API_KEY = "GtBN6lLQW#FgokV_W*fHWOhnDhuVRo"  # API ключ OCS
WORK_URL = "https://connector.b2b.ocs.ru/api/v2"
LAST_UPDATE_FILE = "static/last_update_ocs_sale.txt"

# Кэш категорий
CATEGORIES_CACHE_FILE = "ocs_categories_cache.json"

def get_usd_rate():
    """Получает актуальный курс USD/RUB от ЦБ РФ"""
    try:
        import requests
        response = requests.get('https://www.cbr-xml-daily.ru/daily_json.js', timeout=10)
        if response.status_code == 200:
            data = response.json()
            usd_rate = data['Valute']['USD']['Value']
            logger.info(f"[OCS SALE] Получен курс USD: {usd_rate}")
            return usd_rate
    except Exception as e:
        logger.error(f"[OCS SALE] Не удалось получить курс USD: {e}")
    
    # Возвращаем дефолтный курс если не удалось получить
    return 95.0

async def check_rate_limit():
    """Проверяем rate limit API"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{WORK_URL}/catalog/categories", headers={"X-API-Key": API_KEY})
            if response.status_code == 429:
                logger.warning("[OCS SALE] Rate limit превышен, ждем...")
                await asyncio.sleep(60)
                return False
            return True
    except Exception as e:
        logger.error(f"[OCS SALE] Ошибка при проверке rate limit: {e}")
        return False

async def insert_products_batch(products: List[Dict], distributor_name: str, db: AsyncSession) -> Dict[str, int]:
    """Вставляет товары в БД без очистки (для батчевой обработки)"""
    try:
        if not products:
            return {"inserted": 0, "updated": 0, "deleted": 0}
        
        # Подготавливаем данные для batch insert
        batch_data = []
        for p in products:
            # Создаем полную структуру данных со всеми обязательными полями
            data = {
                'article': p.get('article'),
                'name': p.get('name'),
                'brand': p.get('brand'),
                'part_number': p.get('part_number'),
                'category_code': p.get('category_code'),
                'price_rub': p.get('price_rub', 0),
                'price_usd': p.get('price_usd', 0),
                'stock': p.get('stock', 0),
                'package_volume': p.get('package_volume'),
                'package_weight': p.get('package_weight'),
                'tech_specs': p.get('tech_specs'),
                'transit_date': p.get('transit_date'),
                'distributor': distributor_name,
                'is_active': True
            }
            batch_data.append(data)
        
        if batch_data:
            # Используем raw SQL для правильной установки created_at и updated_at
            from sqlalchemy import text
            
            # Подготавливаем SQL запрос с обработкой дублей
            stmt = text("""
                INSERT INTO products (article, name, brand, part_number, category_code, price_rub, price_usd, stock, package_volume, package_weight, tech_specs, transit_date, distributor, is_active, created_at, updated_at)
                VALUES (:article, :name, :brand, :part_number, :category_code, :price_rub, :price_usd, :stock, :package_volume, :package_weight, :tech_specs, :transit_date, :distributor, :is_active, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                    article = VALUES(article),
                    name = VALUES(name),
                    brand = VALUES(brand),
                    category_code = VALUES(category_code),
                    price_rub = VALUES(price_rub),
                    price_usd = VALUES(price_usd),
                    stock = VALUES(stock),
                    package_volume = VALUES(package_volume),
                    package_weight = VALUES(package_weight),
                    tech_specs = VALUES(tech_specs),
                    transit_date = VALUES(transit_date),
                    is_active = VALUES(is_active),
                    updated_at = NOW()
            """)
            
            # Вставляем товары
            for product_data in batch_data:
                await db.execute(stmt, product_data)
            await db.commit()
            
            # Считаем обработанные товары
            processed = len(batch_data)
            logger.info(f"[{distributor_name}] Обработано {processed} товаров (insert)")
            
            # Проверяем, что товары действительно в БД
            try:
                # Проверяем количество товаров в БД
                result = await db.execute(text("SELECT COUNT(*) FROM products WHERE distributor = :distributor"), {"distributor": distributor_name})
                actual_count = result.scalar()
                logger.info(f"[{distributor_name}][VERIFY] Проверка БД: {actual_count} товаров")
            except Exception as check_error:
                logger.error(f"[{distributor_name}][VERIFY] Ошибка при проверке: {check_error}")
            
            return {"inserted": processed, "updated": 0, "deleted": 0}
        else:
            return {"inserted": 0, "updated": 0, "deleted": 0}
            
    except Exception as e:
        logger.error(f"[{distributor_name}][ERROR] Ошибка при вставке товаров: {e}")
        await db.rollback()
        return {"inserted": 0, "updated": 0, "deleted": 0}

def load_categories_cache() -> Optional[List[str]]:
    """Загружаем кэш категорий из файла"""
    try:
        if os.path.exists(CATEGORIES_CACHE_FILE):
            with open(CATEGORIES_CACHE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Проверяем, что кэш не устарел (старше 24 часов)
                cache_time = datetime.datetime.fromisoformat(data['timestamp'])
                if datetime.datetime.now() - cache_time < datetime.timedelta(hours=24):
                    logger.info(f"[OCS SALE] Загружен кэш категорий: {len(data['categories'])} категорий")
                    return data['categories']
                else:
                    logger.warning("[OCS SALE] Кэш категорий устарел")
        return None
    except Exception as e:
        logger.error(f"[OCS SALE] Ошибка при загрузке кэша категорий: {e}")
        return None

async def fetch_all_categories(api_key):
    """Получаем все категории из API"""
    logger.info("[OCS SALE] Получаем дерево категорий...")
    
    try:
        url = f"{WORK_URL}/catalog/categories"
        headers = {"X-API-Key": api_key}
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            
            # Отладочная информация
            logger.debug(f"[OCS SALE][DEBUG] Тип ответа API: {type(data)}")
            logger.debug(f"[OCS SALE][DEBUG] Содержимое ответа: {str(data)[:200]}...")
            
            # Проверяем структуру ответа
            if isinstance(data, list):
                # API возвращает список категорий напрямую
                categories_tree = data
                logger.info(f"[OCS SALE] API вернул список категорий: {len(categories_tree)} категорий")
            elif isinstance(data, dict) and "result" in data:
                # API возвращает словарь с ключом "result"
                categories_tree = data["result"]
                logger.info(f"[OCS SALE] API вернул словарь с result: {len(categories_tree)} категорий")
            else:
                # Неожиданная структура
                logger.warning(f"[OCS SALE] Неожиданная структура ответа API: {type(data)}")
                categories_tree = data if isinstance(data, list) else []
            
            logger.info(f"[OCS SALE] Получено дерево категорий: {len(categories_tree)} корневых категорий")
            
            return categories_tree
            
    except Exception as e:
        logger.error(f"[OCS SALE][ERROR] Ошибка при получении категорий: {e}")
        return []

def extract_all_category_ids(categories_tree):
    """Извлекаем все ID категорий из дерева"""
    category_ids = []
    
    def extract_recursive(nodes, level=0):
        for node in nodes:
            if isinstance(node, dict):
                # OCS API использует поле 'category' вместо 'id'
                if 'category' in node:
                    category_ids.append(str(node['category']))
                    logger.debug(f"[OCS SALE][DEBUG] Добавлена категория: {node['category']} - {node.get('name', 'N/A')}")
                if 'children' in node and node['children']:
                    extract_recursive(node['children'], level + 1)
    
    extract_recursive(categories_tree)
    logger.info(f"[OCS SALE][DEBUG] Всего извлечено ID категорий: {len(category_ids)}")
    return category_ids

async def fetch_sale_products_batch(
    api_key: str, 
    category_ids: List[str]
) -> List[Dict]:
    """
    Получает ТОЛЬКО акционные товары (Sale) одним батч-запросом
    """
    logger.info(f"[OCS SALE] Начинаем получение АКЦИОННЫХ товаров по {len(category_ids)} категориям...")

    try:
        url = "https://connector.b2b.ocs.ru/api/v2/catalog/categories/batch/products"

        params = {
            "shipmentcity": "Москва",
            "onlyavailable": "false",
            "includeregular": "false",  # ❌ Исключаем обычные товары
            "includesale": "true",      # ✅ ТОЛЬКО акционные товары
            "includeuncondition": "false",
            "includeunconditionalimages": "false",
            "includemissing": "false",
            "withdescriptions": "true"
        }

        body = category_ids

        headers = {
            "X-API-Key": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        print(f"[OCS SALE] Отправляем батч-запрос для {len(category_ids)} категорий (ТОЛЬКО АКЦИИ)...")
        logging.info(f"[OCS SALE] Отправляем батч-запрос для {len(category_ids)} категорий (ТОЛЬКО АКЦИИ)...")

        async with httpx.AsyncClient(timeout=300.0) as client:
            response = await client.post(url, params=params, json=body, headers=headers)
            response.raise_for_status()

            data = response.json()
            products = data.get("result", [])

            print(f"[OCS SALE] Получено {len(products)} АКЦИОННЫХ товаров одним запросом!")
            logging.info(f"[OCS SALE] Получено {len(products)} АКЦИОННЫХ товаров одним запросом!")

            return products

    except Exception as e:
        print(f"[OCS SALE][ERROR] Ошибка при получении акционных товаров: {e}")
        logging.error(f"[OCS SALE][ERROR] Ошибка при получении акционных товаров: {e}")
        return []

def ocs_sale_to_db_products(raw_products, city):
    """Преобразует сырые данные OCS SALE в формат для БД"""
    db_products = []
    
    print(f"[OCS SALE][DEBUG] Обрабатываем {len(raw_products)} АКЦИОННЫХ товаров из API")
    
    # Получаем курс USD ОДИН РАЗ для всех товаров
    usd_rate = get_usd_rate()
    print(f"[OCS SALE] Используем курс USD: {usd_rate} для всех товаров")
    
    sale_count = 0
    discount_count = 0
    
    for i, raw_product in enumerate(raw_products):
        try:
            # Отладка: показываем структуру первых товаров
            if i < 3:
                print(f"[OCS SALE][DEBUG] Товар {i+1} структура: {list(raw_product.keys())}")
                if 'product' in raw_product:
                    print(f"  product поля: {list(raw_product['product'].keys())}")
                if 'locations' in raw_product:
                    print(f"  locations: {raw_product['locations']}")
            
            # Получаем данные из правильной структуры
            product_data = raw_product.get('product', {})
            
            # Базовые поля
            part_number = product_data.get('partNumber', '')  # partNumber, а не part_number
            name = product_data.get('itemNameRus', product_data.get('itemName', ''))
            brand = product_data.get('producer', '')
            
            # Цены - получаем все возможные типы цен
            price_data = raw_product.get('price', {})
            
            # Основная цена заказа (в рублях)
            price_rub = price_data.get('order', {}).get('value', 0.0)
            
            # Дополнительные цены для анализа
            price_list_usd = price_data.get('priceList', {}).get('value', 0.0)
            price_list_rub = price_data.get('priceList', {}).get('value', 0.0)  # Может быть в рублях
            end_user_price = price_data.get('endUser', {}).get('value', 0.0)  # РРЦ
            end_user_web_price = price_data.get('endUserWeb', {}).get('value', 0.0)  # РИЦ
            discount_b2b = price_data.get('discountB2B', 0)
            
            # Состояние товара (должно быть Sale для акционных товаров)
            condition = product_data.get('condition', 'Regular')
            
            # Проверяем, что это действительно акционный товар
            if condition != 'Sale':
                print(f"[OCS SALE][WARNING] Товар {part_number} не в состоянии Sale: {condition}")
                continue
            
            sale_count += 1
            
            # Логируем все доступные цены для отладки
            if i < 3:  # Логируем только первые 3 товара
                print(f"[OCS SALE][DEBUG] АКЦИОННЫЙ товар {i+1} цены: order={price_rub} RUB, priceList={price_list_usd} USD, РРЦ={end_user_price}, РИЦ={end_user_web_price}, discountB2B={discount_b2b}%, condition={condition}")
            
            # Логика выбора цены: для акционных товаров используем акционную цену
            # Если есть скидка B2B, применяем её к основной цене
            final_price_rub = price_rub
            
            # Применяем скидку B2B если есть
            if discount_b2b > 0 and final_price_rub > 0:
                discount_amount = final_price_rub * (discount_b2b / 100)
                final_price_rub = final_price_rub - discount_amount
                discount_count += 1
                if i < 3:  # Логируем применение скидки
                    print(f"[OCS SALE][DEBUG] Применена скидка B2B {discount_b2b}%: {price_rub} -> {final_price_rub} RUB")
            
            # Конвертируем рубли в доллары по курсу ЦБ (курс уже получен выше)
            if final_price_rub and final_price_rub > 0 and usd_rate > 0:
                price_usd = round(final_price_rub / usd_rate, 2)
            else:
                price_usd = None
            
            # Артикул
            article = product_data.get('productKey', '')
            
            # Категория
            category_code = product_data.get('category', '')
            
            # Дата транзита (пока не используется)
            transit_date = None
            
            # Статус активности
            is_available = raw_product.get('isAvailableForOrder', False)
            is_active = is_available
            
            # Суммируем остатки из всех локаций
            total_stock = 0
            locations = raw_product.get('locations', [])
            for location in locations:
                quantity = location.get('quantity', {})
                stock_value = quantity.get('value', 0)
                if isinstance(stock_value, (int, float)):
                    total_stock += stock_value
            
            # Создаем продукт для БД
            db_product = {
                'part_number': part_number,
                'name': name,
                'brand': brand,
                'stock': str(total_stock),  # Суммарный остаток из всех локаций
                'price_rub': final_price_rub,  # Используем финальную цену с учетом скидок
                'price_usd': price_usd,
                'article': article,
                'category_code': category_code,
                'package_volume': None,  # NULL для числового поля
                'package_weight': None,  # NULL для числового поля
                'transit_date': transit_date,
                'distributor': 'Ocs Sale',  # ✅ Отдельный дистрибьютер для акций
                'is_active': is_active
            }
            
            # Проверяем, что part_number не пустой
            if part_number and part_number.strip():
                db_products.append(db_product)
                if i < 3:
                    condition_text = f" ({condition})" if condition != 'Regular' else ""
                    discount_text = f" [Скидка {discount_b2b}%]" if discount_b2b > 0 else ""
                    print(f"  [OCS SALE][DEBUG] Добавлен АКЦИОННЫЙ товар: {part_number} - {name[:50]}... - Остаток: {total_stock}{condition_text}{discount_text}")
            else:
                print(f"[OCS SALE][DEBUG] Пропускаем товар с пустым partNumber: {name[:50]}...")
            
        except Exception as e:
            print(f"[OCS SALE][ERROR] Ошибка при обработке товара {i}: {e}")
            logging.error(f"[OCS SALE][ERROR] Ошибка при обработке товара {i}: {e}")
            continue
    
    print(f"[OCS SALE][DEBUG] Успешно обработано {len(db_products)} АКЦИОННЫХ товаров")
    print(f"[OCS SALE][STATS] Товаров в состоянии Sale: {sale_count}")
    print(f"[OCS SALE][STATS] Товаров со скидкой B2B: {discount_count}")
    return db_products

async def main():
    # Логирование уже отключено глобально в начале файла
    
    start_time = time.time()
    logger.info(f"[OCS SALE] Начинаем обновление OCS SALE в {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Отправляем уведомление о начале экспорта
    await notify_export_start("OCS SALE")
    
    # Проверяем rate limit
    if not await check_rate_limit():
        error_msg = "Rate limit превышен, завершаем работу"
        print(f"[OCS SALE] {error_msg}")
        await notify_export_error("OCS SALE", error_msg)
        return
    
    # Получаем категории (из кэша или API)
    categories = load_categories_cache()
    if not categories:
        print("[OCS SALE] Кэш категорий не найден, получаем из API...")
        categories_tree = await fetch_all_categories(API_KEY)
        if not categories_tree:
            print("[OCS SALE] Не удалось получить категории, завершаем работу")
            return
        
        categories = extract_all_category_ids(categories_tree)
        print(f"[OCS SALE][DEBUG] Извлечено категорий: {len(categories)}")
        print(f"[OCS SALE][DEBUG] Первые 5 категорий: {categories[:5]}")
    
    print(f"[OCS SALE] Работаем с {len(categories)} категориями")
    logging.info(f"[OCS SALE] Работаем с {len(categories)} категориями")
    
    # Получаем ТОЛЬКО акционные товары одним батч-запросом
    print("[OCS SALE] Получаем ТОЛЬКО акционные товары...")
    logging.info("[OCS SALE] Получаем ТОЛЬКО акционные товары...")
    
    all_products = await fetch_sale_products_batch(API_KEY, categories)
    
    if not all_products:
        error_msg = "Не удалось получить акционные товары, завершаем работу"
        print(f"[OCS SALE] {error_msg}")
        logging.error(f"[OCS SALE] {error_msg}")
        await notify_export_error("OCS SALE", error_msg)
        return
    
    print(f"[OCS SALE] Получено {len(all_products)} АКЦИОННЫХ товаров")
    logging.info(f"[OCS SALE] Получено {len(all_products)} АКЦИОННЫХ товаров")
    
    # Преобразуем в формат для БД
    print("[OCS SALE] Преобразуем данные для БД...")
    logging.info("[OCS SALE] Преобразуем данные для БД...")
    
    all_db_products = ocs_sale_to_db_products(all_products, "Москва")
    
    if all_db_products:
        print(f"[OCS SALE] Всего АКЦИОННЫХ товаров для вставки: {len(all_db_products)}")
        logging.info(f"[OCS SALE] Всего АКЦИОННЫХ товаров для вставки: {len(all_db_products)}")
        
        # Разбиваем на батчи для избежания SQL-ошибок
        BATCH_SIZE_UPSERT = 10000  # Меньший размер батча для акционных товаров
        total_batches = (len(all_db_products) + BATCH_SIZE_UPSERT - 1) // BATCH_SIZE_UPSERT
        
        print(f"[OCS SALE] Разбиваем {len(all_db_products)} товаров на {total_batches} батчей по {BATCH_SIZE_UPSERT}")
        logging.info(f"[OCS SALE] Разбиваем {len(all_db_products)} товаров на {total_batches} батчей по {BATCH_SIZE_UPSERT}")
        
        # Очищаем базу от старых товаров OCS SALE перед вставкой
        print("[OCS SALE] Очищаем базу от старых товаров OCS SALE перед вставкой...")
        logging.info("[OCS SALE] Очищаем базу от старых товаров OCS SALE перед вставкой...")
        
        try:
            async with AsyncSessionLocal() as cleanup_db:
                from sqlalchemy import text
                result = await cleanup_db.execute(text("DELETE FROM products WHERE distributor = 'Ocs Sale'"))
                deleted_count = result.rowcount
                await cleanup_db.commit()
                print(f"[OCS SALE] Удалено {deleted_count} старых товаров OCS SALE из базы")
                logging.info(f"[OCS SALE] Удалено {deleted_count} старых товаров OCS SALE из базы")
        except Exception as cleanup_error:
            print(f"[OCS SALE][ERROR] Ошибка при очистке базы: {cleanup_error}")
            logging.error(f"[OCS SALE][ERROR] Ошибка при очистке базы: {cleanup_error}")
        
        # Обрабатываем товары по батчам (без очистки)
        total_stats = {"inserted": 0, "updated": 0, "deleted": 0}
        
        try:
            for batch_num in range(total_batches):
                start_idx = batch_num * BATCH_SIZE_UPSERT
                end_idx = min(start_idx + BATCH_SIZE_UPSERT, len(all_db_products))
                batch_products = all_db_products[start_idx:end_idx]
                
                print(f"[OCS SALE][BATCH {batch_num + 1}/{total_batches}] Обрабатываем батч из {len(batch_products)} АКЦИОННЫХ товаров")
                logging.info(f"[OCS SALE][BATCH {batch_num + 1}/{total_batches}] Обрабатываем батч из {len(batch_products)} АКЦИОННЫХ товаров")
                
                try:
                    async with AsyncSessionLocal() as db:
                        print(f"[OCS SALE][BATCH {batch_num + 1}] Подключение к БД установлено, начинаем вставку...")
                        logging.info(f"[OCS SALE][BATCH {batch_num + 1}] Подключение к БД установлено, начинаем вставку...")
                        
                        # Логируем первые несколько товаров для проверки
                        print(f"[OCS SALE][BATCH {batch_num + 1}] Первые 3 АКЦИОННЫХ товара для вставки:")
                        for i, product in enumerate(batch_products[:3]):
                            print(f"  {i+1}. {product.get('part_number', 'N/A')} - {product.get('name', 'N/A')[:50]}...")
                        
                        # Вставляем товары после очистки (просто INSERT)
                        batch_stats = await insert_products_batch(batch_products, "Ocs Sale", db)
                        print(f"[OCS SALE][BATCH {batch_num + 1}] Статистика: {batch_stats}")
                        logging.info(f"[OCS SALE][BATCH {batch_num + 1}] Статистика: {batch_stats}")
                        
                        # Проверяем, что товары действительно сохранились в БД
                        try:
                            check_stmt = text("SELECT COUNT(*) FROM products WHERE distributor = 'Ocs Sale'")
                            check_result = await db.execute(check_stmt)
                            actual_count = check_result.scalar()
                            print(f"[OCS SALE][BATCH {batch_num + 1}][VERIFY] В БД после батча: {actual_count} товаров")
                            logging.info(f"[OCS SALE][BATCH {batch_num + 1}][VERIFY] В БД после батча: {actual_count} товаров")
                        except Exception as check_error:
                            print(f"[OCS SALE][BATCH {batch_num + 1}][VERIFY] Ошибка при проверке: {check_error}")
                            logging.error(f"[OCS SALE][BATCH {batch_num + 1}][VERIFY] Ошибка при проверке: {check_error}")
                        
                        # Суммируем статистику
                        for key in total_stats:
                            total_stats[key] += batch_stats.get(key, 0)
                            
                except Exception as batch_error:
                    print(f"[OCS SALE][BATCH {batch_num + 1}][ERROR] Ошибка при обработке батча: {batch_error}")
                    logging.error(f"[OCS SALE][BATCH {batch_num + 1}][ERROR] Ошибка при обработке батча: {batch_error}")
                    continue
        
            print(f"[OCS SALE] Общая статистика обновления: {total_stats}")
            logging.info(f"[OCS SALE] Общая статистика обновления: {total_stats}")
            
        except Exception as upsert_error:
            print(f"[OCS SALE][ERROR] КРИТИЧЕСКАЯ ОШИБКА при upsert: {upsert_error}")
            logging.error(f"[OCS SALE][ERROR] КРИТИЧЕСКАЯ ОШИБКА при upsert: {upsert_error}")
            
            # Логируем детали ошибки
            import traceback
            error_traceback = traceback.format_exc()
            print(f"[OCS SALE][ERROR] Детали ошибки:\n{error_traceback}")
            logging.error(f"[OCS SALE][ERROR] Детали ошибки:\n{error_traceback}")
        
        # Детальная проверка результата
        if total_stats.get('inserted', 0) == 0 and total_stats.get('updated', 0) == 0:
            print(f"[OCS SALE][ERROR] ВНИМАНИЕ! Ни один АКЦИОННЫЙ товар не был сохранен в БД!")
            logging.error(f"[OCS SALE][ERROR] ВНИМАНИЕ! Ни один АКЦИОННЫЙ товар не был сохранен в БД!")
            print(f"[OCS SALE][DEBUG] Статистика: {total_stats}")
            logging.error(f"[OCS SALE][DEBUG] Статистика: {total_stats}")
            
            # Проверяем БД напрямую
            try:
                async with AsyncSessionLocal() as check_db:
                    from sqlalchemy import text
                    result = await check_db.execute(text("SELECT COUNT(*) FROM products WHERE distributor = 'Ocs Sale'"))
                    count = result.scalar()
                    print(f"[OCS SALE][DEBUG] Товаров OCS SALE в БД после upsert: {count}")
                    logging.info(f"[OCS SALE][DEBUG] Товаров OCS SALE в БД после upsert: {count}")
            except Exception as db_check_error:
                print(f"[OCS SALE][ERROR] Ошибка при проверке БД: {db_check_error}")
                logging.error(f"[OCS SALE][ERROR] Ошибка при проверке БД: {db_check_error}")
        else:
            print(f"[OCS SALE][SUCCESS] АКЦИОННЫЕ товары успешно сохранены в БД!")
            logging.info(f"[OCS SALE][SUCCESS] АКЦИОННЫЕ товары успешно сохранены в БД!")
        
        # Сохраняем дату обновления
        with open(LAST_UPDATE_FILE, "w", encoding="utf-8") as f:
            f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print(f"[OCS SALE] Дата обновления сохранена в {LAST_UPDATE_FILE}")
        logging.info(f"[OCS SALE] Дата обновления сохранена в {LAST_UPDATE_FILE}")
        
    else:
        print("[OCS SALE] Нет АКЦИОННЫХ данных для вставки в БД.")
        logging.warning("[OCS SALE] Нет АКЦИОННЫХ данных для вставки в БД.")
    
    # Завершение
    end_time = time.time()
    total_time = end_time - start_time
    duration_minutes = total_time / 60
    print(f"[OCS SALE] Обновление завершено за {total_time:.1f} секунд ({duration_minutes:.1f} минут)")
    logging.info(f"[OCS SALE] Обновление завершено за {total_time:.1f} секунд ({duration_minutes:.1f} минут)")
    print(f"[OCS SALE] Время завершения: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"[OCS SALE] Время завершения: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Отправляем уведомление о завершении
    try:
        # Получаем статистику из total_stats
        inserted = total_stats.get('inserted', 0)
        updated = total_stats.get('updated', 0)
        total_products = len(all_db_products) if 'all_db_products' in locals() else 0
        
        await notify_export_complete("OCS SALE", total_products, inserted, updated, 0, duration_minutes)
    except Exception as notify_error:
        print(f"[OCS SALE][ERROR] Ошибка при отправке уведомления: {notify_error}")
        logging.error(f"[OCS SALE][ERROR] Ошибка при отправке уведомления: {notify_error}")
    
    # ФИНАЛЬНАЯ ПРОВЕРКА БД
    print("[OCS SALE][FINAL] ФИНАЛЬНАЯ ПРОВЕРКА БАЗЫ ДАННЫХ...")
    logging.info("[OCS SALE][FINAL] ФИНАЛЬНАЯ ПРОВЕРКА БАЗЫ ДАННЫХ...")
    
    try:
        async with AsyncSessionLocal() as final_db:
            from sqlalchemy import text
            result = await final_db.execute(text("SELECT COUNT(*) FROM products WHERE distributor = 'Ocs Sale'"))
            final_count = result.scalar()
            print(f"[OCS SALE][FINAL] ИТОГО АКЦИОННЫХ товаров OCS SALE в БД: {final_count}")
            logging.info(f"[OCS SALE][FINAL] ИТОГО АКЦИОННЫХ товаров OCS SALE в БД: {final_count}")
            
            if final_count > 0:
                # Проверяем последние добавленные товары
                result = await final_db.execute(text("SELECT part_number, name, updated_at FROM products WHERE distributor = 'Ocs Sale' ORDER BY id DESC LIMIT 3"))
                recent_products = result.fetchall()
                print(f"[OCS SALE][FINAL] Последние добавленные АКЦИОННЫЕ товары:")
                for product in recent_products:
                    print(f"  - {product[0]} - {product[1][:50]}... (обновлен: {product[2]})")
            else:
                print(f"[OCS SALE][FINAL] ВНИМАНИЕ! В БД нет АКЦИОННЫХ товаров OCS SALE!")
                logging.error(f"[OCS SALE][FINAL] ВНИМАНИЕ! В БД нет АКЦИОННЫХ товаров OCS SALE!")
                
    except Exception as final_check_error:
        print(f"[OCS SALE][ERROR] Ошибка при финальной проверке БД: {final_check_error}")
        logging.error(f"[OCS SALE][ERROR] Ошибка при финальной проверке БД: {final_check_error}")
    
    # Правильно закрываем все соединения
    try:
        from core.db_engine import engine
        await engine.dispose()
    except:
        pass

if __name__ == "__main__":
    asyncio.run(main())
