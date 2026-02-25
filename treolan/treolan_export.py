import requests
from typing import List, Dict, Optional
import asyncio
from sqlalchemy import text
import logging
import os
import re
import time
from datetime import datetime
from core.telegram_notify import notify_export_start, notify_export_progress, notify_export_complete, notify_export_error

TREOLAN_AUTH_URL = "https://api.treolan.ru/api/v1/Auth/Token"
TREOLAN_CATALOG_URL = "https://api.treolan.ru/api/v1/Catalog/Get"
TREOLAN_LOGIN = "YOUR_TREOLAN_LOGIN"  # Замените на ваш логин
TREOLAN_PASSWORD = "YOUR_TREOLAN_PASSWORD"  # Замените на ваш пароль

# Настройка логирования для Treolan
logger = logging.getLogger('Treolan')
logger.setLevel(logging.INFO)

# Создаем форматтер для логов
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Создаем файловый обработчик
log_file = 'logs/update_distributors_treolan.log'
os.makedirs(os.path.dirname(log_file), exist_ok=True)
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Добавляем обработчик к логгеру
logger.addHandler(file_handler)

# Курс USD (можно обновлять периодически или получать из API)
USD_RATE = 95.0  # Примерный курс USD/RUB

def get_usd_rate():
    """Получает актуальный курс USD/RUB"""
    try:
        # Можно использовать различные API для получения курса
        # Например, ЦБ РФ или другие источники
        response = requests.get('https://www.cbr-xml-daily.ru/daily_json.js', timeout=10)
        if response.status_code == 200:
            data = response.json()
            usd_rate = data['Valute']['USD']['Value']
            logger.info(f"[Treolan] Получен курс USD: {usd_rate}")
            return usd_rate
    except Exception as e:
        logger.error(f"[Treolan] Не удалось получить курс USD: {e}")
    
    # Возвращаем дефолтный курс если не удалось получить
    return USD_RATE

def get_treolan_token(login: str = TREOLAN_LOGIN, password: str = TREOLAN_PASSWORD) -> Optional[str]:
    try:
        resp = requests.post(
            TREOLAN_AUTH_URL,
            json={"login": login, "password": password},
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=30
        )
        resp.raise_for_status()
        token = resp.text.strip().strip('"')
        if token:
            return f"Bearer {token}"
    except Exception as e:
        logger.error(f"[Treolan] Ошибка авторизации: {e}")
    return None

def convert_stock(stock_value):
    """Конвертирует остатки в числовые значения на основе реальных данных Treolan"""
    if stock_value is None:
        return 0
    
    # Если уже число
    if isinstance(stock_value, (int, float)):
        return int(stock_value)
    
    # Преобразуем в строку и обрабатываем
    stock_str = str(stock_value).strip().lower()
    
    # Специальные случаи для больших количеств
    if stock_str in ['много', 'большое количество', 'в наличии', 'есть', 'достаточно']:
        return 99  # Большое количество
    
    # Специальные случаи для отсутствия
    elif stock_str in ['0*', '0+', '0-', 'нет', 'не указано', '']:
        return 0    # Нет в наличии
    
    # Специальные случаи для малых количеств
    elif stock_str in ['мало', 'несколько', '1-2', '2-3', '3-4', '<10', '< 10']:
        return 5    # Малое количество (5-10 штук)
    
    # Пытаемся извлечь число
    try:
        # Убираем все нечисловые символы, кроме цифр
        numbers = re.findall(r'\d+', stock_str)
        if numbers:
            num = int(numbers[0])
            # Если число маленькое (меньше 10), оставляем как есть
            if num < 10:
                return num
            # Если число большое, возможно это "много"
            elif num > 100:
                return 99
            else:
                return num
        else:
            return 0
    except (ValueError, TypeError):
        return 0

def convert_price(price, currency):
    """Конвертирует цену в RUB и USD"""
    if price is None:
        return 0.0, 0.0
    
    try:
        price = float(price)
        if price <= 0:
            return 0.0, 0.0
    except (ValueError, TypeError):
        return 0.0, 0.0
    
    currency = (currency or '').upper()
    
    if currency in ['RUB', 'RUR']:
        # Цена уже в рублях
        price_rub = price
        price_usd = round(price / USD_RATE, 2) if USD_RATE > 0 else 0.0
    elif currency == 'USD':
        # Цена в долларах
        price_usd = price
        price_rub = round(price * USD_RATE, 2)
    else:
        # Неизвестная валюта, предполагаем что это рубли
        price_rub = price
        price_usd = round(price / USD_RATE, 2) if USD_RATE > 0 else 0.0
    
    return price_rub, price_usd

async def get_treolan_products() -> List[Dict]:
    start_time = time.time()
    logger.info("[Treolan][DEBUG] Получаем токен...")
    
    # notify_export_start - асинхронная функция, нельзя вызывать в синхронном коде
    # Используем только логирование
    
    token = get_treolan_token()
    if not token:
        error_msg = "Не удалось получить токен, выгрузка невозможна."
        logger.error(f"[Treolan] {error_msg}")
        # notify_export_error - асинхронная функция, нельзя вызывать в синхронном коде
        return []
    logger.info(f"[Treolan][DEBUG] Токен получен: {token[:20]}...")
    
    # Получаем актуальный курс USD
    global USD_RATE
    USD_RATE = get_usd_rate()
    logger.info(f"[Treolan] Используем курс USD: {USD_RATE}")
    try:
        logger.info("[Treolan][DEBUG] Делаем запрос к Treolan API...")
        resp = requests.post(
            TREOLAN_CATALOG_URL,
            json={
                "category": "",
                "vendorid": 0,
                "keywords": "",
                "criterion": 1,
                "inArticul": False,
                "inName": False,
                "inMark": False,
                "showNc": 0,
                "freeNom": False
            },
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": token
            },
            timeout=120
        )
        resp.raise_for_status()
        logger.info(f"[Treolan][DEBUG] Ответ от API: {resp.status_code}, {len(resp.content)} bytes")
        data = resp.json()
        logger.info(f"[Treolan][DEBUG] Ключи в ответе: {list(data.keys())}")
    except Exception as e:
        error_msg = f"Ошибка получения каталога: {e}"
        logger.error(f"[Treolan] {error_msg}")
        # notify_export_error - асинхронная функция, нельзя вызывать в синхронном коде
        return []
    products = []
    def parse_category(cat):
        if 'products' in cat and cat['products']:
            for p in cat['products']:
                article = p.get('articul')
                name = p.get('rusName') or p.get('name')
                brand = p.get('vendor')
                stock = p.get('atStock')
                price = p.get('currentPrice')
                currency = p.get('currency')
                
                # Конвертируем цены
                price_rub, price_usd = convert_price(price, currency)
                
                # Конвертируем остатки
                stock_converted = convert_stock(stock)
                
                # Очищаем партномер от пробелов
                clean_article = article.strip() if article else ''
                
                products.append({
                    'id': clean_article,
                    'article': clean_article,
                    'name': name,
                    'brand': brand,
                    'part_number': clean_article,
                    'price_rub': price_rub,
                    'price_usd': price_usd,
                    'stock': stock_converted,  # Используем конвертированный остаток
                })
        if 'children' in cat and cat['children']:
            for child in cat['children']:
                parse_category(child)
    cats = data.get('categories')
    logger.info(f"[Treolan][DEBUG] Количество категорий: {len(cats) if cats else 0}")
    if cats:
        for cat in cats:
            parse_category(cat)
    logger.info(f"[Treolan] Всего товаров выгружено: {len(products)}")
    if products:
        logger.info(f"[Treolan][DEBUG] Пример товара: {products[0]}")
        
        # Рассчитываем время выполнения
        end_time = time.time()
        duration_seconds = end_time - start_time
        duration_minutes = duration_seconds / 60
        
        # notify_export_complete - асинхронная функция, нельзя вызывать в синхронном коде
        # Используем только логирование
        
        logger.info(f"[Treolan][TIME] Выгрузка завершена за {duration_seconds:.1f} сек ({duration_minutes:.1f} мин)")
    
    return products


async def upsert_treolan_products(products, db):
    logger.info(f"[Treolan][DEBUG] Вставляем товаров: {len(products)}")
    await db.execute(text("DELETE FROM products WHERE distributor = 'Treolan'"))
    await db.commit()
    # Удаляем дубли по (article, part_number, 'Treolan')
    unique = {}
    for p in products:
        key = (p.get('article'), p.get('part_number'), 'Treolan')
        if key not in unique:
            unique[key] = p
    products = list(unique.values())
    if products:
        # Используем execute для каждого товара
        for product in products:
            await db.execute(
                text("""
                INSERT INTO products (article, name, brand, part_number, price_rub, price_usd, stock, distributor, is_active, created_at, updated_at)
                VALUES (:article, :name, :brand, :part_number, :price_rub, :price_usd, :stock, 'Treolan', TRUE, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                    article = VALUES(article),
                    name = VALUES(name),
                    brand = VALUES(brand),
                    price_rub = VALUES(price_rub),
                    price_usd = VALUES(price_usd),
                    stock = VALUES(stock),
                    distributor = VALUES(distributor),
                    is_active = VALUES(is_active),
                    updated_at = NOW()
                """),
                product
            )
        await db.commit()
        logger.info(f"[Treolan][DEBUG] Вставка завершена.")
    else:
        logger.info("[Treolan][DEBUG] Нет товаров для вставки.")
    
    # Правильно закрываем соединение
    try:
        await db.close()
    except:
        pass
    
    # Записываем дату обновления
    with open("static/last_update_treolan.txt", "w", encoding="utf-8") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("[Treolan] Дата обновления сохранена в static/last_update_treolan.txt") 