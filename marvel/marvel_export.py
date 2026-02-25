import requests
import logging
import datetime
import json
import os
import time
from core.telegram_notify import notify_export_error

MARVEL_USER = "YOUR_MARVEL_USER"
MARVEL_PASSWORD = "YOUR_MARVEL_PASSWORD"
GETFULLSTOCK_URL = "https://b2b.marvel.ru/Api/GetFullStock"

# Ограничение: не чаще 1 раза в час
MARVEL_MIN_INTERVAL = 60 * 60  # 1 час
MARVEL_LAST_UPDATE_FILE = "static/last_update_marvel.txt"

# (Удалена настройка logging.basicConfig и LOG_FILE)

def can_update_marvel():
    try:
        with open(MARVEL_LAST_UPDATE_FILE, "r", encoding="utf-8") as f:
            last_update = f.read().strip()
            last_update_dt = datetime.datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S")
            now = datetime.datetime.now()
            if (now - last_update_dt).total_seconds() < MARVEL_MIN_INTERVAL:
                return False
    except Exception:
        return True
    return True

async def get_marvel_products():
    """
    Массовая выгрузка всех товаров Marvel через GetFullStock.
    Возвращает список dict строго под структуру базы.
    Логирует примеры товаров для проверки структуры.
    """
    start_time = time.time()
    
    # Уведомление о начале экспорта отправляется в main.py, убираем дублирование
    
    params = {
        "user": MARVEL_USER,
        "password": MARVEL_PASSWORD,
        "secretKey": "",
        "responseFormat": 1,  # JSON
        "packStatus": 0,
        # Добавляем дополнительные параметры для обхода rate limiting
        "forceUpdate": 1,  # Принудительное обновление
        "timestamp": int(time.time()),  # Текущий timestamp
    }
    def safe_float(val):
        try:
            if val is None:
                return None
            return float(str(val).replace(",", "."))
        except Exception:
            return None
    def safe_str(val):
        if val is None:
            return ""
        return str(val)
    
    def safe_stock(val):
        """Безопасно преобразует остаток в число, убирая '+' и другие символы"""
        if val is None:
            return 0
        try:
            # Убираем все нечисловые символы кроме цифр и точки
            clean_val = str(val).replace('+', '').replace(',', '').strip()
            if not clean_val:
                return 0
            return int(float(clean_val))
        except (ValueError, TypeError):
            return 0
    try:
        # Добавляем заголовки для обхода rate limiting
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        resp = requests.post(GETFULLSTOCK_URL, params=params, headers=headers, timeout=180)
        resp.raise_for_status()
        data = resp.json()
        if not data or "Header" not in data or data["Header"].get("Code") != 0:
            msg = data["Header"].get("Message") if data and "Header" in data else "Marvel API: пустой или некорректный ответ"
            error_msg = f"Ошибка API Marvel: {msg}"
            logging.error(f"[Marvel][ERROR] {error_msg}")
            await notify_export_error("Marvel", error_msg)
            return []
        body = data.get("Body", {})
        items = body.get("CategoryItem", [])
        if isinstance(items, dict):
            items = [items]
        for i, item in enumerate(items[:3]):
            logging.info(f"[Marvel][DEBUG] Пример товара {i+1}: {item}")
        products = []
        for item in items:
            # Очищаем партномер от пробелов
            clean_article = str(item.get("WareArticle")).strip() if item.get("WareArticle") else ''
            
            product = {
                "article": clean_article,
                "name": item.get("WareFullName"),
                "brand": item.get("WareVendor"),
                "part_number": clean_article,
                "category_code": item.get("CategoryId"),
                "price_rub": safe_float(item.get("WarePriceRUB")),
                "price_usd": safe_float(item.get("WarePriceUSD")),
                "stock": safe_stock(item.get("TotalInventQty")),
                "package_volume": safe_float(item.get("UnitVolume")),
                "package_weight": safe_float(item.get("Weight")),
                "tech_specs": json.dumps({k: v for k, v in item.items() if k not in [
                    "WareArticle", "WareFullName", "WareVendor", "CategoryId", "WarePriceRUB", "WarePriceUSD", "TotalInventQty", "UnitVolume", "Weight"
                ]}, ensure_ascii=False),
                "transit_date": None,  # Marvel не возвращает дату транзита
                "distributor": "Marvel",
                "is_active": True
            }
            # Добавляем ключи для валидации
            product["id"] = product["article"]
            product["price"] = product["price_rub"]
            products.append(product)
        
        # Рассчитываем время выполнения
        end_time = time.time()
        duration_seconds = end_time - start_time
        duration_minutes = duration_seconds / 60
        
        # Уведомление о завершении отправляется в main.py, убираем дублирование
        
        print(f"[Marvel][TIME] Выгрузка завершена за {duration_seconds:.1f} сек ({duration_minutes:.1f} мин)")
        
        return products
    except Exception as e:
        error_msg = f"Ошибка при выгрузке: {e}"
        logging.error(f"[Marvel][ERROR] {error_msg}")
        await notify_export_error("Marvel", error_msg)
        return [] 

async def import_marvel_products_to_db(products, db):
    from sqlalchemy import text
    # Удаляем все старые товары Marvel
    await db.execute(text("DELETE FROM products WHERE distributor = 'Marvel'"))
    await db.commit()
    # Массовая вставка новых товаров
    if products:
        # Добавляем distributor для всех товаров
        for p in products:
            p['distributor'] = 'Marvel'
        # Удаляем дубликаты по (article, part_number, distributor)
        unique = {}
        for p in products:
            key = (p['article'], p['part_number'], p['distributor'])
            if key not in unique:
                unique[key] = p
        products = list(unique.values())
        logging.info(f"[Marvel][DEBUG] Ключи первого товара: {list(products[0].keys())}")
        logging.info(f"[Marvel][DEBUG] Пример словаря для вставки: {products[0]}")
        # Массовая вставка с обработкой конфликтов
        for product in products:
            await db.execute(
                text("""
                INSERT INTO products (article, name, brand, part_number, category_code, price_rub, price_usd, stock, package_volume, package_weight, tech_specs, transit_date, distributor, is_active, created_at, updated_at)
                VALUES (:article, :name, :brand, :part_number, :category_code, :price_rub, :price_usd, :stock, :package_volume, :package_weight, :tech_specs, :transit_date, :distributor, TRUE, NOW(), NOW())
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
                    distributor = VALUES(distributor),
                    is_active = VALUES(is_active),
                    updated_at = NOW()
                """),
                product
            )
        await db.commit()
        logging.info(f"[Marvel][STATS] Добавлено/обновлено: {len(products)}")
        
        # Записываем дату обновления
        with open("static/last_update_marvel.txt", "w", encoding="utf-8") as f:
            f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        print("[Marvel] Дата обновления сохранена в static/last_update_marvel.txt")
    else:
        logging.warning("[Marvel][DEBUG] Список товаров пуст!")
        await db.commit() 