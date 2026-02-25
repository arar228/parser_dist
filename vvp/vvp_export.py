import os
import requests
from typing import List, Dict, Optional
import logging
import time
import csv
from datetime import datetime

VVP_BASE_URL = "https://b2b.vvpgroup.com/api"
VVP_LOGIN = os.environ.get("VVP_LOGIN") or "YOUR_VVP_LOGIN"
VVP_PASSWORD = os.environ.get("VVP_PASSWORD") or "YOUR_VVP_PASSWORD"


def get_vvp_token(login: Optional[str] = None, password: Optional[str] = None) -> Optional[str]:
    """
    Получить токен авторизации VVP по логину и паролю.
    """
    login = login or VVP_LOGIN
    password = password or VVP_PASSWORD
    logging.info(f"[VVP] Получение токена для пользователя: {login}")
    if not login or not password:
        logging.error("[VVP] Не указан логин или пароль для авторизации!")
        return None
    try:
        url = f"{VVP_BASE_URL}/user/token?login={login}&password={password}"
        resp = requests.get(url, headers={"Accept": "application/json"}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list) and data and "token" in data[0]:
            logging.info("[VVP] Токен успешно получен (list)")
            return data[0]["token"]
        elif isinstance(data, dict) and "token" in data:
            logging.info("[VVP] Токен успешно получен (dict)")
            return data["token"]
        else:
            logging.error(f"[VVP] Не удалось получить токен, ответ: {data}")
    except Exception as e:
        logging.error(f"[VVP] Ошибка авторизации: {e}")
    return None


def save_vvp_products_to_csv(products, filename="vvp_products_dump.csv"):
    if not products:
        logging.warning("[VVP] Нет товаров для сохранения в CSV.")
        return
    keys = ["article", "name", "brand", "part_number", "price_rub", "price_usd", "stock"]
    try:
        with open(filename, "w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(products)
        logging.info(f"[VVP] Товары сохранены в {filename} (всего: {len(products)})")
    except Exception as e:
        logging.error(f"[VVP] Ошибка при сохранении CSV: {e}")


async def upsert_vvp_products(products, db):
    from sqlalchemy import insert, text
    from core.db_models import Product
    logging.info("[VVP][DEBUG] Старт функции upsert_vvp_products")
    logging.info(f"[VVP][DEBUG] Получено товаров для вставки: {len(products)}")
    if products:
        # Добавляем distributor для всех товаров
        for p in products:
            p['distributor'] = 'VVP'
        # Удаляем дубликаты по (article, part_number, distributor)
        unique = {}
        for p in products:
            key = (p['article'], p['part_number'], p['distributor'])
            if key not in unique:
                unique[key] = p
        products = list(unique.values())
        logging.info(f"[VVP][DEBUG] Ключи первого товара: {list(products[0].keys())}")
        logging.info(f"[VVP][DEBUG] Пример словаря для вставки: {products[0]}")
    else:
        logging.warning("[VVP][DEBUG] Список товаров пуст!")
    # Удаляем все старые товары VVP (отдельная транзакция)
    logging.info("[VVP][DEBUG] Удаляю все старые товары VVP из базы...")
    await db.execute(text("DELETE FROM products WHERE distributor = 'VVP'"))
    await db.commit()
    logging.info("[VVP][DEBUG] Старые товары удалены. Начинаю массовую вставку новых товаров...")
    # Вставка в отдельном try/except
    inserted = 0
    try:
        for product in products:
            await db.execute(
                text("""
                INSERT INTO products (article, name, brand, part_number, price_rub, price_usd, stock, distributor, is_active, created_at, updated_at)
                VALUES (:article, :name, :brand, :part_number, :price_rub, :price_usd, :stock, :distributor, TRUE, NOW(), NOW())
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
        inserted = len(products)
        logging.info(f"[VVP][STATS] Добавлено/обновлено: {inserted}")
    except Exception as e:
        logging.error(f"[VVP][ERROR] Ошибка при вставке товаров: {e}")
    logging.info(f"[VVP][END] Выгрузка завершена. Всего обработано: {len(products)}")


# --- ЛОГИРОВАНИЕ ---
# (Удалена настройка logging.basicConfig и LOG_FILE)


async def get_vvp_products() -> List[Dict]:
    """
    Получить все товары VVP с ценами (RUB/USD) и остатками. Возвращает список dict для вставки в БД.
    """
    start_time = time.time()
    # logging.info("[VVP][START] Выгрузка начата")  # Убрано, чтобы не было дублирования
    
    # Импортируем Telegram уведомления
    try:
        from core.telegram_notify import notify_export_error
    except ImportError:
        # Если модуль недоступен, создаем заглушку
        async def notify_export_error(distributor, error_msg):
            logging.error(f"[Telegram] Ошибка: {error_msg}")
    
    token = get_vvp_token()
    if not token:
        error_msg = "Не удалось получить токен, выгрузка невозможна."
        logging.error(f"[VVP] {error_msg}")
        # notify_export_error - асинхронная функция, нельзя вызывать в синхронном коде
        # Используем только логирование
        pass
        return []
    products = []
    page = 1
    total_pages = 1
    while page <= total_pages:
        page_time = time.time()
        try:
            url = f"{VVP_BASE_URL}/products/stocks-prices?page={page}"
            headers = {"Authorization-Token": token, "Accept": "application/json"}
            logging.info(f"[VVP][PAGE:{page}] Запрос: {url}")
            resp = requests.get(url, headers=headers, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if page == 1:
                total_pages = int(data.get("TOTAL_PAGES", 1))
                logging.info(f"[VVP] Всего страниц: {total_pages}")
            items = data.get("ITEMS", [])
            logging.info(f"[VVP][PAGE:{page}] Получено товаров: {len(items)}")
            if len(items) < 100:
                logging.warning(f"[VVP][PAGE:{page}] Мало товаров: {len(items)}")
            for item in items:
                article = item.get("ARTICLE")
                name = item.get("NAME")
                brand = item.get("BRAND")
                part_number = item.get("ARTICLE")
                price_rub = None
                price_usd = None
                # Цены
                for price in item.get("PRICES", []):
                    currency = (price.get("CURRENCY") or "").upper()
                    price_val = price.get("PRICE")
                    if currency == "RUB":
                        try:
                            price_rub = float(price_val)
                        except:
                            price_rub = None
                    elif currency == "USD":
                        try:
                            price_usd = float(price_val)
                        except:
                            price_usd = None
                # Остатки (суммируем по всем складам)
                stock = None
                if "REMAINS" in item:
                    try:
                        stock = sum(float(rem.get("AMOUNT", 0)) for rem in item["REMAINS"] if rem.get("AMOUNT") is not None)
                    except Exception:
                        stock = None
                # Очищаем партномер от пробелов
                clean_part_num = str(part_number).strip() if part_number else ''
                
                products.append({
                    "article": article,
                    "name": name,
                    "brand": brand,
                    "part_number": clean_part_num,
                    "price_rub": price_rub,
                    "price_usd": price_usd,
                    "stock": str(stock) if stock is not None else None,
                })
            logging.info(f"[VVP][PAGE:{page}] Обработка завершена за {time.time() - page_time:.2f} сек.")
            page += 1
        except Exception as e:
            logging.error(f"[VVP][PAGE:{page}] Ошибка: {e}")
            break
    logging.info(f"[VVP][ITEMS] Всего товаров выгружено: {len(products)}")
    if products:
        logging.info(f"[VVP][EXAMPLE] Пример товара: {products[0]}")
    if len(products) < 1000:
        error_msg = f"Резкое уменьшение товаров: {len(products)}"
        logging.error(f"[ALERT][VVP] {error_msg}")
        # notify_export_error - асинхронная функция, нельзя вызывать в синхронном коде
        # Используем только логирование
        pass
    logging.info(f"[VVP][SUCCESS] Выгрузка завершена за {time.time() - start_time:.2f} сек.")
    save_vvp_products_to_csv(products)
    
    # Записываем дату обновления
    with open("static/last_update_vvp.txt", "w", encoding="utf-8") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("[VVP] Дата обновления сохранена в static/last_update_vvp.txt")
    
    return products 