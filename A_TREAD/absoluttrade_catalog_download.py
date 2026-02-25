import aiohttp
import asyncio
from datetime import datetime, timedelta
import os
from update_distributors.database import AsyncSessionLocal
from update_distributors.database_models import Product
from sqlalchemy.dialects.postgresql import insert
import random

API_LOGIN = "YOUR_API_LOGIN"
API_PASSWORD = "YOUR_API_PASSWORD"
AUTH_URL = "https://api.absoluttrade.ru/api/Token/CreateToken"
CATALOG_URL = "https://api.absoluttrade.ru/api/Catalogs/ProductSearch"
PAGE_SIZE = 13824
SYNC_FILE = "static/last_sync_absoluttrade.txt"
CONCURRENT_REQUESTS = 12  # Было 8, увеличено для ускорения
RETRY_LIMIT = 5

async def get_token(session):
    payload = {"username": API_LOGIN, "password": API_PASSWORD}
    async with session.post(AUTH_URL, json=payload) as response:
        response.raise_for_status()
        text = await response.text()
        return text.strip('"').replace('"', '')

def safe_float(val):
    if isinstance(val, list):
        try:
            return float(val[0]) if val else None
        except Exception:
            return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def safe_int_instock(val):
    if isinstance(val, list) and val:
        val = val[0]
    if isinstance(val, list):
        return 0
    try:
        if isinstance(val, dict):
            return int(val.get("quantity", 0))
        return int(val)
    except (ValueError, TypeError):
        return 0

async def fetch_page(session, token, page, page_size, semaphore):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    params = {"page": page, "pageSize": page_size}
    for attempt in range(RETRY_LIMIT):
        try:
            async with semaphore:
                async with session.get(CATALOG_URL, headers=headers, params=params, timeout=120) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    items = data["items"] if isinstance(data, dict) and "items" in data else data
                    print(f"[AbsolutTrade] Загружено товаров: {len(items)} (страница {page})")
                    # Добавим небольшую задержку для защиты от бана
                    await asyncio.sleep(random.uniform(0.1, 0.4))
                    return items
        except Exception as e:
            print(f"[AbsolutTrade] Ошибка на странице {page}: {e}. Попытка {attempt+1}/{RETRY_LIMIT}")
            await asyncio.sleep(2 ** attempt)
    print(f"[AbsolutTrade] Не удалось загрузить страницу {page} после {RETRY_LIMIT} попыток.")
    return []

async def fetch_all_products_async(token):
    async with aiohttp.ClientSession() as session:
        # Получаем первую страницу и определяем количество страниц
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        params = {"page": 1, "pageSize": PAGE_SIZE}
        async with session.get(CATALOG_URL, headers=headers, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
            items = data["items"] if isinstance(data, dict) and "items" in data else data
            total_products = data.get("total", None) if isinstance(data, dict) else None
        if not items:
            return []
        if total_products:
            max_pages = (total_products + PAGE_SIZE - 1) // PAGE_SIZE
        else:
            max_pages = 10000
        all_products = list(items)
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = [fetch_page(session, token, page, PAGE_SIZE, semaphore) for page in range(2, max_pages+1)]
        for i in range(0, len(tasks), CONCURRENT_REQUESTS):
            batch = tasks[i:i+CONCURRENT_REQUESTS]
            results = await asyncio.gather(*batch)
            for its in results:
                all_products.extend(its)
        return all_products

async def get_absoluttrade_products():
    async with aiohttp.ClientSession() as session:
        token = await get_token(session)
        products = await fetch_all_products_async(token)
        return products

def get_last_update_time():
    try:
        with open("static/last_update_absoluttrade.txt", "r", encoding="utf-8") as f:
            return datetime.strptime(f.read().strip(), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now() - timedelta(days=1)

def normalize_absoluttrade_product(p):
    return {
        'id': p.get('productId'),
        'name': p.get('productName'),
        'price': p.get('productPrice'),
        # Можно добавить другие поля по необходимости
    }

async def get_absoluttrade_products_diff():
    """Возвращает только изменённые товары с момента последней синхронизации."""
    from_date = get_last_update_time()
    async with aiohttp.ClientSession() as session:
        token = await get_token(session)
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        params = {"fromDateUpdate": int(from_date.timestamp()), "page": 1, "pageSize": PAGE_SIZE}
        all_products = []
        while True:
            async with session.get(CATALOG_URL, headers=headers, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                items = data["items"] if isinstance(data, dict) and "items" in data else data
                if not items:
                    break
                all_products.extend(items)
                if len(items) < params["pageSize"]:
                    break
                params["page"] += 1
        # Привести к ожидаемой структуре
        return [normalize_absoluttrade_product(p) for p in all_products]

async def save_products_to_db(products):
    async with AsyncSessionLocal() as session:
        stmt = insert(Product).values(products)
        update_dict = {c.name: c for c in stmt.excluded if c.name != 'id'}
        stmt = stmt.on_conflict_do_update(
            index_elements=['article'],
            set_=update_dict
        )
        await session.execute(stmt)
        await session.commit()

async def fast_export_and_save():
    async with aiohttp.ClientSession() as session:
        print("Получение токена...")
        token = await get_token(session)
        print("Выгрузка каталога Абсолют Трейд...")
        products = await fetch_all_products_async(token)
        print(f"Получено товаров: {len(products)}. Сохраняем в базу...")
        db_products = []
        for p in products:
            db_products.append({
                "article": p.get("article"),
                "name": p.get("name"),
                "brand": p.get("brand"),
                "part_number": p.get("partNumber"),
                "category_code": p.get("categoryCode"),
                "price_rub": safe_float(p.get("priceRUB")),
                "price_usd": safe_float(p.get("priceUSD")),
                "stock": safe_int_instock(p.get("inStock")),
                "package_volume": safe_float(p.get("packageVolume")),
                "package_weight": safe_float(p.get("packageWeight")),
                "tech_specs": p.get("techSpecs"),
                "transit_date": p.get("transitDate"),
                "distributor": "AbsolutTrade",
                "is_active": True
            })
        batch_size = 1000
        for i in range(0, len(db_products), batch_size):
            await save_products_to_db(db_products[i:i+batch_size])
            print(f"Сохранено {i+batch_size} товаров...")
        print("Выгрузка и запись завершены!")

if __name__ == "__main__":
    asyncio.run(fast_export_and_save())