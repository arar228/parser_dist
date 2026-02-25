from fastapi import APIRouter, Depends, HTTPException, Query, Form, Request, Response
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Any
from core.db_engine import AsyncSessionLocal
from core.db_models import Product
from sqlalchemy import or_, and_, func
import json
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import requests
import datetime
import typing

router = APIRouter()

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

def get_current_usd_rate():
    """Получает актуальный курс USD/RUB от ЦБ РФ"""
    try:
        response = requests.get('https://www.cbr-xml-daily.ru/daily_json.js', timeout=10)
        if response.status_code == 200:
            data = response.json()
            usd_rate = data['Valute']['USD']['Value']
            return usd_rate
    except Exception as e:
        print(f"Ошибка получения курса USD: {e}")
    return None

def get_usd_rate_info():
    """Получает информацию о курсе USD с датой"""
    try:
        response = requests.get('https://www.cbr-xml-daily.ru/daily_json.js', timeout=10)
        if response.status_code == 200:
            data = response.json()
            usd_rate = data['Valute']['USD']['Value']
            date_str = data.get('Date', '')
            if date_str:
                # Парсим дату из формата "2025-07-31T10:30:00+03:00"
                try:
                    date_obj = datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    date_formatted = date_obj.strftime('%d.%m.%Y')
                    
                    # Проверяем, насколько старая дата
                    today = datetime.datetime.now()
                    days_diff = (today - date_obj).days
                    
                    if days_diff > 1:
                        date_formatted += f" (данные за {days_diff} дн. назад)"
                    elif days_diff == 1:
                        date_formatted += " (данные за вчера)"
                        
                except:
                    date_formatted = date_str[:10]
            else:
                date_formatted = datetime.datetime.now().strftime('%d.%m.%Y')
            return usd_rate, date_formatted
    except Exception as e:
        print(f"Ошибка получения курса USD: {e}")
    return None, None

def transliterate_for_search(text):
    """
    Транслитерирует текст для поиска, заменяя английские буквы на русские и наоборот.
    Только для указанных букв: A=А, K=К, O=О, E=Е, C=С, H=Н, B=В, Р=P, T=Т, Х=X, М=M
    """
    if not text:
        return text
    
    # Словарь замен: английская -> русская
    en_to_ru = {
        'A': 'А', 'K': 'К', 'O': 'О', 'E': 'Е', 'C': 'С', 
        'H': 'Н', 'B': 'В', 'P': 'Р', 'T': 'Т', 'X': 'Х', 'M': 'М'
    }
    
    # Словарь замен: русская -> английская
    ru_to_en = {
        'А': 'A', 'К': 'K', 'О': 'O', 'Е': 'E', 'С': 'C', 
        'Н': 'H', 'В': 'B', 'Р': 'P', 'Т': 'T', 'Х': 'X', 'М': 'M'
    }
    
    # Создаем варианты текста с заменой букв
    variants = [text]
    
    # Вариант 1: заменяем английские на русские
    variant1 = text
    for en, ru in en_to_ru.items():
        variant1 = variant1.replace(en, ru)
    if variant1 != text:
        variants.append(variant1)
    
    # Вариант 2: заменяем русские на английские
    variant2 = text
    for ru, en in ru_to_en.items():
        variant2 = variant2.replace(ru, en)
    if variant2 != text:
        variants.append(variant2)
    
    return variants

def format_tech_specs(specs):
    if isinstance(specs, dict):
        key_map = {
            "AvailableForShippingInMSKCount": "В наличии в МСК (шт)",
            "AvailableForShippingInSPBCount": "В наличии в СПБ (шт)",
            "WarePackStatus": "Статус упаковки",
            "Dimension": "Габариты (тип)",
            "TaxPackagingCount": "Кол-во в упаковке",
            "IsForOrder": "Доступен для заказа",
            "InNearTransitCount": "В пути (ближайший) (шт)",
            "InFarTransitCount": "В пути (дальний) (шт)",
            "Categories": "Категории (структура)",
            "AvailableForB2BOrderQty": "Доступно для B2B заказа (шт)",
            "CanBeOrdered": "Можно заказать",
            "APIReservedQty": "Зарезервировано через API (шт)",
            "APIAvailableReservedQty": "Доступно для резерва через API (шт)",
            "IsAvailablePreOrder": "Доступен для предзаказа",
            "PromoDescription": "Описание акции",
            "ExtraSets": "Комплектация",
        }
        duplicate_keys = {
            "CategoryName", "NetWeight", "Width", "Height", "Depth",
            "WarePrice", "WarePriceCurrency", "BaseWarePrice", "BaseWarePriceCurrency",
            "PurchaseQty", "Stock", "Brand", "Name", "PartNumber", "CategoryCode",
            "PackageVolume", "PackageWeight"
        }
        lines = []
        for k in key_map:
            if k in specs and specs[k] not in (None, "", [], {}) and k not in duplicate_keys:
                val = specs[k]
                # Красиво форматируем булевы значения
                if isinstance(val, bool):
                    val = "Да" if val else "Нет"
                # Красиво форматируем Categories
                if k == "Categories":
                    cats = val
                    if isinstance(cats, dict) and "Category" in cats:
                        cats = cats["Category"]
                    if isinstance(cats, list):
                        cat_names = []
                        for c in cats:
                            if isinstance(c, dict):
                                name = c.get("CategoryId") or c.get("Name") or str(c)
                                cat_names.append(str(name))
                            else:
                                cat_names.append(str(c))
                        val = ", ".join(cat_names)
                lines.append(f"{key_map[k]}: {val}")
        # Остальные ключи, которые не дублируются и не переведены
        for k, v in specs.items():
            if k not in key_map and v not in (None, "", [], {}) and k not in duplicate_keys:
                # Булевы значения
                if isinstance(v, bool):
                    v = "Да" if v else "Нет"
                lines.append(f"Доп. информация — {k}: {v}")
        return "\n".join(lines)
    return str(specs)

def safe_parse_tech_specs(specs):
    if isinstance(specs, dict):
        return specs
    if isinstance(specs, str):
        try:
            return json.loads(specs)
        except Exception:
            return specs
    return specs

def clean_part_number(part_number):
    """Очищает партномер от пробелов и приводит к верхнему регистру для сравнения"""
    if not part_number:
        return ""
    return str(part_number).strip().upper()

@router.get("/search")
async def search_products(
    response: Response,
    part_numbers: str = Query(None, description="Партномера через запятую"),
    name: str = Query(None, description="Часть названия товара"),
    brand: str = Query(None, description="Бренд"),
    distributor: str = Query(None, description="Дистрибьютор"),
    price_rub_min: float = Query(None, description="Мин. цена (RUB)"),
    price_rub_max: float = Query(None, description="Макс. цена (RUB)"),
    price_usd_min: float = Query(None, description="Мин. цена (USD)"),
    price_usd_max: float = Query(None, description="Макс. цена (USD)"),
    limit: int = Query(50, ge=1, le=500, description="Сколько товаров вернуть"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    db: AsyncSession = Depends(get_db)
):
    stmt = select(Product)
    filters = []
    if part_numbers:
        parts = [clean_part_number(p) for p in part_numbers.split(",") if p.strip()]
        if parts:
            # Используем func.upper для регистронезависимого сравнения
            filters.append(func.upper(Product.part_number).in_(parts))
    if name:
        filters.append(Product.name.ilike(f"%{name}%"))
    if brand:
        filters.append(Product.brand.ilike(f"%{brand}%"))
    if distributor:
        filters.append(Product.distributor.ilike(f"%{distributor}%"))
    if price_rub_min is not None:
        filters.append(Product.price_rub >= price_rub_min)
    if price_rub_max is not None:
        filters.append(Product.price_rub <= price_rub_max)
    if price_usd_min is not None:
        filters.append(Product.price_usd >= price_usd_min)
    if price_usd_max is not None:
        filters.append(Product.price_usd <= price_usd_max)
    if filters:
        stmt = stmt.where(and_(*filters))
    # Считаем общее количество
    count_stmt = select(func.count()).select_from(Product)
    if filters:
        count_stmt = count_stmt.where(and_(*filters))
    total = (await db.execute(count_stmt)).scalar()
    # Пагинация
    stmt = stmt.offset(offset).limit(limit)
    result = await db.execute(stmt)
    products = result.scalars().all()
    response.headers["X-Total-Count"] = str(total)
    products_dict = {str(p.part_number): p for p in products}
    ordered = [products_dict.get(str(pn)) for pn in parts if products_dict.get(str(pn)) is not None] if part_numbers else products
    ordered = [p for p in ordered if p is not None]
    return [
        {
            "Артикул": p.article,
            "Наименование": p.name,
            "Бренд": p.brand,
            "Партномер": p.part_number,
            "Код категории": p.category_code,
            "Цена (RUB)": p.price_rub,
            "Цена (USD)": p.price_usd,
            "Наличие (шт)": p.stock,
            "Объем упаковки, м3": p.package_volume,
            "Вес упаковки, кг": p.package_weight,
            "Технические характеристики": (
                format_tech_specs(safe_parse_tech_specs(p.tech_specs))
                if str(p.distributor).lower() == "marvel"
                else str(safe_parse_tech_specs(p.tech_specs))
            ),
            "Дата транзита": p.transit_date,
            "Дистрибьютор": p.distributor,
        }
        for p in ordered
    ]

@router.post("/search")
async def search_products_post(
    request: Request,
    part_numbers: str = Form(...),
    db: AsyncSession = Depends(get_db)
):
    # Разбиваем по строкам, убираем пустые и пробелы
    parts = [clean_part_number(p) for p in part_numbers.splitlines() if p.strip()]
    if not parts:
        return []
    # Используем func.upper для регистронезависимого сравнения
    stmt = select(Product).where(func.upper(Product.part_number).in_(parts))
    result = await db.execute(stmt)
    products = result.scalars().all()
    products_dict = {str(p.part_number): p for p in products}
    # Сохраняем порядок, как в списке пользователя
    ordered = [products_dict.get(str(pn)) for pn in parts if products_dict.get(str(pn)) is not None] if part_numbers else products
    ordered = [p for p in ordered if p is not None]
    return [
        {
            "Артикул": p.article,
            "Наименование": p.name,
            "Бренд": p.brand,
            "Партномер": p.part_number,
            "Код категории": p.category_code,
            "Цена (RUB)": p.price_rub,
            "Цена (USD)": p.price_usd,
            "Наличие (шт)": p.stock,
            "Объем упаковки, м3": p.package_volume,
            "Вес упаковки, кг": p.package_weight,
            "Технические характеристики": (
                format_tech_specs(safe_parse_tech_specs(p.tech_specs))
                if str(p.distributor).lower() == "marvel"
                else str(safe_parse_tech_specs(p.tech_specs))
            ),
            "Дата транзита": p.transit_date,
            "Дистрибьютор": p.distributor,
        }
        for p in ordered
    ]

@router.get("/search/grouped")
async def search_products_grouped(
    response: Response,
    part_numbers: str = Query(None, description="Партномера через запятую"),
    name: str = Query(None, description="Часть названия товара"),
    brand: str = Query(None, description="Бренд"),
    price_rub_min: float = Query(None, description="Мин. цена (RUB)"),
    price_rub_max: float = Query(None, description="Макс. цена (RUB)"),
    price_usd_min: float = Query(None, description="Мин. цена (USD)"),
    price_usd_max: float = Query(None, description="Макс. цена (USD)"),
    limit: int = Query(50, ge=1, le=500, description="Сколько товаров вернуть"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    db: AsyncSession = Depends(get_db)
):
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("search_products_grouped")
    logger.info(f"[API] Входные параметры: part_numbers={part_numbers}, name={name}, brand={brand}")
    stmt = select(Product)
    filters = []
    parts = []
    if part_numbers:
        # Приводим к нижнему регистру и убираем пробелы для сравнения
        parts = [p.strip().upper() for p in part_numbers.split(",") if p.strip()]
        logger.info(f"[API] parts (очищенные): {parts}")
        if parts:
            # Фильтрация по верхнему регистру
            filters.append(func.upper(Product.part_number).in_(parts))
    if name:
        filters.append(Product.name.ilike(f"%{name}%"))
    if brand:
        filters.append(Product.brand.ilike(f"%{brand}%"))
    if price_rub_min is not None:
        filters.append(Product.price_rub >= price_rub_min)
    if price_rub_max is not None:
        filters.append(Product.price_rub <= price_rub_max)
    if price_usd_min is not None:
        filters.append(Product.price_usd >= price_usd_min)
    if price_usd_max is not None:
        filters.append(Product.price_usd <= price_usd_max)
    if filters:
        stmt = stmt.where(and_(*filters))
    # Пагинация
    stmt = stmt.offset(offset).limit(limit)
    logger.info(f"[API] SQL-запрос: {stmt}")
    result = await db.execute(stmt)
    products = result.scalars().all()
    logger.info(f"[API] Найдено товаров: {len(products)}")
    logger.info(f"[API] Найденные part_number: {[str(p.part_number) for p in products]}")
    
    # Группируем по part_number
    grouped = {}
    for p in products:
        pn = str(p.part_number)
        offer = {
            "Артикул": p.article,
            "Наименование": p.name,
            "Бренд": p.brand,
            "Партномер": p.part_number,
            "Код категории": p.category_code,
            "Цена (RUB)": p.price_rub,
            "Цена (USD)": p.price_usd,
            "Наличие (шт)": p.stock,
            "Объем упаковки, м3": p.package_volume,
            "Вес упаковки, кг": p.package_weight,
            "Технические характеристики": (
                format_tech_specs(safe_parse_tech_specs(p.tech_specs))
                if str(p.distributor).lower() == "marvel"
                else str(safe_parse_tech_specs(p.tech_specs))
            ),
            "Дата транзита": p.transit_date,
            "Дистрибьютор": p.distributor,
        }
        if pn not in grouped:
            grouped[pn] = {
                "part_number": pn,
                "name": p.name,
                "brand": p.brand,
                "offers": []
            }
        grouped[pn]["offers"].append(offer)
    
    # Сортируем офферы внутри каждой группы: сначала с наличием > 0, потом с наличием = 0
    for part_num, group in grouped.items():
        if group["offers"] and len(group["offers"]) > 1:
            # Функция для парсинга stock
            def parse_stock_for_sorting(stock_val):
                try:
                    if stock_val is None:
                        return 0
                    if isinstance(stock_val, (int, float)):
                        return int(stock_val)
                    s = str(stock_val).strip().lower().replace(',', '.')
                    if s == 'много':
                        return 99
                    if s == '' or s == '0':
                        return 0
                    if '+' in s:
                        base = s.split('+', 1)[0]
                        try:
                            return int(float(base))
                        except:
                            pass
                    if s.startswith('>'):
                        try:
                            return int(float(s[1:])) + 1
                        except:
                            return 0
                    if s.startswith('<'):
                        try:
                            v = int(float(s[1:]))
                            return max(v - 1, 0)
                        except:
                            return 0
                    digits = ''.join(ch for ch in s if (ch.isdigit() or ch == '.'))
                    if digits:
                        try:
                            return int(float(digits))
                        except:
                            return 0
                    return 0
                except:
                    return 0
            
            # Функция для получения цены USD
            def get_price_usd_for_sorting(offer):
                try:
                    price_usd = offer.get("Цена (USD)")
                    if price_usd is None or price_usd == "":
                        return float('inf')
                    if isinstance(price_usd, str):
                        return float(price_usd.replace(',', '.'))
                    return float(price_usd)
                except:
                    return float('inf')
            
            # Сортируем офферы: сначала по наличию (наличие > 0), потом по цене USD
            group["offers"].sort(key=lambda x: (
                0 if parse_stock_for_sorting(x.get("Наличие (шт)", 0)) > 0 else 1,  # 0 = есть наличие, 1 = нет наличия
                get_price_usd_for_sorting(x)  # Второй ключ сортировки - цена USD
            ))
            
            logger.info(f"[API] Отсортировано офферов для {part_num}: {len(group['offers'])}")
            for i, offer in enumerate(group["offers"]):
                stock = parse_stock_for_sorting(offer.get("Наличие (шт)", 0))
                price_usd = get_price_usd_for_sorting(offer)
                logger.info(f"[API]   {i+1}. {offer.get('Дистрибьютор')}: stock={stock}, price_usd={price_usd}")
    
    # Возвращаем список сгруппированных товаров
    return list(grouped.values())

@router.get("/search/export_excel")
async def export_products_excel(
    response: Response,
    part_numbers: str = Query(None, description="Партномера через запятую"),
    name: str = Query(None, description="Часть названия товара"),
    brand: str = Query(None, description="Бренд"),
    distributor: str = Query(None, description="Дистрибьютор"),
    price_rub_min: float = Query(None, description="Мин. цена (RUB)"),
    price_rub_max: float = Query(None, description="Макс. цена (RUB)"),
    price_usd_min: float = Query(None, description="Мин. цена (USD)"),
    price_usd_max: float = Query(None, description="Макс. цена (USD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Сколько товаров вернуть"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    db: AsyncSession = Depends(get_db)
):
    stmt = select(Product)
    filters = []
    if part_numbers:
        parts = [clean_part_number(p) for p in part_numbers.split(",") if p.strip()]
        if parts:
            # Используем func.upper для регистронезависимого сравнения
            filters.append(func.upper(Product.part_number).in_(parts))
    if name:
        filters.append(Product.name.ilike(f"%{name}%"))
    if brand:
        filters.append(Product.brand.ilike(f"%{brand}%"))
    if distributor:
        filters.append(Product.distributor.ilike(f"%{distributor}%"))
    if price_rub_min is not None:
        filters.append(Product.price_rub >= price_rub_min)
    if price_rub_max is not None:
        filters.append(Product.price_rub <= price_rub_max)
    if price_usd_min is not None:
        filters.append(Product.price_usd >= price_usd_min)
    if price_usd_max is not None:
        filters.append(Product.price_usd <= price_usd_max)
    if filters:
        stmt = stmt.where(and_(*filters))
    stmt = stmt.offset(offset).limit(limit)
    result = await db.execute(stmt)
    products = result.scalars().all()
    rows = [
        {
            "Артикул": p.article,
            "Наименование": p.name,
            "Бренд": p.brand,
            "Партномер": p.part_number,
            "Код категории": p.category_code,
            "Цена (RUB)": p.price_rub,
            "Цена (USD)": p.price_usd,
            "Наличие (шт)": p.stock,
            "Объем упаковки, м3": p.package_volume,
            "Вес упаковки, кг": p.package_weight,
            "Технические характеристики": (
                format_tech_specs(safe_parse_tech_specs(p.tech_specs))
                if str(p.distributor).lower() == "marvel"
                else str(safe_parse_tech_specs(p.tech_specs))
            ),
            "Дата транзита": p.transit_date,
            "Дистрибьютор": p.distributor,
        }
        for p in products
    ]
    df = pd.DataFrame(rows)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Products")
    output.seek(0)
    headers = {"Content-Disposition": "attachment; filename=products_export.xlsx"}
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)

@router.get("/search/export_csv")
async def export_products_csv(
    response: Response,
    part_numbers: str = Query(None, description="Партномера через запятую"),
    name: str = Query(None, description="Часть названия товара"),
    brand: str = Query(None, description="Бренд"),
    distributor: str = Query(None, description="Дистрибьютор"),
    price_rub_min: float = Query(None, description="Мин. цена (RUB)"),
    price_rub_max: float = Query(None, description="Макс. цена (RUB)"),
    price_usd_min: float = Query(None, description="Мин. цена (USD)"),
    price_usd_max: float = Query(None, description="Макс. цена (USD)"),
    limit: int = Query(1000, ge=1, le=10000, description="Сколько товаров вернуть"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    db: AsyncSession = Depends(get_db)
):
    stmt = select(Product)
    filters = []
    if part_numbers:
        parts = [clean_part_number(p) for p in part_numbers.split(",") if p.strip()]
        if parts:
            # Используем func.upper для регистронезависимого сравнения
            filters.append(func.upper(Product.part_number).in_(parts))
    if name:
        filters.append(Product.name.ilike(f"%{name}%"))
    if brand:
        filters.append(Product.brand.ilike(f"%{brand}%"))
    if distributor:
        filters.append(Product.distributor.ilike(f"%{distributor}%"))
    if price_rub_min is not None:
        filters.append(Product.price_rub >= price_rub_min)
    if price_rub_max is not None:
        filters.append(Product.price_rub <= price_rub_max)
    if price_usd_min is not None:
        filters.append(Product.price_usd >= price_usd_min)
    if price_usd_max is not None:
        filters.append(Product.price_usd <= price_usd_max)
    if filters:
        stmt = stmt.where(and_(*filters))
    stmt = stmt.offset(offset).limit(limit)
    result = await db.execute(stmt)
    products = result.scalars().all()
    rows = [
        {
            "Артикул": p.article,
            "Наименование": p.name,
            "Бренд": p.brand,
            "Партномер": p.part_number,
            "Код категории": p.category_code,
            "Цена (RUB)": p.price_rub,
            "Цена (USD)": p.price_usd,
            "Наличие (шт)": p.stock,
            "Объем упаковки, м3": p.package_volume,
            "Вес упаковки, кг": p.package_weight,
            "Технические характеристики": (
                format_tech_specs(safe_parse_tech_specs(p.tech_specs))
                if str(p.distributor).lower() == "marvel"
                else str(safe_parse_tech_specs(p.tech_specs))
            ),
            "Дата транзита": p.transit_date,
            "Дистрибьютор": p.distributor,
        }
        for p in products
    ]
    df = pd.DataFrame(rows)
    output = io.StringIO()
    df.to_csv(output, index=False, encoding="utf-8-sig", sep=",")
    output.seek(0)
    headers = {"Content-Disposition": "attachment; filename=products_export.csv"}
    return StreamingResponse(io.BytesIO(output.getvalue().encode("utf-8-sig")), media_type="text/csv", headers=headers)

@router.get("/usd_rate")
async def get_usd_rate():
    """Получает актуальный курс USD/RUB от ЦБ РФ"""
    usd_rate, date = get_usd_rate_info()
    if usd_rate and date:
        return {
            "rate": usd_rate,
            "date": date,
            "formatted": f"1 USD = {usd_rate} RUB ({date})"
        }
    else:
        return {
            "rate": None,
            "date": None,
            "formatted": "Курс недоступен"
        }

@router.get("/search/name")
async def search_by_name(
    response: Response,
    query: str = Query(..., description="Поисковый запрос (например: 'процессор core', 'материнская плата')"),
    sort_by: str = Query("price_rub", description="Сортировка: price_rub, price_usd, stock, name"),
    sort_order: str = Query("asc", description="Порядок сортировки: asc, desc"),
    limit: int = Query(100, ge=1, le=1000000, description="Сколько товаров вернуть"),
    offset: int = Query(0, ge=0, description="Смещение для пагинации"),
    db: AsyncSession = Depends(get_db)
):
    """
    Умный поиск по наименованию с группировкой по партномерам.
    Находит товары по названию, группирует по партномерам и убирает дубликаты.
    """
    try:
        print(f"[DEBUG] Начало поиска: query='{query}', sort_by='{sort_by}', sort_order='{sort_order}'")
        

        
        # Очищаем поисковый запрос
        query = query.strip().lower()
        if not query:
            print("[DEBUG] Пустой поисковый запрос")
            return {"error": "Пустой поисковый запрос"}

        # Строим базовый запрос с улучшенным поиском
        # Разбиваем запрос на слова для поиска
        query_words = [word.strip() for word in query.split() if word.strip()]
        print(f"[DEBUG] Слова запроса: {query_words}")
        
        # Создаем более точные условия поиска
        if len(query_words) >= 2:
            # Если есть несколько слов, используем более точную логику
            # Первое слово должно быть в названии (основной товар)
            # Остальные слова могут быть в любом поле
            main_word = query_words[0].lower()
            other_words = query_words[1:]
            
            print(f"[DEBUG] Многословный поиск: main_word='{main_word}', other_words={other_words}")
            
            # Создаем варианты основного слова с транслитерацией
            main_word_variants = transliterate_for_search(main_word)
            print(f"[DEBUG] Варианты основного слова: {main_word_variants}")
            
            # Основное условие - первое слово должно быть в названии (с вариантами)
            main_conditions = []
            for variant in main_word_variants:
                main_conditions.append(func.lower(Product.name).contains(variant))
            main_condition = or_(*main_conditions)
            
            # Дополнительные условия для остальных слов
            other_conditions = []
            for word in other_words:
                # Создаем варианты слова с транслитерацией
                word_variants = transliterate_for_search(word.lower())
                print(f"[DEBUG] Варианты слова '{word}': {word_variants}")
                
                word_conditions = []
                for variant in word_variants:
                    word_conditions.append(or_(
                        func.lower(Product.name).contains(variant),
                        func.lower(Product.brand).contains(variant),
                        func.lower(Product.article).contains(variant),
                        func.lower(Product.part_number).contains(variant)
                    ))
                other_conditions.append(or_(*word_conditions))
            
            # Все дополнительные слова должны присутствовать (AND логика)
            if other_conditions:
                base_query = select(Product).where(
                    and_(
                        Product.is_active == True,
                        main_condition,
                        *other_conditions
                    )
                )
            else:
                base_query = select(Product).where(
                    and_(
                        Product.is_active == True,
                        main_condition
                    )
                )
        else:
            # Если одно слово или нет слов, используем старую логику
            print(f"[DEBUG] Однословный поиск: query='{query}'")
            conditions = []
            for word in query_words:
                # Создаем варианты слова с транслитерацией
                word_variants = transliterate_for_search(word.lower())
                print(f"[DEBUG] Варианты слова '{word}': {word_variants}")
                
                word_conditions = []
                for variant in word_variants:
                    word_conditions.append(or_(
                        func.lower(Product.name).contains(variant),
                        func.lower(Product.brand).contains(variant),
                        func.lower(Product.article).contains(variant),
                        func.lower(Product.part_number).contains(variant)
                    ))
                conditions.append(or_(*word_conditions))
            
            if conditions:
                base_query = select(Product).where(
                    and_(
                        Product.is_active == True,
                        or_(*conditions)
                    )
                )
            else:
                # Если нет слов, ищем по полному запросу с транслитерацией
                query_variants = transliterate_for_search(query)
                print(f"[DEBUG] Варианты полного запроса: {query_variants}")
                
                query_conditions = []
                for variant in query_variants:
                    query_conditions.append(or_(
                        func.lower(Product.name).contains(variant),
                        func.lower(Product.brand).contains(variant),
                        func.lower(Product.article).contains(variant),
                        func.lower(Product.part_number).contains(variant)
                    ))
                
                base_query = select(Product).where(
                    and_(
                        Product.is_active == True,
                        or_(*query_conditions)
                    )
                )

        print(f"[DEBUG] Выполняем SQL запрос...")
        # Получаем все товары, соответствующие запросу
        result = await db.execute(base_query)
        all_products = result.scalars().all()
        print(f"[DEBUG] Найдено товаров в БД: {len(all_products)}")

        # Группируем по партномерам
        part_number_groups = {}
        for product in all_products:
            part_num = clean_part_number(product.part_number) if product.part_number else ""
            if part_num:
                if part_num not in part_number_groups:
                    part_number_groups[part_num] = []
                part_number_groups[part_num].append(product)

        print(f"[DEBUG] Сгруппировано по партномерам: {len(part_number_groups)} групп")

        # Создаем результат с группировкой
        grouped_results = []
        for part_num, products in part_number_groups.items():
            if not products:
                continue

            try:
                # Берем первый продукт как основной
                main_product = products[0]

                # Собираем информацию о всех дистрибьюторах для этого партномера
                distributors_info = []
                total_stock = 0
                min_price_rub = float('inf')
                min_price_usd = float('inf')
                
                for product in products:
                    # Подсчитываем общий остаток
                    try:
                        stock_val = 0
                        if product.stock:
                            # Убираем пробелы и приводим к строке
                            stock_str = str(product.stock).strip()
                            # Извлекаем только цифры
                            stock_digits = ''.join(filter(str.isdigit, stock_str))
                            if stock_digits:
                                stock_val = int(stock_digits)
                        total_stock += stock_val
                    except Exception as e:
                        print(f"Ошибка парсинга stock '{product.stock}': {e}")
                        pass

                    # Находим минимальные цены
                    if product.price_rub and product.price_rub > 0:
                        min_price_rub = min(min_price_rub, product.price_rub)
                    if product.price_usd and product.price_usd > 0:
                        min_price_usd = min(min_price_usd, product.price_usd)

                    # Добавляем информацию о дистрибьюторе
                    distributors_info.append({
                        "distributor": product.distributor,
                        "name": product.name,
                        "price_rub": product.price_rub,
                        "price_usd": product.price_usd,
                        "stock": product.stock,
                        "brand": product.brand
                    })

                # Если не нашли цены, используем значения из основного продукта
                if min_price_rub == float('inf'):
                    min_price_rub = main_product.price_rub or 0
                if min_price_usd == float('inf'):
                    min_price_usd = main_product.price_usd or 0

                grouped_results.append({
                    "part_number": part_num,
                    "name": main_product.name,
                    "brand": main_product.brand,
                    "min_price_rub": min_price_rub,
                    "min_price_usd": min_price_usd,
                    "total_stock": total_stock,
                    "distributors_count": len(distributors_info),
                    "distributors": distributors_info
                })
            except Exception as e:
                print(f"[WARNING] Ошибка обработки товара {part_num}: {e}")
                continue

        print(f"[DEBUG] Обработано товаров: {len(grouped_results)}")
        
        # Проверяем, есть ли результаты
        if not grouped_results:
            print("[DEBUG] Нет результатов для возврата")
            return {
                "query": query,
                "total_count": 0,
                "results": [],
                "sort_by": sort_by,
                "sort_order": sort_order
            }

        # Сортировка
        reverse_order = sort_order.lower() == "desc"
        print(f"[DEBUG] Сортировка: {sort_by}, порядок: {sort_order}, reverse: {reverse_order}")
        
        # Функция для парсинга stock с поддержкой Treolan
        def parse_stock_for_sorting(stock_val):
            try:
                if stock_val is None:
                    return 0
                if isinstance(stock_val, (int, float)):
                    return int(stock_val)
                s = str(stock_val).strip().lower().replace(',', '.')
                if s == 'много':
                    return 99
                if s == '' or s == '0':
                    return 0
                if '+' in s:
                    base = s.split('+', 1)[0]
                    try:
                        return int(float(base))
                    except:
                        pass
                if s.startswith('>'):
                    try:
                        return int(float(s[1:])) + 1
                    except:
                        return 0
                if s.startswith('<'):
                    try:
                        v = int(float(s[1:]))
                        return max(v - 1, 0)
                    except:
                        return 0
                digits = ''.join(ch for ch in s if (ch.isdigit() or ch == '.'))
                if digits:
                    try:
                        return int(float(digits))
                    except:
                        return 0
                return 0
            except:
                return 0
        
        if sort_by == "price_rub":
            grouped_results.sort(key=lambda x: x["min_price_rub"] or 0, reverse=reverse_order)
        elif sort_by == "price_usd":
            grouped_results.sort(key=lambda x: x["min_price_usd"] or 0, reverse=reverse_order)
        elif sort_by == "stock":
            # Для сортировки по наличию: desc = сначала большие остатки, asc = сначала маленькие остатки
            print(f"[DEBUG] Первые 5 товаров до сортировки по stock:")
            for i, item in enumerate(grouped_results[:5]):
                print(f"  {i}: {item['part_number']} - total_stock: {item['total_stock']} (type: {type(item['total_stock'])})")
            
            # Сортируем по total_stock (суммарное наличие по всем дистрибьюторам)
            # Но сначала нужно отсортировать офферы внутри каждого товара по наличию
            for item in grouped_results:
                if item.get('distributors') and len(item['distributors']) > 0:
                    # Сортируем дистрибьюторов по наличию (по убыванию для desc, по возрастанию для asc)
                    try:
                        item['distributors'].sort(
                            key=lambda x: parse_stock_for_sorting(x.get('stock')),
                            reverse=reverse_order
                        )
                    except (ValueError, TypeError) as e:
                        print(f"[WARNING] Ошибка сортировки дистрибьюторов для {item.get('part_number', 'unknown')}: {e}")
                        # Если не удалось отсортировать, оставляем как есть
                        pass
            
            # Теперь сортируем товары по наличию у первого (лучшего) дистрибьютора
            # Добавляем проверку на существование distributors
            def get_first_stock(item):
                try:
                    if item.get('distributors') and len(item['distributors']) > 0 and item['distributors'][0].get('stock') is not None:
                        return parse_stock_for_sorting(item['distributors'][0]['stock'])
                    return 0
                except (ValueError, TypeError, IndexError) as e:
                    print(f"[WARNING] Ошибка получения stock для {item.get('part_number', 'unknown')}: {e}")
                    return 0
            
            try:
                grouped_results.sort(key=get_first_stock, reverse=reverse_order)
            except Exception as e:
                print(f"[ERROR] Ошибка сортировки товаров по stock: {e}")
                # Если сортировка не удалась, оставляем как есть
                pass
            
            print(f"[DEBUG] Первые 5 товаров после сортировки по stock:")
            for i, item in enumerate(grouped_results[:5]):
                first_stock = get_first_stock(item)
                print(f"  {i}: {item['part_number']} - first_stock: {first_stock}")
        elif sort_by == "name":
            grouped_results.sort(key=lambda x: x["name"] or "", reverse=reverse_order)

        # Пагинация
        total_count = len(grouped_results)
        paginated_results = grouped_results[offset:offset + limit]

        print(f"[DEBUG] Итоговый результат: total_count={total_count}, offset={offset}, limit={limit}, возвращаем={len(paginated_results)}")

        response.headers["X-Total-Count"] = str(total_count)
        response.headers["X-Page-Size"] = str(limit)
        response.headers["X-Page-Offset"] = str(offset)

        return {
            "query": query,
            "total_count": total_count,
            "results": paginated_results,
            "sort_by": sort_by,
            "sort_order": sort_order
        }

    except Exception as e:
        print(f"[ERROR] Ошибка в search_by_name: {str(e)}")
        import traceback
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Ошибка поиска: {str(e)}") 