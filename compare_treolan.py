#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import asyncio
from sqlalchemy import select, and_
from core.db_engine import AsyncSessionLocal
from core.db_models import Product

async def compare_treolan_data():
    """
    Сравнивает данные Treolan из Excel с базой данных
    """
    print("=== СРАВНЕНИЕ ДАННЫХ TREOLAN ===\n")
    
    # 1. Загружаем данные из Excel
    try:
        excel_data = pd.read_excel('22.08.2025_catalog.xlsx')
        print(f"✅ Загружено {len(excel_data)} строк из Excel")
        print(f"Колонки Excel: {list(excel_data.columns)}")
        print()
    except Exception as e:
        print(f"❌ Ошибка загрузки Excel: {e}")
        return
    
    # 2. Получаем данные из базы
    try:
        async with AsyncSessionLocal() as session:
            query = select(Product).where(
                and_(Product.distributor == 'Treolan', Product.is_active == True)
            )
            result = await session.execute(query)
            db_products = result.scalars().all()
            
            print(f"✅ Загружено {len(db_products)} товаров Treolan из базы")
            print()
    except Exception as e:
        print(f"❌ Ошибка загрузки из базы: {e}")
        return
    
    # 3. Анализируем данные
    print("=== АНАЛИЗ ДАННЫХ ===")
    
    # Создаем словари для быстрого поиска
    excel_dict = {}
    db_dict = {}
    
    # Обрабатываем Excel данные
    for idx, row in excel_data.iterrows():
        part_number = str(row.get('part_number', '')).strip().upper()
        if part_number and part_number != 'nan':
            excel_dict[part_number] = {
                'name': str(row.get('name', '')),
                'brand': str(row.get('brand', '')),
                'stock': row.get('stock', 0),
                'price': row.get('price', 0)
            }
    
    # Обрабатываем данные из базы
    for product in db_products:
        part_number = str(product.part_number).strip().upper()
        if part_number:
            db_dict[part_number] = {
                'name': str(product.name or ''),
                'brand': str(product.brand or ''),
                'stock': product.stock or 0,
                'price_usd': product.price_usd or 0,
                'price_rub': product.price_rub or 0
            }
    
    print(f"Excel: {len(excel_dict)} уникальных партномеров")
    print(f"База: {len(db_dict)} уникальных партномеров")
    print()
    
    # 4. Сравниваем
    print("=== СРАВНЕНИЕ ===")
    
    # Находим общие партномера
    common_part_numbers = set(excel_dict.keys()) & set(db_dict.keys())
    excel_only = set(excel_dict.keys()) - set(db_dict.keys())
    db_only = set(db_dict.keys()) - set(excel_dict.keys())
    
    print(f"Общие партномера: {len(common_part_numbers)}")
    print(f"Только в Excel: {len(excel_only)}")
    print(f"Только в базе: {len(db_only)}")
    print()
    
    # 5. Детальное сравнение общих партномеров
    if common_part_numbers:
        print("=== ДЕТАЛЬНОЕ СРАВНЕНИЕ ОБЩИХ ПАРТНОМЕРОВ ===")
        
        differences = []
        for part_num in list(common_part_numbers)[:10]:  # Показываем первые 10
            excel_item = excel_dict[part_num]
            db_item = db_dict[part_num]
            
            # Сравниваем названия
            name_match = excel_item['name'].lower() == db_item['name'].lower()
            
            # Сравниваем бренды
            brand_match = excel_item['brand'].lower() == db_item['brand'].lower()
            
            # Сравниваем остатки
            stock_diff = abs(excel_item['stock'] - db_item['stock'])
            
            if not name_match or not brand_match or stock_diff > 0:
                differences.append({
                    'part_number': part_num,
                    'excel_name': excel_item['name'],
                    'db_name': db_item['name'],
                    'excel_brand': excel_item['brand'],
                    'db_brand': db_item['brand'],
                    'excel_stock': excel_item['stock'],
                    'db_stock': db_item['stock'],
                    'stock_diff': stock_diff
                })
        
        if differences:
            print("Найдены различия:")
            for diff in differences:
                print(f"\nПартномер: {diff['part_number']}")
                print(f"  Название: Excel='{diff['excel_name']}' vs База='{diff['db_name']}'")
                print(f"  Бренд: Excel='{diff['excel_brand']}' vs База='{diff['db_brand']}'")
                print(f"  Остаток: Excel={diff['excel_stock']} vs База={diff['db_stock']} (разница: {diff['stock_diff']})")
        else:
            print("Все общие партномера идентичны!")
    
    # 6. Примеры для тестирования транслитерации
    print("\n=== ПРИМЕРЫ ДЛЯ ТЕСТИРОВАНИЯ ТРАНСЛИТЕРАЦИИ ===")
    
    if common_part_numbers:
        sample_part = list(common_part_numbers)[0]
        excel_sample = excel_dict[sample_part]
        db_sample = db_dict[sample_part]
        
        print(f"Пример партномера: {sample_part}")
        print(f"Excel название: {excel_sample['name']}")
        print(f"База название: {db_sample['name']}")
        print(f"Excel бренд: {excel_sample['brand']}")
        print(f"База бренд: {db_sample['brand']}")
        
        # Проверяем, есть ли русские/английские буквы
        import re
        russian_chars = re.findall(r'[А-Яа-я]', excel_sample['name'])
        english_chars = re.findall(r'[A-Za-z]', excel_sample['name'])
        
        if russian_chars or english_chars:
            print(f"В названии найдены:")
            if russian_chars:
                print(f"  Русские буквы: {set(russian_chars)}")
            if english_chars:
                print(f"  Английские буквы: {set(english_chars)}")
        else:
            print("В названии нет букв для транслитерации")

if __name__ == "__main__":
    asyncio.run(compare_treolan_data()) 