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
    
    # 1. Загружаем данные из Excel с правильными параметрами
    try:
        # Пробуем разные варианты чтения Excel
        excel_data = pd.read_excel('22.08.2025_catalog.xlsx', header=None)
        print(f"✅ Загружено {len(excel_data)} строк из Excel")
        print(f"Колонки Excel (индексы): {list(excel_data.columns)}")
        print(f"Первые 5 строк:")
        print(excel_data.head())
        print()
        
        # Показываем структуру данных
        print("=== СТРУКТУРА EXCEL ===")
        for i in range(min(10, len(excel_data))):
            row = excel_data.iloc[i]
            print(f"Строка {i}: {list(row)}")
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
    
    # Обрабатываем Excel данные - пробуем разные колонки
    print("Пробуем найти партномера в Excel...")
    
    # Ищем колонку с партномерами (обычно это первая колонка)
    for idx, row in excel_data.iterrows():
        # Пробуем разные колонки для партномера
        for col_idx in [0, 1, 2]:  # Первые 3 колонки
            if col_idx < len(row):
                value = str(row.iloc[col_idx]).strip()
                if value and value != 'nan' and len(value) > 3:  # Партномер должен быть длиннее 3 символов
                    # Проверяем, похоже ли это на партномер
                    if any(char.isalnum() for char in value):
                        excel_dict[value.upper()] = {
                            'row': idx,
                            'col': col_idx,
                            'value': value
                        }
                        break  # Нашли партномер в этой строке
    
    print(f"Найдено потенциальных партномеров в Excel: {len(excel_dict)}")
    if excel_dict:
        print("Примеры партномеров из Excel:")
        for i, (part_num, info) in enumerate(list(excel_dict.items())[:5]):
            print(f"  {i+1}. {part_num} (строка {info['row']}, колонка {info['col']})")
    print()
    
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
    
    print(f"Excel: {len(excel_dict)} потенциальных партномеров")
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
    
    # 5. Показываем примеры
    if common_part_numbers:
        print("=== ПРИМЕРЫ ОБЩИХ ПАРТНОМЕРОВ ===")
        for part_num in list(common_part_numbers)[:5]:
            excel_info = excel_dict[part_num]
            db_info = db_dict[part_num]
            print(f"\nПартномер: {part_num}")
            print(f"  Excel: строка {excel_info['row']}, колонка {excel_info['col']}")
            print(f"  База: {db_info['name']} | {db_info['brand']} | Остаток: {db_info['stock']}")
    
    if excel_only:
        print(f"\n=== ПРИМЕРЫ ТОЛЬКО В EXCEL ===")
        for part_num in list(excel_only)[:5]:
            excel_info = excel_dict[part_num]
            print(f"  {part_num} (строка {excel_info['row']}, колонка {excel_info['col']})")
    
    if db_only:
        print(f"\n=== ПРИМЕРЫ ТОЛЬКО В БАЗЕ ===")
        for part_num in list(db_only)[:5]:
            db_info = db_dict[part_num]
            print(f"  {part_num}: {db_info['name']} | {db_info['brand']}")

if __name__ == "__main__":
    asyncio.run(compare_treolan_data()) 