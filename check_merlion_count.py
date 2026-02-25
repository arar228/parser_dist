#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Проверка количества товаров Merlion в базе данных
"""

import asyncio
from sqlalchemy import func
from core.db_engine import AsyncSessionLocal
from core.db_models import Product

async def check_merlion_count():
    """Проверяет количество товаров Merlion в базе"""
    try:
        async with AsyncSessionLocal() as session:
            # Подсчитываем товары Merlion
            result = await session.execute(
                func.count(Product.id).select().where(Product.distributor == 'Merlion')
            )
            merlion_count = result.scalar()
            
            print(f"=== СТАТИСТИКА MERLION ===")
            print(f"Товаров Merlion в базе: {merlion_count}")
            
            if merlion_count > 0:
                # Показываем примеры товаров
                products = await session.execute(
                    Product.__table__.select().where(Product.distributor == 'Merlion').limit(5)
                )
                rows = products.fetchall()
                
                print(f"\nПримеры товаров Merlion:")
                for i, row in enumerate(rows, 1):
                    print(f"{i}. ID: {row.id}, Партномер: {row.part_number}, Название: {row.name[:50]}...")
            else:
                print("❌ Товаров Merlion в базе НЕТ!")
                
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_merlion_count()) 