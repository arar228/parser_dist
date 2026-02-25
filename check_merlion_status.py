#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Проверка статуса товаров Merlion в базе данных
"""

import asyncio
from core.db_engine import AsyncSessionLocal
from core.db_models import Product
from sqlalchemy import select, func

async def check_merlion_status():
    """Проверяет статус товаров Merlion"""
    print("=== ПРОВЕРКА СТАТУСА MERLION ===")
    
    async with AsyncSessionLocal() as session:
        # Общее количество товаров Merlion
        result = await session.execute(
            select(func.count(Product.id)).where(Product.distributor == 'Merlion')
        )
        total_count = result.scalar()
        
        # Количество активных товаров
        result = await session.execute(
            select(func.count(Product.id)).where(
                Product.distributor == 'Merlion',
                Product.is_active == True
            )
        )
        active_count = result.scalar()
        
        # Количество неактивных товаров
        result = await session.execute(
            select(func.count(Product.id)).where(
                Product.distributor == 'Merlion',
                Product.is_active == False
            )
        )
        inactive_count = result.scalar()
        
        print(f"📊 Общая статистика:")
        print(f"   Всего товаров Merlion: {total_count}")
        print(f"   Активных товаров: {active_count}")
        print(f"   Неактивных товаров: {inactive_count}")
        
        if total_count > 0:
            # Показываем несколько примеров
            print(f"\n🔍 Примеры товаров:")
            result = await session.execute(
                select(Product).where(Product.distributor == 'Merlion').limit(3)
            )
            products = result.scalars().all()
            
            for i, product in enumerate(products, 1):
                print(f"   {i}. ID: {product.id}")
                print(f"      Название: {product.name[:50]}...")
                print(f"      Бренд: {product.brand}")
                print(f"      Остаток: {product.stock}")
                print(f"      Цена: {product.price_rub} RUB")
                print(f"      Активен: {product.is_active}")
                print()
        
        # Проверяем товары с остатками
        result = await session.execute(
            select(func.count(Product.id)).where(
                Product.distributor == 'Merlion',
                Product.stock > 0
            )
        )
        with_stock = result.scalar()
        
        # Проверяем товары с ценами
        result = await session.execute(
            select(func.count(Product.id)).where(
                Product.distributor == 'Merlion',
                Product.price_rub > 0
            )
        )
        with_price = result.scalar()
        
        print(f"📦 Дополнительная информация:")
        print(f"   Товаров с остатками: {with_stock}")
        print(f"   Товаров с ценами: {with_price}")

if __name__ == "__main__":
    asyncio.run(check_merlion_status()) 