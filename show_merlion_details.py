#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Детальный просмотр 10 товаров Merlion со всеми характеристиками
"""

import asyncio
from core.db_engine import AsyncSessionLocal
from core.db_models import Product
from sqlalchemy import select
from sqlalchemy import func

async def show_merlion_details():
    """Показывает детальную информацию о 10 товарах Merlion"""
    print("=== ДЕТАЛЬНЫЙ ПРОСМОТР 10 ТОВАРОВ MERLION ===\n")
    
    async with AsyncSessionLocal() as session:
        # Получаем 10 товаров Merlion
        result = await session.execute(
            select(Product).where(Product.distributor == 'Merlion').limit(10)
        )
        products = result.scalars().all()
        
        for i, product in enumerate(products, 1):
            print(f"🔍 ТОВАР #{i}")
            print(f"   ID: {product.id}")
            print(f"   Артикул: {product.article}")
            print(f"   Партномер: {product.part_number}")
            print(f"   Название: {product.name}")
            print(f"   Бренд: {product.brand}")
            print(f"   Категория: {product.category_code}")
            print(f"   Остаток: {product.stock} шт")
            print(f"   Цена RUB: {product.price_rub}")
            print(f"   Цена USD: {product.price_usd}")
            print(f"   Объем упаковки: {product.package_volume} м³")
            print(f"   Вес упаковки: {product.package_weight} кг")
            print(f"   Тех. характеристики: {product.tech_specs}")
            print(f"   Дата транзита: {product.transit_date}")
            print(f"   Дистрибьютор: {product.distributor}")
            print(f"   Активен: {product.is_active}")
            print("-" * 80)
        
        # Общая статистика по ценам
        print(f"\n📊 СТАТИСТИКА ПО ЦЕНАМ:")
        
        result = await session.execute(
            select(func.count(Product.id)).where(
                Product.distributor == 'Merlion',
                Product.price_rub > 0
            )
        )
        with_price_rub = result.scalar()
        
        result = await session.execute(
            select(func.count(Product.id)).where(
                Product.distributor == 'Merlion',
                Product.price_usd > 0
            )
        )
        with_price_usd = result.scalar()
        
        result = await session.execute(
            select(func.count(Product.id)).where(
                Product.distributor == 'Merlion',
                Product.price_rub.is_(None)
            )
        )
        without_price_rub = result.scalar()
        
        print(f"   Товаров с ценой RUB: {with_price_rub}")
        print(f"   Товаров с ценой USD: {with_price_usd}")
        print(f"   Товаров без цены RUB: {without_price_rub}")
        
        # Примеры товаров с ценами
        if with_price_rub > 0:
            print(f"\n💰 ПРИМЕРЫ ТОВАРОВ С ЦЕНАМИ:")
            result = await session.execute(
                select(Product).where(
                    Product.distributor == 'Merlion',
                    Product.price_rub > 0
                ).limit(3)
            )
            priced_products = result.scalars().all()
            
            for i, product in enumerate(priced_products, 1):
                print(f"   {i}. {product.part_number} - {product.name[:50]}...")
                print(f"      Цена: {product.price_rub} RUB / {product.price_usd} USD")
                print(f"      Остаток: {product.stock} шт")

if __name__ == "__main__":
    asyncio.run(show_merlion_details()) 