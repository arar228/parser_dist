#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
from sqlalchemy import select, and_, or_
from core.db_engine import AsyncSessionLocal
from core.db_models import Product

async def check_petc1_m01():
    """
    Проверяет товар PETC1-M01 в базе данных
    """
    part_number = "PETC1-M01"
    
    print(f"=== ПРОВЕРКА ТОВАРА {part_number} В БАЗЕ ДАННЫХ ===\n")
    
    try:
        async with AsyncSessionLocal() as session:
            # Ищем все записи с этим партномером
            query = select(Product).where(
                and_(
                    Product.is_active == True,
                    or_(
                        Product.part_number == part_number,
                        Product.part_number == part_number.upper(),
                        Product.part_number == part_number.lower()
                    )
                )
            )
            result = await session.execute(query)
            products = result.scalars().all()
            
            if not products:
                print(f"❌ Товар {part_number} НЕ НАЙДЕН в базе!")
                print("\n🔍 Возможные причины:")
                print("   1. Товар не был импортирован")
                print("   2. Товар деактивирован")
                print("   3. Ошибка в экспорте Marvel")
                print("   4. Партномер записан по-другому")
                return
            
            print(f"✅ Найдено {len(products)} записей для {part_number}:\n")
            
            for i, product in enumerate(products, 1):
                print(f"--- Запись {i} ---")
                print(f"ID: {product.id}")
                print(f"Партномер: {product.part_number}")
                print(f"Название: {product.name}")
                print(f"Бренд: {product.brand}")
                print(f"Дистрибьютор: {product.distributor}")
                print(f"Остаток: {product.stock}")
                print(f"Цена USD: {product.price_usd}")
                print(f"Цена RUB: {product.price_rub}")
                print(f"Активен: {product.is_active}")
                print(f"Дата обновления: {product.updated_at}")
                print()
            
            # Группируем по дистрибьюторам
            distributors = {}
            for product in products:
                dist = product.distributor
                if dist not in distributors:
                    distributors[dist] = []
                distributors[dist].append(product)
            
            print("=== ГРУППИРОВКА ПО ДИСТРИБЬЮТОРАМ ===")
            for dist, prods in distributors.items():
                total_stock = sum(p.stock or 0 for p in prods)
                min_price_usd = min((p.price_usd or float('inf')) for p in prods)
                min_price_rub = min((p.price_rub or float('inf')) for p in prods)
                
                print(f"\n{dist}:")
                print(f"  Количество записей: {len(prods)}")
                print(f"  Общий остаток: {total_stock}")
                print(f"  Мин. цена USD: {min_price_usd if min_price_usd != float('inf') else 'N/A'}")
                print(f"  Мин. цена RUB: {min_price_rub if min_price_rub != float('inf') else 'N/A'}")
                
                # Показываем детали по каждой записи
                for p in prods:
                    print(f"    - {p.part_number}: остаток {p.stock}, цена {p.price_usd} USD")
            
            # Проверяем, есть ли Marvel
            marvel_products = [p for p in products if p.distributor == 'Marvel']
            if not marvel_products:
                print(f"\n❌ Товар {part_number} НЕ НАЙДЕН у Marvel!")
                print("Возможные причины:")
                print("1. Marvel не обновлялся")
                print("2. Ошибка в экспорте Marvel")
                print("3. Товар не импортировался")
            else:
                print(f"\n✅ Товар {part_number} найден у Marvel!")
                
                # Проверяем остатки Marvel
                marvel_stock = sum(p.stock or 0 for p in marvel_products)
                if marvel_stock == 0:
                    print(f"   ⚠️  НО остаток Marvel: {marvel_stock} (должно быть 10)")
                    print("   🔍 Возможные причины:")
                    print("      1. Marvel не обновлялся")
                    print("      2. Ошибка в API Marvel")
                    print("      3. Товар закончился у Marvel")
                    print("      4. Проблема с парсингом остатков")
                else:
                    print(f"   ✅ Остаток Marvel: {marvel_stock}")
            
            # Проверяем все дистрибьюторы
            print(f"\n=== ПРОВЕРКА ВСЕХ ДИСТРИБЬЮТОРОВ ===")
            all_distributors = ['Merlion', 'Netlab', 'Treolan', 'VVP', 'Marvel', 'OCS']
            
            for dist in all_distributors:
                dist_products = [p for p in products if p.distributor == dist]
                if dist_products:
                    total_stock = sum(p.stock or 0 for p in dist_products)
                    print(f"   {dist}: {total_stock} шт")
                else:
                    print(f"   {dist}: НЕТ в базе")
                
    except Exception as e:
        print(f"❌ Ошибка при проверке: {e}")

if __name__ == "__main__":
    asyncio.run(check_petc1_m01()) 