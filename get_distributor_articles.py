#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для получения артикулов разных дистрибьюторов
"""

import asyncio
from core.db_engine import AsyncSessionLocal
from sqlalchemy import text

async def get_distributor_articles():
    """Получает артикулы разных дистрибьюторов"""
    
    async with AsyncSessionLocal() as session:
        # Получаем список дистрибьюторов
        print("=== ДИСТРИБЬЮТОРЫ В БАЗЕ ===")
        result = await session.execute(
            text("SELECT DISTINCT distributor FROM products WHERE distributor IS NOT NULL ORDER BY distributor")
        )
        distributors = [row[0] for row in result.fetchall()]
        
        for distributor in distributors:
            print(f"\n--- {distributor} ---")
            
            # Получаем 5 случайных артикулов для каждого дистрибьютора
            result = await session.execute(
                text("""
                    SELECT part_number, name, price_rub, stock 
                    FROM products 
                    WHERE distributor = :dist AND part_number IS NOT NULL AND part_number != ''
                    ORDER BY RANDOM() 
                    LIMIT 5
                """),
                {"dist": distributor}
            )
            
            articles = result.fetchall()
            if articles:
                for i, (part_number, name, price_rub, stock) in enumerate(articles, 1):
                    print(f"{i}. {part_number} | {name[:60]}... | {price_rub} RUB | {stock} шт")
            else:
                print("Нет артикулов")

async def get_total_counts():
    """Получает общую статистику по дистрибьюторам"""
    
    async with AsyncSessionLocal() as session:
        print("\n=== ОБЩАЯ СТАТИСТИКА ===")
        
        result = await session.execute(
            text("""
                SELECT 
                    distributor,
                    COUNT(*) as total_products,
                    COUNT(CASE WHEN part_number IS NOT NULL AND part_number != '' THEN 1 END) as with_articles,
                    COUNT(CASE WHEN price_rub > 0 THEN 1 END) as with_prices,
                    COUNT(CASE WHEN stock > 0 THEN 1 END) as with_stock
                FROM products 
                WHERE distributor IS NOT NULL
                GROUP BY distributor 
                ORDER BY total_products DESC
            """)
        )
        
        rows = result.fetchall()
        for distributor, total, articles, prices, stock in rows:
            print(f"{distributor}: {total} товаров, {articles} с артикулами, {prices} с ценами, {stock} с остатками")

if __name__ == "__main__":
    print("Получаем артикулы дистрибьюторов...")
    asyncio.run(get_distributor_articles())
    asyncio.run(get_total_counts())
