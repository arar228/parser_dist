#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Добавление индексов для MySQL
"""

import asyncio
from core.db_engine import AsyncSessionLocal
from sqlalchemy import text

async def add_mysql_indexes():
    """Добавляет необходимые индексы для MySQL"""
    
    print("🔧 Добавляем индексы для MySQL...")
    
    try:
        async with AsyncSessionLocal() as db:
            # Проверяем существующие индексы
            result = await db.execute(text("SHOW INDEX FROM products"))
            existing_indexes = [row[2] for row in result.fetchall()]
            print(f"📋 Существующие индексы: {existing_indexes}")
            
            # Добавляем уникальный индекс для (part_number, distributor)
            if 'idx_products_part_distributor' not in existing_indexes:
                print("📊 Добавляем уникальный индекс для (part_number, distributor)...")
                await db.execute(text("""
                    CREATE UNIQUE INDEX idx_products_part_distributor 
                    ON products (part_number, distributor)
                """))
                print("✅ Уникальный индекс добавлен")
            else:
                print("✅ Уникальный индекс уже существует")
            
            # Добавляем обычные индексы для производительности
            print("📊 Добавляем индексы для производительности...")
            
            # Индекс по дистрибьютору
            if 'idx_products_distributor' not in existing_indexes:
                await db.execute(text("""
                    CREATE INDEX idx_products_distributor 
                    ON products (distributor)
                """))
                print("✅ Индекс по дистрибьютору добавлен")
            
            # Индекс по артикулу
            if 'idx_products_article' not in existing_indexes:
                await db.execute(text("""
                    CREATE INDEX idx_products_article 
                    ON products (article)
                """))
                print("✅ Индекс по артикулу добавлен")
            
            # Индекс по бренду
            if 'idx_products_brand' not in existing_indexes:
                await db.execute(text("""
                    CREATE INDEX idx_products_brand 
                    ON products (brand)
                """))
                print("✅ Индекс по бренду добавлен")
            
            # Индекс по активности
            if 'idx_products_is_active' not in existing_indexes:
                await db.execute(text("""
                    CREATE INDEX idx_products_is_active 
                    ON products (is_active)
                """))
                print("✅ Индекс по активности добавлен")
            
            await db.commit()
            print("✅ Все индексы добавлены")
            
            # Проверяем индексы
            result = await db.execute(text("SHOW INDEX FROM products"))
            indexes = result.fetchall()
            print(f"📋 Найдено индексов: {len(indexes)}")
            for idx in indexes:
                print(f"  - {idx[2]}: {idx[4]} ({idx[10]})")
            
            return True
            
    except Exception as e:
        print(f"❌ Ошибка при добавлении индексов: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    print("🚀 Добавление индексов MySQL")
    print("=" * 50)
    
    success = await add_mysql_indexes()
    
    if success:
        print("\n🎉 Индексы добавлены успешно!")
        print("💡 Теперь VVP и другие дистрибьюторы смогут работать с MySQL")
    else:
        print("\n❌ Ошибка при добавлении индексов")

if __name__ == "__main__":
    asyncio.run(main())
