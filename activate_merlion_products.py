#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Активация всех товаров Merlion в базе данных
"""

import asyncio
from core.db_engine import AsyncSessionLocal
from core.db_models import Product
from sqlalchemy import text

async def activate_merlion_products():
    """Активирует все товары Merlion"""
    print("=== АКТИВАЦИЯ ТОВАРОВ MERLION ===")
    
    async with AsyncSessionLocal() as session:
        try:
            # Активируем все товары Merlion
            print("🔧 Активируем все товары Merlion...")
            
            result = await session.execute(
                text("UPDATE products SET is_active = TRUE WHERE distributor = 'Merlion'")
            )
            activated_count = result.rowcount
            await session.commit()
            
            print(f"✅ Активировано товаров: {activated_count}")
            
            # Проверяем результат
            result = await session.execute(
                text("SELECT COUNT(*) FROM products WHERE distributor = 'Merlion' AND is_active = TRUE")
            )
            active_count = result.scalar()
            
            result = await session.execute(
                text("SELECT COUNT(*) FROM products WHERE distributor = 'Merlion' AND is_active = FALSE")
            )
            inactive_count = result.scalar()
            
            print(f"\n📊 Результат активации:")
            print(f"   Активных товаров: {active_count}")
            print(f"   Неактивных товаров: {inactive_count}")
            
            if active_count > 0:
                print(f"\n🎉 Активация завершена успешно!")
                print(f"   Теперь у вас {active_count} активных товаров Merlion")
            else:
                print(f"\n❌ Ошибка: товары не были активированы")
                
        except Exception as e:
            print(f"❌ Ошибка при активации: {e}")
            await session.rollback()

if __name__ == "__main__":
    asyncio.run(activate_merlion_products()) 