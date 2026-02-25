#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для миграции данных с PostgreSQL на MySQL
"""

import asyncio
import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text
from core.db_models import Product
from core.db_engine import AsyncSessionLocal

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def migrate_data():
    """Мигрирует данные с PostgreSQL на MySQL"""
    
    # Старая PostgreSQL конфигурация
    OLD_DB_URL = "postgresql+asyncpg://mysite_user:12345@localhost:5432/myproject"
    
    try:
        # Подключаемся к старой базе PostgreSQL
        logger.info("🔗 Подключаемся к старой базе PostgreSQL...")
        old_engine = create_async_engine(OLD_DB_URL, echo=False)
        
        # Подключаемся к новой базе MySQL
        logger.info("🔗 Подключаемся к новой базе MySQL...")
        
        async with old_engine.begin() as old_conn:
            # Получаем все данные из старой базы
            logger.info("📥 Читаем данные из PostgreSQL...")
            result = await old_conn.execute(text("SELECT * FROM products"))
            products = result.fetchall()
            
            logger.info(f"📊 Найдено {len(products)} записей в PostgreSQL")
            
            if not products:
                logger.info("ℹ️ Нет данных для миграции")
                return
            
            # Показываем пример данных
            if products:
                first_product = products[0]
                logger.info(f"📝 Пример записи: {dict(first_product._mapping)}")
        
        # Сохраняем в новую базу MySQL
        logger.info("💾 Сохраняем данные в MySQL...")
        
        async with AsyncSessionLocal() as new_db:
            # Очищаем новую базу
            logger.info("🧹 Очищаем новую базу...")
            await new_db.execute(text("DELETE FROM products"))
            await new_db.commit()
            
            # Конвертируем данные
            migrated_count = 0
            for product_row in products:
                try:
                    product_data = dict(product_row._mapping)
                    
                    # Создаем новый объект Product
                    new_product = Product(
                        article=product_data.get('article'),
                        name=product_data.get('name'),
                        brand=product_data.get('brand'),
                        part_number=product_data.get('part_number'),
                        category_code=product_data.get('category_code'),
                        price_rub=product_data.get('price_rub'),
                        price_usd=product_data.get('price_usd'),
                        stock=product_data.get('stock'),
                        package_volume=product_data.get('package_volume'),
                        package_weight=product_data.get('package_weight'),
                        tech_specs=product_data.get('tech_specs'),
                        transit_date=product_data.get('transit_date'),
                        distributor=product_data.get('distributor'),
                        is_active=product_data.get('is_active', True)
                    )
                    
                    new_db.add(new_product)
                    migrated_count += 1
                    
                    if migrated_count % 1000 == 0:
                        logger.info(f"📈 Мигрировано {migrated_count} записей...")
                        await new_db.commit()
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка при миграции записи {product_data.get('id', 'N/A')}: {e}")
                    continue
            
            # Финальный коммит
            await new_db.commit()
            logger.info(f"✅ Миграция завершена! Перенесено {migrated_count} записей")
            
            # Проверяем результат
            result = await new_db.execute(text("SELECT COUNT(*) FROM products"))
            count = result.scalar()
            logger.info(f"📊 Итоговое количество записей в MySQL: {count}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка миграции: {e}")
        raise

async def main():
    """Основная функция миграции"""
    logger.info("🚀 Начинаем миграцию данных с PostgreSQL на MySQL...")
    logger.info("=" * 60)
    
    try:
        await migrate_data()
        logger.info("🎉 Миграция завершена успешно!")
        logger.info("💡 Теперь можно использовать MySQL для всех операций")
        
    except Exception as e:
        logger.error(f"💥 Критическая ошибка миграции: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
