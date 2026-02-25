#!/usr/bin/env python3
"""
Модуль для upsert операций с товарами
"""

import logging
import os
from sqlalchemy import insert, text, bindparam
from core.db_models import Product

# ОТКЛЮЧАЕМ ТОЛЬКО SQL ПАРАМЕТРЫ, НО ОСТАВЛЯЕМ ЛОГИРОВАНИЕ ОШИБОК
# Отключаем детальные SQL логи (но оставляем ошибки)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.dialects').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.orm').setLevel(logging.WARNING)

# Отключаем PostgreSQL драйверы (но оставляем ошибки)
logging.getLogger('asyncpg').setLevel(logging.WARNING)
logging.getLogger('postgresql').setLevel(logging.WARNING)
logging.getLogger('psycopg2').setLevel(logging.WARNING)

# Отключаем распространение для всех логгеров
for logger_name in ['sqlalchemy', 'sqlalchemy.engine', 'sqlalchemy.pool', 'sqlalchemy.dialects', 'sqlalchemy.orm', 'asyncpg', 'postgresql', 'psycopg2']:
    logger = logging.getLogger(logger_name)
    logger.propagate = False
    logger.handlers = []

# Устанавливаем переменные окружения для отключения asyncpg параметров
os.environ['ASYNC_PG_QUIET'] = '1'
os.environ['PGQUIET'] = '1'
os.environ['ASYNC_PG_QUIET_PARAMS'] = '1'

# Отключаем все HTTP логи
logging.getLogger('httpx').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('asyncio').setLevel(logging.CRITICAL)

# Отключаем все остальные логгеры
logging.getLogger('').setLevel(logging.INFO)


def send_alert(distributor, message):
    logging.error(f"[ALERT][{distributor}] {message}")

async def upsert_products(products, distributor_name, db):
    # Логирование уже отключено глобально в начале файла
    
    distributor_key = (distributor_name or '').lower().replace(' ', '')
    distributor_fixed = {
        'merlion': 'Merlion',
        'resursmedia': 'ResursMedia',
        'treolan': 'Treolan',
        'ocs': 'Ocs',
        'marvel': 'Marvel',
        'vvp': 'VVP',
        'netlab': 'Netlab',
    }.get(distributor_key, (distributor_name or '').strip().capitalize())
    
    print(f"[{distributor_fixed}][DEBUG] Начинаем upsert для {len(products)} товаров...")
    logging.info(f"[{distributor_fixed}][DEBUG] Начинаем upsert для {len(products)} товаров...")
    
    # Удаляем старые товары этого дистрибьютора перед записью новых
    # ВАЖНО: Полная замена данных - удаляем старые, вставляем новые
    print(f"[{distributor_fixed}] Удаляем старые товары {distributor_fixed}...")
    logging.info(f"[{distributor_fixed}] Удаляем старые товары {distributor_fixed}...")
    
    deleted_count = 0
    try:
        result = await db.execute(text("DELETE FROM products WHERE distributor = :distributor"), {"distributor": distributor_fixed})
        deleted_count = result.rowcount
        # Commit будет в конце функции
        print(f"[{distributor_fixed}] Удалено {deleted_count} старых товаров {distributor_fixed}")
        logging.info(f"[{distributor_fixed}] Удалено {deleted_count} старых товаров {distributor_fixed}")
    except Exception as cleanup_error:
        print(f"[{distributor_fixed}][ERROR] Ошибка при удалении: {cleanup_error}")
        logging.error(f"[{distributor_fixed}][ERROR] Ошибка при удалении: {cleanup_error}")
    
    # Логируем первые несколько товаров для проверки
    print(f"[{distributor_fixed}][DEBUG] Первые 3 товара для upsert:")
    for i, product in enumerate(products[:3]):
        print(f"  {i+1}. {product.get('part_number', 'N/A')} - {product.get('name', 'N/A')[:50]}...")
    
    try:
        logging.info(f"[{distributor_fixed}][START] Upsert товаров...")
        if not products:
            logging.warning(f"[{distributor_fixed}] Новых данных нет, старая база не тронута!")
            return {"inserted": 0, "updated": 0, "deleted": 0}
        
        new_keys = set((p.get("article"), p.get("part_number"), distributor_fixed) for p in products)
        
        # Получаем список допустимых полей из модели Product
        allowed_fields = set(c.name for c in Product.__table__.columns)
        
        # Подготавливаем данные для batch insert
        batch_data = []
        for p in products:
            # Создаем полную структуру данных со всеми обязательными полями
            data = {
                'article': p.get('article'),
                'name': p.get('name'),
                'brand': p.get('brand'),
                'part_number': p.get('part_number'),
                'category_code': p.get('category_code'),
                'price_rub': p.get('price_rub', 0) if p.get('price_rub') not in ['', None] else 0,
                'price_usd': p.get('price_usd', 0) if p.get('price_usd') not in ['', None] else 0,
                'stock': p.get('stock', 0) if p.get('stock') not in ['', None] else 0,
                'package_volume': p.get('package_volume'),
                'package_weight': p.get('package_weight'),
                'tech_specs': p.get('tech_specs'),
                'transit_date': p.get('transit_date'),
                'distributor': distributor_fixed,
                'is_active': True
            }
            batch_data.append(data)
        
        # Детальное логирование для диагностики
        if batch_data:
            logging.info(f"[{distributor_fixed}][DEBUG] Подготовлено {len(batch_data)} товаров для вставки")
            logging.info(f"[{distributor_fixed}][DEBUG] Пример подготовленного товара: {batch_data[0]}")
            logging.info(f"[{distributor_fixed}][DEBUG] Ключи подготовленного товара: {list(batch_data[0].keys())}")
        else:
            logging.warning(f"[{distributor_fixed}][DEBUG] Нет данных для вставки после фильтрации!")
            logging.warning(f"[{distributor_fixed}][DEBUG] Исходные ключи: {list(products[0].keys()) if products else 'Нет товаров'}")
            logging.warning(f"[{distributor_fixed}][DEBUG] Допустимые поля: {allowed_fields}")
        
        if batch_data:
            # Инициализируем счетчики
            inserted = 0
            updated = 0
            
            # Используем raw SQL для правильной установки created_at и updated_at
            try:
                # Подготавливаем SQL запрос с правильной обработкой NULL значений
                stmt = text("""
                    INSERT INTO products (
                        article, name, brand, part_number, category_code, price_rub, price_usd, stock,
                        package_volume, package_weight, tech_specs, transit_date, distributor, is_active,
                        created_at, updated_at
                    )
                    VALUES (
                        :article, :name, :brand, :part_number, :category_code, :price_rub, :price_usd, :stock,
                        :package_volume, :package_weight, :tech_specs, :transit_date, :distributor, :is_active,
                        NOW(), NOW()
                    )
                    ON DUPLICATE KEY UPDATE
                        article = VALUES(article),
                        name = VALUES(name),
                        brand = VALUES(brand),
                        category_code = VALUES(category_code),
                        price_rub = VALUES(price_rub),
                        price_usd = VALUES(price_usd),
                        stock = VALUES(stock),
                        package_volume = VALUES(package_volume),
                        package_weight = VALUES(package_weight),
                        tech_specs = VALUES(tech_specs),
                        transit_date = VALUES(transit_date),
                        is_active = VALUES(is_active),
                        updated_at = NOW()
                """)
                
                # Вставляем товары по одному
                for product_data in batch_data:
                    await db.execute(stmt, product_data)
                # Commit будет в конце функции
                
                # Если вставка прошла успешно
                inserted = len(batch_data)
                updated = 0
                logging.info(f"[{distributor_fixed}] Успешно вставлено {inserted} товаров")
                    
            except Exception as insert_error:
                # Если вставка не удалась, значит есть дубли - обновляем существующие товары
                logging.info(f"[{distributor_fixed}] Вставка не удалась, обновляем существующие товары: {insert_error}")
                print(f"[{distributor_fixed}][DEBUG] Ошибка вставки: {insert_error}")
                await db.rollback()
                
                # Обновляем существующие товары по одному
                inserted = 0
                updated = 0
                
                for product_data in batch_data:
                    try:
                        # Проверяем, существует ли товар
                        existing = await db.execute(
                            text("SELECT id FROM products WHERE part_number = :part_number AND distributor = :distributor"),
                            {"part_number": product_data['part_number'], "distributor": distributor_fixed}
                        )
                        
                        if existing.scalar():
                            # Товар существует - обновляем
                            # Подготавливаем данные для UPDATE с значениями по умолчанию
                            update_data = {
                                'name': product_data.get('name'),
                                'brand': product_data.get('brand'),
                                'stock': product_data.get('stock'),
                                'price_rub': product_data.get('price_rub'),
                                'price_usd': product_data.get('price_usd'),
                                'tech_specs': product_data.get('tech_specs') or None,
                                'article': product_data.get('article') or None,
                                'category_code': product_data.get('category_code') or None,
                                'package_volume': product_data.get('package_volume') or None,
                                'package_weight': product_data.get('package_weight') or None,
                                'transit_date': product_data.get('transit_date') or None,
                                'is_active': True,
                                'part_number': product_data.get('part_number'),
                                'distributor': distributor_fixed
                            }
                            
                            # Обновляем только те поля, которые есть в данных
                            update_fields = []
                            update_params = {}
                            
                            if 'name' in update_data and update_data['name'] is not None:
                                update_fields.append("name = :name")
                                update_params['name'] = update_data['name']
                            
                            if 'brand' in update_data and update_data['brand'] is not None:
                                update_fields.append("brand = :brand")
                                update_params['brand'] = update_data['brand']
                            
                            if 'stock' in update_data and update_data['stock'] is not None:
                                # Исправляем пустые строки для числовых полей
                                stock_value = update_data['stock']
                                if stock_value == '':
                                    stock_value = 0
                                update_fields.append("stock = :stock")
                                update_params['stock'] = stock_value
                            
                            if 'price_rub' in update_data and update_data['price_rub'] is not None:
                                # Исправляем пустые строки для числовых полей
                                price_rub_value = update_data['price_rub']
                                if price_rub_value == '':
                                    price_rub_value = 0
                                update_fields.append("price_rub = :price_rub")
                                update_params['price_rub'] = price_rub_value
                            
                            if 'price_usd' in update_data and update_data['price_usd'] is not None:
                                # Исправляем пустые строки для числовых полей
                                price_usd_value = update_data['price_usd']
                                if price_usd_value == '':
                                    price_usd_value = 0
                                update_fields.append("price_usd = :price_usd")
                                update_params['price_usd'] = price_usd_value
                            
                            if 'tech_specs' in update_data:
                                update_fields.append("tech_specs = :tech_specs")
                                update_params['tech_specs'] = update_data['tech_specs']
                            
                            if 'article' in update_data:
                                update_fields.append("article = :article")
                                update_params['article'] = update_data['article']
                            
                            if 'category_code' in update_data:
                                update_fields.append("category_code = :category_code")
                                update_params['category_code'] = update_data['category_code']
                            
                            if 'package_volume' in update_data:
                                # Исправляем пустые строки для числовых полей
                                package_volume_value = update_data['package_volume']
                                if package_volume_value == '':
                                    package_volume_value = 0
                                update_fields.append("package_volume = :package_volume")
                                update_params['package_volume'] = package_volume_value
                            
                            if 'package_weight' in update_data:
                                # Исправляем пустые строки для числовых полей
                                package_weight_value = update_data['package_weight']
                                if package_weight_value == '':
                                    package_weight_value = 0
                                update_fields.append("package_weight = :package_weight")
                                update_params['package_weight'] = package_weight_value
                            
                            if 'transit_date' in update_data:
                                update_fields.append("transit_date = :transit_date")
                                update_params['transit_date'] = update_data['transit_date']
                            
                            # Добавляем обязательные поля
                            update_fields.append("is_active = :is_active")
                            # Используем CURRENT_TIMESTAMP для MySQL совместимости
                            update_fields.append("updated_at = CURRENT_TIMESTAMP")
                            update_params['is_active'] = True
                            update_params['part_number'] = update_data['part_number']
                            update_params['distributor'] = distributor_fixed
                            
                            update_stmt = text(f"""
                                UPDATE products SET 
                                    {', '.join(update_fields)}
                                WHERE part_number = :part_number AND distributor = :distributor
                            """)
                            
                            await db.execute(update_stmt, update_params)
                            updated += 1
                        else:
                            # Товар не существует - вставляем через raw SQL
                            insert_stmt = text("""
                                INSERT INTO products (article, name, brand, part_number, category_code, price_rub, price_usd, stock, package_volume, package_weight, tech_specs, transit_date, distributor, is_active, created_at, updated_at)
                                VALUES (:article, :name, :brand, :part_number, :category_code, :price_rub, :price_usd, :stock, :package_volume, :package_weight, :tech_specs, :transit_date, :distributor, :is_active, NOW(), NOW())
                            """)
                            await db.execute(insert_stmt, product_data)
                            inserted += 1
                            
                    except Exception as single_error:
                        logging.error(f"[{distributor_fixed}] Ошибка при обработке товара {product_data.get('part_number', 'N/A')}: {single_error}")
                        continue
                
                # Commit будет в конце функции
                logging.info(f"[{distributor_fixed}] Обработка завершена: вставлено {inserted}, обновлено {updated}")
        
        # Подсчёт удалённых товаров
        # deleted_count содержит количество удалённых товаров из начала функции
        if hasattr(Product, 'is_active'):
            logging.info(f"[{distributor_fixed}] Удалено {deleted_count} старых товаров")
            # НЕ деактивируем товары - мы уже удалили старые!
            logging.info(f"[{distributor_fixed}] Деактивация не требуется - старые товары уже удалены")
        
        logging.info(f"[{distributor_fixed}][STATS] Добавлено: {inserted}, Обновлено: {updated}, Удалено: {deleted_count}")
        logging.info(f"[{distributor_fixed}][END] Upsert завершён. Всего обработано: {len(products)}")
        
        print(f"[{distributor_fixed}][DEBUG] Upsert завершен, результат: {{'inserted': {inserted}, 'updated': {updated}, 'deleted': {deleted_count}}}")
        logging.info(f"[{distributor_fixed}][DEBUG] Upsert завершен, результат: {{'inserted': {inserted}, 'updated': {updated}, 'deleted': {deleted_count}}}")
        
        # Финальный commit всех изменений
        await db.commit()
        print(f"[{distributor_fixed}][DEBUG] Все изменения зафиксированы в базе данных")
        
        # Сохраняем статистику для последующего использования
        global _last_upsert_stats
        _last_upsert_stats = {"inserted": inserted, "updated": updated, "deleted": deleted_count}
        
        return {"inserted": inserted, "updated": updated, "deleted": deleted_count}
        
    except Exception as e:
        print(f"[{distributor_fixed}][ERROR] КРИТИЧЕСКАЯ ОШИБКА в upsert_products: {e}")
        logging.error(f"[{distributor_fixed}][ERROR] КРИТИЧЕСКАЯ ОШИБКА в upsert_products: {e}")
        
        # Логируем детали ошибки
        import traceback
        error_traceback = traceback.format_exc()
        print(f"[{distributor_fixed}][ERROR] Детали ошибки:\n{error_traceback}")
        logging.error(f"[{distributor_fixed}][ERROR] Детали ошибки:\n{error_traceback}")
        
        await db.rollback()
        return {"inserted": 0, "updated": 0, "deleted": 0}

# Глобальная переменная для хранения последней статистики
_last_upsert_stats = {}

def get_last_upsert_stats():
    """Возвращает статистику последнего upsert"""
    return _last_upsert_stats.copy() 