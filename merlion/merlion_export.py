"""
Этот скрипт автоматически выгружает и обновляет товары из дистрибьютора Merlion.
ИСПРАВЛЕННАЯ ВЕРСИЯ - использует два API для получения полных данных.
"""
import sys
import os
import asyncio
import logging
import time
from sqlalchemy.ext.asyncio import AsyncSession
from core.db_engine import AsyncSessionLocal
from core.db_models import Product
from core.telegram_notify import notify_export_start, notify_export_progress, notify_export_complete, notify_export_error
from datetime import datetime

async def upsert_merlion_products(products, db):
    from sqlalchemy import text
    # Удаляем все старые товары Merlion
    await db.execute(text("DELETE FROM products WHERE distributor = 'Merlion'"))
    await db.commit()
    # Удаляем дубли по (article, part_number, 'Merlion')
    unique = {}
    for p in products:
        key = (p.get('article'), p.get('part_number'), 'Merlion')
        if key not in unique:
            unique[key] = p
    products = list(unique.values())
    # Массовая вставка новых товаров
    if products:
        stmt = text("""
            INSERT INTO products (article, name, brand, part_number, category_code, price_rub, price_usd, stock, package_volume, package_weight, tech_specs, transit_date, distributor, is_active, created_at, updated_at)
            VALUES (:article, :name, :brand, :part_number, :category_code, :price_rub, :price_usd, :stock, :package_volume, :package_weight, :tech_specs, :transit_date, 'Merlion', TRUE, NOW(), NOW())
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
                distributor = VALUES(distributor),
                is_active = VALUES(is_active),
                updated_at = NOW()
        """)
        for product in products:
            await db.execute(stmt, product)
        await db.commit()

async def main():
    """
    Основная логика выгрузки Merlion находится в merlion_downloader_fixed.py
    Этот файл содержит вспомогательные функции для работы с данными Merlion
    """
    from merlion.merlion_downloader_fixed import get_merlion_products
    
    print("[Merlion] Начинаем выгрузку Merlion (ИСПРАВЛЕННАЯ ВЕРСИЯ)...")
    print("[Merlion] Используем два API: mlservice2 (партномеры) + mlservice3 (цены)")
    
    try:
        # Получаем товары из двух API
        products = await get_merlion_products()
        
        if not products:
            print("[Merlion] Нет данных для импорта.")
            return
            
        print(f"[Merlion] Получено товаров: {len(products)}")
        
        # Показываем примеры товаров
        if products:
            print(f"[Merlion] Пример первого товара:")
            first_product = products[0]
            print(f"  ID: {first_product.get('id')}")
            print(f"  Партномер: {first_product.get('part_number')}")
            print(f"  Название: {first_product.get('name')}")
            print(f"  Бренд: {first_product.get('brand')}")
            print(f"  Остаток: {first_product.get('stock')}")
            print(f"  Цена RUB: {first_product.get('price_rub')}")
            print(f"  Цена USD: {first_product.get('price_usd')}")
        
        # Сохраняем в базу данных
        async with AsyncSessionLocal() as db:
            await upsert_merlion_products(products, db)
            
        print(f"[Merlion] Импортировано в базу: {len(products)} товаров.")
        
        # Правильно закрываем все соединения
        try:
            from core.db_engine import engine
            await engine.dispose()
        except:
            pass
        
        # Записываем дату обновления
        with open("static/last_update_merlion.txt", "w", encoding="utf-8") as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            
        print("[Merlion] Выгрузка завершена успешно.")
        
    except Exception as e:
        error_msg = f"Ошибка при выгрузке Merlion: {e}"
        print(f"[Merlion] {error_msg}")
        logging.error(f"[Merlion] {error_msg}")

if __name__ == "__main__":
    asyncio.run(main()) 