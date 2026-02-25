#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import sys
sys.path.append('.')

async def update_treolan():
    print('ОБНОВЛЕНИЕ TREOLAN')
    print('=' * 40)
    
    try:
        from treolan.treolan_export import get_treolan_products
        from core.upsert import upsert_products
        from core.db_engine import AsyncSessionLocal
        from datetime import datetime
        
        print('Получаем товары от Treolan...')
        products = await get_treolan_products()
        print(f'Получено товаров: {len(products)}')
        
        print('Сохраняем в базу данных...')
        async with AsyncSessionLocal() as db:
            result = await upsert_products(products, 'Treolan', db)
            print(f'Результат: {result}')
        
        print('Обновляем файл даты...')
        with open('static/last_update_treolan.txt', 'w', encoding='utf-8') as f:
            f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        print('Treolan успешно обновлен!')
        
    except Exception as e:
        print(f'Ошибка: {e}')

if __name__ == "__main__":
    asyncio.run(update_treolan())



