#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
from datetime import datetime

print('ПРОВЕРКА TREOLAN')
print('=' * 50)

try:
    conn = pymysql.connect(
        host='YOUR_MYSQL_HOST', 
        port=3306, 
        user='parser_dist', 
        password='YOUR_MYSQL_PASSWORD', 
        database='parser_dist', 
        charset='utf8mb4', 
        autocommit=True
    )
    cur = conn.cursor()
    
    # Проверяем Treolan
    cur.execute('SELECT COUNT(*) FROM products WHERE distributor = "Treolan"')
    treolan_count = cur.fetchone()[0]
    print(f'Treolan товаров в БД: {treolan_count:,}')
    
    # Последнее обновление
    cur.execute('SELECT MAX(updated_at) FROM products WHERE distributor = "Treolan"')
    last_update = cur.fetchone()[0]
    print(f'Последнее обновление: {last_update}')
    
    # Проверяем, есть ли товары сегодня
    cur.execute('SELECT COUNT(*) FROM products WHERE distributor = "Treolan" AND DATE(updated_at) = CURDATE()')
    today_count = cur.fetchone()[0]
    print(f'Обновлено сегодня: {today_count:,}')
    
    # Последние 3 товара
    cur.execute('''
        SELECT part_number, name, updated_at 
        FROM products 
        WHERE distributor = "Treolan" 
        ORDER BY updated_at DESC 
        LIMIT 3
    ''')
    
    print('\nПоследние 3 товара Treolan:')
    for row in cur.fetchall():
        part_num, name, updated = row
        print(f'  {part_num} - {name[:50]}... | {updated}')
    
    conn.close()
    
except Exception as e:
    print(f'Ошибка: {e}')
