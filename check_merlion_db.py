#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
from datetime import datetime

print('ПРОВЕРКА БАЗЫ ДАННЫХ ПОСЛЕ MERLION')
print('=' * 60)

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
    
    print('Подключение к базе данных: УСПЕШНО')
    
    # Общая статистика
    cur.execute('SELECT COUNT(*) FROM products')
    total = cur.fetchone()[0]
    print(f'Всего товаров в базе: {total:,}')
    
    # Статистика по дистрибьюторам
    print('\nСТАТИСТИКА ПО ДИСТРИБЬЮТОРАМ:')
    cur.execute('''
        SELECT distributor, COUNT(*) as count, MAX(updated_at) as last_update
        FROM products 
        GROUP BY distributor 
        ORDER BY count DESC
    ''')
    
    for row in cur.fetchall():
        distributor, count, last_update = row
        print(f'  {distributor}: {count:,} товаров (последнее обновление: {last_update})')
    
    # Проверяем Merlion конкретно
    print('\nДЕТАЛЬНАЯ ПРОВЕРКА MERLION:')
    cur.execute('SELECT COUNT(*) FROM products WHERE distributor = "Merlion"')
    merlion_count = cur.fetchone()[0]
    print(f'Merlion товаров в БД: {merlion_count:,}')
    
    # Последние товары Merlion
    cur.execute('''
        SELECT part_number, name, price_rub, stock, updated_at 
        FROM products 
        WHERE distributor = "Merlion" 
        ORDER BY updated_at DESC 
        LIMIT 5
    ''')
    
    print('\nПОСЛЕДНИЕ 5 ТОВАРОВ MERLION:')
    for row in cur.fetchall():
        part_num, name, price, stock, updated = row
        print(f'  {part_num} - {name[:50]}... (цена: {price}, остаток: {stock}, обновлен: {updated})')
    
    # Проверяем время последнего обновления
    cur.execute('SELECT MAX(updated_at) FROM products WHERE distributor = "Merlion"')
    last_merlion_update = cur.fetchone()[0]
    print(f'\nПоследнее обновление Merlion: {last_merlion_update}')
    
    # Проверяем, есть ли товары, обновленные сегодня
    cur.execute('SELECT COUNT(*) FROM products WHERE distributor = "Merlion" AND DATE(updated_at) = CURDATE()')
    today_merlion = cur.fetchone()[0]
    print(f'Merlion товаров, обновленных сегодня: {today_merlion:,}')
    
    conn.close()
    print('\nПроверка завершена успешно!')
    
except Exception as e:
    print(f'Ошибка при проверке: {e}')
