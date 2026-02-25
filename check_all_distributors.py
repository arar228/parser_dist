#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
from datetime import datetime

print('ПРОВЕРКА ВСЕХ ДИСТРИБЬЮТОРОВ В БАЗЕ ДАННЫХ')
print('=' * 70)

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
    print('-' * 70)
    cur.execute('''
        SELECT distributor, COUNT(*) as count, MAX(updated_at) as last_update
        FROM products 
        GROUP BY distributor 
        ORDER BY count DESC
    ''')
    
    for row in cur.fetchall():
        distributor, count, last_update = row
        print(f'{distributor:12} | {count:8,} товаров | Обновлено: {last_update}')
    
    # Проверяем товары, обновленные сегодня
    print('\nТОВАРЫ, ОБНОВЛЕННЫЕ СЕГОДНЯ:')
    print('-' * 70)
    cur.execute('''
        SELECT distributor, COUNT(*) as count
        FROM products 
        WHERE DATE(updated_at) = CURDATE()
        GROUP BY distributor 
        ORDER BY count DESC
    ''')
    
    today_total = 0
    for row in cur.fetchall():
        distributor, count = row
        today_total += count
        print(f'{distributor:12} | {count:8,} товаров обновлено сегодня')
    
    print(f'{"ИТОГО":12} | {today_total:8,} товаров обновлено сегодня')
    
    # Проверяем последние добавленные товары по каждому дистрибьютеру
    print('\nПОСЛЕДНИЕ ТОВАРЫ ПО ДИСТРИБЬЮТОРАМ:')
    print('-' * 70)
    
    distributors = ['Merlion', 'Ocs', 'Treolan', 'Marvel', 'VVP', 'Ocs Sale', 'Netlab']
    
    for distributor in distributors:
        cur.execute('''
            SELECT part_number, name, updated_at 
            FROM products 
            WHERE distributor = %s
            ORDER BY updated_at DESC 
            LIMIT 1
        ''', (distributor,))
        
        result = cur.fetchone()
        if result:
            part_num, name, updated = result
            print(f'{distributor:12} | {part_num:15} | {name[:40]}... | {updated}')
        else:
            print(f'{distributor:12} | НЕТ ДАННЫХ')
    
    # Проверяем файлы обновления
    print('\nПРОВЕРКА ФАЙЛОВ ОБНОВЛЕНИЯ:')
    print('-' * 70)
    
    import os
    update_files = [
        'static/last_update_merlion.txt',
        'static/last_update_ocs.txt', 
        'static/last_update_treolan.txt',
        'static/last_update_marvel.txt',
        'static/last_update_vvp.txt',
        'static/last_update_ocs_sale.txt',
        'static/last_update_netlab.txt'
    ]
    
    for file_path in update_files:
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                print(f'{os.path.basename(file_path):25} | {content}')
            except:
                print(f'{os.path.basename(file_path):25} | ОШИБКА ЧТЕНИЯ')
        else:
            print(f'{os.path.basename(file_path):25} | ФАЙЛ НЕ НАЙДЕН')
    
    conn.close()
    print('\nПроверка завершена успешно!')
    
except Exception as e:
    print(f'Ошибка при проверке: {e}')
