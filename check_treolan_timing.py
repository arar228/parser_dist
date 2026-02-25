#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from datetime import datetime, timedelta

# Проверяем файл last_update_treolan.txt
file_path = 'static/last_update_treolan.txt'
if os.path.exists(file_path):
    with open(file_path, 'r') as f:
        content = f.read().strip()
    print(f'Файл last_update_treolan.txt: {content}')
    
    # Парсим дату
    try:
        last_update = datetime.strptime(content, '%Y-%m-%d %H:%M:%S')
        now = datetime.now()
        diff = now - last_update
        print(f'Последнее обновление: {last_update}')
        print(f'Текущее время: {now}')
        print(f'Разница: {diff}')
        print(f'Часов прошло: {diff.total_seconds() / 3600:.1f}')
        
        # Treolan должен обновляться каждые 60 минут
        if diff.total_seconds() > 3600:
            print('Treolan ДОЛЖЕН обновляться (прошло больше 60 минут)')
        else:
            print('Treolan НЕ должен обновляться (прошло меньше 60 минут)')
    except Exception as e:
        print(f'Ошибка парсинга даты: {e}')
else:
    print('Файл last_update_treolan.txt НЕ НАЙДЕН')

