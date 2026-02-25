#!/usr/bin/env python3
"""
Скрипт для проверки статуса обновлений всех дистрибьюторов
Читает файлы last_update_*.txt и показывает актуальную информацию
"""

import os
import datetime
from pathlib import Path

# Список активных дистрибьюторов и их файлов
DISTRIBUTORS = {
    'Merlion': 'static/last_update_merlion.txt',
    'Netlab': 'static/last_update_netlab.txt', 
    'Treolan': 'static/last_update_treolan.txt',
    'VVP': 'static/last_update_vvp.txt',
    'Marvel': 'static/last_update_marvel.txt',
    'OCS': 'static/last_update_ocs.txt'
}

def read_last_update(file_path):
    """Читает дату последнего обновления из файла"""
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                date_str = f.read().strip()
                return date_str
        else:
            return "файл не найден"
    except Exception as e:
        return f"ошибка чтения: {e}"

def calculate_time_ago(date_str):
    """Вычисляет сколько времени прошло с последнего обновления"""
    try:
        if date_str in ["файл не найден", "дата неизвестна"] or "ошибка" in date_str:
            return ""
        
        last_update = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        now = datetime.datetime.now()
        diff = now - last_update
        
        if diff.days > 0:
            return f"({diff.days} дн. назад)"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            return f"({hours} ч. назад)"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            return f"({minutes} мин. назад)"
        else:
            return "(только что)"
    except Exception:
        return ""

def main():
    print("📊 СТАТУС ДИСТРИБЬЮТОРОВ")
    print("=" * 50)
    
    for distributor, file_path in DISTRIBUTORS.items():
        last_update = read_last_update(file_path)
        time_ago = calculate_time_ago(last_update)
        
        if last_update == "файл не найден":
            status = "❌ не запускался"
        elif "ошибка" in last_update:
            status = f"⚠️ {last_update}"
        else:
            status = f"✅ {last_update} {time_ago}"
        
        print(f"• {distributor:12s}: {status}")
    
    print("=" * 50)
    print(f"🕐 Текущее время: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main() 