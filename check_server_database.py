#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Проверяем базу данных на сервере через Python
"""

import pymysql
from datetime import datetime

def check_server_database():
    """Проверяем базу данных на сервере"""
    
    try:
        connection = pymysql.connect(
            host='YOUR_MYSQL_HOST',
            port=3306,
            user='parser_dist',
            password='YOUR_MYSQL_PASSWORD',
            database='parser_dist',
            charset='utf8mb4'
        )
        
        print("Подключение к базе данных: УСПЕШНО")
        print("=" * 80)
        print("ПРОВЕРКА БАЗЫ ДАННЫХ НА СЕРВЕРЕ")
        print("=" * 80)
        
        with connection.cursor() as cursor:
            # Статистика по дистрибьюторам
            cursor.execute("""
                SELECT 
                    distributor,
                    COUNT(*) as count,
                    MAX(created_at) as last_update
                FROM products 
                GROUP BY distributor
                ORDER BY last_update DESC
            """)
            
            stats = cursor.fetchall()
            total_products = 0
            
            print("СТАТИСТИКА ПО ДИСТРИБЬЮТОРАМ:")
            print("-" * 50)
            
            for stat in stats:
                distributor, count, last_update = stat
                total_products += count
                print(f"{distributor:<12}: {count:>8,} товаров (последнее обновление: {last_update})")
            
            print(f"\nВСЕГО ТОВАРОВ: {total_products:,}")
            
            # Последние 20 записей
            cursor.execute("""
                SELECT distributor, part_number, created_at 
                FROM products 
                ORDER BY created_at DESC 
                LIMIT 20
            """)
            
            recent_records = cursor.fetchall()
            print(f"\nПОСЛЕДНИЕ 20 ЗАПИСЕЙ:")
            print("-" * 70)
            
            for record in recent_records:
                distributor, part_number, created_at = record
                print(f"{created_at} | {distributor:<12} | {part_number}")
            
            # Проверяем записи за сегодня
            today = datetime.now().date()
            cursor.execute("""
                SELECT 
                    distributor,
                    COUNT(*) as count
                FROM products 
                WHERE DATE(created_at) = %s
                GROUP BY distributor
                ORDER BY count DESC
            """, (today,))
            
            today_records = cursor.fetchall()
            print(f"\nЗАПИСИ ЗА СЕГОДНЯ ({today}):")
            print("-" * 40)
            
            if today_records:
                for record in today_records:
                    distributor, count = record
                    print(f"{distributor:<12}: {count:>8,} товаров")
            else:
                print("Нет записей за сегодня")
            
            # Проверяем записи за последние 7 дней
            from datetime import timedelta
            week_ago = datetime.now() - timedelta(days=7)
            cursor.execute("""
                SELECT 
                    distributor,
                    COUNT(*) as count
                FROM products 
                WHERE created_at >= %s
                GROUP BY distributor
                ORDER BY count DESC
            """, (week_ago,))
            
            week_records = cursor.fetchall()
            print(f"\nЗАПИСИ ЗА ПОСЛЕДНИЕ 7 ДНЕЙ:")
            print("-" * 40)
            
            if week_records:
                for record in week_records:
                    distributor, count = record
                    print(f"{distributor:<12}: {count:>8,} товаров")
            else:
                print("Нет записей за последние 7 дней")
        
        connection.close()
        print(f"\n" + "=" * 80)
        print("ПРОВЕРКА ЗАВЕРШЕНА!")
        return True
        
    except Exception as e:
        print(f"Ошибка при проверке базы данных: {e}")
        return False

if __name__ == "__main__":
    check_server_database()
