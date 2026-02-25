#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Глубокая диагностика проблемы с базой данных
"""

import pymysql
from datetime import datetime, timedelta
import os

def deep_debug_database():
    """Глубокая диагностика проблемы с базой данных"""
    
    print("ГЛУБОКАЯ ДИАГНОСТИКА БАЗЫ ДАННЫХ")
    print("=" * 80)
    
    # Проверяем конфигурацию
    print("1. ПРОВЕРКА КОНФИГУРАЦИИ:")
    print("-" * 40)
    
    try:
        from mysql_config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
        print(f"   Host: {MYSQL_HOST}")
        print(f"   Port: {MYSQL_PORT}")
        print(f"   User: {MYSQL_USER}")
        print(f"   Database: {MYSQL_DATABASE}")
        print(f"   Password: {'*' * len(MYSQL_PASSWORD)}")
    except Exception as e:
        print(f"   Ошибка импорта конфигурации: {e}")
    
    # Проверяем подключение
    print(f"\n2. ПРОВЕРКА ПОДКЛЮЧЕНИЯ:")
    print("-" * 40)
    
    try:
        connection = pymysql.connect(
            host='YOUR_MYSQL_HOST',
            port=3306,
            user='parser_dist',
            password='YOUR_MYSQL_PASSWORD',
            database='parser_dist',
            charset='utf8mb4'
        )
        
        print("   ✅ Подключение к базе данных: УСПЕШНО")
        
        with connection.cursor() as cursor:
            # Проверяем информацию о базе данных
            cursor.execute("SELECT DATABASE()")
            current_db = cursor.fetchone()[0]
            print(f"   Текущая база данных: {current_db}")
            
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            print(f"   Версия MySQL: {version}")
            
            cursor.execute("SELECT USER()")
            current_user = cursor.fetchone()[0]
            print(f"   Текущий пользователь: {current_user}")
            
            cursor.execute("SELECT @@hostname")
            hostname = cursor.fetchone()[0]
            print(f"   Хост базы данных: {hostname}")
            
            # Проверяем все таблицы
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print(f"\n   Таблицы в базе данных:")
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"      {table_name}: {count:,} записей")
            
            # Детальная проверка таблицы products
            print(f"\n3. ДЕТАЛЬНАЯ ПРОВЕРКА ТАБЛИЦЫ PRODUCTS:")
            print("-" * 40)
            
            cursor.execute("DESCRIBE products")
            columns = cursor.fetchall()
            print("   Структура таблицы products:")
            for col in columns:
                print(f"      {col[0]} {col[1]} {'NULL' if col[2] == 'YES' else 'NOT NULL'}")
            
            # Проверяем индексы
            cursor.execute("SHOW INDEX FROM products")
            indexes = cursor.fetchall()
            print(f"\n   Индексы таблицы products:")
            for idx in indexes:
                print(f"      {idx[2]} ({idx[4]})")
            
            # Проверяем все записи по времени создания
            cursor.execute("""
                SELECT 
                    distributor,
                    COUNT(*) as count,
                    MIN(created_at) as first_created,
                    MAX(created_at) as last_created,
                    MIN(updated_at) as first_updated,
                    MAX(updated_at) as last_updated
                FROM products 
                GROUP BY distributor
                ORDER BY last_created DESC
            """)
            
            all_stats = cursor.fetchall()
            print(f"\n   Все записи в таблице products:")
            for stat in all_stats:
                distributor, count, first_created, last_created, first_updated, last_updated = stat
                print(f"      {distributor}: {count:,} записей")
                print(f"         Создано: {first_created} - {last_created}")
                print(f"         Обновлено: {first_updated} - {last_updated}")
            
            # Проверяем записи за последние 24 часа
            yesterday = datetime.now() - timedelta(hours=24)
            cursor.execute("""
                SELECT 
                    distributor,
                    COUNT(*) as count,
                    MIN(created_at) as first_created,
                    MAX(created_at) as last_created
                FROM products 
                WHERE created_at >= %s
                GROUP BY distributor
                ORDER BY last_created DESC
            """, (yesterday,))
            
            recent_24h = cursor.fetchall()
            print(f"\n   Записи за последние 24 часа:")
            if recent_24h:
                for record in recent_24h:
                    distributor, count, first_created, last_created = record
                    print(f"      {distributor}: {count:,} записей ({first_created} - {last_created})")
            else:
                print("      Нет записей за последние 24 часа")
            
            # Проверяем последние 50 записей
            cursor.execute("""
                SELECT distributor, part_number, created_at, updated_at
                FROM products 
                ORDER BY created_at DESC 
                LIMIT 50
            """)
            
            recent_50 = cursor.fetchall()
            print(f"\n   Последние 50 записей:")
            for i, record in enumerate(recent_50, 1):
                distributor, part_number, created_at, updated_at = record
                print(f"      {i:2d}. {created_at} | {distributor:<12} | {part_number}")
        
        connection.close()
        
    except Exception as e:
        print(f"   ❌ Ошибка подключения: {e}")
        return False
    
    print(f"\n" + "=" * 80)
    print("ДИАГНОСТИКА ЗАВЕРШЕНА!")
    return True

if __name__ == "__main__":
    deep_debug_database()
