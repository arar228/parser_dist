#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Конфигурационный файл для подключения к MySQL
"""

import os
from pathlib import Path

# MySQL настройки подключения
MYSQL_HOST = "YOUR_MYSQL_HOST"
MYSQL_PORT = "3306"  # Стандартный порт MySQL
MYSQL_USER = "parser_dist"  # Пользователь MySQL
MYSQL_PASSWORD = "YOUR_MYSQL_PASSWORD"
MYSQL_DATABASE = "parser_dist"

# Формируем URL для подключения к MySQL
MYSQL_URL = f"mysql+aiomysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

# Альтернативный URL для синхронного подключения (если понадобится)
MYSQL_URL_SYNC = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

print(f"MySQL конфигурация загружена:")
print(f"   Host: {MYSQL_HOST}:{MYSQL_PORT}")
print(f"   Database: {MYSQL_DATABASE}")
print(f"   User: {MYSQL_USER}")
print(f"   URL: {MYSQL_URL}")
