#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Конфигурационный файл для API ключей и настроек
"""

import os
from pathlib import Path

# Путь к файлу с учетными данными
CREDENTIALS_FILE = Path(__file__).parent / ".env"

# Загружаем переменные окружения из .env файла
def load_credentials():
    """Загружает учетные данные из .env файла или переменных окружения"""
    
    # Сначала пробуем переменные окружения
    client_id = os.getenv('MERLION_CLIENT_ID')
    login = os.getenv('MERLION_LOGIN') 
    password = os.getenv('MERLION_PASSWORD')
    
    # Если нет в переменных окружения, пробуем .env файл
    if not all([client_id, login, password]) and CREDENTIALS_FILE.exists():
        try:
            with open(CREDENTIALS_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        if key == 'MERLION_CLIENT_ID':
                            client_id = value.strip('"\'')
                        elif key == 'MERLION_LOGIN':
                            login = value.strip('"\'')
                        elif key == 'MERLION_PASSWORD':
                            password = value.strip('"\'')
        except Exception as e:
            print(f"⚠️ Ошибка чтения .env файла: {e}")
    
    return client_id, login, password

# Merlion API настройки
MERLION_API_URL = "https://api.merlion.com/dl/mlservice3?wsdl"

# Получаем учетные данные
MERLION_CLIENT_ID, MERLION_LOGIN, MERLION_PASSWORD = load_credentials()

# Fallback значения если не удалось загрузить из .env
if not MERLION_CLIENT_ID:
    MERLION_CLIENT_ID = "YOUR_MERLION_CLIENT_ID"
if not MERLION_LOGIN:
    MERLION_LOGIN = "API"
if not MERLION_PASSWORD:
    MERLION_PASSWORD = "YOUR_MERLION_PASSWORD"

# Дополнительный пароль для mlservice3
MERLION_PASSWORD_ML3 = "YOUR_MERLION_PASSWORD_ML3"

# Проверяем, что все данные загружены
if all([MERLION_CLIENT_ID, MERLION_LOGIN, MERLION_PASSWORD]):
    print("✅ Учетные данные Merlion загружены успешно")
else:
    print("❌ ВНИМАНИЕ: Не удалось загрузить учетные данные Merlion!")
    print("Создайте файл .env в корне проекта со следующим содержимым:")
    print("MERLION_CLIENT_ID=ваш_код_клиента")
    print("MERLION_LOGIN=ваш_логин")
    print("MERLION_PASSWORD=ваш_пароль")
    print()
    print("Или установите переменные окружения:")
    print("set MERLION_CLIENT_ID=ваш_код_клиента")
    print("set MERLION_LOGIN=ваш_логин") 
    print("set MERLION_PASSWORD=ваш_пароль")

# Формируем логин для аутентификации
if MERLION_CLIENT_ID and MERLION_LOGIN:
    MERLION_AUTH_LOGIN = f"{MERLION_CLIENT_ID}|{MERLION_LOGIN}"
else:
    MERLION_AUTH_LOGIN = None 