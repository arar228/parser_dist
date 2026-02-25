# -*- coding: utf-8 -*-
"""
ИСПРАВЛЕННАЯ версия Merlion Downloader
Использует два API: mlservice2 (партномеры) + mlservice3 (цены)
Соблюдает лимиты API согласно документации
"""

import os
import time
import csv
import datetime
import json
import asyncio

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from zeep import Client
from zeep.exceptions import Fault
from zeep.helpers import serialize_object
from zeep.transports import Transport
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
import sys
import logging

# ОТКЛЮЧАЕМ ПРОКСИ для решения проблем с подключением
proxy_vars = ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY']
for var in proxy_vars:
    if var in os.environ:
        del os.environ[var]
os.environ['no_proxy'] = '*'
os.environ['NO_PROXY'] = '*'

# Добавляем путь к модулю core
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.telegram_notify import notify_export_error

# Импортируем конфигурацию
from config import MERLION_CLIENT_ID, MERLION_LOGIN, MERLION_PASSWORD, MERLION_PASSWORD_ML3

# --- КОНФИГУРАЦИЯ ДВУХ API ---

# mlservice2 - для получения партномеров и описаний
MERLION_API_URL_2 = "https://apitest.merlion.com/dl/mlservice2?wsdl"
MERLION_CLIENT_ID_2 = MERLION_CLIENT_ID
MERLION_LOGIN_2 = MERLION_LOGIN
MERLION_PASSWORD_2 = MERLION_PASSWORD
AUTH_LOGIN_2 = f"{MERLION_CLIENT_ID_2}|{MERLION_LOGIN_2}"

# mlservice3 - для получения цен и остатков
MERLION_API_URL_3 = "https://api.merlion.com/dl/mlservice3?wsdl"
MERLION_CLIENT_ID_3 = MERLION_CLIENT_ID
MERLION_LOGIN_3 = MERLION_LOGIN
MERLION_PASSWORD_3 = MERLION_PASSWORD_ML3
AUTH_LOGIN_3 = f"{MERLION_CLIENT_ID_3}|{MERLION_LOGIN_3}"

# --- ЛИМИТЫ API (согласно документации) ---
# getItems: 3 запроса в секунду, 20 000 в сутки
# getItemsAvail: 5 запросов в секунду, 100 000 в сутки
# getCatalog: 1 запрос в секунду, 10 000 в сутки

ITEMS_PER_PAGE = 1000  # Уменьшаем для соблюдения лимитов
PRICES_PER_BATCH = 100  # Уменьшаем для соблюдения лимитов
OUTPUT_FILE = "Full_Database.xlsx"
REQUEST_TIMEOUT = 300
MAX_RETRIES = 3
RETRY_DELAY = 5

# --- ЗАДЕРЖКИ ДЛЯ СОБЛЮДЕНИЯ ЛИМИТОВ ---
DELAY_BETWEEN_ITEMS = 0.4  # 1/3 = 0.33 сек, берем 0.4 для надежности
DELAY_BETWEEN_PRICES = 0.2  # 1/5 = 0.2 сек
DELAY_BETWEEN_CATEGORIES = 1.1  # 1/1 = 1 сек, берем 1.1 для надежности

MAX_CATEGORY_THREADS = 1  # Уменьшаем для соблюдения лимитов
MAX_PRICE_THREADS = 1     # Уменьшаем для соблюдения лимитов

# --- ТЕСТОВЫЙ РЕЖИМ ---
TEST_MODE = False
FORCE_UPDATE_CATEGORIES = False
CATEGORIES_FILE = "categories.csv"
CATEGORIES_MAX_AGE_DAYS = 7

# --- ЛОГИРОВАНИЕ ---
LOG_FILE = os.path.join("logs", "update_distributors_merlion.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    encoding='utf-8'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

def get_all_categories(client):
    """Получает все категории товаров из Merlion API (рекурсивно), либо из файла."""
    logging.info("[Merlion][CATEGORIES] Получение категорий...")
    if not FORCE_UPDATE_CATEGORIES and os.path.exists(CATEGORIES_FILE):
        logging.info(f"[Merlion][CATEGORIES] Загружаю из файла {CATEGORIES_FILE}")
        categories = {}
        with open(CATEGORIES_FILE, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if len(row) == 2:
                    categories[row[0]] = row[1]
        logging.info(f"[Merlion][CATEGORIES] Загружено {len(categories)} категорий из файла.")
        return categories

    logging.info("[Merlion][CATEGORIES] Выгружаю из API...")
    categories = {}
    def fetch_recursively(cat_id):
        logging.info(f"[Merlion][CATEGORIES] Запрос категории: {cat_id}")
        response = client.service.getCatalog(cat_id=cat_id)
        if not response:
            return
        for cat in response:
            if not cat.ID or cat.ID in categories:
                continue
            categories[cat.ID] = cat.ID_PARENT
            time.sleep(DELAY_BETWEEN_CATEGORIES)  # Соблюдаем лимит API
            fetch_recursively(cat.ID)
    fetch_recursively("All")
    logging.info(f"[Merlion][CATEGORIES] Всего найдено {len(categories)} категорий.")
    logging.info(f"[Merlion][CATEGORIES] Сохраняю в {CATEGORIES_FILE}...")
    with open(CATEGORIES_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for k, v in categories.items():
            writer.writerow([k, v])
    return categories

def get_items_for_category(client2, category_id, error_summary=None):
    """Получает все товары для указанной категории из mlservice2 (с партномерами)."""
    items = []
    page = 0
    total_for_cat = 0
    
    while True:
        response = None
        for attempt in range(MAX_RETRIES):
            try:
                logging.info(f"[Merlion][CATEGORY:{category_id}] Запрос страницы: {page} (попытка {attempt+1})")
                
                # Соблюдаем лимит API
                time.sleep(DELAY_BETWEEN_ITEMS)

                response = client2.service.getItems(
                    cat_id=category_id,
                    page=page,
                    rows_on_page=ITEMS_PER_PAGE
                )
                
                if response is not None:
                    break

            except Fault as e:
                logging.error(f"[Merlion][CATEGORY:{category_id}] SOAP ошибка: {e}")
                if "Rate limit exceeded" in str(e):
                    current_delay = RETRY_DELAY * (attempt + 1)
                    logging.warning(f"[Merlion][CATEGORY:{category_id}] Повторная попытка через {current_delay} сек...")
                    time.sleep(current_delay)
                else:
                    error_msg = f"SOAP ошибка в категории {category_id}: {e}"
                    logging.error(f"[Merlion][CATEGORY:{category_id}] {error_msg}")
                    if error_summary is not None:
                        error_summary.append(error_msg)
                    time.sleep(RETRY_DELAY)
            except Exception as e:
                error_msg = f"Ошибка запроса в категории {category_id}: {e}"
                logging.error(f"[Merlion][CATEGORY:{category_id}] {error_msg}")
                if error_summary is not None:
                    error_summary.append(error_msg)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    logging.error(f"[Merlion][CATEGORY:{category_id}] Не удалось получить страницу {page} после {MAX_RETRIES} попыток.")
                    return items 
        
        if not response:
            logging.warning(f"[Merlion][CATEGORY:{category_id}] Страница {page}: получено 0 товаров. Завершение для этой категории.")
            return items

        num_items_received = len(response)
        total_for_cat += num_items_received
        logging.info(f"[Merlion][CATEGORY:{category_id}] Страница {page}: получено {num_items_received} товаров.")

        for item in response:
            # Правильно извлекаем данные из SOAP объекта
            try:
                # Используем serialize_object для конвертации SOAP объекта в словарь
                item_dict = serialize_object(item)
                
                items.append({
                    "ID Товара (No)": item_dict.get('No'),
                    "Наименование": item_dict.get('Name'),
                    "Бренд": item_dict.get('Brand'),
                    "Партномер": item_dict.get('Vendor_part'),
                    "Код Категории": category_id,
                    "Описание": item_dict.get('Description'),
                    "Гарантия": item_dict.get('Warranty'),
                    "Вес": item_dict.get('Weight'),
                    "Объём": item_dict.get('Volume'),
                })
            except Exception as e:
                logging.error(f"[Merlion][CATEGORY:{category_id}] Ошибка обработки товара: {e}")
                # Fallback: пытаемся получить данные напрямую
                try:
                    items.append({
                        "ID Товара (No)": getattr(item, 'No', None),
                        "Наименование": getattr(item, 'Name', None),
                        "Бренд": getattr(item, 'Brand', None),
                        "Партномер": getattr(item, 'Vendor_part', None),
                        "Код Категории": category_id,
                        "Описание": getattr(item, 'Description', None),
                        "Гарантия": getattr(item, 'Warranty', None),
                        "Вес": getattr(item, 'Weight', None),
                        "Объём": getattr(item, 'Volume', None),
                    })
                except Exception as fallback_error:
                    logging.error(f"[Merlion][CATEGORY:{category_id}] Fallback обработка тоже не удалась: {fallback_error}")

        if num_items_received < ITEMS_PER_PAGE:
            logging.info(f"[Merlion][CATEGORY:{category_id}] Всего товаров по категории: {total_for_cat}")
            return items

        page += 1

def add_prices_and_stock(df, client3, shipment_method_code, shipment_date, error_summary=None):
    """Получает цены и остатки из mlservice3."""
    print(f"Этап 4 из 4: Получение цен и остатков из mlservice3...")
    if df.empty:
        print("Нет товаров для обновления цен.")
        return df
    
    # Проверяем партномеры ДО обработки mlservice3
    part_numbers_before = df["Партномер"].dropna().apply(str).str.strip()
    valid_part_numbers_before = part_numbers_before[part_numbers_before != ""]
    logging.info(f"[Merlion][DEBUG] add_prices_and_stock ДО - валидных партномеров: {len(valid_part_numbers_before)}")
    
    df["Цена (RUB)"] = None
    df["Наличие (шт)"] = None
    df["Цена (USD)"] = None
    
    try:
        all_shipment_methods = client3.service.getShipmentMethods()
        print(f"Доступные методы отгрузки: {[m.Code for m in all_shipment_methods]}")
    except Exception as e:
        error_msg = f"Ошибка получения методов отгрузки: {e}"
        print(f"{error_msg}")
        if error_summary is not None:
            error_summary.append(error_msg)
        all_shipment_methods = [type('obj', (object,), {'Code': shipment_method_code})]
    
    item_ids = df["ID Товара (No)"].dropna().unique().tolist()
    
    # Уменьшаем размер батча для соблюдения лимитов
    batch_size = PRICES_PER_BATCH
    
    main_stock = {}
    total_prices = {}
    total_prices_usd = {}
    
    print(f"Обрабатываем метод отгрузки: {shipment_method_code}")
    
    i = 0
    total_batches = (len(item_ids) + batch_size - 1) // batch_size
    current_batch = 0
    
    while i < len(item_ids):
        current_batch += 1
        end = min(i + batch_size, len(item_ids))
        batch = item_ids[i:end]
        
        print(f"[{shipment_method_code}] Батч {current_batch}/{total_batches} ({len(batch)} товаров)")
        
        for attempt in range(3):
            try:
                # Соблюдаем лимит API
                time.sleep(DELAY_BETWEEN_PRICES)
                
                response = client3.service.getItemsAvail(
                    item_id={'item': batch},  # Правильный формат для mlservice3
                    shipment_method=shipment_method_code,
                    shipment_date=shipment_date,
                    only_avail="0"
                )
                
                if response:
                    # Правильно обрабатываем SOAP ответ
                    try:
                        result_dict = serialize_object(response)
                        
                        for item in result_dict:
                            if item.get('No'):
                                item_id = item['No']
                                
                                # Остатки
                                current_stock = item.get("AvailableClient", 0)
                                if current_stock is not None and current_stock != "":
                                    try:
                                        current_stock = int(current_stock)
                                    except (ValueError, TypeError):
                                        current_stock = 0
                                    main_stock[item_id] = current_stock
                                
                                # Цены
                                usd_price = item.get("PriceClient")
                                rub_price = item.get("PriceClientRUB")
                                if usd_price not in [None, 0, "0", "0.0", ""]:
                                    total_prices_usd[item_id] = usd_price
                                if rub_price not in [None, 0, "0", "0.0", ""]:
                                    total_prices[item_id] = rub_price
                    except Exception as e:
                        logging.error(f"Ошибка сериализации ответа mlservice3: {e}")
                        # Fallback: пытаемся обработать напрямую
                        for item in response:
                            try:
                                item_id = getattr(item, 'No', None)
                                if item_id:
                                    # Остатки
                                    current_stock = getattr(item, 'AvailableClient', 0)
                                    if current_stock is not None and current_stock != "":
                                        try:
                                            current_stock = int(current_stock)
                                        except (ValueError, TypeError):
                                            current_stock = 0
                                        main_stock[item_id] = current_stock
                                    
                                    # Цены
                                    usd_price = getattr(item, 'PriceClient', None)
                                    rub_price = getattr(item, 'PriceClientRUB', None)
                                    if usd_price not in [None, 0, "0", "0.0", ""]:
                                        total_prices_usd[item_id] = usd_price
                                    if rub_price not in [None, 0, "0", "0.0", ""]:
                                        total_prices[item_id] = rub_price
                            except Exception as item_error:
                                logging.error(f"Ошибка обработки товара в mlservice3: {item_error}")
                        
                    print(f"[{shipment_method_code}] Батч {current_batch} успешно обработан")
                    break
                else:
                    print(f"Пустой ответ от API для метода {shipment_method_code}, попытка {attempt+1}")
                    time.sleep(RETRY_DELAY)
                    
            except Exception as e:
                error_msg = f"Ошибка при запросе цен/остатков для метода {shipment_method_code} (batch_size={batch_size}, попытка {attempt+1}): {e}"
                print(f"{error_msg}")
                if error_summary is not None:
                    error_summary.append(error_msg)
                if attempt < 2:
                    time.sleep(RETRY_DELAY)
        
        i = end
        # Соблюдаем лимит API между батчами
        time.sleep(DELAY_BETWEEN_PRICES)
    
    # Применяем данные к DataFrame
    for item_id, stock in main_stock.items():
        df.loc[df["ID Товара (No)"] == item_id, "Наличие (шт)"] = stock
    
    for item_id, price in total_prices.items():
        df.loc[df["ID Товара (No)"] == item_id, "Цена (RUB)"] = price
        
    for item_id, price_usd in total_prices_usd.items():
        df.loc[df["ID Товара (No)"] == item_id, "Цена (USD)"] = price_usd
    
    print(f"Остатки получены: {len(main_stock)}")
    print(f"Цены RUB получены: {len(total_prices)}")
    print(f"Цены USD получены: {len(total_prices_usd)}")
    
    # Проверяем партномеры ПОСЛЕ обработки mlservice3
    part_numbers_after = df["Партномер"].dropna().apply(str).str.strip()
    valid_part_numbers_after = part_numbers_after[part_numbers_after != ""]
    logging.info(f"[Merlion][DEBUG] add_prices_and_stock ПОСЛЕ - валидных партномеров: {len(valid_part_numbers_after)}")
    
    # Проверяем, не изменились ли партномеры
    if len(valid_part_numbers_before) != len(valid_part_numbers_after):
        logging.warning(f"[Merlion][DEBUG] ВНИМАНИЕ! В add_prices_and_stock количество партномеров изменилось: было {len(valid_part_numbers_before)}, стало {len(valid_part_numbers_after)}")
        if len(valid_part_numbers_before) > 0 and len(valid_part_numbers_after) > 0:
            logging.info(f"[Merlion][DEBUG] Примеры партномеров ДО: {valid_part_numbers_before.head(3).tolist()}")
            logging.info(f"[Merlion][DEBUG] Примеры партномеров ПОСЛЕ: {valid_part_numbers_after.head(3).tolist()}")
    
    return df

async def get_merlion_products(limit=None):
    """Возвращает список товаров Merlion, используя два API.
    
    Args:
        limit (int, optional): Максимальное количество товаров для получения
    """
    logging.info(f"[Merlion][START] Выгрузка начата (два API) - лимит: {limit if limit else 'без ограничений'}")
    start_time = time.time()
    
    error_summary = []
    
    try:
        # --- Создаем два клиента ---
        logging.info("[Merlion] Создаем клиент для mlservice2 (партномеры)...")
        session2 = requests.Session()
        session2.auth = HTTPBasicAuth(AUTH_LOGIN_2, MERLION_PASSWORD_2)
        transport2 = Transport(session=session2, timeout=REQUEST_TIMEOUT)
        client2 = Client(MERLION_API_URL_2, transport=transport2)
        
        logging.info("[Merlion] Создаем клиент для mlservice3 (цены и остатки)...")
        session3 = requests.Session()
        session3.auth = HTTPBasicAuth(AUTH_LOGIN_3, MERLION_PASSWORD_3)
        transport3 = Transport(session=session3, timeout=REQUEST_TIMEOUT)
        client3 = Client(MERLION_API_URL_3, transport=transport3)
        
        # --- Получаем параметры для запроса цен ---
        logging.info("[Merlion] Получение методов и дат отгрузки...")
        shipment_methods = client3.service.getShipmentMethods()
        target_method_code = "С/В"
        method_to_use = next((m for m in shipment_methods if m.Code.strip().upper() == target_method_code), None)
        if not method_to_use:
            error_msg = f"Не найден метод отгрузки '{target_method_code}'!"
            logging.error(f"[Merlion] {error_msg}")
            await notify_export_error("Merlion", error_msg)
            return []
        shipment_method_code = method_to_use.Code
        shipment_dates = client3.service.getShipmentDates(ShipmentMethodCode=shipment_method_code)
        if not shipment_dates:
            error_msg = "Не найдены даты отгрузки!"
            logging.error(f"[Merlion] {error_msg}")
            await notify_export_error("Merlion", error_msg)
            return []
        shipment_date = shipment_dates[0].Date
        logging.info(f"[Merlion][PARAMS] shipment_method_code={shipment_method_code}, shipment_date={shipment_date}")
        
    except Exception as e:
        error_msg = f"Ошибка инициализации SOAP клиентов: {e}"
        logging.error(f"[Merlion] {error_msg}")
        await notify_export_error("Merlion", error_msg)
        return []

    # --- Получаем категории ---
    all_categories = get_all_categories(client2)  # Используем client2
    if not all_categories:
        error_msg = "Не удалось получить категории."
        logging.error(f"[Merlion] {error_msg}")
        await notify_export_error("Merlion", error_msg)
        return []
    logging.info(f"[Merlion][CATEGORIES] Всего {len(all_categories)} категорий для обработки.")
    
    parent_categories = set(all_categories.values())
    leaf_categories = [cat_id for cat_id in all_categories if cat_id not in parent_categories]
    categories_to_process = leaf_categories

    # --- Получаем товары из mlservice2 (с партномерами) ---
    logging.info("[Merlion] Получение товаров из mlservice2...")
    all_items = []
    total_items_with_part_numbers = 0
    total_items_without_part_numbers = 0
    
    for category_id in categories_to_process:
        # Проверяем лимит
        if limit and len(all_items) >= limit:
            logging.info(f"[Merlion][LIMIT] Достигнут лимит {limit} товаров, останавливаемся")
            break
            
        items_in_category = get_items_for_category(client2, category_id, error_summary)
        logging.info(f"[Merlion][CATEGORY:{category_id}] Получено {len(items_in_category)} товаров.")
        
        # Проверяем партномеры в категории
        for item in items_in_category:
            part_number = item.get("Партномер", "")
            if part_number and str(part_number).strip():
                total_items_with_part_numbers += 1
            else:
                total_items_without_part_numbers += 1
                logging.warning(f"[Merlion][DEBUG] Товар без партномера в категории {category_id}: ID={item.get('ID Товара (No)', 'N/A')}")
        
        if items_in_category:
            all_items.extend(items_in_category)
            
        # Дополнительная проверка лимита после добавления товаров
        if limit and len(all_items) >= limit:
            all_items = all_items[:limit]  # Обрезаем до лимита
            logging.info(f"[Merlion][LIMIT] Обрезали до {limit} товаров")
            break
    
    logging.info(f"[Merlion][DEBUG] mlservice2 - товаров с партномерами: {total_items_with_part_numbers}")
    logging.info(f"[Merlion][DEBUG] mlservice2 - товаров без партномеров: {total_items_without_part_numbers}")
    
    if not all_items:
        logging.warning("[Merlion] Нет товаров.")
        return []
    
    logging.info(f"[Merlion][ITEMS] Всего товаров получено: {len(all_items)}")
    
    # --- Создаем DataFrame и получаем цены из mlservice3 ---
    logging.info("[Merlion] Создаем DataFrame из mlservice2 данных...")
    df = pd.DataFrame(all_items)
    
    # Проверяем партномеры в DataFrame ДО mlservice3
    part_numbers_before_ml3 = df["Партномер"].dropna().apply(str).str.strip()
    valid_part_numbers_before = part_numbers_before_ml3[part_numbers_before_ml3 != ""]
    logging.info(f"[Merlion][DEBUG] DataFrame ДО mlservice3 - валидных партномеров: {len(valid_part_numbers_before)}")
    if len(valid_part_numbers_before) > 0:
        logging.info(f"[Merlion][DEBUG] Примеры партномеров ДО mlservice3: {valid_part_numbers_before.head(3).tolist()}")
    
    logging.info("[Merlion] Получаем цены и остатки из mlservice3...")
    df = add_prices_and_stock(df, client3, shipment_method_code, shipment_date, error_summary)
    
    # Проверяем партномеры в DataFrame ПОСЛЕ mlservice3
    part_numbers_after_ml3 = df["Партномер"].dropna().apply(str).str.strip()
    valid_part_numbers_after = part_numbers_after_ml3[part_numbers_after_ml3 != ""]
    logging.info(f"[Merlion][DEBUG] DataFrame ПОСЛЕ mlservice3 - валидных партномеров: {len(valid_part_numbers_after)}")
    if len(valid_part_numbers_after) > 0:
        logging.info(f"[Merlion][DEBUG] Примеры партномеров ПОСЛЕ mlservice3: {valid_part_numbers_after.head(3).tolist()}")
    
    # Проверяем, не изменились ли партномеры
    if len(valid_part_numbers_before) != len(valid_part_numbers_after):
        logging.warning(f"[Merlion][DEBUG] ВНИМАНИЕ! Количество партномеров изменилось: было {len(valid_part_numbers_before)}, стало {len(valid_part_numbers_after)}")
    
    logging.info(f"[Merlion][ITEMS] После получения цен и остатков: {len(df)}")
    
    # --- Приводим к формату для базы данных ---
    logging.info("[Merlion][FORMAT] Приводим данные к формату для БД")
    
    result = []
    skipped_no_part_number = 0
    
    for _, row in df.iterrows():
        # Пропускаем товары без партномеров
        part_number = str(row.get("Партномер", "") or "").strip()
        if not part_number:
            skipped_no_part_number += 1
            continue  # Пропускаем товар без партномера
        
        # Получаем остатки
        stock_val = row.get("Наличие (шт)", 0)
        if pd.notnull(stock_val):
            try:
                stock_val = float(stock_val)
                if stock_val.is_integer():
                    stock = str(int(stock_val))
                else:
                    stock = str(stock_val)
            except Exception:
                stock = '0'
        else:
            stock = '0'
        
        # Получаем цены
        price_rub = float(row.get("Цена (RUB)", 0)) if pd.notnull(row.get("Цена (RUB)", 0)) else 0
        price_usd = float(row.get("Цена (USD)", 0)) if pd.notnull(row.get("Цена (USD)", 0)) else 0
        
        # Создаем товар только если есть партномер
        product = {
            'id': str(row.get("ID Товара (No)", "")),
            'part_number': part_number,
            'name': str(row.get("Наименование", "") or ""),
            'brand': str(row.get("Бренд", "") or ""),
            'stock': stock,
            'price_usd': price_usd,
            'price_rub': price_rub,
            'price': price_rub,  # Основная цена в рублях для совместимости
            'article': str(row.get("ID Товара (No)", "") or ""),  # Используем ID как article
            'category_code': str(row.get("Код Категории", "") or ""),
            'package_volume': float(row.get("Объем", 0)) if pd.notnull(row.get("Объем", 0)) else 0,
            'package_weight': float(row.get("Вес", 0)) if pd.notnull(row.get("Вес", 0)) else 0,
            'tech_specs': str(row.get("Описание", "") or ""),
            'transit_date': None,
            'distributor': "Merlion",
            'is_active': True
        }
        result.append(product)
    
    logging.info(f"[Merlion][FILTER] Пропущено товаров без партномеров: {skipped_no_part_number}")
    logging.info(f"[Merlion][FILTER] Товаров с партномерами для БД: {len(result)}")
    
    # Убираем дублирующиеся партномеры
    unique_products = {}
    duplicates_removed = 0
    for product in result:
        part_number = product.get('part_number', '').strip()
        if part_number not in unique_products:
            unique_products[part_number] = product
        else:
            duplicates_removed += 1
            logging.warning(f"[Merlion][DEBUG] Дублирующийся партномер пропущен: {part_number}")
    
    result = list(unique_products.values())
    logging.info(f"[Merlion][DEDUP] Убрано дублирующихся товаров: {duplicates_removed}")
    logging.info(f"[Merlion][DEDUP] Уникальных товаров для вставки: {len(result)}")
    
    # Применяем лимит к финальному результату
    if limit and len(result) > limit:
        result = result[:limit]
        logging.info(f"[Merlion][LIMIT] Применен финальный лимит: {len(result)} товаров")
    
    # Проверяем наличие поля part_number в результате
    if result:
        sample_product = result[0]
        logging.info(f"[Merlion][DEBUG] Пример товара для upsert: {sample_product}")
        logging.info(f"[Merlion][DEBUG] Ключи товара: {list(sample_product.keys())}")
        logging.info(f"[Merlion][DEBUG] part_number в примере: '{sample_product.get('part_number', 'ОТСУТСТВУЕТ')}'")
        
        # Проверяем сколько товаров имеют part_number
        with_part_number = sum(1 for p in result if p.get('part_number'))
        without_part_number = len(result) - with_part_number
        logging.info(f"[Merlion][DEBUG] Товаров с part_number: {with_part_number}/{len(result)}")
        if without_part_number > 0:
            logging.warning(f"[Merlion][DEBUG] Товаров БЕЗ part_number: {without_part_number}")
    
    end_time = time.time()
    duration_seconds = end_time - start_time
    duration_minutes = duration_seconds / 60
    
    logging.info(f"[Merlion][END] Выгрузка завершена за {duration_seconds:.1f} сек ({duration_minutes:.1f} мин)")
    
    # УБИРАЕМ: Сохранение в базу данных (main.py уже делает upsert)
    # if result:
    #     try:
    #         from core.upsert import upsert_products
    #         from core.db_engine import AsyncSessionLocal
    #         
    #         logging.info(f"[Merlion][DB] Начинаем upsert для {len(result)} товаров...")
    #         
    #         # Создаем сессию базы данных
    #         async with AsyncSessionLocal() as db:
    #             await upsert_products(result, "Merlion", db)
    #         
    #         logging.info(f"[Merlion][DB] Upsert завершен успешно!")
    #     except Exception as e:
    #         error_msg = f"Ошибка при сохранении в БД: {e}"
    #         logging.error(f"[Merlion][DB] {error_msg}")
    #         await notify_export_error("Merlion", error_msg)
    
    return result

if __name__ == "__main__":
    asyncio.run(get_merlion_products()) 