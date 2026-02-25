# -*- coding: utf-8 -*-
import os
import time
import csv
import datetime
import json

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from zeep import Client
from zeep.exceptions import Fault
from zeep.helpers import serialize_object
from zeep.transports import Transport
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import logging
from core.telegram_notify import notify_export_error

# --- КОНФИГУРАЦИЯ ---

# WSDL URL для API v3 (document/literal style)
MERLION_API_URL = "https://api.merlion.com/dl/mlservice3?wsdl"

# Учетные данные Merlion API
MERLION_CLIENT_ID = "YOUR_MERLION_CLIENT_ID"
MERLION_LOGIN = "API" 
MERLION_PASSWORD = "YOUR_MERLION_PASSWORD_ML3"

# Формируем логин для аутентификации в формате "КодКлиента|Логин"
AUTH_LOGIN = f"{MERLION_CLIENT_ID}|{MERLION_LOGIN}"

# ОПТИМИЗАЦИЯ: Увеличиваем размеры для ускорения
ITEMS_PER_PAGE = 5000  # Возвращаем как было (было 10000)
PRICES_PER_BATCH = 500  # Возвращаем как было (было 1000)
OUTPUT_FILE = "Full_Database.xlsx"
REQUEST_TIMEOUT = 300  # 5 минут таймаут для запросов
MAX_RETRIES = 3  # Максимальное количество повторных попыток
RETRY_DELAY = 5  # Задержка между попытками в секундах

# ОПТИМИЗАЦИЯ: Увеличиваем количество потоков
MAX_CATEGORY_THREADS = 12  # Возвращаем как было (было 16)
MAX_PRICE_THREADS = 12     # Возвращаем как было (было 16)

# --- ТЕСТОВЫЙ РЕЖИМ ---
# Установите True, чтобы выгрузить только небольшую часть данных для быстрой проверки
TEST_MODE = False

FORCE_UPDATE_CATEGORIES = False  # Установите True, чтобы принудительно обновить категории из API
CATEGORIES_FILE = "categories.csv"

# --- NEW: Автоматическая проверка возраста файла категорий ---
CATEGORIES_MAX_AGE_DAYS = 7  # Максимальный возраст файла категорий (в днях)
if os.path.exists(CATEGORIES_FILE):
    file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(CATEGORIES_FILE))
    if (datetime.datetime.now() - file_mtime).days >= CATEGORIES_MAX_AGE_DAYS:
        print(f"Файл {CATEGORIES_FILE} старше {CATEGORIES_MAX_AGE_DAYS} дней. Категории будут обновлены из API.")
        FORCE_UPDATE_CATEGORIES = True

# --- ЛОГИРОВАНИЕ ---
LOG_FILE = os.path.join("logs", "update_distributors_merlion.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    encoding='utf-8'
)
# Добавить вывод в консоль:
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
        # Диагностика удалена, используем правильные поля
        for cat in response:
            if not cat.ID or cat.ID in categories:
                continue  # Пропускать None и уже обработанные
            categories[cat.ID] = cat.ID_PARENT
            time.sleep(1)  # Задержка для обхода лимита API
            fetch_recursively(cat.ID)
    fetch_recursively("All")
    logging.info(f"[Merlion][CATEGORIES] Всего найдено {len(categories)} категорий.")
    logging.info(f"[Merlion][CATEGORIES] Сохраняю в {CATEGORIES_FILE}...")
    with open(CATEGORIES_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for k, v in categories.items():
            writer.writerow([k, v])
    return categories

def get_items_for_category(client, category_id, error_summary=None):
    """Получает все товары для указанной категории с постраничной навигацией."""
    items = []
    page = 0  # ОПТИМИЗАЦИЯ: Начинаем с 0 (согласно API)
    total_for_cat = 0
    
    while True:
        response = None
        for attempt in range(MAX_RETRIES):
            try:
                logging.info(f"[Merlion][CATEGORY:{category_id}] Запрос страницы: {page} (попытка {attempt+1})")
                
                # ОПТИМИЗАЦИЯ: Уменьшаем задержку между запросами
                time.sleep(0.5)  # Возвращаем как было (было 0.1 сек)

                response = client.service.getItems(
                    cat_id=category_id,
                    page=page,
                    rows_on_page=ITEMS_PER_PAGE
                )
                
                # Если ответ получен, выходим из цикла попыток
                if response is not None:
                    break

            except Fault as e:
                logging.error(f"[Merlion][CATEGORY:{category_id}] SOAP ошибка: {e}")
                if "Rate limit exceeded" in str(e):
                    current_delay = RETRY_DELAY * (attempt + 1)  # ОПТИМИЗАЦИЯ: Уменьшили агрессивность
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
        
        # Если после всех попыток ответа нет, или ответ пустой - завершаем
        if not response:
            logging.warning(f"[Merlion][CATEGORY:{category_id}] Страница {page}: получено 0 товаров. Завершение для этой категории.")
            return items

        num_items_received = len(response)
        total_for_cat += num_items_received
        logging.info(f"[Merlion][CATEGORY:{category_id}] Страница {page}: получено {num_items_received} товаров.")

        for item in response:
            items.append({
                "ID Товара (No)": item.No,
                "Наименование": item.Name,
                "Бренд": item.Brand,
                "Партномер": item.Vendor_part,
                "Код Категории": category_id,
                "Описание": getattr(item, "Description", None),
                "Гарантия": getattr(item, "Warranty", None),
                "Вес": getattr(item, "Weight", None),
                "Объём": getattr(item, "Volume", None),
            })

        # ОПТИМИЗАЦИЯ: Если товаров получено меньше, чем размер страницы, значит это последняя страница
        if num_items_received < ITEMS_PER_PAGE:
            logging.info(f"[Merlion][CATEGORY:{category_id}] Всего товаров по категории: {total_for_cat}")
            return items

        page += 1

def add_prices_and_stock(df, client, shipment_method_code, shipment_date, error_summary=None):
    print(f"Этап 4 из 4: Получение цен и остатков ТОЛЬКО с основного склада (С/В)...")
    if df.empty:
        print("Нет товаров для обновления цен.")
        return df
    
    df["Цена (RUB)"] = None
    df["Наличие (шт)"] = None
    
    # Получаем все доступные методы отгрузки
    try:
        all_shipment_methods = client.service.getShipmentMethods()
        print(f"Доступные методы отгрузки: {[m.Code for m in all_shipment_methods]}")
    except Exception as e:
        error_msg = f"Ошибка получения методов отгрузки: {e}"
        print(f"{error_msg}")
        if error_summary is not None:
            error_summary.append(error_msg)
        all_shipment_methods = [type('obj', (object,), {'Code': shipment_method_code})]
    
    item_ids = df["ID Товара (No)"].dropna().unique().tolist()
    
    # ОПТИМИЗАЦИЯ: Увеличиваем размеры батчей для ускорения
    max_batch_size = 500  # Возвращаем как было (было 1000)
    min_batch_size = 100  # Возвращаем как было (было 200)
    batch_size = max_batch_size
    
    # ОПТИМИЗАЦИЯ: Уменьшаем задержки для ускорения
    min_pause = 0.5  # Возвращаем как было (было 0.2 сек)
    max_pause = 5    # Возвращаем как было (было 3 сек)
    pause = min_pause
    
    success_count = 0
    restore_threshold = 5
    
    # Словарь для хранения остатков ТОЛЬКО с основного склада
    main_stock = {}
    total_prices = {}
    total_prices_usd = {} # Новый словарь для цен в USD
    
    # Обрабатываем ТОЛЬКО основной метод отгрузки (С/В) - как в Netlab
    print(f"Обрабатываем ТОЛЬКО основной метод отгрузки: {shipment_method_code}")
    
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
                response = client.service.getItemsAvail(
                    item_id={'item': batch},
                    shipment_method=shipment_method_code,  # ТОЛЬКО основной склад
                    shipment_date=shipment_date,
                    only_avail="0"
                )
                
                if response:
                    result_dict = serialize_object(response)
                    
                    for item in result_dict:
                        if item.get('No'):
                            item_id = item['No']
                            
                            # Берем остатки ТОЛЬКО с основного склада (С/В) - НЕ СУММИРУЕМ!
                            current_stock = item.get("AvailableClient", 0)
                            if current_stock is not None and current_stock != "":
                                try:
                                    current_stock = int(current_stock)
                                except (ValueError, TypeError):
                                    current_stock = 0
                                
                                # Сохраняем остатки ТОЛЬКО с основного склада
                                main_stock[item_id] = current_stock
                            
                            # Сохраняем цену в USD и RUB отдельно
                            usd_price = item.get("PriceClient")
                            rub_price = item.get("PriceClientRUB")
                            if usd_price not in [None, 0, "0", "0.0", ""]:
                                total_prices_usd[item_id] = usd_price
                            if rub_price not in [None, 0, "0", "0.0", ""]:
                                total_prices[item_id] = rub_price
                        
                    print(f"[{shipment_method_code}] Батч {current_batch} успешно обработан")
                    success_count += 1
                    
                    # ОПТИМИЗАЦИЯ: Восстанавливаем размер батча при успехе
                    if success_count >= restore_threshold and batch_size < max_batch_size:
                        batch_size = min(batch_size * 2, max_batch_size)
                        pause = max(pause / 2, min_pause)
                        print(f"Восстанавливаю batch_size до {batch_size}, уменьшаю pause до {pause}")
                        success_count = 0
                    
                    break
                else:
                    print(f"Пустой ответ от API для метода {shipment_method_code}, попытка {attempt+1}")
                    success_count = 0
                    time.sleep(pause)
                    
            except Exception as e:
                error_msg = f"Ошибка при запросе цен/остатков для метода {shipment_method_code} (batch_size={batch_size}, попытка {attempt+1}): {e}"
                print(f"{error_msg}")
                if error_summary is not None:
                    error_summary.append(error_msg)
                if attempt == 2 and batch_size > min_batch_size:
                    print(f"Уменьшаю размер батча до {batch_size // 2}")
                    batch_size = max(batch_size // 2, min_batch_size)
                    pause = min(pause * 2, max_pause)
                    print(f"Уменьшаю batch_size до {batch_size}, увеличиваю pause до {pause}")
                    success_count = 0
                    time.sleep(pause)
        
        i = end
        # ОПТИМИЗАЦИЯ: Минимальная задержка между батчами
        time.sleep(min_pause)
    
    # Применяем остатки ТОЛЬКО с основного склада к DataFrame
    for item_id, stock in main_stock.items():
        df.loc[df["ID Товара (No)"] == item_id, "Наличие (шт)"] = stock
    
    for item_id, price in total_prices.items():
        df.loc[df["ID Товара (No)"] == item_id, "Цена (RUB)"] = price
    for item_id, price_usd in total_prices_usd.items():
        df.loc[df["ID Товара (No)"] == item_id, "Цена (USD)"] = price_usd
    
    print(f"Остатки с основного склада (С/В): {len(main_stock)}")
    
    # Логируем примеры остатков с основного склада
    sample_items = list(main_stock.items())[:5]
    for item_id, stock_val in sample_items:
        print(f"Товар {item_id}: остаток с основного склада = {stock_val}")
    
    return df

def main():
    """Главная функция для запуска процесса выгрузки."""
    print("Запуск процесса выгрузки полной базы из Merlion API v3...")
    start_time = time.time()

    # --- Этап 0: Подготовка клиента Zeep ---
    try:
        # Настройка сессии requests с HTTP Basic Authentication
        session = requests.Session()
        session.auth = HTTPBasicAuth(AUTH_LOGIN, MERLION_PASSWORD)
        
        # Настройка транспорта с сессией и таймаутом
        transport = Transport(session=session, timeout=REQUEST_TIMEOUT)
        
        # Создание клиента
        client = Client(MERLION_API_URL, transport=transport)
        
        print("Клиент SOAP успешно создан.")
        
        # --- Получение параметров для запроса цен ---
        print("Получение доступных методов и дат отгрузки...")
        shipment_methods = client.service.getShipmentMethods()
        
        if not shipment_methods:
            print("Не удалось получить методы отгрузки. Проверьте учетные данные и доступ к API.")
            return

        # Ищем метод отгрузки 'С/В', как указано в требованиях.
        target_method_code = "С/В"
        
        # Для информации выводим все доступные методы
        print("Доступные методы отгрузки:")
        for m in shipment_methods:
            print(f"  - Код: {m.Code}, Наименование: {m.Description}, По умолчанию: {'Да' if m.IsDefault else 'Нет'}")
            
        method_to_use = next((m for m in shipment_methods if m.Code.strip().upper() == target_method_code), None)
        
        if method_to_use:
            shipment_method_code = method_to_use.Code
            print(f"\nВыбран метод отгрузки: '{method_to_use.Description}' (Код: {shipment_method_code})")
        else:
            print(f"\nВНИМАНИЕ: Метод отгрузки с кодом '{target_method_code}' не найден!")
            print("Пожалуйста, проверьте доступные методы и скорректируйте 'target_method_code' в скрипте.")
            return

        shipment_dates = client.service.getShipmentDates(ShipmentMethodCode=shipment_method_code)
        if not shipment_dates:
            print(f"Не удалось получить даты отгрузки для метода '{shipment_method_code}'.")
            return
            
        # Берем первую доступную дату (обычно это сегодня)
        shipment_date = shipment_dates[0].Date

    except Exception as e:
        print(f"Критическая ошибка при инициализации SOAP клиента: {e}")
        return

    # --- Этап 1: Определение списка категорий для обработки ---
    output_filename = OUTPUT_FILE
    if TEST_MODE:
        print(f"\n*** РАБОТА В ТЕСТОВОМ РЕЖИМЕ. Будут обработаны категории, рекомендованные Merlion. ***\n")
        # В тестовом режиме используем категории, указанные поддержкой Merlion (ML41, ML03)
        categories_to_process = ['ML41', 'ML03'] 
        output_filename = f"Test_{OUTPUT_FILE}"
        print(f"Будут обработаны категории: {categories_to_process}")
    else:
        # --- Этап 1: Сбор всех категорий ---
        all_categories = get_all_categories(client)
        if not all_categories:
            print("Не удалось получить категории. Завершение работы.")
            return

        # --- Этап 2: Определение "листовых" категорий (в которых есть товары) ---
        print("\nЭтап 2 из 4: Определение конечных категорий...")
        parent_categories = set(all_categories.values())
        leaf_categories = [cat_id for cat_id in all_categories if cat_id not in parent_categories]
        categories_to_process = leaf_categories
        print(f"Найдено {len(categories_to_process)} конечных категорий для выгрузки товаров.")


    # --- Этап 3: Получение товаров для каждой категории ---
    print(f"\nЭтап 3 из 4: Получение товаров из {len(categories_to_process)} категорий...")
    all_items = []
    
    # Используем общий прогресс-бар для категорий
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    def get_all_items_parallel(client, categories):
        all_items = []
        with ThreadPoolExecutor(max_workers=MAX_CATEGORY_THREADS) as executor:
            futures = {executor.submit(get_items_for_category, client, cat_id, error_summary): cat_id for cat_id in categories}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Категории"):
                items = future.result()
                if items:
                    all_items.extend(items)
        return all_items
    
    # В main замените цикл по категориям:
    # all_items = []
    # for category_id in tqdm(categories_to_process, desc="Загрузка категорий"):
    #     items_in_category = get_items_for_category(client, category_id)
    #     if items_in_category:
    #         all_items.extend(items_in_category)
    # НА:
    all_items = get_all_items_parallel(client, categories_to_process)
        
        # Убрана задержка между категориями для ускорения

    if not all_items:
        print("Не найдено ни одного товара. Проверьте правильность категорий или доступ к API.")
        return

    # Создаем DataFrame из всех полученных товаров
    df = pd.DataFrame(all_items)
    
    # --- Этап 4: Добавление цен и остатков ---
    df = add_prices_and_stock(df, client, shipment_method_code, shipment_date, error_summary)

    # --- Этап 5: Сохранение в Excel ---
    print(f"\nЭтап 5 из 5: Сохранение данных в файл {output_filename}...")
    try:
        df.to_excel(output_filename, index=False, engine='openpyxl')
        print(f"Успешно сохранено! Файл находится здесь: {os.path.abspath(output_filename)}")
        # Дополнительно сохраняем в CSV
        csv_filename = output_filename.replace('.xlsx', '.csv')
        df.to_csv(csv_filename, index=False, encoding='utf-8')
        print(f"Также сохранено в CSV: {os.path.abspath(csv_filename)}")
    except Exception as e:
        error_msg = f"Не удалось сохранить файл Excel/CSV: {e}"
        print(f"{error_msg}")
        error_summary.append(error_msg)

    end_time = time.time()
    print(f"Выгрузка завершена. Общее время выполнения: {time.strftime('%H:%M:%S', time.gmtime(end_time - start_time))}")
    
    # ИТОГОВЫЙ ОТЧЕТ ОБ ОШИБКАХ
    if error_summary:
        print(f"\n{'='*60}")
        print("ИТОГОВЫЙ ОТЧЕТ ОБ ОШИБКАХ:")
        print(f"{'='*60}")
        print(f"Всего ошибок: {len(error_summary)}")
        for i, error in enumerate(error_summary, 1):
            print(f"{i}. {error}")
        print(f"{'='*60}")
    else:
        print(f"\n{'='*60}")
        print("ОШИБОК НЕ ОБНАРУЖЕНО! ✅")
        print(f"{'='*60}")

KNOWN_IDS_FILE = os.path.join("static", "known_merlion_ids.json")
LAST_UPDATE_FILE = os.path.join("static", "last_update_merlion.txt")

async def get_merlion_products():
    """Возвращает список товаров Merlion в виде dict для загрузки в базу данных. Реализует diff-выгрузку по новым/изменённым товарам."""
    logging.info("[Merlion][START] Выгрузка начата")
    start_time = time.time()
    
    # Сбор ошибок для итогового отчета
    error_summary = []
    
    # Уведомление о начале экспорта отправляется в main.py, убираем дублирование
    
    # --- Этап 0: Подготовка клиента Zeep ---
    try:
        session = requests.Session()
        session.auth = HTTPBasicAuth(AUTH_LOGIN, MERLION_PASSWORD)
        transport = Transport(session=session, timeout=REQUEST_TIMEOUT)
        client = Client(MERLION_API_URL, transport=transport)
        shipment_methods = client.service.getShipmentMethods()
        target_method_code = "С/В"
        method_to_use = next((m for m in shipment_methods if m.Code.strip().upper() == target_method_code), None)
        if not method_to_use:
            error_msg = f"Не найден метод отгрузки '{target_method_code}'!"
            logging.error(f"[Merlion] {error_msg}")
            await notify_export_error("Merlion", error_msg)
            return []
        shipment_method_code = method_to_use.Code
        shipment_dates = client.service.getShipmentDates(ShipmentMethodCode=shipment_method_code)
        if not shipment_dates:
            error_msg = "Не найдены даты отгрузки!"
            logging.error(f"[Merlion] {error_msg}")
            await notify_export_error("Merlion", error_msg)
            return []
        shipment_date = shipment_dates[0].Date
        logging.info(f"[Merlion][PARAMS] shipment_method_code={shipment_method_code}, shipment_date={shipment_date}")
    except Exception as e:
        error_msg = f"Ошибка инициализации SOAP клиента: {e}"
        logging.error(f"[Merlion] {error_msg}")
        await notify_export_error("Merlion", error_msg)
        return []

    # --- Категории ---
    all_categories = get_all_categories(client)
    if not all_categories:
        error_msg = "Не удалось получить категории."
        logging.error(f"[Merlion] {error_msg}")
        await notify_export_error("Merlion", error_msg)
        return []
    logging.info(f"[Merlion][CATEGORIES] Всего {len(all_categories)} категорий для обработки.")
    parent_categories = set(all_categories.values())
    leaf_categories = [cat_id for cat_id in all_categories if cat_id not in parent_categories]
    categories_to_process = leaf_categories

    # --- Получение товаров ---
    all_items = []
    for category_id in categories_to_process:
        # Если API поддерживает фильтрацию по дате, добавь параметр last_update/modified_date:
        # items_in_category = get_items_for_category(client, category_id, last_update=last_update)
        # Если нет — оставь как есть:
        items_in_category = get_items_for_category(client, category_id, error_summary)
        logging.info(f"[Merlion][CATEGORY:{category_id}] Получено {len(items_in_category)} товаров.")
        if items_in_category:
            all_items.extend(items_in_category)
    if not all_items:
        logging.warning("[Merlion] Нет товаров.")
        return []
    logging.info(f"[Merlion][ITEMS] Всего товаров до фильтрации: {len(all_items)}")
    df = pd.DataFrame(all_items)
    df = add_prices_and_stock(df, client, shipment_method_code, shipment_date, error_summary)
    logging.info(f"[Merlion][ITEMS] После получения цен и остатков: {len(df)}")
    
    # --- ПРИВЕДЕНИЕ К ФОРМАТУ NETLAB ---
    logging.info("[Merlion][FORMAT] Приводим данные к формату Netlab для корректного импорта в БД")
    
    result = []
    for _, row in df.iterrows():
        # Получаем остатки ТОЛЬКО с основного склада (С/В)
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
        
        # Очищаем партномер
        part_number = str(row.get("Партномер", "") or "").strip()
        if not part_number or part_number.lower() in ['null', 'none', 'undefined', '']:
            logging.warning(f"[Merlion][DEBUG] Пропущен товар с невалидным партномером '{part_number}': ID={row.get('ID Товара (No)', 'N/A')}")
            continue
        
        # Создаем товар в формате Netlab
        product = {
            'id': str(row.get("ID Товара (No)", "")),
            'part_number': part_number,
            'name': str(row.get("Наименование", "") or ""),
            'brand': str(row.get("Бренд", "") or ""),
            'stock': stock,  # строка, как в Netlab
            'price_usd': price_usd,  # число
            'price_rub': price_rub,  # число
            'article': str(row.get("Артикул", "") or ""),
            'category_code': str(row.get("Код категории", "") or ""),
            'package_volume': float(row.get("Объем упаковки", 0)) if pd.notnull(row.get("Объем упаковки", 0)) else 0,
            'package_weight': float(row.get("Вес упаковки", 0)) if pd.notnull(row.get("Вес упаковки", 0)) else 0,
            'tech_specs': str(row.get("Тех. характеристики", "") or ""),
            'transit_date': row.get("Дата транзита", None),
            'distributor': "Merlion",
            'is_active': True
        }
        result.append(product)
    
    # Убираем дублирующиеся партномеры (как в Netlab)
    unique_products = {}
    duplicates_removed = 0
    for product in result:
        part_number = product.get('part_number', '').strip()
        if part_number:
            if part_number not in unique_products:
                unique_products[part_number] = product
            else:
                duplicates_removed += 1
                logging.warning(f"[Merlion][DEBUG] Дублирующийся партномер пропущен: {part_number}")
    
    result = list(unique_products.values())
    logging.info(f"[Merlion][DEDUP] Убрано дублирующихся товаров: {duplicates_removed}")
    logging.info(f"[Merlion][DEDUP] Уникальных товаров для вставки: {len(result)}")
    
    # Товары собраны, вставка происходит в main.py
    logging.info(f"[Merlion][COLLECTED] Товары собраны для вставки в базу данных")
    
    # Уведомление о завершении отправляется в main.py, убираем дублирование
    end_time = time.time()
    duration_seconds = end_time - start_time
    duration_minutes = duration_seconds / 60
    
    logging.info(f"[Merlion][END] Выгрузка завершена за {duration_seconds:.1f} сек ({duration_minutes:.1f} мин)")
    return result

if __name__ == "__main__":
    main()