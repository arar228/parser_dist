import logging
import os
logging.basicConfig(
    filename=os.path.join("logs", "update_distributors_resursmedio.log"),
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
import requests
from requests.auth import HTTPBasicAuth
from lxml import etree
import csv
import os
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
import re
import textwrap
import base64
import json
from core.telegram_notify import send_telegram_message

# --- НАСТРОЙКИ ---
URL = "https://api.resurs-media.ru/resursmedia/ws/WSAPI"
CREDENTIALS = ("ARMK", "pD7qPg")
MAX_THREADS = 12
MIN_BATCH_SIZE = 50
MAX_BATCH_SIZE = 500
BATCH_SIZE = MAX_BATCH_SIZE
PAUSE_BETWEEN_BATCHES = 1.0

GET_MATERIAL_DATA_FULL_BODY = """
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="http://resurs-media.ru/api">
   <soapenv:Header/>
   <soapenv:Body>
      <api:GetMaterialData>
         <api:MaterialID_Tab/>
         <api:MaterialGroup_Tab/>
         <api:VendorPart_Tab/>
         <api:WithCharacteristics>true</api:WithCharacteristics>
         <api:WithBarCodes>true</api:WithBarCodes>
         <api:WithCertificates>true</api:WithCertificates>
         <api:WithImages>true</api:WithImages>
      </api:GetMaterialData>
   </soapenv:Body>
</soapenv:Envelope>
"""

GET_PRICES_BODY_TEMPLATE = """
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="http://resurs-media.ru/api">
  <soapenv:Header/>
  <soapenv:Body>
    <api:GetPrices>
      <api:WareHouseID>{warehouse_id}</api:WareHouseID>
      <api:MaterialID_Tab>{material_ids}</api:MaterialID_Tab>
      <api:MaterialGroup_Tab></api:MaterialGroup_Tab>
      <api:GetAvailableCount>true</api:GetAvailableCount>
    </api:GetPrices>
  </soapenv:Body>
</soapenv:Envelope>
"""

GET_MATERIAL_DATA_BY_ID_BODY_TEMPLATE = """
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="http://resurs-media.ru/api">
  <soapenv:Header/>
  <soapenv:Body>
    <api:GetMaterialData>
      <api:MaterialID_Tab>{material_ids_xml}</api:MaterialID_Tab>
      <api:MaterialGroup_Tab/>
      <api:VendorPart_Tab/>
      <api:WithCharacteristics>true</api:WithCharacteristics>
      <api:WithBarCodes>true</api:WithBarCodes>
      <api:WithCertificates>true</api:WithCertificates>
      <api:WithImages>true</api:WithImages>
    </api:GetMaterialData>
  </soapenv:Body>
</soapenv:Envelope>
"""

def parse_result_and_wait(root, soap_action):
    """Обрабатывает Result=3/4/1/0 в ответе ResursMedio. Если Result=3 — ждет нужный интервал и возвращает False (повторить), если 4 — бросает исключение."""
    result_node = root.find('.//{http://resurs-media.ru/api}Result')
    if result_node is not None:
        result_code = int(result_node.text)
        error_msg_node = root.find('.//{http://resurs-media.ru/api}ErrorMessage')
        error_msg = error_msg_node.text if error_msg_node is not None else ''
        if result_code == 3:
            wait_seconds = extract_seconds_from_error(error_msg)
            logging.warning(f"[ResursMedio][{soap_action}] Превышен лимит вызова. Ждем {wait_seconds} сек. Сообщение: {error_msg}")
            time.sleep(wait_seconds)
            return False  # Повторить попытку
        elif result_code == 4:
            send_telegram_message(f"[ResursMedio][{soap_action}] API отключен. {error_msg}")
            logging.error(f"[ResursMedio][{soap_action}] API отключен. {error_msg}")
            raise Exception(f"API отключен: {error_msg}")
        elif result_code == 1:
            send_telegram_message(f"[ResursMedio][{soap_action}] Ошибка: {error_msg}")
            logging.error(f"[ResursMedio][{soap_action}] Ошибка: {error_msg}")
            raise Exception(f"Ошибка API: {error_msg}")
        elif result_code == 0:
            return True  # Всё хорошо
    return True  # Если нет поля Result, считаем что всё хорошо

def extract_seconds_from_error(error_msg):
    m = re.search(r'(\d+) сек', error_msg)
    return int(m.group(1)) if m else 60

# Модифицируем make_api_call для поддержки повторов при Result=3

def make_api_call(soap_body, soap_action, max_retries=5):
    headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": soap_action}
    for attempt in range(max_retries):
        try:
            response = requests.post(URL, headers=headers, data=soap_body.encode('utf-8'), auth=CREDENTIALS, timeout=60)
            response.raise_for_status()
            if 'soap:Fault' in response.text:
                logging.warning(f"API вернуло SOAP Fault ({soap_action})")
                return None
            root = etree.fromstring(response.content)
            # Явная обработка Result=3/4
            should_continue = parse_result_and_wait(root, soap_action)
            if should_continue:
                return root
            # Если Result=3 — повторяем попытку после паузы (см. parse_result_and_wait)
        except Exception as e:
            logging.error(f"Ошибка при вызове API ({soap_action}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1))
            else:
                return None
    return None

def get_first_warehouse_id():
    get_clients_body = """<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:api=\"http://resurs-media.ru/api\"><soapenv:Header/><soapenv:Body><api:GetClientsAvail/></soapenv:Body></soapenv:Envelope>"""
    root = make_api_call(get_clients_body, "http://resurs-media.ru/api#WSAPI:GetClientsAvail")
    if root is None: return None
    client_id_element = root.find('.//{http://resurs-media.ru/api}ClientID')
    if client_id_element is None:
        logging.error("Не удалось найти ClientID.")
        return None
    client_id = client_id_element.text
    get_warehouses_body = f"""<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:api=\"http://resurs-media.ru/api\"><soapenv:Header/><soapenv:Body><api:GetWareHouses><api:ClientID>{client_id}</api:ClientID></api:GetWareHouses></soapenv:Body></soapenv:Envelope>"""
    root = make_api_call(get_warehouses_body, "http://resurs-media.ru/api#WSAPI:GetWareHouses")
    if root is None: return None
    warehouse_id_element = root.find('.//{http://resurs-media.ru/api}WareHouseID')
    if warehouse_id_element is None:
        logging.warning("Не найдено ни одного склада.")
        return None
    return warehouse_id_element.text

def save_to_csv_incremental(data_iter, headers, filename):
    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    full_path = os.path.join(desktop, filename)
    with open(full_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in data_iter:
            writer.writerow(row)
    logging.info(f"Данные сохранены в файл: {full_path}")

def get_material_descriptions():
    root = make_api_call(GET_MATERIAL_DATA_FULL_BODY, "http://resurs-media.ru/api#WSAPI:GetMaterialData")
    desc_headers = ["MaterialID", "MaterialText", "PartNum", "Vendor", "MaterialGroup", "Weight", "Volume"]
    descriptions_data = {}
    if root is not None:
        items = root.findall('.//{http://resurs-media.ru/api}MaterialData_Tab/{http://resurs-media.ru/api}Item')
        for item in items:
            item_data = {}
            for h in desc_headers:
                node = item.find(f'{{http://resurs-media.ru/api}}{h}')
                item_data[h] = node.text if node is not None else ''
            mid = item_data["MaterialID"]
            descriptions_data[mid] = item_data
    else:
        logging.error("Ошибка при получении описаний.")
    return descriptions_data, desc_headers

def get_prices_for_batch(warehouse_id, batch, price_headers):
    ids_xml = ''.join([f'<api:MaterialID>{mid}</api:MaterialID>' for mid in batch])
    body = GET_PRICES_BODY_TEMPLATE.format(warehouse_id=warehouse_id, material_ids=ids_xml)
    batch_size = len(batch)
    for attempt in range(3):
        root = make_api_call(body, "http://resurs-media.ru/api#WSAPI:GetPrices")
        if root is not None:
            items = root.findall('.//{http://resurs-media.ru/api}Material_Tab')
            batch_prices = {}
            for item in items:
                item_data = {}
                for h in price_headers:
                    node = item.find(f'{{http://resurs-media.ru/api}}{h}')
                    item_data[h] = node.text if node is not None else ''
                mid = item_data["MaterialID"]
                batch_prices[mid] = item_data
            return batch_prices
        else:
            logging.warning(f"[ResursMedio] Ошибка при получении батча, попытка {attempt+1}/3. Пауза...")
            time.sleep(PAUSE_BETWEEN_BATCHES * (attempt+1) + random.uniform(0.2, 0.5))
    return {}

def get_prices_batches(warehouse_id, material_ids):
    price_headers = ["MaterialID", "PartNum", "Price", "PriceUSD", "AvailableCount"]
    prices_data = {}
    batch_size = MAX_BATCH_SIZE
    batches = [material_ids[i:i+batch_size] for i in range(0, len(material_ids), batch_size)]
    logging.info(f"[ResursMedio] Получение цен параллельно, батчей: {len(batches)}")
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(get_prices_for_batch, warehouse_id, batch, price_headers) for batch in batches]
        for i, future in enumerate(as_completed(futures), 1):
            batch_prices = future.result()
            prices_data.update(batch_prices)
            logging.info(f"[ResursMedio] Обработан батч {i}/{len(batches)} (товаров: {len(batch_prices)})")
    return prices_data, price_headers

def get_all_prices(warehouse_id):
    body = GET_PRICES_BODY_TEMPLATE.format(warehouse_id=warehouse_id)
    root = make_api_call(body, "http://resurs-media.ru/api#WSAPI:GetPrices")
    price_headers = ["MaterialID", "PartNum", "Price", "PriceUSD", "AvailableCount"]
    prices_data = {}
    if root is not None:
        items = root.findall('.//{http://resurs-media.ru/api}Material_Tab')
        for item in items:
            item_data = {}
            for h in price_headers:
                node = item.find(f'{{http://resurs-media.ru/api}}{h}')
                item_data[h] = node.text if node is not None else ''
            mid = item_data["MaterialID"]
            prices_data[mid] = item_data
    else:
        logging.error("Ошибка при получении цен.")
    return prices_data, price_headers

def get_material_data_by_ids(material_ids):
    if not material_ids:
        return {}
    items_xml = ''.join(f'<api:Item><api:MaterialID>{mid}</api:MaterialID></api:Item>' for mid in material_ids)
    body = GET_MATERIAL_DATA_BY_ID_BODY_TEMPLATE.format(material_ids_xml=items_xml)
    root = make_api_call(body, "http://resurs-media.ru/api#WSAPI:GetMaterialData")
    desc_headers = ["MaterialID", "MaterialText", "PartNum", "Vendor", "MaterialGroup", "Weight", "Volume"]
    descriptions_data = {}
    if root is not None:
        items = root.findall('.//{http://resurs-media.ru/api}MaterialData_Tab/{http://resurs-media.ru/api}Item')
        for item in items:
            item_data = {}
            for h in desc_headers:
                node = item.find(f'{{http://resurs-media.ru/api}}{h}')
                item_data[h] = node.text if node is not None else ''
            mid = item_data["MaterialID"]
            descriptions_data[mid] = item_data
    else:
        logging.error("Ошибка при получении описаний по новым ID.")
    return descriptions_data

async def get_resursmedio_products():
    logging.info(f"[ResursMedio] === СТАРТ ЭКСПОРТА ДАННЫХ ===")
    start_time = datetime.now()
    try:
        logging.info(f"[ResursMedio] Получение ID склада...")
        warehouse_id = get_first_warehouse_id()
        logging.info(f"[ResursMedio] warehouse_id: {warehouse_id}")
        if not warehouse_id:
            logging.error("[ResursMedio][ERROR] Не удалось получить ID склада. Выход.")
            return []
        logging.info(f"[ResursMedio] Получение описаний товаров (GetMaterialData)...")
        descriptions_data, desc_headers = get_material_descriptions()
        logging.info(f"[ResursMedio] Описаний получено: {len(descriptions_data)}")
        logging.info(f"[ResursMedio] Получение цен и остатков (GetPrices)...")
        prices_data, price_headers = get_all_prices(warehouse_id)
        logging.info(f"[ResursMedio] Цен получено: {len(prices_data)}")
        all_ids = set(descriptions_data.keys()) | set(prices_data.keys())
        logging.info(f"[ResursMedio] Всего уникальных товаров для объединения: {len(all_ids)}")
        products = []
        for mid in all_ids:
            row = {}
            if mid in descriptions_data:
                row.update(descriptions_data[mid])
            if mid in prices_data:
                for h in price_headers:
                    if h not in row:
                        row[h] = prices_data[mid][h]
            product = {
                'id': mid,
                'name': row.get('MaterialText', ''),
                'price': row.get('Price', ''),
                'part_number': row.get('PartNum', ''),
                'vendor': row.get('Vendor', ''),
                'category_code': row.get('MaterialGroup', ''),
                'price_usd': row.get('PriceUSD', ''),
                'available_count': row.get('AvailableCount', ''),
            }
            products.append(product)
        logging.info(f"[ResursMedio] Экспорт завершён. Товаров: {len(products)}")
        try:
            with open("static/last_update_resursmedio.txt", "w", encoding="utf-8") as f:
                f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            logging.info(f"[ResursMedio] Дата синхронизации успешно записана.")
        except Exception as e:
            logging.error(f"[ResursMedio][ERROR] Ошибка записи даты синхронизации: {e}")
        logging.info(f"[ResursMedio] === КОНЕЦ ЭКСПОРТА. Время выполнения: {datetime.now() - start_time} ===")
        return products
    except Exception as e:
        logging.error(f"[ResursMedio][FATAL ERROR] Исключение в get_resursmedio_products: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_notifications(from_date=None):
    # SOAP-запрос Notification
    if from_date:
        body = f'''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="http://resurs-media.ru/api"><soapenv:Header/><soapenv:Body><api:Notification><api:FromDate>{from_date}</api:FromDate></api:Notification></soapenv:Body></soapenv:Envelope>'''
    else:
        body = '''<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="http://resurs-media.ru/api"><soapenv:Header/><soapenv:Body><api:Notification/></soapenv:Body></soapenv:Envelope>'''
    root = make_api_call(body, "http://resurs-media.ru/api#WSAPI:Notification")
    if root is None:
        return []
    notifications = []
    for notif in root.findall('.//{http://resurs-media.ru/api}Notification'):
        notif_id = notif.attrib.get('NotificationID')
        text = notif.findtext('{http://resurs-media.ru/api}Text', '')
        attachment = notif.find('{http://resurs-media.ru/api}Attachment')
        attachment_name = notif.findtext('{http://resurs-media.ru/api}AttachmentName', '')
        notifications.append({
            'id': notif_id,
            'text': text,
            'attachment': attachment.text if attachment is not None else None,
            'attachment_name': attachment_name
        })
        # Уведомляем программистов о каждом новом уведомлении
        msg = f"[ResursMedio][Notification] {text}"
        send_telegram_message(msg)
        # Если есть вложение, можно реализовать отправку файла (см. send_telegram_file)
    return notifications

def save_notification_attachment(attachment_b64, filename):
    path = os.path.join('static', 'resursmedio_attachments', filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        f.write(base64.b64decode(attachment_b64))
    return path

def load_sent_notification_ids():
    if os.path.exists(NOTIFICATION_IDS_FILE):
        with open(NOTIFICATION_IDS_FILE, 'r', encoding='utf-8') as f:
            return set(json.load(f))
    return set()

def save_sent_notification_ids(ids):
    with open(NOTIFICATION_IDS_FILE, 'w', encoding='utf-8') as f:
        json.dump(list(ids), f, ensure_ascii=False)

def log_notification(notif):
    with open(NOTIFICATION_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(json.dumps(notif, ensure_ascii=False) + '\n')

def process_notifications():
    sent_ids = load_sent_notification_ids()
    notifications = get_notifications()
    new_notifs = [n for n in notifications if n['id'] not in sent_ids]
    for notif in new_notifs:
        msg = f"<b>ResursMedio Notification</b>\nID: {notif['id']}\n{textwrap.shorten(notif['text'], 1000)}"
        # send_telegram_message(msg) # This line was commented out in the original file, so it's commented out here.
        if notif['attachment'] and notif['attachment_name']:
            filepath = save_notification_attachment(notif['attachment'], notif['attachment_name'])
            # send_telegram_file(filepath, caption=f"Вложение к уведомлению {notif['id']}") # This line was commented out in the original file, so it's commented out here.
        log_notification(notif)
        sent_ids.add(notif['id'])
    save_sent_notification_ids(sent_ids)

def main():
    logging.info("Получение ID склада...")
    warehouse_id = get_first_warehouse_id()
    if not warehouse_id:
        logging.error("Не удалось получить ID склада. Выход.")
        return
    logging.info(f"Используется склад с ID: {warehouse_id}")
    logging.info("Получение описаний по всему каталогу...")
    descriptions_data, desc_headers = get_material_descriptions()
    material_ids = list(descriptions_data.keys())
    logging.info(f"Получено описаний: {len(material_ids)}")
    logging.info("Параллельное получение цен и остатков по батчам...")
    prices_data, price_headers = get_prices_batches(warehouse_id, material_ids)
    logging.info(f"Получено цен/остатков: {len(prices_data)}")
    all_ids = set(descriptions_data.keys()) | set(prices_data.keys())
    headers = desc_headers + [h for h in price_headers if h not in desc_headers]
    def row_iter():
        for mid in all_ids:
            row = {}
            if mid in descriptions_data:
                row.update(descriptions_data[mid])
            if mid in prices_data:
                for h in price_headers:
                    if h not in row:
                        row[h] = prices_data[mid][h]
            yield row
    save_to_csv_incremental(row_iter(), headers, "all_data.csv")
    logging.info("Выгрузка завершена.")

if __name__ == "__main__":
    main()