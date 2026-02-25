import requests
import openpyxl
from requests.auth import HTTPBasicAuth
from lxml import etree
from openpyxl import Workbook
from openpyxl.styles import Font
import time
from datetime import datetime, timedelta
import os
import asyncio
from merlion.merlion_export import upsert_resursmedio_products
from core.db_engine import AsyncSessionLocal

# --- ГЛОБАЛЬНЫЕ НАСТРОЙКИ ---
URL = "https://api.resurs-media.ru/resursmedia/ws/WSAPI"
CREDENTIALS = ("ARMK", "pD7qPg")
PAUSE_DURATION = 60 # Пауза в секундах между шагами

# --- SOAP ШАБЛОНЫ ---
GET_CATALOG_BODY = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="http://resurs-media.ru/api"><soapenv:Header/><soapenv:Body><api:GetCatalog/></soapenv:Body></soapenv:Envelope>"""
NOTIFICATION_BODY_TEMPLATE = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="http://resurs-media.ru/api"><soapenv:Header/><soapenv:Body><api:Notification><api:FromDate>{from_date}</api:FromDate></api:Notification></soapenv:Body></soapenv:Envelope>"""

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
      <api:MaterialID_Tab>{material_ids_xml}</api:MaterialID_Tab>
      <api:MaterialGroup_Tab>{group_ids_xml}</api:MaterialGroup_Tab>
      <api:GetAvailableCount>{get_available}</api:GetAvailableCount>
    </api:GetPrices>
  </soapenv:Body>
</soapenv:Envelope>
"""

GET_ITEMS_AVAIL_BODY_TEMPLATE = """
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="http://resurs-media.ru/api">
  <soapenv:Header/>
  <soapenv:Body>
    <api:GetItemsAvail>
      <api:WareHouseID>{warehouse_id}</api:WareHouseID>
      <api:MaterialID_Tab/>
      <api:MaterialGroup_Tab/>
    </api:GetItemsAvail>
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

GET_MATERIAL_DATA_BY_GROUP_BODY_TEMPLATE = """
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="http://resurs-media.ru/api">
  <soapenv:Header/>
  <soapenv:Body>
    <api:GetMaterialData>
      <api:MaterialID_Tab/>
      <api:MaterialGroup_Tab>{group_ids_xml}</api:MaterialGroup_Tab>
      <api:VendorPart_Tab/>
      <api:WithCharacteristics>true</api:WithCharacteristics>
      <api:WithBarCodes>true</api:WithBarCodes>
      <api:WithCertificates>true</api:WithCertificates>
      <api:WithImages>true</api:WithImages>
    </api:GetMaterialData>
  </soapenv:Body>
</soapenv:Envelope>
"""

# --- УТИЛИТЫ ---

def print_sep(title):
    """Печатает красивый разделитель в консоль."""
    print('\n' + '='*50)
    print(f" {title} ".center(50, '='))
    print('='*50)

def pause():
    """Делает паузу."""
    print(f'\nПауза {PAUSE_DURATION} секунд...')
    time.sleep(PAUSE_DURATION)

def make_api_call(soap_body, soap_action):
    """Отправляет SOAP-запрос и возвращает распарсенный XML (lxml) или None."""
    headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": soap_action}
            try:
            response = requests.post(URL, headers=headers, data=soap_body.encode('utf-8'), auth=CREDENTIALS, timeout=30)
            response.raise_for_status()
        
        # Проверяем ответ на наличие soap:Fault
        if 'soap:Fault' in response.text:
            print(f"!!! API вернуло SOAP Fault ({soap_action}):")
            print(response.text)
            return None
            
        return etree.fromstring(response.content)
    except requests.exceptions.RequestException as e:
        print(f"!!! Ошибка при вызове API ({soap_action}): {e}")
        if 'response' in locals() and response.content:
            print(f"Ответ сервера: {response.text}")
        return None
    except etree.XMLSyntaxError as e:
        print(f"!!! Ошибка парсинга XML ({soap_action}): {e}")
        print(f"Ответ сервера: {response.text}")
        return None
        
def check_api_error(root, ns={'m': 'http://resurs-media.ru/api'}):
    """Проверяет ответ на стандартную ошибку Result/ErrorMessage."""
    if root is None:
        return True # Считаем ошибкой, если ответа не было
    result = root.find('.//m:Result', namespaces=ns)
    if result is not None and result.text != '0':
        error_msg = root.find('.//m:ErrorMessage', namespaces=ns)
        error_text = error_msg.text if error_msg is not None and error_msg.text else 'Нет текста ошибки'
        print(f"      !!! API вернуло ошибку: {error_text} (Код: {result.text})")
        return True
    return False

def save_to_excel(data, headers, filename):
    """Сохраняет список словарей в Excel."""
    if not data:
        print(f"      -> Нет данных для сохранения в {filename}.")
        return
    wb = Workbook()
    ws = wb.active
    ws.title = "Data"
    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for item in data:
        row = [item.get(h, '') for h in headers]
        ws.append(row)
    # Сохраняем файл на рабочий стол текущего пользователя
    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    output_dir = desktop
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, filename)
    wb.save(full_path)
    print(f"      -> Данные ({len(data)} строк) успешно сохранены в файл: {full_path}")

# --- ФУНКЦИИ ДЛЯ ШАГОВ ТЕСТИРОВАНИЯ ---

def get_first_warehouse_id():
    """Получает ID первого доступного склада."""
    # Для получения складов нужен ClientID, получаем его первым
    get_clients_body = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="http://resurs-media.ru/api"><soapenv:Header/><soapenv:Body><api:GetClientsAvail/></soapenv:Body></soapenv:Envelope>"""
    root = make_api_call(get_clients_body, "http://resurs-media.ru/api#WSAPI:GetClientsAvail")
    if root is None: return None
    client_id_element = root.find('.//{http://resurs-media.ru/api}ClientID')
    if client_id_element is None:
        print("!!! Не удалось найти ClientID.")
        return None
    client_id = client_id_element.text

    # Получаем склады для этого ClientID
    get_warehouses_body = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="http://resurs-media.ru/api"><soapenv:Header/><soapenv:Body><api:GetWareHouses><api:ClientID>{client_id}</api:ClientID></api:GetWareHouses></soapenv:Body></soapenv:Envelope>"""
    root = make_api_call(get_warehouses_body, "http://resurs-media.ru/api#WSAPI:GetWareHouses")
    if root is None: return None
    warehouse_id_element = root.find('.//{http://resurs-media.ru/api}WareHouseID')
    if warehouse_id_element is None:
        print("!!! Не найдено ни одного склада.")
        return None
    
    return warehouse_id_element.text

# --- ОСНОВНОЙ СЦЕНАРИЙ ---

def run_test_scenario():
    """Главная функция, выполняющая все шаги тестирования последовательно."""
    print_sep("ЗАПУСК СЦЕНАРИЯ ТЕСТИРОВАНИЯ API")
    
    # Для многих операций нужен ID склада, получим его один раз в начале
    print("...Получение ID склада для тестов...")
    warehouse_id = get_first_warehouse_id()
    if not warehouse_id:
        print("!!! Не удалось получить ID склада. Дальнейшее выполнение невозможно.")
        return
    print(f"      -> Используется склад с ID: {warehouse_id}")

    # Переменная для хранения ID из шага 3 для использования в шаге 7
    material_ids_from_step3 = []

    # ==================================================================
    # ШАГ 1: Получение уведомлений
    # ==================================================================
    print_sep("ШАГ 1: Получение уведомлений")
    from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    body = NOTIFICATION_BODY_TEMPLATE.format(from_date=from_date)
    root = make_api_call(body, "http://resurs-media.ru/api#WSAPI:Notification")
    if not check_api_error(root):
        notifications = root.findall('.//{http://resurs-media.ru/api}Notification')
        print(f"      -> Получено уведомлений: {len(notifications)}")
        for n in notifications[:5]: # Выводим не больше 5
            text_element = n.find('{http://resurs-media.ru/api}Text')
            print(f"        - {text_element.text[:100] if text_element is not None and text_element.text else 'Нет текста'}")
    pause()

    # ==================================================================
    # ШАГ 2: Получение описаний по всему каталогу
    # ==================================================================
    print_sep("ШАГ 2: Получение описаний по всему каталогу")
    print("      -> Используется метод GetMaterialData без фильтров")
    root = make_api_call(GET_MATERIAL_DATA_FULL_BODY, "http://resurs-media.ru/api#WSAPI:GetMaterialData")
    descriptions_data = []
    descriptions_dict = {}
    if not check_api_error(root):
        items = root.findall('.//{http://resurs-media.ru/api}MaterialData_Tab/{http://resurs-media.ru/api}Item')
        print(f"      -> Получено описаний: {len(items)}")
        headers = ["MaterialID", "MaterialText", "PartNum", "Vendor", "MaterialGroup", "Weight", "Volume"]
        for item in items:
            item_data = {}
            for h in headers:
                node = item.find(f'{{http://resurs-media.ru/api}}{h}')
                item_data[h] = node.text if node is not None else ''
            descriptions_data.append(item_data)
            descriptions_dict[item_data["MaterialID"]] = item_data
        # save_to_excel(descriptions_data, headers, "all_descriptions.xlsx")
    pause()

    # ==================================================================
    # ШАГ 3: Получение остатков и цен по всему каталогу
    # ==================================================================
    print_sep("ШАГ 3: Получение остатков и цен по всему каталогу")
    body = GET_PRICES_BODY_TEMPLATE.format(warehouse_id=warehouse_id, get_available="true", material_ids_xml="", group_ids_xml="")
    root = make_api_call(body, "http://resurs-media.ru/api#WSAPI:GetPrices")
    prices_data = []
    prices_dict = {}
    if not check_api_error(root):
        items = root.findall('.//{http://resurs-media.ru/api}Material_Tab')
        print(f"      -> Получено товаров с ценами и остатками: {len(items)}")
        headers = ["MaterialID", "PartNum", "Price", "PriceUSD", "AvailableCount"]
        for item in items:
            item_data = {}
            for h in headers:
                node = item.find(f'{{http://resurs-media.ru/api}}{h}')
                item_data[h] = node.text if node is not None else ''
            prices_data.append(item_data)
            prices_dict[item_data["MaterialID"]] = item_data
        # save_to_excel(prices_data, headers, "all_prices_and_stock.xlsx")
        material_ids_from_step3 = [item.get("MaterialID") for item in prices_data if item.get("MaterialID")]
    pause()

    # --- Объединяем описания и цены по MaterialID ---
    all_ids = set(descriptions_dict.keys()) | set(prices_dict.keys())
    products = []
    for mid in all_ids:
        desc = descriptions_dict.get(mid, {})
        price = prices_dict.get(mid, {})
        product = {
            'id': mid,
            'name': desc.get('MaterialText', ''),
            'price': price.get('Price', ''),
            'part_number': desc.get('PartNum', ''),
            'vendor': desc.get('Vendor', ''),
            'category_code': desc.get('MaterialGroup', ''),
            'price_usd': price.get('PriceUSD', ''),
            'available_count': price.get('AvailableCount', ''),
        }
        products.append(product)

    # --- Импортируем в базу ---
    async def import_to_db():
        async with AsyncSessionLocal() as db:
            await upsert_resursmedio_products(products, db)
        print(f"Импортировано в базу: {len(products)} товаров.")
    asyncio.run(import_to_db())

    # ==================================================================
    # ШАГ 4: Запросы для 10 произвольных групп (ОДНИМ ЗАПРОСОМ)
    # ==================================================================
    print_sep("ШАГ 4: Запросы для 10 произвольных групп (одним запросом)")
    root = make_api_call(GET_CATALOG_BODY, "http://resurs-media.ru/api#WSAPI:GetCatalog")
    if not check_api_error(root):
        groups = root.findall('.//{http://resurs-media.ru/api}MaterialGroup_Tab/{http://resurs-media.ru/api}MaterialGroup')
        group_codes = [g.text for g in groups if g.text]
        print(f"      -> Всего найдено групп: {len(group_codes)}. Берем первые 10.")
        selected_groups = group_codes[:10]
        # Формируем XML для 10 групп
        group_xml = ''.join(f'<api:Item><api:MaterialGroup>{code.strip()}</api:MaterialGroup></api:Item>' for code in selected_groups)

        # 4.1 Описания для 10 групп одним запросом
        print(f"      -> Запрос описаний для 10 групп одним запросом")
        body = GET_MATERIAL_DATA_BY_GROUP_BODY_TEMPLATE.format(group_ids_xml=group_xml)
        desc_root = make_api_call(body, "http://resurs-media.ru/api#WSAPI:GetMaterialData")
        if not check_api_error(desc_root):
            desc_items = desc_root.findall('.//{http://resurs-media.ru/api}MaterialData_Tab/{http://resurs-media.ru/api}Item')
            print(f"        -> Получено описаний: {len(desc_items)}")

        # 4.2 Цены и остатки для 10 групп одним запросом
        print(f"      -> Запрос цен и остатков для 10 групп одним запросом")
        body = GET_PRICES_BODY_TEMPLATE.format(warehouse_id=warehouse_id, get_available="true", material_ids_xml="", group_ids_xml=group_xml)
        price_root = make_api_call(body, "http://resurs-media.ru/api#WSAPI:GetPrices")
        if not check_api_error(price_root):
            price_items = price_root.findall('.//{http://resurs-media.ru/api}Material_Tab')
            print(f"        -> Получено товаров с ценами: {len(price_items)}")
    pause()

    # ==================================================================
    # ШАГ 5: Получение цен без остатков по всему каталогу
    # ==================================================================
    print_sep("ШАГ 5: Получение цен БЕЗ остатков по всему каталогу")
    body = GET_PRICES_BODY_TEMPLATE.format(warehouse_id=warehouse_id, get_available="false", material_ids_xml="", group_ids_xml="")
    root = make_api_call(body, "http://resurs-media.ru/api#WSAPI:GetPrices")
    prices_only_data = []
    if not check_api_error(root):
        items = root.findall('.//{http://resurs-media.ru/api}Material_Tab')
        print(f"      -> Получено товаров с ценами (без остатков): {len(items)}")
        headers = ["MaterialID", "PartNum", "Price", "PriceUSD"]
        for item in items:
            item_data = {}
            for h in headers:
                node = item.find(f'{{http://resurs-media.ru/api}}{h}')
                item_data[h] = node.text if node is not None else ''
            prices_only_data.append(item_data)
        # save_to_excel(prices_only_data, headers, "prices_only.xlsx")
    pause()

    # ==================================================================
    # ШАГ 6: Получение остатков без цен по всему каталогу
    # ==================================================================
    print_sep("ШАГ 6: Получение остатков БЕЗ цен по всему каталогу")
    body = GET_ITEMS_AVAIL_BODY_TEMPLATE.format(warehouse_id=warehouse_id)
    root = make_api_call(body, "http://resurs-media.ru/api#WSAPI:GetItemsAvail")
    stock_only_data = []
    if not check_api_error(root):
        items = root.findall('.//{http://resurs-media.ru/api}Material_Tab')
        print(f"      -> Получено товаров с остатками (без цен): {len(items)}")
        headers = ["MaterialID", "PartNum", "AvailableCount"]
        for item in items:
            item_data = {}
            for h in headers:
                node = item.find(f'{{http://resurs-media.ru/api}}{h}')
                item_data[h] = node.text if node is not None else ''
            stock_only_data.append(item_data)
        # save_to_excel(stock_only_data, headers, "stock_only.xlsx")
    pause()

    # ==================================================================
    # ШАГ 7: Обработка "новых" MaterialID
    # ==================================================================
    print_sep("ШАГ 7: Обработка 'новых' MaterialID")
    if material_ids_from_step3:
        new_ids = material_ids_from_step3[:5] # Берем первые 5 ID из шага 3 как "новые"
        print(f"      -> Обнаружено {len(new_ids)} 'новых' ID. Запрашиваем для них полное описание...")
        print(f"      -> ID для запроса: {new_ids}")
        
        items_xml = ''.join(f'<api:Item><api:MaterialID>{mid}</api:MaterialID></api:Item>' for mid in new_ids)
        body = GET_MATERIAL_DATA_BY_ID_BODY_TEMPLATE.format(material_ids_xml=items_xml)
        root = make_api_call(body, "http://resurs-media.ru/api#WSAPI:GetMaterialData")
        
        if not check_api_error(root):
            items = root.findall('.//{http://resurs-media.ru/api}MaterialData_Tab/{http://resurs-media.ru/api}Item')
            print(f"      -> Получено полных описаний для 'новых' ID: {len(items)}")
            for item in items:
                mid_element = item.find('{http://resurs-media.ru/api}MaterialID')
                mid = mid_element.text if mid_element is not None else "N/A"
                
                m_text_element = item.find('{http://resurs-media.ru/api}MaterialText')
                m_text = m_text_element.text if m_text_element is not None else "N/A"

                print(f"        - ID: {mid}, Text: {m_text}")
    else:
        print("      -> Не удалось получить ID из шага 3 для демонстрации этого шага.")


if __name__ == "__main__":
    start_time_utc = datetime.utcnow()
    run_test_scenario()
    end_time_utc = datetime.utcnow()

    total_duration = end_time_utc - start_time_utc

    moscow_offset = timedelta(hours=3)
    start_time_msk = start_time_utc + moscow_offset
    end_time_msk = end_time_utc + moscow_offset

    print_sep("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print(f"Общее время выполнения: {total_duration}")
    print(f"Интервал выполнения (МСК): с {start_time_msk.strftime('%H:%M:%S %d.%m.%Y')} по {end_time_msk.strftime('%H:%M:%S %d.%m.%Y')}")