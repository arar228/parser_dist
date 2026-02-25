from flask import Flask, send_from_directory, request, jsonify
import os
import re

STATIC_FOLDER = 'static'
app = Flask(__name__, static_folder=STATIC_FOLDER)

# Главная страница (отдаём index.html, где твой интерфейс)
@app.route('/')
def index():
    return send_from_directory(STATIC_FOLDER, 'index.html')

# Отдача статических файлов (txt, js, css и т.д.)
@app.route('/<path:filename>')
def static_files(filename):
    file_path = os.path.join(STATIC_FOLDER, filename)
    if os.path.exists(file_path):
        return send_from_directory(STATIC_FOLDER, filename)
    else:
        return "File not found", 404

# Поиск по названию (GET name=...) и по списку партномеров (POST part_numbers=...)
@app.route('/search', methods=['GET', 'POST'])
def search():
    # Пример структуры товара (можно заменить на реальную логику поиска по БД или файлам)
    # Здесь для примера ищем по last_update_*.txt (или можно подключить свою БД)
    products = []
    # --- Поиск по названию (GET) ---
    name = request.args.get('name')
    if name:
        name_lower = name.lower()
        # Пример: ищем по всем last_update_*.txt (или подключи свою БД)
        for fname in os.listdir(STATIC_FOLDER):
            if fname.startswith('last_update_') and fname.endswith('.txt'):
                file_path = os.path.join(STATIC_FOLDER, fname)
                try:
                    with open(file_path, encoding='utf-8') as f:
                        content = f.read()
                        if name_lower in content.lower() or name_lower in fname.lower():
                            products.append({
                                "Артикул": fname.replace('last_update_', '').replace('.txt', ''),
                                "Наименование": content.strip(),
                                "Бренд": '-',
                                "Партномер": '-',
                                "Код категории": '-',
                                "Цена (RUB)": '-',
                                "Цена (USD)": '-',
                                "Наличие (шт)": '-',
                                "Объем упаковки, м3": '-',
                                "Вес упаковки, кг": '-',
                                "Технические характеристики": '-',
                                "Дата транзита": '-',
                                "Дистрибьютор": fname.replace('last_update_', '').replace('.txt', '')
                            })
                except Exception:
                    continue
        return jsonify(products)
    # --- Поиск по списку партномеров (POST) ---
    if request.method == 'POST':
        part_numbers = request.form.get('part_numbers')
        if part_numbers:
            part_list = [p.strip() for p in part_numbers.splitlines() if p.strip()]
            # Пример: ищем совпадения по названию файла (можно заменить на реальную БД)
            for pn in part_list:
                for fname in os.listdir(STATIC_FOLDER):
                    if fname.startswith('last_update_') and fname.endswith('.txt'):
                        if re.search(re.escape(pn), fname, re.IGNORECASE):
                            file_path = os.path.join(STATIC_FOLDER, fname)
                            try:
                                with open(file_path, encoding='utf-8') as f:
                                    content = f.read()
                                    products.append({
                                        "Артикул": fname.replace('last_update_', '').replace('.txt', ''),
                                        "Наименование": content.strip(),
                                        "Бренд": '-',
                                        "Партномер": pn,
                                        "Код категории": '-',
                                        "Цена (RUB)": '-',
                                        "Цена (USD)": '-',
                                        "Наличие (шт)": '-',
                                        "Объем упаковки, м3": '-',
                                        "Вес упаковки, кг": '-',
                                        "Технические характеристики": '-',
                                        "Дата транзита": '-',
                                        "Дистрибьютор": fname.replace('last_update_', '').replace('.txt', '')
                                    })
                            except Exception:
                                continue
            return jsonify(products)
    # Если ничего не найдено или не передан параметр
    return jsonify([])

# favicon (чтобы не было 404)
@app.route('/favicon.ico')
def favicon():
    return '', 204

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080) 