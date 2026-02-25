def clean_part_number(part_number):
    """Очищает партномер от пробелов и приводит к нижнему регистру для сравнения"""
    if not part_number:
        return ""
    return str(part_number).strip().lower()

def clean_product_data(product_data):
    """Очищает данные товара от пробелов в ключевых полях"""
    if not product_data:
        return product_data
    
    cleaned = product_data.copy()
    
    # Очищаем партномер
    if 'part_number' in cleaned:
        cleaned['part_number'] = clean_part_number(cleaned['part_number'])
    
    # Очищаем артикул
    if 'article' in cleaned:
        cleaned['article'] = str(cleaned['article']).strip() if cleaned['article'] else ''
    
    # Очищаем ID (если используется как партномер)
    if 'id' in cleaned:
        cleaned['id'] = str(cleaned['id']).strip() if cleaned['id'] else ''
    
    return cleaned 