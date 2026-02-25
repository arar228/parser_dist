#!/usr/bin/env python3
"""
Модуль для Telegram уведомлений о статусе экспорта
"""

import requests
import logging
from typing import Dict, Optional
from datetime import datetime
import asyncio
import os

# ОТКЛЮЧАЕМ ПРОКСИ для решения проблем с подключением
proxy_vars = ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'all_proxy', 'ALL_PROXY']
for var in proxy_vars:
    if var in os.environ:
        del os.environ[var]
os.environ['no_proxy'] = '*'
os.environ['NO_PROXY'] = '*'

# Конфигурация Telegram бота
TELEGRAM_BOT_TOKEN = "8067594536:AAHNDCqVdMzYZuLUjdRCeyZj2Ju4dzTWXUk"
TELEGRAM_CHAT_ID = "-1002258896791"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Глобальный словарь для отслеживания уже отправленных уведомлений
_sent_notifications = {}
_current_export_session = None

def clear_notification_history():
    """Очищает историю отправленных уведомлений"""
    global _sent_notifications, _current_export_session
    _sent_notifications.clear()
    _current_export_session = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    import logging
    logging.info(f"[Telegram] История уведомлений очищена. Новая сессия: {_current_export_session}")

def get_notification_status(distributor: str):
    """Возвращает статус уведомлений для дистрибьютера"""
    start_key = f"{distributor}_start"
    complete_key = f"{distributor}_complete"
    
    return {
        'start_sent': start_key in _sent_notifications,
        'complete_sent': complete_key in _sent_notifications,
        'start_time': _sent_notifications.get(start_key),
        'complete_time': _sent_notifications.get(complete_key),
        'current_session': _current_export_session
    }

def send_telegram_message(message: str, chat_id: Optional[str] = None) -> bool:
    """
    Отправляет сообщение в Telegram
    
    Args:
        message: Текст сообщения
        chat_id: ID чата (если не указан, используется дефолтный)
    
    Returns:
        bool: True если сообщение отправлено успешно
    """
    if not chat_id:
        chat_id = TELEGRAM_CHAT_ID
    
    try:
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'HTML'
        }
        
        response = requests.post(
            f"{TELEGRAM_API_URL}/sendMessage",
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            logging.info(f"[Telegram] Сообщение отправлено успешно")
            return True
        else:
            logging.error(f"[Telegram] Ошибка отправки: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logging.error(f"[Telegram] Ошибка отправки сообщения: {e}")
        return False

async def notify_export_start(distributor: str) -> None:
    """
    Уведомляет о начале экспорта дистрибьютера
    
    Args:
        distributor: Название дистрибьютера
    
    Returns:
        bool: True если уведомление отправлено
    """
    # Проверяем, не было ли уже отправлено уведомление о начале для этого дистрибьютера
    notification_key = f"{distributor}_start"
    if notification_key in _sent_notifications:
        import logging
        logging.warning(f"[Telegram][DUPLICATE] Уведомление о начале для {distributor} уже отправлено в этой сессии, пропускаем")
        logging.warning(f"[Telegram][DUPLICATE] Текущая сессия: {_current_export_session}")
        return
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"""
🚀 <b>ЭКСПОРТ НАЧАТ</b>

📊 <b>Дистрибьютер:</b> {distributor}
⏰ <b>Время начала:</b> {timestamp}

🔄 <b>Статус:</b> Сбор данных...
    """
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, send_telegram_message, message.strip())
    await asyncio.sleep(1)
    
    # Отмечаем, что уведомление отправлено
    _sent_notifications[notification_key] = timestamp
    import logging
    logging.info(f"[Telegram] Уведомление о начале для {distributor} отправлено и записано в сессии {_current_export_session}")

def notify_export_progress(distributor: str, total_items: int, current_items: int, 
                          catalog: str = "", category: str = "") -> bool:
    """
    Уведомляет о прогрессе экспорта
    
    Args:
        distributor: Название дистрибьютера
        total_items: Общее количество собранных товаров
        current_items: Текущее количество в категории
        catalog: Название каталога
        category: ID категории
    
    Returns:
        bool: True если уведомление отправлено
    """
    # Отправляем уведомления только каждые 1000 товаров или при смене каталога
    if total_items % 1000 == 0 or (catalog and category):
        progress_percent = min(100, int((total_items / 50000) * 100))  # Примерная оценка
        
        message = f"""
📈 <b>ПРОГРЕСС ЭКСПОРТА</b>

📊 <b>Дистрибьютер:</b> {distributor}
🔢 <b>Всего товаров:</b> {total_items:,}
📁 <b>Каталог:</b> {catalog or 'N/A'}
🏷️ <b>Категория:</b> {category or 'N/A'}
📊 <b>Прогресс:</b> {progress_percent}%
        """
        
        return send_telegram_message(message.strip())
    
    return True

async def notify_export_complete(distributor: str, total_items: int, 
                                inserted: int, updated: int, 
                                errors: int = 0, duration_minutes: float = 0) -> None:
    """
    Уведомляет о завершении экспорта и обновляет дату последней выгрузки для дистрибьютера
    """
    # Проверяем, не было ли уже отправлено уведомление о завершении для этого дистрибьютера
    notification_key = f"{distributor}_complete"
    if notification_key in _sent_notifications:
        import logging
        logging.warning(f"[Telegram][DUPLICATE] Уведомление о завершении для {distributor} уже отправлено в этой сессии, пропускаем")
        logging.warning(f"[Telegram][DUPLICATE] Текущая сессия: {_current_export_session}")
        return
    
    # Детальное логирование для отладки
    import logging
    logging.info(f"[Telegram][DEBUG] notify_export_complete вызван:")
    logging.info(f"[Telegram][DEBUG] - distributor: '{distributor}'")
    logging.info(f"[Telegram][DEBUG] - total_items: {total_items}")
    logging.info(f"[Telegram][DEBUG] - inserted: {inserted}")
    logging.info(f"[Telegram][DEBUG] - updated: {updated}")
    logging.info(f"[Telegram][DEBUG] - errors: {errors}")
    logging.info(f"[Telegram][DEBUG] - duration_minutes: {duration_minutes}")
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if errors == 0:
        status_emoji = "✅"
        status_text = "УСПЕШНО"
    else:
        status_emoji = "⚠️"
        status_text = "С ОШИБКАМИ"
    
    # Форматируем время более точно
    if duration_minutes < 1:
        # Если меньше минуты, показываем в секундах
        duration_text = f"{duration_minutes * 60:.0f} сек"
    elif duration_minutes < 10:
        # Если меньше 10 минут, показываем с двумя знаками после запятой
        duration_text = f"{duration_minutes:.2f} мин"
    else:
        # Если больше 10 минут, показываем с одним знаком после запятой
        duration_text = f"{duration_minutes:.1f} мин"
    
    message = f"""
{status_emoji} <b>ЭКСПОРТ ЗАВЕРШЕН</b>

📊 <b>Дистрибьютер:</b> {distributor}
⏰ <b>Время завершения:</b> {timestamp}
⏱️ <b>Продолжительность:</b> {duration_text}

📈 <b>Результаты:</b>
   • Всего товаров: {total_items:,}
   • Вставлено: {inserted:,}
   • Обновлено: {updated:,}
   • Ошибки: {errors}

🎯 <b>Статус:</b> {status_text}
    """
    
    logging.info(f"[Telegram][DEBUG] Сформированное сообщение для {distributor}:")
    logging.info(f"[Telegram][DEBUG] {message}")
    
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, send_telegram_message, message.strip())
    
    # Отмечаем, что уведомление отправлено
    _sent_notifications[notification_key] = timestamp
    logging.info(f"[Telegram] Уведомление о завершении для {distributor} отправлено и записано в сессии {_current_export_session}")

async def notify_export_error(distributor: str, error_message: str, 
                             total_items: int = 0, current_progress: str = "") -> None:
    """
    Уведомляет об ошибке в экспорте
    
    Args:
        distributor: Название дистрибьютера
        error_message: Описание ошибки
        total_items: Количество собранных товаров до ошибки
        current_progress: Текущий прогресс
    
    Returns:
        bool: True если уведомление отправлено
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    message = f"""
❌ <b>ОШИБКА ЭКСПОРТА</b>

📊 <b>Дистрибьютер:</b> {distributor}
⏰ <b>Время ошибки:</b> {timestamp}
🔢 <b>Товаров собрано:</b> {total_items:,}
📈 <b>Прогресс:</b> {current_progress or 'N/A'}

🚨 <b>Ошибка:</b>
{error_message}

⚠️ <b>Требует внимания!</b>
    """
    
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, send_telegram_message, message.strip())

def notify_system_status(total_products: int, distributors: Dict[str, int]) -> bool:
    """
    Отправляет общий статус системы
    
    Args:
        total_products: Общее количество товаров в системе
        distributors: Словарь с количеством товаров по дистрибьютерам
    
    Returns:
        bool: True если уведомление отправлено
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Формируем список дистрибьютеров
    distributor_list = "\n".join([
        f"   • {dist}: {count:,}" for dist, count in distributors.items()
    ])
    
    message = f"""
📊 <b>СТАТУС СИСТЕМЫ</b>

⏰ <b>Время проверки:</b> {timestamp}
🔢 <b>Всего товаров:</b> {total_products:,}

📋 <b>По дистрибьютерам:</b>
{distributor_list}

✅ <b>Система работает</b>
    """
    
    return send_telegram_message(message.strip()) 