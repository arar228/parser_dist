import pymysql
import os
from core.telegram_notify import send_telegram_message  # Используй свою функцию отправки

def get_db_stats():
    try:
        # Используем MySQL конфигурацию
        from mysql_config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
        
        conn = pymysql.connect(
            host=MYSQL_HOST,
            port=int(MYSQL_PORT),
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        cur = conn.cursor()
        cur.execute("SELECT distributor, COUNT(*) as count FROM products GROUP BY distributor ORDER BY count DESC;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"[ERROR] Ошибка подключения к базе: {e}")
        return []

def format_stats(rows):
    if not rows:
        return "Нет данных или ошибка подключения к базе."
    lines = [f"{d:<10} | {c}" for d, c in rows]
    return "\n".join(lines)

if __name__ == "__main__":
    rows = get_db_stats()
    text = "📊 Количество товаров в базе:\n\n" + format_stats(rows)
    print(text)  # Для отладки
    try:
        send_telegram_message(text)
        print("✅ Сообщение отправлено в Telegram")
    except Exception as e:
        print(f"[ERROR] Не удалось отправить сообщение в Telegram: {e}") 