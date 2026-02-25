# Руководство по миграции на MySQL

## Обзор изменений

Проект был адаптирован для работы с MySQL вместо PostgreSQL. Все основные компоненты обновлены для совместимости с MySQL.

## Что изменилось

### 1. Новые файлы
- `mysql_config.py` - конфигурация для подключения к MySQL
- `requirements_mysql.txt` - зависимости для MySQL
- `test_mysql_connection.py` - тест подключения к MySQL
- `migrate_to_mysql.py` - скрипт миграции данных
- `MYSQL_MIGRATION_GUIDE.md` - это руководство

### 2. Обновленные файлы
- `core/db_engine.py` - теперь использует MySQL конфигурацию
- `core/db_models.py` - добавлены поля created_at/updated_at, улучшена совместимость
- `core/db_create_tables.py` - поддержка MySQL синтаксиса
- `core/upsert.py` - исправлен синтаксис для MySQL

## Пошаговая миграция

### Шаг 1: Установка зависимостей

```bash
pip install -r requirements_mysql.txt
```

### Шаг 2: Тестирование подключения

```bash
python test_mysql_connection.py
```

Этот скрипт:
- Проверит подключение к MySQL
- Создаст таблицы если их нет
- Покажет статус базы данных

### Шаг 3: Миграция данных (опционально)

Если у вас есть данные в старой PostgreSQL базе:

```bash
python migrate_to_mysql.py
```

**Внимание**: Убедитесь, что старая PostgreSQL база доступна для миграции.

### Шаг 4: Создание таблиц (если нужно)

```bash
python core/db_create_tables.py
```

### Шаг 5: Тестирование работы

```bash
python main.py
```

## Конфигурация MySQL

В файле `mysql_config.py` настроены следующие параметры:

```python
MYSQL_HOST = "YOUR_MYSQL_HOST"
MYSQL_PORT = "1501"
MYSQL_USER = "root"
MYSQL_PASSWORD = "YOUR_MYSQL_PASSWORD!"
MYSQL_DATABASE = "parser_dist"
```

## Структура таблицы products

```sql
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    article VARCHAR(255),
    name VARCHAR(500),
    brand VARCHAR(255),
    part_number VARCHAR(255),
    category_code VARCHAR(255),
    price_rub FLOAT,
    price_usd FLOAT,
    stock DECIMAL(10,2),
    package_volume FLOAT,
    package_weight FLOAT,
    tech_specs TEXT,
    transit_date DATE,
    distributor VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

## Логика работы

Система работает по тому же принципу:

1. **Выгрузка данных** - API дистрибьюторов выкачивают данные
2. **Очистка старых данных** - удаляются старые записи дистрибьютора
3. **Вставка новых данных** - сохраняются новые данные
4. **Полная замена** - каждый дистрибьютор полностью обновляет свои данные

## Поддерживаемые дистрибьюторы

- Merlion
- Netlab  
- OCS
- VVP
- Treolan
- ResursMedia
- Marvel
- AbsolutTrade

## Отличия от PostgreSQL

1. **Синтаксис дат**: `CURRENT_TIMESTAMP` вместо `NOW()`
2. **TRUNCATE**: без `RESTART IDENTITY CASCADE`
3. **Типы данных**: явное указание размеров VARCHAR
4. **Драйвер**: `aiomysql` вместо `asyncpg`

## Устранение проблем

### Ошибка подключения
1. Проверьте доступность сервера MySQL
2. Убедитесь в правильности учетных данных
3. Проверьте сетевые настройки

### Ошибки миграции
1. Убедитесь, что старая PostgreSQL база доступна
2. Проверьте права доступа к обеим базам
3. Запустите миграцию повторно

### Проблемы с зависимостями
```bash
pip install --upgrade -r requirements_mysql.txt
```

## Мониторинг

После миграции все логи и уведомления будут работать как прежде. Система полностью совместима с существующей логикой работы.

## Откат на PostgreSQL

Если нужно вернуться к PostgreSQL:
1. Удалите `mysql_config.py`
2. Восстановите оригинальные версии файлов из git
3. Установите зависимости для PostgreSQL

## Поддержка

При возникновении проблем проверьте:
1. Логи в папке `logs/`
2. Статус подключения к MySQL
3. Права доступа к базе данных
