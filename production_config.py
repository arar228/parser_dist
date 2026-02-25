# Настройки для production окружения на AlmaLinux

import os
from pathlib import Path

# Базовые пути
BASE_DIR = Path("/opt/parser_dist")
LOGS_DIR = BASE_DIR / "logs"
STATIC_DIR = BASE_DIR / "static"

# Настройки базы данных MySQL
DATABASE_CONFIG = {
    "user": "parser_dist",
    "password": "YOUR_MYSQL_PASSWORD",
    "host": "YOUR_MYSQL_HOST",
    "port": "3306",
    "database": "parser_dist"
}

# Настройки веб-сервера
WEB_SERVER_CONFIG = {
    "host": "0.0.0.0",
    "port": 8080,
    "debug": False
}

# Настройки логирования
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        },
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
        },
        "file": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": str(LOGS_DIR / "app.log"),
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5,
        },
    },
    "loggers": {
        "": {
            "handlers": ["default", "file"],
            "level": "INFO",
            "propagate": False
        }
    }
}

# Настройки безопасности
SECURITY_CONFIG = {
    "allowed_hosts": ["*"],  # Настройте под ваши домены
    "csrf_enabled": True,
    "session_timeout": 3600,  # 1 час
}

# Настройки производительности
PERFORMANCE_CONFIG = {
    "max_connections": 100,
    "connection_timeout": 30,
    "request_timeout": 60,
    "worker_processes": 4,
}

# Настройки резервного копирования
BACKUP_CONFIG = {
    "backup_dir": "/opt/backups/parser_dist",
    "retention_days": 30,
    "compress_backups": True,
}

# Настройки мониторинга
MONITORING_CONFIG = {
    "enable_health_check": True,
    "health_check_interval": 300,  # 5 минут
    "enable_metrics": True,
    "metrics_port": 9090,
}

# Настройки уведомлений
NOTIFICATION_CONFIG = {
    "enable_email": False,
    "enable_telegram": False,
    "email_smtp_server": "",
    "email_smtp_port": 587,
    "email_username": "",
    "email_password": "",
    "telegram_bot_token": "",
    "telegram_chat_id": "",
}

# Настройки кэширования
CACHE_CONFIG = {
    "enable_cache": True,
    "cache_type": "memory",  # или "redis"
    "cache_ttl": 3600,  # 1 час
    "redis_host": "localhost",
    "redis_port": 6379,
    "redis_db": 0,
}

# Настройки API
API_CONFIG = {
    "rate_limit": 100,  # запросов в минуту
    "enable_cors": True,
    "cors_origins": ["*"],  # Настройте под ваши домены
    "api_version": "v1",
}

# Настройки файлов
FILE_CONFIG = {
    "max_file_size": 10485760,  # 10MB
    "allowed_extensions": [".txt", ".csv", ".xlsx", ".json"],
    "upload_dir": str(STATIC_DIR / "uploads"),
}

# Настройки задач
TASK_CONFIG = {
    "enable_scheduler": True,
    "scheduler_interval": 1800,  # 30 минут
    "max_retries": 3,
    "retry_delay": 300,  # 5 минут
}

# Настройки дистрибьюторов
DISTRIBUTOR_CONFIG = {
    "merlion": {
        "enabled": True,
        "update_interval": 3600,  # 1 час
        "max_products": 10000,
    },
    "resursmedio": {
        "enabled": True,
        "update_interval": 5400,  # 1.5 часа
        "max_products": 10000,
    },
    "netlab": {
        "enabled": True,
        "update_interval": 1800,  # 30 минут
        "max_products": 10000,
    },
    "treolan": {
        "enabled": True,
        "update_interval": 1800,  # 30 минут
        "max_products": 10000,
    },
    "vvp": {
        "enabled": True,
        "update_interval": 3600,  # 1 час
        "max_products": 10000,
    },
    "marvel": {
        "enabled": True,
        "update_interval": 3600,  # 1 час
        "max_products": 10000,
    },
    "ocs": {
        "enabled": True,
        "update_interval": 3600,  # 1 час
        "max_products": 10000,
    },
}

# Функция для получения конфигурации
def get_config():
    """Возвращает конфигурацию для production окружения"""
    return {
        "database": DATABASE_CONFIG,
        "web_server": WEB_SERVER_CONFIG,
        "logging": LOGGING_CONFIG,
        "security": SECURITY_CONFIG,
        "performance": PERFORMANCE_CONFIG,
        "backup": BACKUP_CONFIG,
        "monitoring": MONITORING_CONFIG,
        "notification": NOTIFICATION_CONFIG,
        "cache": CACHE_CONFIG,
        "api": API_CONFIG,
        "file": FILE_CONFIG,
        "task": TASK_CONFIG,
        "distributor": DISTRIBUTOR_CONFIG,
    }

# Функция для проверки конфигурации
def validate_config():
    """Проверяет корректность конфигурации"""
    errors = []
    
    # Проверка директорий
    if not BASE_DIR.exists():
        errors.append(f"Базовая директория {BASE_DIR} не существует")
    
    if not LOGS_DIR.exists():
        errors.append(f"Директория логов {LOGS_DIR} не существует")
    
    if not STATIC_DIR.exists():
        errors.append(f"Статическая директория {STATIC_DIR} не существует")
    
    # Проверка настроек БД
    if not DATABASE_CONFIG.get("password"):
        errors.append("Пароль базы данных не установлен")
    
    # Проверка портов
    if WEB_SERVER_CONFIG["port"] < 1024 or WEB_SERVER_CONFIG["port"] > 65535:
        errors.append("Некорректный порт веб-сервера")
    
    return errors

# Функция для создания директорий
def create_directories():
    """Создает необходимые директории"""
    directories = [BASE_DIR, LOGS_DIR, STATIC_DIR]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"Создана директория: {directory}")

if __name__ == "__main__":
    # Проверка конфигурации
    errors = validate_config()
    if errors:
        print("Ошибки в конфигурации:")
        for error in errors:
            print(f"  - {error}")
    else:
        print("Конфигурация корректна")
    
    # Создание директорий
    create_directories() 