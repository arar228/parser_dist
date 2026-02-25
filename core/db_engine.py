import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
import logging

# Полностью отключаем логирование SQL запросов
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
logging.getLogger('sqlalchemy.pool').setLevel(logging.ERROR)
logging.getLogger('sqlalchemy.dialects').setLevel(logging.ERROR)
logging.getLogger('sqlalchemy.orm').setLevel(logging.ERROR)

# Отключаем распространение логов
for logger_name in ['sqlalchemy', 'sqlalchemy.engine', 'sqlalchemy.pool', 'sqlalchemy.dialects', 'sqlalchemy.orm']:
    logger = logging.getLogger(logger_name)
    logger.propagate = False
    logger.handlers.clear()

# Дополнительно отключаем все SQLAlchemy логи
os.environ['SQLALCHEMY_WARN_20'] = 'false'
os.environ['SQLALCHEMY_SILENCE_UBER_WARNING'] = '1'

# Импортируем MySQL конфигурацию
try:
    from mysql_config import MYSQL_URL
    DATABASE_URL = MYSQL_URL
    print("Используется MySQL конфигурация")
except ImportError:
    # Fallback на PostgreSQL если MySQL конфиг не найден
    DB_USER = "mysite_user"
    DB_PASS = "12345"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "myproject"
    DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print("⚠️ Используется PostgreSQL конфигурация (fallback)")

# Создаем engine с полностью отключенным логированием
engine = create_async_engine(
    DATABASE_URL, 
    echo=False,  # Отключаем echo
    future=True,
    # Дополнительные параметры для отключения логирования
    logging_name=None,
    pool_logging_name=None,
    # Полностью отключаем логирование
    echo_pool=False
)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession) 