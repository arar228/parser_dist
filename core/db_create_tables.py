from core.db_models import Base
from sqlalchemy.ext.asyncio import create_async_engine
import asyncio

# Импортируем MySQL конфигурацию
try:
    from mysql_config import MYSQL_URL
    DATABASE_URL = MYSQL_URL
    print("✅ Используется MySQL конфигурация для создания таблиц")
except ImportError:
    # Fallback на PostgreSQL если MySQL конфиг не найден
    DB_USER = "mysite_user"
    DB_PASS = "12345"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "myproject"
    DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print("⚠️ Используется PostgreSQL конфигурация (fallback)")

engine = create_async_engine(DATABASE_URL, echo=True)

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def truncate_products():
    async with engine.begin() as conn:
        # Для MySQL используем TRUNCATE без RESTART IDENTITY
        if "mysql" in DATABASE_URL:
            await conn.execute("TRUNCATE TABLE products;")
        else:
            # Для PostgreSQL
            await conn.execute("TRUNCATE TABLE products RESTART IDENTITY CASCADE;")
        await conn.commit()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "truncate":
        asyncio.run(truncate_products())
    else:
        asyncio.run(create_tables()) 