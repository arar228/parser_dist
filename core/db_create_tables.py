from core.db_models import Base
from sqlalchemy.ext.asyncio import create_async_engine
import asyncio

# Импортируем MySQL конфигурацию
from core.runtime_config import get_database_url

DATABASE_URL = get_database_url()

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