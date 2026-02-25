# Для работы требуется установить SQLAlchemy: pip install sqlalchemy
from sqlalchemy import Column, Integer, String, Float, Text, Date, Boolean, Numeric, create_engine, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class Product(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True, autoincrement=True)
    article = Column(String(255), nullable=True)  # Артикул
    name = Column(String(500), nullable=True)  # Наименование
    brand = Column(String(255), nullable=True)  # Бренд
    part_number = Column(String(255), nullable=True)  # Партномер
    category_code = Column(String(255), nullable=True)  # Код категории
    price_rub = Column(Float, nullable=True)  # Цена (RUB)
    price_usd = Column(Float, nullable=True)  # Цена (USD)
    stock = Column(Numeric(10, 2), nullable=True)  # Наличие (шт) теперь число
    package_volume = Column(Float, nullable=True)  # Объем упаковки, м3
    package_weight = Column(Float, nullable=True)  # Вес упаковки, кг
    tech_specs = Column(Text, nullable=True)  # Технические характеристики
    transit_date = Column(Date, nullable=True)  # Дата транзита
    distributor = Column(String(255), nullable=True)  # Дистрибьютор (название/ID)
    is_active = Column(Boolean, nullable=False, default=True)  # Актуален ли товар
    created_at = Column(DateTime, nullable=False, default=func.now())  # Дата создания
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())  # Дата обновления

# Пример создания таблицы:
# engine = create_engine('postgresql://user:password@localhost/dbname')
# Base.metadata.create_all(engine) 