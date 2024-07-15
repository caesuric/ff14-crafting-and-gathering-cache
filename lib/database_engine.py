"""
Database models and setup.
"""
#pylint: disable=too-few-public-methods
import json
import os.path
from sqlalchemy import create_engine, Column, DateTime, Float, Integer, String
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base
from sqlalchemy_utils import database_exists, create_database

Base = declarative_base()

class OverallScrapingData(Base):
    """
    Overall data about the scraping process.
    """
    __tablename__ = 'overall_scraping_data'

    id = Column(Integer, primary_key=True)
    last_world_data_pull = Column(DateTime)
    last_tax_data_pull = Column(DateTime)

class ItemData(Base):
    """
    Basic data about items.
    """
    __tablename__ = 'item_data'

    id = Column(Integer, primary_key=True)
    level = Column(Integer)
    name = Column(String)
    icon_path = Column(String)
    last_data_pull = Column(DateTime)


class ItemMarketDataCurrent(Base):
    """
    Market data about items.
    """
    __tablename__ = 'item_market_data_current'

    id = Column(Integer, primary_key=True)
    current_min_price_nq = Column(Integer)
    last_data_pull = Column(DateTime)


class ItemMarketDataHistorical(Base):
    """
    Historical market data about items.
    """
    __tablename__ = 'item_market_data_historical'

    id = Column(Integer, primary_key=True)
    nq_daily_sale_velocity = Column(Integer)
    average_price_per_unit = Column(Integer)
    num_items_sold = Column(Integer)
    possible_money_per_day = Column(Integer)
    median_stack_size = Column(Float)
    median_price = Column(Float)
    last_data_pull = Column(DateTime)


class World(Base):
    """
    Game world.
    """
    __tablename__ = 'world'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    region = Column(String)

class TaxRate(Base):
    """
    Tax rates for each city by world.
    """
    __tablename__ = 'tax_rate'

    id = Column(Integer, primary_key=True)
    world = Column(String)
    city = Column(String)
    tax_rate = Column(Integer)


class CraftingType(Base):
    """
    Crafting type.
    """
    __tablename__ = 'crafting_type'

    id = Column(Integer, primary_key=True)
    name = Column(String)


def init_database() -> Engine:
    """
    Initializes the database connection and creates it if needed.

    Returns:
        Engine: The engine object for accessing this database.
    """
    with open(os.path.join('auth', 'db-auth.json'), 'r', encoding='utf-8') as auth_file:
        auth_data = json.loads(auth_file.read())
        username = auth_data['username']
        password = auth_data['password']
        port = auth_data['port']

    db_string = f'postgresql+psycopg2://{username}:{password}@localhost:{port}/xivmarketstats'
    engine = create_engine(db_string)
    if not database_exists(db_string):
        create_database(engine.url)
    Base.metadata.create_all(engine)
    return engine
