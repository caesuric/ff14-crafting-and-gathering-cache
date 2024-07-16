"""
Functions for pulling data from Universalis.
"""
from datetime import datetime, timedelta, timezone
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from lib.constants import (
    TAX_RATE_SCHEDULE_IN_HOURS,
    UNIVERSALIS_API_BASE_URL,
    WORLD_DATA_SCHEDULE_IN_DAYS,
    WORLDS_PATH,
    TAX_RATES_PATH,
    HISTORICAL_DATA_PATH
)
from lib.database_engine import OverallScrapingData, TaxRate, World
from lib.scraping_utils import make_get_request

def pull_current_item_data(world: str, item_ids: list[int]) -> dict:
    """
    Pulls current item data from Universalis.

    Args:
        world (str): World for which to pull data.
        item_ids (list[int]): ID of the item to pull.

    Returns:
        dict: Current item market data.
    """
    url = f'{UNIVERSALIS_API_BASE_URL}/{world}/{",".join(item_ids)}'
    response = make_get_request(url)
    if response is None:
        return {}
    return response.json()

def pull_historical_item_data(world: str, item_ids: list[int]) -> dict:
    """
    Pulls historical item data from Universalis.

    Args:
        world (str): World for which to pull data.
        item_ids (list[int]): ID of the item to pull.

    Returns:
        dict: Historical item market data.
    """
    url = f'{UNIVERSALIS_API_BASE_URL}/{HISTORICAL_DATA_PATH}/{world}/{",".join(item_ids)}'
    response = make_get_request(url)
    if response is None:
        return {}
    return response.json()


def pull_worlds() -> list[str]:
    """
    Pulls a list of worlds from Universalis.

    Returns:
        list[str]: List of worlds.
    """
    url = f'{UNIVERSALIS_API_BASE_URL}/{WORLDS_PATH}'
    response = make_get_request(url)
    if response is None:
        return []
    return response.json()


def get_worlds(engine: Engine) -> list[str]:
    """
    Retrieves a list of worlds from the database if present, otherwise pulls it from Universalis.

    Args:
        engine: SQLAlchemy engine.
    
    Returns:
        list[str]: List of worlds.
    """
    with Session(engine) as session:
        if is_world_data_old(session):
            worlds = pull_worlds()
            for world in worlds:
                existing_world = session.query(World).filter(World.name == world['name']).first()
                if existing_world is None:
                    new_world = World(name=world['name'], id=world['id'])
                    session.add(new_world)
            session.commit()
            existing_worlds = session.query(World).all()
            world_names = [world['name'] for world in worlds]
            for world in existing_worlds:
                if world.name not in world_names:
                    session.delete(world)
            if len(worlds) > 0:
                overall_data = session.query(OverallScrapingData).first()
                overall_data.last_world_data_pull = datetime.now().astimezone(timezone.utc)
                session.add(overall_data)
            session.commit()
            return world_names
        worlds = session.query(World).all()
        return [world.name for world in worlds]


def is_world_data_old(session) -> bool:
    """
    Checks if world data is old enough to require refreshing.

    Args:
        session: SQLAlchemy session.
    
    Returns:
        bool: True if world data is old enough to require refreshing, otherwise False.
    """
    overall_data = session.query(OverallScrapingData).first()
    if overall_data is None:
        overall_data = OverallScrapingData()
        session.add(overall_data)
        session.commit()
        return True
    last_pull = overall_data.last_world_data_pull
    if last_pull is None:
        return True
    last_pull = last_pull.astimezone(tz=timezone.utc)
    now = datetime.now().astimezone(tz=None)
    return now - last_pull >= timedelta(days=WORLD_DATA_SCHEDULE_IN_DAYS)


def pull_tax_rates(world: str) -> dict:
    """
    Pulls tax rates from Universalis.

    Args:
        world (str): World for which to pull data.

    Returns:
        dict: Tax rates by city.
    """
    url = f'{UNIVERSALIS_API_BASE_URL}/{TAX_RATES_PATH}?world={world}'
    response = make_get_request(url)
    if response is None:
        return {}
    return response.json()

def get_tax_rates(world: str, engine: Engine) -> dict:
    """
    Retrieves tax rates from the database if present, otherwise pulls them from Universalis.

    Args:
        world (str): World for which to pull data.
        engine (Engine): SQLAlchemy engine.
    
    Returns:
        dict: Tax rates by city.
    """
    with Session(engine) as session:
        if is_tax_rate_data_old(session):
            tax_rates = pull_tax_rates(world)
            for key, value in tax_rates.items():
                existing_tax_rate = session.query(TaxRate).filter(
                    TaxRate.world == world,
                    TaxRate.city == key
                ).first()
                if existing_tax_rate is None:
                    new_tax_rate = TaxRate(
                        world=world,
                        city=key,
                        tax_rate=value
                    )
                    session.add(new_tax_rate)
            session.commit()
            if len(tax_rates.keys()) > 0:
                overall_data = session.query(OverallScrapingData).first()
                overall_data.last_tax_data_pull = datetime.now().astimezone(timezone.utc)
                session.add(overall_data)
                session.commit()
            return tax_rates
        tax_rates = session.query(TaxRate).filter(TaxRate.world == world).all()
        return {tax_rate.city: tax_rate.tax_rate for tax_rate in tax_rates}


def is_tax_rate_data_old(session: Session) -> bool:
    """
    Checks if tax rate data is old enough to require refreshing.

    Args:
        session: SQLAlchemy session.
    """
    overall_data = session.query(OverallScrapingData).first()
    if overall_data is None:
        overall_data = OverallScrapingData()
        session.add(overall_data)
        session.commit()
        return True
    last_pull = overall_data.last_tax_data_pull
    if last_pull is None:
        return True
    last_pull = last_pull.astimezone(tz=timezone.utc)
    now = datetime.now().astimezone(tz=None)
    return now - last_pull >= timedelta(hours=TAX_RATE_SCHEDULE_IN_HOURS)
