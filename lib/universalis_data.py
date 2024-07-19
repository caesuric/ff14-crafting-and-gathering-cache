"""
Functions for pulling data from Universalis.
"""
from datetime import datetime, timedelta, timezone
import math
from statistics import median
import time
from typing import Generator
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from lib.constants import (
    CURRENT_ITEM_MARKET_DATA_SCHEDULE_IN_HOURS,
    HISTORICAL_ITEM_MARKET_DATA_SCHEDULE_IN_DAYS,
    TAX_RATE_SCHEDULE_IN_HOURS,
    UNIVERSALIS_API_BASE_URL,
    WORLD_DATA_SCHEDULE_IN_DAYS,
    WORLDS_PATH,
    TAX_RATES_PATH,
    HISTORICAL_DATA_PATH
)
from lib.database_engine import (
    ItemMarketDataCurrent,
    ItemMarketDataHistorical,
    OverallScrapingData,
    TaxRate,
    World
)
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
    if len(item_ids) == 0:
        return {}
    if len(item_ids) == 1:
        url = f'{UNIVERSALIS_API_BASE_URL}/{world}/{item_ids[0]}'
    else:
        url = f'{UNIVERSALIS_API_BASE_URL}/{world}/{",".join([str(item) for item in item_ids])}'
    response = make_get_request(url)
    if response is None:
        return {}
    return response.json()


def get_current_item_data(
        world: str,
        item_ids: list[int],
        engine: Engine
    ) -> Generator[dict, None, dict]:
    """
    Retrieves current item data from the database if present, otherwise pulls it from Universalis.

    Args:
        world (str): World for which to pull data.
        item_ids (list[int]): ID of the item to pull.
        engine (Engine): SQLAlchemy engine.
    """
    unhandled_ids = item_ids.copy()
    stale_ids = []
    with Session(engine) as session:
        results = session.query(ItemMarketDataCurrent).where(
            ItemMarketDataCurrent.ffxiv_id.in_(item_ids)
        ).filter(
            ItemMarketDataCurrent.world == world
        ).all()
        output = {
            'complete': False,
            'estimated_operation_time': math.inf,
            'operation_time_so_far': 0,
            'last_update': time.time(),
            'items': {}
        }
        for result in results:
            now = datetime.now().astimezone(tz=None)
            stale_date = now - \
                timedelta(hours=CURRENT_ITEM_MARKET_DATA_SCHEDULE_IN_HOURS)
            if result.last_data_pull.astimezone(timezone.utc) < stale_date:
                stale_ids.append(result.ffxiv_id)
                continue
            unhandled_ids.remove(result.ffxiv_id)
            output['items'][result.ffxiv_id] = {
                'current_min_price_nq': result.current_min_price_nq
            }
        start_of_operation = datetime.now().astimezone(tz=None)
        overall_scraping_data = session.query(OverallScrapingData).first()
        if overall_scraping_data and \
                overall_scraping_data.average_item_market_pull_time_in_seconds and \
                overall_scraping_data.total_item_market_pulls:
            output['estimated_operation_time'] = \
                overall_scraping_data.average_item_market_pull_time_in_seconds * \
                len(unhandled_ids)
        yield output
        sets_of_a_hundred = [unhandled_ids.copy()]
        total_data = {}
        while len(sets_of_a_hundred[-1]) > 100:
            sets_of_a_hundred.append(sets_of_a_hundred[-1][100:])
        for batch in sets_of_a_hundred:
            now = datetime.now().astimezone(tz=None)
            output['operation_time_so_far'] = (now - start_of_operation).total_seconds()
            output['last_update'] = time.time(),
            yield output
            data = pull_current_item_data(world, batch)
            if data:
                if len(unhandled_ids) == 1:
                    total_data[str(unhandled_ids[0])] = data
                else:
                    for key, value in data['items'].items():
                        total_data[key] = value
        for unhandled_id in unhandled_ids:
            if str(unhandled_id) not in total_data:
                continue
            now = datetime.now().astimezone(tz=None)
            if unhandled_id not in stale_ids:
                new_entry = ItemMarketDataCurrent(
                    ffxiv_id=unhandled_id,
                    world=world,
                    current_min_price_nq=total_data[str(unhandled_id)]['minPriceNQ'],
                    last_data_pull=now
                )
            else:
                new_entry = session.query(ItemMarketDataCurrent).filter(
                    ItemMarketDataCurrent.ffxiv_id == unhandled_id
                    ).filter(
                        ItemMarketDataCurrent.world == world
                    ).first()
                new_entry.last_data_pull = now
                new_entry.current_min_price_nq = total_data[str(unhandled_id)]['minPriceNQ']
            session.add(new_entry)
            output['items'][unhandled_id] = {
                'current_min_price_nq': new_entry.current_min_price_nq
            }
        end_of_operation = datetime.now().astimezone(tz=None)
        operation_duration = end_of_operation - start_of_operation
        overall_scraping_data = session.query(OverallScrapingData).first()
        if not overall_scraping_data:
            overall_scraping_data = OverallScrapingData(
                last_world_data_pull=None,
                last_tax_data_pull=None,
                average_item_data_pull_time_in_seconds=None,
                total_item_data_pulls=None,
                average_item_market_pull_time_in_seconds=operation_duration.total_seconds(),
                total_item_market_pulls=len(item_ids),
                average_historical_item_market_pull_time_in_seconds=None,
                total_historical_item_market_pulls=None,
                crafting_and_gathering_data_last_pull=None
            )
        else:
            if not overall_scraping_data.average_item_market_pull_time_in_seconds \
                or not overall_scraping_data.total_item_market_pulls:
                total_duration = 0
            else:
                total_duration = overall_scraping_data.average_item_market_pull_time_in_seconds * \
                    overall_scraping_data.total_item_market_pulls
            total_duration += operation_duration.total_seconds()
            if not overall_scraping_data.total_item_market_pulls:
                overall_scraping_data.total_item_market_pulls = 0
            overall_scraping_data.total_item_market_pulls += len(item_ids)
            overall_scraping_data.average_item_market_pull_time_in_seconds = total_duration / \
                overall_scraping_data.total_item_market_pulls
        session.add(overall_scraping_data)
        session.commit()
        output['complete'] = True
        yield output
        return output


def pull_historical_item_data(world: str, item_ids: list[int]) -> dict:
    """
    Pulls historical item data from Universalis.

    Args:
        world (str): World for which to pull data.
        item_ids (list[int]): ID of the item to pull.

    Returns:
        dict: Historical item market data.
    """
    if len(item_ids) == 0:
        return {}
    if len(item_ids) == 1:
        url = f'{UNIVERSALIS_API_BASE_URL}/{HISTORICAL_DATA_PATH}/{world}/{item_ids[0]}?entriesWithin=2592000'
    else:
        url = f'{UNIVERSALIS_API_BASE_URL}/{HISTORICAL_DATA_PATH}/{world}/{",".join([str(item) for item in item_ids])}?entriesWithin=2592000'
    response = make_get_request(url)
    if response is None:
        return {}
    return response.json()


def get_historical_item_data(
        world: str,
        item_ids: list[int],
        engine: Engine
    ) -> Generator[dict, None, dict]:
    """
    Retrieves historical item data from the database if present,
        otherwise pulls it from Universalis.

    Args:
        world (str): World for which to pull data.
        item_ids (list[int]): ID of the item to pull.
        engine (Engine): SQLAlchemy engine.
    """
    unhandled_ids = item_ids.copy()
    stale_ids = []
    with Session(engine) as session:
        results = session.query(
            ItemMarketDataHistorical
        ).where(
            ItemMarketDataHistorical.ffxiv_id.in_(item_ids)
        ).where(
            ItemMarketDataHistorical.world == world
        ).all()
        output = {
            'complete': False,
            'estimated_operation_time': math.inf,
            'operation_time_so_far': 0,
            'last_update': time.time(),
            'items': {}
        }
        for result in results:
            now = datetime.now().astimezone(tz=None)
            stale_date = now - \
                timedelta(days=HISTORICAL_ITEM_MARKET_DATA_SCHEDULE_IN_DAYS)
            if result.last_data_pull.astimezone(timezone.utc) < stale_date:
                stale_ids.append(result.ffxiv_id)
                continue
            unhandled_ids.remove(result.ffxiv_id)
            output['items'][result.ffxiv_id] = {
                'nq_daily_sale_velocity': result.nq_daily_sale_velocity,
                'average_price_per_unit': result.average_price_per_unit,
                'num_items_sold': result.num_items_sold,
                'possible_money_per_day': result.possible_money_per_day,
                'median_stack_size': result.median_stack_size,
                'median_price': result.median_price
            }
        start_of_operation = datetime.now().astimezone(tz=None)
        overall_scraping_data = session.query(OverallScrapingData).first()
        if overall_scraping_data and \
                overall_scraping_data.average_historical_item_market_pull_time_in_seconds and \
                overall_scraping_data.total_historical_item_market_pulls:
            output['estimated_operation_time'] = \
                overall_scraping_data.average_historical_item_market_pull_time_in_seconds * \
                len(unhandled_ids)
        yield output
        sets_of_a_hundred = [unhandled_ids.copy()]
        total_data = {}
        while len(sets_of_a_hundred[-1]) > 100:
            sets_of_a_hundred.append(sets_of_a_hundred[-1][100:])
        for batch in sets_of_a_hundred:
            now = datetime.now().astimezone(tz=None)
            output['operation_time_so_far'] = (
                now - start_of_operation).total_seconds()
            output['last_update'] = time.time(),
            yield output
            data = pull_historical_item_data(world, batch)
            if data:
                if len(unhandled_ids) == 1:
                    total_data[str(unhandled_ids[0])] = data
                else:
                    for key, value in data['items'].items():
                        total_data[key] = value
        for unhandled_id in unhandled_ids:
            if str(unhandled_id) not in total_data:
                continue
            now = datetime.now().astimezone(tz=None)
            if unhandled_id not in stale_ids:
                num_items_sold = 0
                average_price = 0
                stack_sizes = []
                prices = []
                for entry in total_data[str(unhandled_id)]['entries']:
                    num_items_sold += entry['quantity']
                    average_price += entry['pricePerUnit'] * entry['quantity']
                    stack_sizes.append(entry['quantity'])
                    prices.append(entry['pricePerUnit'])
                if num_items_sold > 0:
                    average_price /= num_items_sold
                    median_stack_size = median(stack_sizes)
                    median_price = median(prices)
                else:
                    median_stack_size = 0
                    median_price = 0
                possible_money_per_day = int(
                    num_items_sold *
                    median_price /
                    30
                )
                new_entry = ItemMarketDataHistorical(
                    ffxiv_id=unhandled_id,
                    world=world,
                    nq_daily_sale_velocity=int(
                        total_data[str(unhandled_id)]['nqSaleVelocity']
                    ),
                    average_price_per_unit=int(average_price),
                    num_items_sold=num_items_sold,
                    possible_money_per_day = possible_money_per_day,
                    median_stack_size = median_stack_size,
                    median_price = median_price,
                    last_data_pull=now
                )
            else:
                new_entry = session.query(ItemMarketDataHistorical).where(
                    ItemMarketDataHistorical.ffxiv_id == unhandled_id
                ).where(
                    ItemMarketDataHistorical.world == world
                ).first()
                num_items_sold = 0
                average_price = 0
                stack_sizes = []
                prices = []
                for entry in total_data[str(unhandled_id)]['entries']:
                    num_items_sold += entry['quantity']
                    average_price += entry['pricePerUnit'] * entry['quantity']
                    stack_sizes.append(entry['quantity'])
                    prices.append(entry['pricePerUnit'])
                if num_items_sold > 0:
                    average_price /= num_items_sold
                    median_stack_size = median(stack_sizes)
                    median_price = median(prices)
                else:
                    median_stack_size = 0
                    median_price = 0
                possible_money_per_day = int(
                    num_items_sold *
                    median_price /
                    30
                )
                new_entry.last_data_pull = now
                new_entry.nq_daily_sale_velocity = int(
                    total_data[str(unhandled_id)]['nqSaleVelocity']
                )
                new_entry.average_price_per_unit = int(average_price)
                new_entry.num_items_sold = num_items_sold
                new_entry.possible_money_per_day = possible_money_per_day
                new_entry.median_stack_size = median_stack_size
                new_entry.median_price = median_price
            session.add(new_entry)
            output['items'][unhandled_id] = {
                'nq_daily_sale_velocity': new_entry.nq_daily_sale_velocity,
                'average_price_per_unit': new_entry.average_price_per_unit,
                'num_items_sold': new_entry.num_items_sold,
                'possible_money_per_day': new_entry.possible_money_per_day,
                'median_stack_size': new_entry.median_stack_size,
                'median_price': new_entry.median_price
            }
        end_of_operation = datetime.now().astimezone(tz=None)
        operation_duration = end_of_operation - start_of_operation
        overall_scraping_data = session.query(OverallScrapingData).first()
        if not overall_scraping_data:
            overall_scraping_data = OverallScrapingData(
                last_world_data_pull=None,
                last_tax_data_pull=None,
                average_item_data_pull_time_in_seconds=None,
                total_item_data_pulls=None,
                average_item_market_pull_time_in_seconds=None,
                total_item_market_pulls=None,
                average_historical_item_market_pull_time_in_seconds=operation_duration.total_seconds(),
                total_historical_item_market_pulls=len(item_ids),
                crafting_and_gathering_data_last_pull=None
            )
        else:
            if not overall_scraping_data.average_historical_item_market_pull_time_in_seconds \
                    or not overall_scraping_data.total_historical_item_market_pulls:
                total_duration = 0
            else:
                total_duration = overall_scraping_data.average_historical_item_market_pull_time_in_seconds * \
                    overall_scraping_data.total_historical_item_market_pulls
            total_duration += operation_duration.total_seconds()
            if not overall_scraping_data.total_historical_item_market_pulls:
                overall_scraping_data.total_historical_item_market_pulls = 0
            overall_scraping_data.total_historical_item_market_pulls += len(item_ids)
            overall_scraping_data.average_historical_item_market_pull_time_in_seconds = total_duration / \
                overall_scraping_data.total_historical_item_market_pulls
        session.add(overall_scraping_data)
        session.commit()
        output['complete'] = True
        yield output
        return output


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
                existing_world = session.query(World).filter(
                    World.name == world['name']).first()
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
