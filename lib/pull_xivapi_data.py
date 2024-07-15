"""
Functions for pulling data from XIVAPI.
"""
import datetime
import math
from typing import Optional
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from lib.constants import (
    BASIC_ITEM_DATA_SCHEDULE_IN_DAYS,
    XIVAPI_BASE_URL,
    ITEM_DATA_PATH,
    ITEM_DATA_FIELDS
)
from lib.database_engine import ItemData, OverallScrapingData
from lib.scraping_utils import make_get_request


def pull_basic_item_data(item_id: int) -> Optional[dict]:
    """
    Pulls basic item data from XIVAPI.

    Args:
        item_id (int): ID of the item to pull.

    Returns:
        Optional[dict]: Basic item data, or None if not found.
    """
    url = f'{XIVAPI_BASE_URL}/{ITEM_DATA_PATH}/{item_id}?Fields={ITEM_DATA_FIELDS}'
    response = make_get_request(url)
    if response is None:
        return None
    return response.json()


def get_basic_item_data(item_ids: list[int], engine: Engine) -> dict:
    """
    Retrieves basic item data from the database if it exists, otherwise pulls it from XIVAPI.

    Args:
        item_ids (list[int]): IDs of the items to retrieve.
        engine (Engine): SQLAlchemy engine.

    Returns:
        dict: Basic item data for each ID, or operation status if not complete.
    """
    with Session(engine) as session:
        statement = select(ItemData).where(ItemData.id.in_(item_ids))
        results = session.execute(statement)
        unhandled_ids = item_ids.copy()
        stale_ids = []
        output = {
            'complete': False,
            'estimated_operation_time': math.inf,
            'operation_time_so_far': 0,
            'items': {}
        }
        for result in results:
            now = datetime.datetime.now(datetime.timezone.utc)
            stale_date = now - datetime.timedelta(days=BASIC_ITEM_DATA_SCHEDULE_IN_DAYS)
            if result.last_data_pull < stale_date:
                stale_ids.append(result.id)
                continue
            unhandled_ids.remove(result.id)
            output['items'][result.id] = {
                'level': result.level,
                'name': result.name,
                'icon_path': result.icon_path
            }
        start_of_operation = datetime.datetime.now(datetime.timezone.utc)
        overall_scraping_data = session.query(OverallScrapingData).first()
        if overall_scraping_data:
            output['estimated_operation_time'] = (
                overall_scraping_data.average_item_data_pull_time_in_seconds *
                len(unhandled_ids)
            )
        for unhandled_id in unhandled_ids:
            now = datetime.datetime.now(datetime.timezone.utc)
            output['operation_time_so_far'] = now - start_of_operation
            yield output
            data = pull_basic_item_data(unhandled_id)
            if data:
                icon_path = data['fields']['Icon']['path']
                icon_path = icon_path.replace('ui/icon/', 'i/').replace('.tex', '')
                if unhandled_id not in stale_ids:
                    new_entry = ItemData(
                        id=unhandled_id,
                        name=data['fields']['Name'],
                        icon_path=icon_path,
                        last_data_pull=now
                    )
                else:
                    new_entry = session.query(ItemData).filter(ItemData.id == unhandled_id).first()
                    new_entry.last_data_pull = now
                    new_entry.name = data['fields']['Name']
                    new_entry.icon_path = icon_path
                session.add(new_entry)
                output['items'][unhandled_id] = {
                    'level': new_entry.level,
                    'name': new_entry.name,
                    'icon_path': new_entry.icon_path
                }
        end_of_operation = datetime.datetime.now(datetime.timezone.utc)
        operation_duration = end_of_operation - start_of_operation
        overall_scraping_data = session.query(OverallScrapingData).first()
        if not overall_scraping_data:
            overall_scraping_data = OverallScrapingData(
                last_world_data_pull=None,
                last_tax_data_pull=None,
                average_item_data_pull_time_in_seconds=operation_duration.total_seconds(),
                total_item_data_pulls=len(item_ids),
                average_item_market_pull_time_in_seconds=None,
                total_item_market_pulls=None,
                average_historical_item_market_pull_time_in_seconds=None,
                total_historical_item_market_pulls=None
            )
        else:
            total_duration = (
                overall_scraping_data.average_item_data_pull_time_in_seconds *
                overall_scraping_data.total_item_data_pulls
            )
            total_duration += operation_duration.total_seconds()
            overall_scraping_data.total_item_data_pulls += len(item_ids)
            overall_scraping_data.average_item_data_pull_time_in_seconds = (
                total_duration /
                overall_scraping_data.total_item_data_pulls
            )
        session.add(overall_scraping_data)
        session.commit()
        output['complete'] = True
        return output
