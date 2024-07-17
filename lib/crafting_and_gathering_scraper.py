"""
Data scraper for crafting and gathering lists.
"""
from datetime import datetime, timedelta, timezone
from threading import Thread
from time import sleep
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from lib.constants import CRAFTING_AND_GATHERING_DATA_SCHEDULE_IN_DAYS
from lib.database_engine import CraftingType, ItemData, OverallScrapingData
from lib.scraping_utils import make_get_request
from lib.xivapi_data import get_basic_item_data

mining_types = [0, 1]
botany_types = [2, 3]
gathering_items_for_lookup = {}
items = []
mining_items = []
botany_items = []
fishing_items = []
crafting_items = {}


def start_crafting_and_gathering_scraper(engine: Engine):
    """
    Kicks off the thread for crafting and gathering data scraping.

    Args:
        engine (Engine): SQLAlchemy engine.
    """
    scraper_process = Thread(target=scraper_main_thread, args=(engine,))
    scraper_process.start()


def scraper_main_thread(engine: Engine):
    """
    Main thread for crafting and gathering data scraping.

    Args:
        engine (Engine): SQLAlchemy engine.
    """
    with Session(engine) as session:
        overall_data = session.query(OverallScrapingData).first()
        if overall_data is None:
            overall_data = OverallScrapingData()
            session.add(overall_data)
            session.commit()
        while True:
            if (
                overall_data.crafting_and_gathering_data_last_pull is None or
                is_old(overall_data.crafting_and_gathering_data_last_pull)
            ):
                pull_data()
                store_data(session, engine)
                overall_data.crafting_and_gathering_data_last_pull = datetime.now().astimezone(timezone.utc)
                session.add(overall_data)
                session.commit()
            sleep(24 * 60 * 60)


def update_item_in_database(item: dict, item_type: str, session: Session, engine: Engine):
    """
    Updates an item in the database.

    Args:
        item (dict): Item data.
        item_type (str): Type of item.
        session (Session): SQLAlchemy session.
        engine (Engine): SQLAlchemy engine.
    """
    entry = session.query(ItemData).filter_by(id=item['id']).first()
    if entry is None or entry.source_classes is None or entry.source_class_levels is None:
        for _ in get_basic_item_data([item['id']], engine):
            pass
        entry = session.query(ItemData).filter_by(id=item['id']).first()
        if entry is None:
            return
    source_classes = entry.source_classes.copy()
    source_class_levels = entry.source_class_levels.copy()
    if item_type not in entry.source_classes:
        source_classes.append(item_type)
        source_class_levels.append(item['level'])
    else:
        index = source_classes.index(item_type)
        source_class_levels[index] = item['level']
    entry.source_classes = source_classes
    entry.source_class_levels = source_class_levels
    session.add(entry)
    session.commit()

def update_crafting_type(crafting_type: str, session: Session):
    """
    Updates a crafting type in the database.

    Args:
        crafting_type (str): Crafting type.
        session (Session): SQLAlchemy session.
    """
    entry = session.query(CraftingType).filter_by(name=crafting_type).first()
    if entry is None:
        entry = CraftingType(name=crafting_type)
        session.add(entry)
        session.commit()


def store_data(session: Session, engine: Engine):
    """
    Stores the crafting and gathering data in the database.

    Args:
        session (Session): SQLAlchemy session.
        engine (Engine): SQLAlchemy engine.
    """
    print('Storing data in database...')
    print('\tMining items...')
    for item in mining_items:
        update_item_in_database(item, 'Mining', session, engine)
    print('\tBotany items...')
    for item in botany_items:
        update_item_in_database(item, 'Botany', session, engine)
    print('\tFishing items...')
    for item in fishing_items:
        update_item_in_database(item, 'Fishing', session, engine)
    print('\tCrafting items...')
    for craft_type, subtype_items in crafting_items.items():
        update_crafting_type(craft_type, session)
        for item in subtype_items:
            update_item_in_database(item, craft_type, session, engine)
    print('\tDONE.')


def is_old(last_pull: datetime) -> bool:
    """
    Determines if the last pull has expired.

    Args:
        last_pull (datetime): The last time data was pulled.

    Returns:
        bool: True if the data is old, otherwise False.
    """
    last_pull = last_pull.astimezone(tz=timezone.utc)
    now = datetime.now().astimezone(tz=None)
    return now - last_pull >= timedelta(days=CRAFTING_AND_GATHERING_DATA_SCHEDULE_IN_DAYS)


def get_data_for_page(base_url, i):
    """
    Retrieves data from XIVAPI starting from a specific result.
    """
    if i == 0:
        req = make_get_request(base_url)
    else:
        req = make_get_request(f'{base_url}&after={i}')
    if req is None:
        return []
    return req.json()['rows']


def get_paginated_data(base_url):
    """
    Retrieves paginated data from XIVAPI.
    """
    i = 0
    data = []
    sub_data = get_data_for_page(base_url, i)
    while len(sub_data) > 0:
        data.extend(sub_data)
        i = sub_data[-1]['row_id']
        sub_data = get_data_for_page(base_url, i)
    return data


def construct_xivapi_url(sheet_name, fields):
    """
    Constructs a URL for XIVAPI with fields.
    """
    return f'https://beta.xivapi.com/api/1/sheet/{sheet_name}?fields={",".join(fields)}'


def get_gathering_points():
    """
    Retrieves gathering points from XIVAPI.
    """
    print('\tRetrieving gathering points...')
    url = construct_xivapi_url(
        'GatheringPointBase',
        [
            'Item[].value',
            'GatheringType.value',
            'Item[].GatheringItemLevel.value'
        ]
    )
    gathering_points = get_paginated_data(url)
    for gathering_point in gathering_points:
        current_type = gathering_point['fields']['GatheringType']['value']
        for item in gathering_point['fields']['Item']:
            if item['value'] == 0:
                continue
            if item['value'] not in gathering_items_for_lookup:
                gathering_items_for_lookup[item['value']] = {
                    'types': [current_type],
                    'level': item['fields']['GatheringItemLevel']['value']
                }
            elif current_type not in gathering_items_for_lookup[item['value']]['types']:
                gathering_items_for_lookup[item['value']]['types'].append(current_type)


def get_gathering_items():
    """
    Retrieves gathering items from XIVAPI.
    """
    print('\tRetrieving gathering items...')
    url = construct_xivapi_url('GatheringItem', ['Item.value'])
    gathering_items = get_paginated_data(url)
    for gathering_item in gathering_items:
        item_id = gathering_item['fields']['Item']['value']
        if gathering_item['row_id'] in gathering_items_for_lookup:
            for item_type in gathering_items_for_lookup[gathering_item['row_id']]['types']:
                items.append(
                    {
                        'id': item_id,
                        'level': gathering_items_for_lookup[gathering_item['row_id']]['level'],
                        'type': item_type
                    }
                )


def convert_gathering_item_levels():
    """
    Converts gathering item levels to raw levels.
    """
    print('\tConverting gathering item levels...')
    url = construct_xivapi_url('GatheringItemLevelConvertTable', [
                               'GatheringItemLevel'])
    level_conversions = get_paginated_data(url)
    for item in items:
        lookup = level_conversions[item['level']]
        item['level'] = lookup['fields']['GatheringItemLevel']


def sort_mining_and_botany_items():
    """
    Sorts mining and botany items.
    """
    print('\tSorting mining and botany items...')
    for item in items:
        item_type = item['type']
        if item_type in mining_types:
            mining_items.append({'id': item['id'], 'level': item['level']})
        elif item_type in botany_types:
            botany_items.append({'id': item['id'], 'level': item['level']})


def get_fishing_spots():
    """
    Retrieves fishing spots from XIVAPI.
    """
    print('\tRetrieving fishing spots...')
    url = construct_xivapi_url(
        'FishingSpot', ['Item[].value', 'GatheringLevel'])
    fishing_spots = get_paginated_data(url)
    for fishing_spot in fishing_spots:
        for item in fishing_spot['fields']['Item']:
            if item['value'] == 0:
                continue
            fishing_items.append(
                {
                    'id': item['value'],
                    'level': fishing_spot['fields']['GatheringLevel']
                }
            )


def get_recipes():
    """
    Retrieves recipes from XIVAPI.
    """
    print('\tRetrieving recipes...')
    url = construct_xivapi_url(
        'Recipe',
        [
            'CraftType.Name',
            'ItemResult.Value',
            'RecipeLevelTable.ClassJobLevel'
        ]
    )
    recipes = get_paginated_data(url)
    for recipe in recipes:
        if recipe['fields']['ItemResult']['value'] == 0:
            continue
        craft_type = recipe['fields']['CraftType']['fields']['Name']
        item_id = recipe['fields']['ItemResult']['value']
        item_level = recipe['fields']['RecipeLevelTable']['fields']['ClassJobLevel']
        if craft_type not in crafting_items:
            crafting_items[craft_type] = []
        crafting_items[craft_type].append({'id': item_id, 'level': item_level})


def pull_data():
    """
    Pulls crafting and gathering data from XIVAPI.
    """
    print('Pulling crafting and gathering data...')
    gathering_items_for_lookup.clear()
    items.clear()
    mining_items.clear()
    botany_items.clear()
    fishing_items.clear()
    crafting_items.clear()
    get_gathering_points()
    get_gathering_items()
    convert_gathering_item_levels()
    sort_mining_and_botany_items()
    get_fishing_spots()
    get_recipes()
