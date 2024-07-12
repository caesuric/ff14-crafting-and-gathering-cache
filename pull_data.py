"""
Pulls data on craftable and gatherable items, and stores it in JSON files.
"""
import json
import os
import requests

mining_types = [0,1]
botany_types = [2,3]
gathering_items_for_lookup = {}
items = []
mining_items = []
botany_items = []
fishing_items = []
crafting_items = {}

def get_data_for_page(base_url, i):
    """
    Retrieves data from XIVAPI starting from a specific result.
    """
    retries = 10
    while retries > 0:
        try:
            if i == 0:
                req = requests.get(base_url, timeout=30)
            else:
                req = requests.get(f'{base_url}&after={i - 1}', timeout=30)
            if req.status_code != 200:
                print(f'Retrying for {base_url} for results from {i}')
                retries -= 1
                continue
            return req.json()['rows']
        except ConnectionError:
            print(f'Retrying for {base_url} for results from {i}')
            retries -= 1
    print(f'OUT OF RETRIES FOR {base_url} for results from {i}')
    return []

def get_paginated_data(base_url):
    """
    Retrieves paginated data from XIVAPI.
    """
    i = 0
    data = []
    sub_data = get_data_for_page(base_url, i)
    while len(sub_data) > 0:
        data.extend(sub_data)
        i += 100
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
    print('Retrieving gathering points...')
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
            gathering_items_for_lookup[item['value']] = {
                'type': current_type,
                'level': item['fields']['GatheringItemLevel']['value']
            }

def get_gathering_items():
    """
    Retrieves gathering items from XIVAPI.
    """
    print('Retrieving gathering items...')
    url = construct_xivapi_url('GatheringItem', ['Item.value'])
    gathering_items = get_paginated_data(url)
    for gathering_item in gathering_items:
        item_id = gathering_item['fields']['Item']['value']
        if gathering_item['row_id'] in gathering_items_for_lookup:
            items.append(
                {
                    'id': item_id,
                    'level': gathering_items_for_lookup[gathering_item['row_id']]['level'],
                    'type': gathering_items_for_lookup[gathering_item['row_id']]['type']
                }
            )

def sort_mining_and_botany_items():
    """
    Sorts mining and botany items.
    """
    print('Sorting mining and botany items...')
    for item in items:
        if item['type'] in mining_types:
            mining_items.append({'id': item['id'], 'level': item['level']})
        elif item['type'] in botany_types:
            botany_items.append({'id': item['id'], 'level': item['level']})

def get_fishing_spots():
    """
    Retrieves fishing spots from XIVAPI.
    """
    print('Retrieving fishing spots...')
    url = construct_xivapi_url('FishingSpot', ['Item[].value', 'GatheringLevel'])
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
    print('Retrieving recipes...')
    url = construct_xivapi_url(
        'Recipe',
        [
            'CraftType.Name',
            'ItemResult.Value',
            'RecipeLevelTable.value'
        ]
    )
    recipes = get_paginated_data(url)
    for recipe in recipes:
        if recipe['fields']['ItemResult']['value'] == 0:
            continue
        craft_type = recipe['fields']['CraftType']['fields']['Name']
        item_id = recipe['fields']['ItemResult']['value']
        item_level = recipe['fields']['RecipeLevelTable']['value']
        if craft_type not in crafting_items:
            crafting_items[craft_type] = []
        crafting_items[craft_type].append({'id': item_id, 'level': item_level})

def pull_data():
    """
    Pulls crafting and gathering data from XIVAPI.
    """
    get_gathering_points()
    get_gathering_items()
    sort_mining_and_botany_items()
    get_fishing_spots()
    get_recipes()

def write_file(file_name, data):
    """
    Writes data to a file
    """
    with open(f'data/{file_name}.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(data))

def main():
    """
    Main function that pulls data and stores it in JSON files
    """
    pull_data()
    if not os.path.exists('data') or not os.path.isdir('data'):
        os.mkdir('data')
    write_file('mining', mining_items)
    write_file('botany', botany_items)
    write_file('fishing', fishing_items)
    write_file('crafting', crafting_items)
    print('Data successfully pulled and stored in JSON files.')
    print(f'Mining items: {len(mining_items)}')
    print(f'Botany items: {len(botany_items)}')
    print(f'Fishing items: {len(fishing_items)}')
    crafting_item_total = 0
    for _,v in crafting_items.items():
        crafting_item_total += len(v)
    print(f'Crafting items: {crafting_item_total}')

if __name__=='__main__':
    main()
