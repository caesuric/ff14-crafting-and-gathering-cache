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

def pull_data():
    gathering_points_req = requests.get('https://beta.xivapi.com/api/1/sheet/GatheringPointBase?fields=Item[].value,GatheringType.value,Item[].GatheringItemLevel.value', timeout=30)
    gathering_points = gathering_points_req.json()['rows']
    i = 0
    while len(gathering_points) > 0:
        for gathering_point in gathering_points:
            current_type = 0
            if 'fields' in gathering_point:
                if 'GatheringType' in gathering_point['fields']:
                    current_type = gathering_point['fields']['GatheringType']['value']
                if 'Item' in gathering_point['fields']:
                    for item in gathering_point['fields']['Item']:
                        if item['value'] == 0:
                            continue
                        gathering_items_for_lookup[item['value']] = {'type': current_type, 'level': item['fields']['GatheringItemLevel']['fields']['value']}
        i += 100
        gathering_points_req = requests.get(f'https://beta.xivapi.com/api/1/sheet/GatheringPointBase?fields=Item[].value,GatheringType.value&after={i - 1}', timeout=30)
        gathering_points = gathering_points_req.json()['rows']

    gathering_items_req = requests.get('https://beta.xivapi.com/api/1/sheet/GatheringItem?fields=Item.value', timeout=30)
    gathering_items = gathering_items_req.json()['rows']
    i = 0
    while len(gathering_items) > 0:
        for gathering_item in gathering_items:
            if 'fields' in gathering_item:
                if 'Item' in gathering_item['fields']:
                    item_id = gathering_item['fields']['Item']['value']
                    if gathering_item['row_id'] in gathering_items_for_lookup:
                        items.append({'id': item_id, 'level': gathering_items_for_lookup[gathering_item['row_id']['level']], 'type': gathering_items_for_lookup[gathering_item['row_id']['type']]})
        i += 100
        gathering_items_req = requests.get(f'https://beta.xivapi.com/api/1/sheet/GatheringItem?fields=Item.value&after={i - 1}', timeout=30)
        gathering_items = gathering_items_req.json()['rows']
    for item in items:
        if item['type'] in mining_types:
            mining_items.append({'id': item['id'], 'level': item['level']})
        elif item['type'] in botany_types:
            botany_items.append({'id': item['id'], 'level': item['level']})
    fishing_spots_req = requests.get('https://beta.xivapi.com/api/1/sheet/FishingSpot?fields=Item[].value,GatheringLevel', timeout=30)
    fishing_spots = fishing_spots_req.json()['rows']
    i = 0
    while len(fishing_spots) > 0:
        for fishing_spot in fishing_spots:
            if 'fields' in fishing_spot:
                if 'Item' in fishing_spot['fields']:
                    for item in fishing_spot['fields']['Item']:
                        if item['value'] == 0:
                            continue
                        if 'LevelItem' in item['fields']:
                            item_level = item['fields']['LevelItem']['value']
                            fishing_items.append({'id': item['value'], 'level': fishing_spot['fields']['GatheringLevel']})
        i += 100
        fishing_spots_req = requests.get(f'https://beta.xivapi.com/api/1/sheet/FishingSpot?fields=Item[].value,GatheringLevel&after={i - 1}', timeout=30)
        fishing_spots = fishing_spots_req.json()['rows']
    recipes_req = requests.get('https://beta.xivapi.com/api/1/sheet/Recipe?fields=CraftType.Name,ItemResult.Value,RecipeLevelTable.value', timeout=30)
    recipes = recipes_req.json()['rows']
    i = 0
    while (len(recipes) > 0):
        for recipe in recipes:
            if recipe['fields']['ItemResult']['value'] == 0:
                continue
            craft_type = recipe['fields']['CraftType']['fields']['Name']
            item_id = recipe['fields']['ItemResult']['value']
            item_level = recipe['fields']['RecipeLevelTable']['value']
            if craft_type not in crafting_items:
                crafting_items[craft_type] = []
            crafting_items[craft_type].append({'id': item_id, 'level': item_level})
        i += 100
        recipes_req = requests.get(f'https://beta.xivapi.com/api/1/sheet/Recipe?fields=CraftType.Name,ItemResult.Value,RecipeLevelTable.value&after={i - 1}', timeout=30)
        recipes = recipes_req.json()['rows']

def main():
    pull_data()
    if not os.path.exists('data') or not os.path.isdir('data'):
        os.mkdir('data')
    with open('data/mining.json', 'w') as f:
        f.write(json.dumps(mining_items))
    with open('data/botany.json', 'w') as f:
        f.write(json.dumps(botany_items))
    with open('data/fishing.json', 'w') as f:
        f.write(json.dumps(fishing_items))
    with open('data/crafting.json', 'w') as f:
        f.write(json.dumps(crafting_items))

if __name__=='__main__':
    main()
