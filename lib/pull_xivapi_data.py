"""
Functions for pulling data from XIVAPI.
"""
from lib.constants import XIVAPI_BASE_URL, ITEM_DATA_PATH, ITEM_DATA_FIELDS
from lib.scraping_utils import make_get_request

def pull_basic_item_data(item_id: int) -> dict:
    """
    Pulls basic item data from XIVAPI.

    Args:
        item_id (int): ID of the item to pull.

    Returns:
        dict: Basic item data.
    """
    url = f'{XIVAPI_BASE_URL}/{ITEM_DATA_PATH}/{item_id}?Fields={ITEM_DATA_FIELDS}'
    response = make_get_request(url)
    if response is None:
        return {}
    return response.json()
