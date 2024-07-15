"""
Functions for pulling data from Universalis.
"""
from lib.constants import (
    UNIVERSALIS_API_BASE_URL,
    WORLDS_PATH,
    TAX_RATES_PATH,
    HISTORICAL_DATA_PATH
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
