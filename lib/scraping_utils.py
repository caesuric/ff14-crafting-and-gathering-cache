"""
Utility functions for scraping.
"""
from typing import Optional
from requests import Response, Session
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import RequestException

def make_get_request(url: str) -> Optional[Response]:
    """
    Makes a get request and retries until it succeeds or retry limit is reached.

    Args:
        url (str): URL from which to GET.

    Returns:
        Optional[Response]: Response object from the GET request.
    """
    session = Session()
    retry = Retry(
        total = 10,
        read = 10,
        connect = 10,
        backoff_factor = 0.3,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries = retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    try:
        response = session.get(url)
        if response.status_code != 200:
            print(f'Failed to GET {url}. Status code: {response.status_code}')
            return None
        return response
    except RequestException:
        return None
