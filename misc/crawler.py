import json
import random
import logging
from typing import Any
from pathlib import Path

import requests
import urllib.parse

PROXY_FILE = Path(__file__).parent / 'proxies.txt'

logger = logging.getLogger('Crawler')
logging.getLogger("urllib3").setLevel(logging.WARNING)


def url_encode(input_str: str) -> str:
    return urllib.parse.quote(input_str.encode('utf8'))


class Proxy:
    """
    A class for working with the proxy list.
    """

    _proxies: list[str]

    def __init__(self):
        self._prepare_proxy_list()

    def _prepare_proxy_list(self) -> None:
        """
        Collects the proxy from the file and prepares it in the format for the "requests" library.

        :return: List of proxy.
        """

        file_proxies = PROXY_FILE.read_text().split('\n')

        self._proxies = [
            {
                "http": _proxy,
                "https": _proxy,
            }
            for _proxy in file_proxies
        ]

    def get(self) -> dict[str, str]:
        """
        Returns a random proxy

        :return: Dictionary with proxy values.
            e.g.: {
                    "http": username:password@host:port,
                    "https": username:password@host:port,
                }
        """
        return random.choice(self._proxies)


class Crawler:
    """
    A class for working with requests to the target server.
    """
    max_retry = 3       # maximum number of requests per url

    def __init__(self):
        self.proxies = Proxy()
        self.session = requests.session()
        self.session.headers['user-agent'] = 'Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0'

    def get_url(self, url: str, retry: int = 0,) -> list[dict] | dict[str, Any] | None:
        """
        Sends a get request to the passed url.

        :param url: Target url.
        :param retry: Number of retries.
        :return: Response data from the server.
        """
        if retry >= self.max_retry:
            logger.warning(f'Max retry error. Skipped {url!r}')
            return
        logger.debug(f'Getting {url!r}')

        try:
            response = self.session.get(url, proxies=self.proxies.get(), timeout=5)
        except (requests.exceptions.Timeout, requests.exceptions.ProxyError):
            logger.error(f'Timeout error. Retry {url!r}')
            return self.get_url(url, retry=retry + 1)

        if response.status_code != 200:
            logger.error(f'Error <{response.status_code}> ')
            return self.get_url(url, retry=retry + 1)

        try:
            return response.json()
        except json.decoder.JSONDecodeError:
            logger.error(f'JSONDecodeError. Retry {url!r}')
            return self.get_url(url, retry=retry + 1)

    def search_city(self, city_name: str) -> list[dict]:
        """
        Searches data about cities by city_name.

        :param city_name: Name of the city.
        :return: Response data about the cities found.
        """
        encoded_name = url_encode(city_name)
        url = f'https://www.bethowen.ru/api/local/v1/cities/search?term={encoded_name}&city_type=all'
        response = self.get_url(url)
        return response['cities']

    def search_store_ids(self, city_id: int) -> list[int]:
        """
        Searches data about stores in the target city.

        :param city_id: ID of the target city.
        :return: Response data about the stores found.
        """
        url = 'https://www.bethowen.ru/local/ajax/getRegionalityData.php'
        self.session.headers['cookie'] = f'BETHOWEN_GEO_TOWN_ID={city_id};'
        response = self.get_url(url)
        return response['stores']

    def set_city_id(self, city_id: str) -> None:
        """
        Sets the city_id in the server settings.

        :param city_id: ID of the target city.
        """
        logger.debug('Setting a city_id in a requests.session')
        url = 'https://www.bethowen.ru/api/local/v1/users/location'
        self.session.post(url, json={"location_id": city_id})


    def get_number_products(self) -> int:
        """
        Searches for the number of products in the target city.

        :return: Number of products.
        """
        url = 'https://www.bethowen.ru/api/local/v1/catalog/list?limit=1&offset=0&sort_type=popular&id[]'
        response = self.get_url(url)
        number_products = response['metadata']['count']
        logger.debug(f'{number_products} products found')
        return response['metadata']['count']
