import csv
import logging
from typing import Any, Never
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from misc import Crawler, NothingToSaveError
import configs


logger = logging.getLogger('Parser')


class ParserMixin:
    """
    Additional functions for the Parser class.
    """
    result = []
    products = []
    product_list_limit = 100

    def __init__(self, city_name: str) -> None:
        """
        Initializing the class and collecting primary data.

        :param city_name: Target city name.
        """
        self._start_message()

        self.crawler = Crawler()
        self.city_name = city_name
        self.city_id = self._get_city_id(city_name)
        self.crawler.set_city_id(self.city_id)
        self.store_ids = self._get_store_ids()
        self.number_products = self.crawler.get_number_products()
        self.target_categories = self._prepare_categories(configs.CATEGORY_NAMES)

        self._init_message()

    def _get_city_id(self, city_name: str) -> str:
        """
        Returns city_id from the data received from the server.

        :param city_name: Target city name.
        :return: City ID.
        """
        cities: list[dict] = self.crawler.search_city(city_name)

        for city_data in cities:
            if city_data['name'] == city_name:
                return city_data['id']
        raise AttributeError(f'{city_name!r} not found in cities.')

    def _get_store_ids(self) ->list[int]:
        """
        Checks the availability of stores in the target city.

        :return: Store ID list.
        """
        store_ids = self.crawler.search_store_ids(self.city_id)
        if not store_ids or (len(store_ids) == 1 and store_ids[0] == 0):
            raise AttributeError(f'Stores not found in {self.city_name!r} city.')
        return store_ids

    @staticmethod
    def _start_message() -> None:
        """
        Displays a message about the initial configuration of the parser.
        """
        logger.debug(
            '\nBethowen parser starts working with the configuration:\n'
            f'\t\tTHREADS: {configs.THREADS}\n'
            f'\t\tCITY_NAME: {configs.CITY_NAME}\n'
            f'\t\tSTORE_ADDRESS: {configs.STORE_ADDRESS}\n'
            f'\t\tCATEGORY_NAMES: {configs.CATEGORY_NAMES}\n'
        )

    def _init_message(self) -> None:
        """
        Displays a message about the primary data.
        """
        logger.debug(
            '\nAfter initialization, the following values were found:\n'
            f'\t\tcity_id: {self.city_id}\n'
            f'\t\tNumber of stores in the city: {len(self.store_ids)}\n'
            f'\t\tThe number of products in all stores: {self.number_products}\n'
            '\t\tData collection begins ...\n'
        )

    def _prepare_product_list_urls(self) -> list[str]:
        """
        Generates a list of links to collect information about products.

        :return: List of product catalog links.
        """
        url_part = f'https://www.bethowen.ru/api/local/v1/catalog/list?limit={self.product_list_limit}&'
        offset = 0
        urls = []
        while self.number_products > 0:
            urls.append(f'{url_part}offset={offset}&sort_type=popular&id[]')
            self.number_products -= self.product_list_limit
            offset += 100

        return urls

    def _get_product_list(self, url) -> list[dict] | None:
        """
        Collects information about products from the catalog page.

        :param url: Link to catalog page.
        :return: Product list data.
        """
        if response := self.crawler.get_url(url):
            self.products.extend(response['products'])

    def _get_offer_details(self, product_data: dict[str, Any]) -> None:
        """
        Collects information from the offer if it matches the given city and category.

        :param product_data: Product information.
        """
        offer_urls = self._prepare_offer_list_urls(product_data.get('offers', []))
        offer_responses = list(map(self.crawler.get_url, offer_urls))
        for offer_data in offer_responses:
            if self._is_target_store(offer_data) and self._is_target_category(offer_data['id'], product_data):
                self.result.append(
                    {
                        'city': configs.CITY_NAME,
                        'city_id': self.city_id,
                        'store_address': configs.STORE_ADDRESS,
                        'product_id': product_data['id'],
                        'product_name': product_data['name'],
                        'product_size': offer_data['size'],
                        'retail_price': offer_data['retail_price'],
                        'discount_price': offer_data['discount_price'],
                    }
                )

    @staticmethod
    def _prepare_offer_list_urls(offers_data: list[dict]) -> set[str]:
        """
        Generates a list of links to collect information about offers.

        :param offers_data: Offer information from in the product information.
        :return: Set of offer links.
        """
        offer_urls = set()
        for offer in offers_data:
            offer_urls.add(f'https://www.bethowen.ru/api/local/v1/catalog/offers/{offer['id']}/details')

        return offer_urls

    @staticmethod
    def _is_target_store(offer_data: dict[str, Any]) -> bool:
        """
        Checks if the offer is available in the target store address.

        :param offer_data: Offer information.
        :return: True, if offer is available in target address, else False.
        """
        offer_stores = offer_data['availability_info']['offer_store_amount']

        for store_data in offer_stores:
            if configs.STORE_ADDRESS.lower() in store_data['address'].lower():
                return True

        return False

    @staticmethod
    def _prepare_categories(config_categories: list[tuple] | list[str] | str | None) -> list[tuple] | list[Never]:
        """
        Checks the config categories and prepares them for further processing.

        :param config_categories: Categories from the configuration file.
        :return: Verified category list.
        """
        if isinstance(config_categories, list):
            if len(config_categories) == 1 and isinstance(config_categories[0], str):
                return [config_categories]

            for category in config_categories:
                if not isinstance(category, str):
                    raise AttributeError(f'Not correct CATEGORY_NAMES format. Expected list of string(s)')

            return config_categories

        if isinstance(config_categories, str):
            return [config_categories]

        return []

    @staticmethod
    def _get_offer_categories(offer_id: str, product_data: dict[str, Any]) -> dict[str, str] | dict[Never]:
        """
        Collects categories from the offer information in the product data.

        :param offer_id: Offer ID.
        :param product_data: Product information.
        :return: Dict with offer categories.
            e.g.: {
                  "1": "Для собак",
                  "2": "Миски",
                  "3": "Миски на подставке"
                }
        """
        offer_categories = {}

        product_offers = product_data.get('offers', [])
        for offer_data in product_offers:
            if offer_data['id'] == offer_id:
                offer_categories = offer_data['categories_chain']
                break

        return offer_categories

    def _is_target_category(self, offer_id: str, product_data: dict[str, Any]) -> bool:
        """
        Checks the offer categories to match the target categories.

        :param offer_id: Offer ID.
        :param product_data: Product information.
        :return: True, if offer has all target categories, else False.
        """
        if self.target_categories:
            offer_categories = self._get_offer_categories(offer_id, product_data)
            for category in self.target_categories:
                if category not in offer_categories.values():
                    return False

        return True



class Parser(ParserMixin):
    """
    Data parsing class.
    """

    def __init__(self, city_name: str):
        super().__init__(city_name)

    def _parse_products(self):
        """
        Collects data on all products available in a target city.
        """
        urls = self._prepare_product_list_urls()
        with ThreadPoolExecutor(max_workers=configs.THREADS) as pool:
            pool.map(self._get_product_list, urls)
        logger.debug(f'{len(self.products)} products parsed')

    def _parse_offers(self):
        """
        Collects data on all offers that match with config options.
        """
        with ThreadPoolExecutor(max_workers=configs.THREADS) as pool:
            pool.map(self._get_offer_details, self.products)

    def _save_result(self) -> None:
        """
        Saves the result to a csv file.
        """
        if not self.result:
            raise NothingToSaveError('Result is empty. Nothing to save.')

        date_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        file_name = f'{date_str}.csv'
        columns = ['city', 'city_id', 'store_address', 'product_id', 'product_name', 'product_size', 'retail_price',
                   'discount_price']

        with open(file_name, "w", newline="") as _file:
            writer = csv.DictWriter(_file, fieldnames=columns)
            writer.writeheader()
            writer.writerows(self.result)

        logger.debug(f'{len(self.result)} items were saved to {file_name!r}')

    def run(self):
        """
        Starts the main data collection process.
        """
        if not self.number_products:
            logger.error(f'Products not found in {self.city_name!r}')
            return

        self._parse_products()
        self._parse_offers()
        self._save_result()


if __name__ == '__main__':
    logging.basicConfig(format='[%(asctime)s] [%(name)s] [%(levelname)s] > %(message)s', level=logging.DEBUG)
    p = Parser(configs.CITY_NAME)
    p.run()