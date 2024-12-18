import logging
import re
from datetime import datetime
from typing import List

import dateparser
import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, field_validator

from .utils.dates import date_to_weekday

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SepaHistoricalDataItem(BaseModel):
    """
    Represents a historical data item extracted from the SEPA website.
    """
    date: str
    resource_id: str
    dataset_id: str

    _base_url = "https://datos.produccion.gob.ar/dataset"

    @field_validator('date')
    @classmethod
    def validate_date(cls, v: str) -> str:
        try:
            datetime.strptime(v.strip(), '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError('Invalid date format. Expected YYYY-MM-DD.')

    def get_resource_link(self) -> str:
        """Returns the link to the resource page."""
        return f"{self._base_url}/{self.dataset_id}/archivo/{self.resource_id}"

    def get_download_link(self) -> str:
        """Returns the direct download link for the ZIP file."""
        lowercase_weekday = date_to_weekday(self.date).lower()
        # note that links different. we use ``resource`` instead of ``archivo``
        return f"{self._base_url}/{self.dataset_id}/resource/{self.resource_id}/download/sepa_{lowercase_weekday}.zip"

    def fetch_into_memory(self) -> bytes:
        """Fetches the ZIP file content into memory."""
        response = requests.get(self.get_download_link())
        response.raise_for_status()
        return response.content

    def fetch_into_file(self, file_path: str) -> None:
        """Fetches the ZIP file and saves it to the specified path."""
        with open(file_path, 'wb') as file:
            file.write(self.fetch_into_memory())


class SepaPreciosScraper:
    """
    A scraper for extracting historical "SEPA Precios" data from the specified page.
    """
    main_url = "https://datos.produccion.gob.ar/dataset/sepa-precios"
    historical_data_selector = '.activity'

    def __init__(self, delay_seconds: float = 0.1):
        self.session = requests.Session()
        self.delay_seconds = delay_seconds
        self.session.headers.update({
            "User-Agent": "SEPA Precios Scraper (https://github.com/truflation)"
        })

    def __del__(self):
        self.session.close()

    def _get_soup(self) -> BeautifulSoup:
        """Fetches and parses the main page."""
        try:
            response = self.session.get(self.main_url)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            logger.error(f"Failed to fetch data: {e}")
            raise ConnectionError(f"Failed to fetch data: {e}")

    def scrape_historical_items(self) -> List[SepaHistoricalDataItem]:
        """
        Scrapes historical data items, handling potential errors gracefully.
        """
        soup = self._get_soup()
        items: List[SepaHistoricalDataItem] = []

        main_element = soup.select_one(self.historical_data_selector)
        if main_element is None:
            logger.error("Main historical data element not found.")
            raise ValueError("Main historical data element not found.")

        inner_item_selector = 'li.item.changed-resource'
        for item in main_element.select(inner_item_selector):
            try:
                data_item = self._extract_data_item(item)
                items.append(data_item)
            except Exception as e:
                logger.error(f"Error processing item: {e}, Item HTML: {item}")

        if not items:
            logger.error("No historical data items found.")
            raise ValueError("No historical data items found.")

        return items

    def _extract_data_item(self, item) -> SepaHistoricalDataItem:
        """Extracts a SepaHistoricalDataItem from a BeautifulSoup item element."""
        date_element = item.select_one('.date')
        if not date_element:
            raise ValueError("Date element not found in item")

        date_element_title = date_element.get('title')
        if not isinstance(date_element_title, str):
            raise ValueError("Date element title not found in item")

        # Parse the date
        # example: '16 Diciembre, 2024, 14:06 (-03)'
        # fixed: '16 Diciembre, 2024, 14:06 (-0300)'
        fixed_date_element_title = date_element_title.replace('(-03)', '(-0300)')
        parsed_date = dateparser.parse(
            fixed_date_element_title,
            languages=['es'],
            date_formats=['%d %B, %Y, %H:%M (%z)']
        )
        if not parsed_date:
            raise ValueError(f"Could not parse date: {date_element_title}")
        date_string = parsed_date.strftime('%Y-%m-%d')

        resource_anchor = item.select_one('a[href*="archivo"]')
        if not resource_anchor:
            raise ValueError("Resource anchor not found in item")

        # expected: /dataset/<dataset_id>/archivo/<resource_id>
        resource_href = resource_anchor.get('href')
        if not isinstance(resource_href, str):
            raise ValueError("Resource href is not a string")

        # validate the href
        regex = r'/dataset/(.+?)/archivo/(.+?)$'
        match = re.match(regex, resource_href)
        if not match:
            raise ValueError("Resource href does not match expected pattern")

        dataset_id, resource_id = match.groups()

        return SepaHistoricalDataItem(
            date=date_string,
            resource_id=resource_id,
            dataset_id=dataset_id
        ) 