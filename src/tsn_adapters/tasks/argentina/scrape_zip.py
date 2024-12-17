import re
from pydantic import BaseModel, field_validator
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from typing import List
import dateparser 

es_weekday_map = {
    'Monday': 'Lunes',
    'Tuesday': 'Martes',
    'Wednesday': 'Miercoles',
    'Thursday': 'Jueves',
    'Friday': 'Viernes',
    'Saturday': 'Sabado',
    'Sunday': 'Domingo'
}

class DataItem(BaseModel):
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
            raise ValueError('Invalid date format')

    def get_resource_link(self) -> str:
        return f"{self._base_url}/{self.dataset_id}/archivo/{self.resource_id}"

    def get_download_link(self) -> str:
        lowercase_weekday = self._get_weekday_name().lower()
        return f"{self._base_url}/{self.dataset_id}/resource/{self.resource_id}/download/sepa_{lowercase_weekday}.zip"


    def fetch_into_memory(self) -> bytes:
        response = requests.get(self.get_download_link())
        return response.content
    
    def fetch_into_file(self, file_path: str) -> None:
        with open(file_path, 'wb') as file:
            file.write(self.fetch_into_memory())
    

    def _get_weekday_name(self) -> str:
        en_value: str = datetime.strptime(self.date, '%Y-%m-%d').strftime('%A')
        if en_value not in es_weekday_map:
            raise ValueError(f"Invalid weekday: {en_value}")
        es_value: str = es_weekday_map[en_value]
        return es_value

class SepaPreciosWebScraper:
    """
    A scraper for extracting historical "SEPA Precios" data from the specified page.

    This class retrieves the main page, parses the historical activities section,
    and extracts data items consisting of a date and a resource ID.
    If the page structure differs from expectations, it raises ValueError.
    """


    main_url = "https://datos.produccion.gob.ar/dataset/sepa-precios"
    historical_data_selector = '.activity'

    def __init__(self):
        self.session = requests.Session()
        self.soup = self._get_soup()

    def __del__(self):
        self.session.close()

    def _get_soup(self) -> BeautifulSoup:
        try:
            response = self.session.get(self.main_url)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            return BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            raise ConnectionError(f"Failed to fetch data: {str(e)}")

    def _get_historical_items(self) -> List[DataItem]:
        inner_item_selector = 'li.item.changed-resource'
        date_element_selector = '.date'
        resource_anchor_selector = 'a[href*="archivo"]'
        items = []

        main_element = self.soup.select_one(self.historical_data_selector)
        if main_element is None:
            raise ValueError("Main element not found in page")

        for item in main_element.select(inner_item_selector):
            try:
                date_element = item.select_one(date_element_selector)
                if not date_element:
                    raise ValueError("Date element not found in item")

                date_element_title = date_element.get('title')
                if not isinstance(date_element_title, str):
                    raise ValueError("Date element title not found in item")

                # Parse the date 
                # example: '16 Diciembre, 2024, 14:06 (-03)'
                # however %z expects full offset (e.g. -0300)
                # so we need to replace correctly
                date_element_title = date_element_title.replace('(-03)', '(-0300)')
                parsed_date = dateparser.parse(date_element_title, languages=['es'], date_formats=['%d %B, %Y, %H:%M (%z)'])
                if not parsed_date:
                    raise ValueError(f"Could not parse date: {date_element_title}")
                date_string = parsed_date.strftime('%Y-%m-%d')

                resource_anchor = item.select_one(resource_anchor_selector)
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
                
                items.append(DataItem(date=date_string, 
                                    resource_id=resource_id,
                                    dataset_id=dataset_id))
            except Exception as e:
                raise ValueError(f"Error processing item: {str(e)}")

        return items

if __name__ == "__main__":
    try:
        scraper = SepaPreciosWebScraper()
        items = scraper._get_historical_items()
        print(items)
    except Exception as e:
        print(f"Error: {str(e)}")
