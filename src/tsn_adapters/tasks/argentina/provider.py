"""
SEPA data provider implementation.
"""

from datetime import timedelta
import tempfile
from typing import cast
import os

import pandas as pd
from prefect import get_run_logger, task

from tsn_adapters.tasks.argentina.interfaces.base import IProviderGetter
from tsn_adapters.tasks.argentina.scrapers.resource_processor import SepaDirectoryProcessor
from tsn_adapters.tasks.argentina.scrapers.sepa_scraper import (
    SepaHistoricalDataItem,
    SepaPreciosScraper,
    task_scrape_historical_items,
)
from tsn_adapters.tasks.argentina.types import DateStr, SepaDF


class SepaByDateGetter(IProviderGetter[DateStr, SepaDF]):
    """Fetches SEPA data by date."""

    def __init__(self, sepa_scraper: SepaPreciosScraper):
        """
        Initialize with a SEPA scraper.

        Args:
            sepa_scraper: The SEPA scraper instance
        """
        self.scraper = sepa_scraper
        self._historical_items: dict[DateStr, SepaHistoricalDataItem] = {}

    def load_items(self) -> None:
        """Initialize by fetching available historical items."""
        items = task_scrape_historical_items(scraper=self.scraper)
        self._historical_items = {cast(DateStr, item.website_date): item for item in items}

    def get_data_for(self, key: DateStr) -> SepaDF:
        """
        Get SEPA data for a specific date.

        Args:
            key: The date to fetch data for (YYYY-MM-DD)

        Returns:
            DataFrame: The SEPA data for the given date

        Raises:
            KeyError: If the date is not available
            ValueError: If the data is invalid
        """
        if key not in self._historical_items:
            raise KeyError(f"No data available for date: {key}")

        data_item = self._historical_items[key]
        try:
            return task_get_sepa_data(data_item=data_item)
        except DatesNotMatchError as e:
            logger = get_run_logger()
            logger.warning(f"Dates not match: {e}")
            return cast(SepaDF, pd.DataFrame())


class DatesNotMatchError(Exception):
    """Exception raised when the date does not match."""

    def __init__(self, real_date: DateStr, website_date: DateStr):
        """Initialize the exception."""
        self.real_date = real_date
        self.website_date = website_date
        super().__init__(f"Real date {real_date} does not match website date {website_date}")


@task(
    cache_key_fn=lambda _, args: f"sepa-data-{args['data_item'].website_date}",
    cache_expiration=timedelta(days=7),
    retries=3,
)
def task_get_sepa_data(
    data_item: SepaHistoricalDataItem,
) -> SepaDF:
    """
    Task to get SEPA data for a specific historical data item.

    Args:
        data_item: The historical data item to fetch

    Returns:
        DataFrame: The SEPA data
    """
    # Create a temporary directory for extraction
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download the zip file
        zip_content = data_item.fetch_into_memory()

        # Create a temporary file for the zip
        with tempfile.NamedTemporaryFile(suffix=".zip", dir=temp_dir, delete=False) as temp_zip:
            temp_zip.write(zip_content)
            temp_zip_path = temp_zip.name

        # Process the data
        extract_dir = os.path.join(temp_dir, "data")
        os.makedirs(extract_dir, exist_ok=True)
        processor = SepaDirectoryProcessor.from_zip_path(temp_zip_path, extract_dir)
        df = processor.get_all_products_data_merged()

        # skip empty dataframes
        if df.empty:
            return cast(SepaDF, pd.DataFrame())

        # Validate the date matches
        real_date = df["date"].iloc[0]
        if data_item.website_date != real_date:
            # we need to raise an error so cache is invalidated
            raise DatesNotMatchError(real_date, DateStr(data_item.website_date))

        return cast(SepaDF, df)


@task(name="Create SEPA Provider")
def create_sepa_provider(scraper: SepaPreciosScraper) -> SepaByDateGetter:
    """
    Create a SEPA provider instance.

    Args:
        scraper: The SEPA scraper to use

    Returns:
        SepaByDateGetter: The provider instance
    """
    return SepaByDateGetter(scraper)
