"""
Website-based SEPA data provider implementation.
"""

from typing import cast

from prefect import task

from tsn_adapters.common.interfaces.provider import IProviderGetter
from tsn_adapters.tasks.argentina.models.sepa import SepaWebsiteDataItem
from tsn_adapters.tasks.argentina.models.sepa.website_item import SepaWebsiteScraper
from tsn_adapters.tasks.argentina.provider.data_processor import process_sepa_data
from tsn_adapters.tasks.argentina.types import DateStr, SepaDF


class SepaWebsiteProvider(IProviderGetter[DateStr, SepaDF]):
    """Website-based provider for SEPA data."""

    def __init__(self, scraper: SepaWebsiteScraper):
        """
        Initialize with a SEPA scraper.

        Args:
            scraper: The SEPA scraper instance
        """
        self.scraper = scraper
        self._historical_items: dict[DateStr, SepaWebsiteDataItem] = {}

    def list_available_keys(self) -> list[DateStr]:
        """
        Return a list of available dates from the website.

        Returns:
            list[DateStr]: List of available dates in YYYY-MM-DD format
        """
        items = self.scraper.scrape_items()
        self._historical_items = {cast(DateStr, item.item_reported_date): item for item in items}
        sorted_keys = sorted(self._historical_items.keys())
        return sorted_keys

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
        return process_sepa_data(data_item=data_item, source_name="website")


@task(name="Create SEPA Website Provider")
def create_sepa_website_provider(delay_seconds: float = 0.1, show_progress_bar: bool = False) -> SepaWebsiteProvider:
    """
    Create a SEPA website provider instance.

    Args:
        delay_seconds: Delay between requests to avoid rate limiting
        show_progress_bar: Whether to show a progress bar during downloads

    Returns:
        SepaWebsiteProvider: The provider instance
    """
    scraper = SepaWebsiteScraper(delay_seconds=delay_seconds, show_progress_bar=show_progress_bar)
    return SepaWebsiteProvider(scraper=scraper)
