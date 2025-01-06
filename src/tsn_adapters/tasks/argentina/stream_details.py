"""
Concrete implementations of stream details fetchers.
"""

from typing import cast

from prefect import task

from tsn_adapters.blocks.primitive_source_descriptor import (
    GithubPrimitiveSourcesDescriptor,
    UrlPrimitiveSourcesDescriptor,
    get_descriptor_from_github,
    get_descriptor_from_url,
)
from tsn_adapters.tasks.argentina.base_types import DateStr, PrimitiveSourcesTypeStr
from tsn_adapters.tasks.argentina.interfaces.base import IStreamDetailsFetcher
from tsn_adapters.tasks.argentina.models.stream_metadata import StreamMetadataModel
from tsn_adapters.tasks.argentina.scrapers.sepa_scraper import SepaPreciosScraper, task_scrape_historical_items
from tsn_adapters.tasks.argentina.types import StreamMetadataDF


class GitHubStreamDetailsFetcher(IStreamDetailsFetcher):
    """Fetches stream details from a GitHub source."""

    def __init__(self, block_name: str):
        """
        Initialize with a GitHub primitive sources descriptor block name.

        Args:
            block_name: Name of the GithubPrimitiveSourcesDescriptor block
        """
        self.block_name = block_name
        self.block = GithubPrimitiveSourcesDescriptor.load(block_name)
        self.scraper = SepaPreciosScraper()

    def get_streams(self) -> StreamMetadataDF:
        """
        Fetch stream metadata from GitHub.

        Returns:
            StreamMetadataDF: DataFrame containing stream metadata
        """
        # Get stream metadata from GitHub
        source_metadata_df = get_descriptor_from_github(block=self.block)
        if source_metadata_df is None:
            raise ValueError("Source metadata is None")

        # Get available dates from SEPA
        historical_items = task_scrape_historical_items(scraper=self.scraper)
        available_dates = [cast(DateStr, item.website_date) for item in historical_items]
        available_dates.sort()  # Sort dates chronologically

        # Add available dates to each stream
        source_metadata_df["available_dates"] = [available_dates] * len(source_metadata_df)

        # Validate and coerce using the model
        validated_df = StreamMetadataModel.validate(source_metadata_df)
        return cast(StreamMetadataDF, validated_df)


class URLStreamDetailsFetcher(IStreamDetailsFetcher):
    """Fetches stream details from a URL source."""

    def __init__(self, block_name: str):
        """
        Initialize with a URL primitive sources descriptor block name.

        Args:
            block_name: Name of the UrlPrimitiveSourcesDescriptor block
        """
        self.block_name = block_name
        self.block = UrlPrimitiveSourcesDescriptor.load(block_name)
        self.scraper = SepaPreciosScraper()

    def get_streams(self) -> StreamMetadataDF:
        """
        Fetch stream metadata from URL.

        Returns:
            StreamMetadataDF: DataFrame containing stream metadata
        """
        # Get stream metadata from URL
        source_metadata_df = get_descriptor_from_url(block=self.block)
        if source_metadata_df is None:
            raise ValueError("Source metadata is None")

        # Get available dates from SEPA
        historical_items = task_scrape_historical_items(scraper=self.scraper)
        available_dates = [cast(DateStr, item.website_date) for item in historical_items]
        available_dates.sort()  # Sort dates chronologically

        # Add available dates to each stream
        source_metadata_df["available_dates"] = [available_dates] * len(source_metadata_df)

        # Validate and coerce using the model
        validated_df = StreamMetadataModel.validate(source_metadata_df)
        return cast(StreamMetadataDF, validated_df)


@task(name="Create Stream Details Fetcher")
def create_stream_details_fetcher(source_type: PrimitiveSourcesTypeStr, block_name: str) -> IStreamDetailsFetcher:
    """
    Factory function to create the appropriate stream details fetcher.

    Args:
        source_type: Type of source ("url" or "github")
        block_name: Name of the source descriptor block

    Returns:
        IStreamDetailsFetcher: The appropriate fetcher implementation
    """
    if source_type == "github":
        return GitHubStreamDetailsFetcher(block_name)
    elif source_type == "url":
        return URLStreamDetailsFetcher(block_name)
    else:
        raise ValueError(f"Invalid source type: {source_type}")
