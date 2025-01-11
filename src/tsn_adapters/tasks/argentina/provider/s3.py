"""
S3-based SEPA data provider implementation.
"""

from collections.abc import Coroutine
import re
from typing import cast

from prefect import task
from prefect_aws import S3Bucket

from tsn_adapters.tasks.argentina.models.sepa import SepaS3DataItem
from tsn_adapters.tasks.argentina.provider.data_processor import process_sepa_data
from tsn_adapters.tasks.argentina.provider.interfaces import IProviderGetter
from tsn_adapters.tasks.argentina.types import DateStr, SepaDF
from tsn_adapters.utils.logging import get_logger_safe


class SepaS3Provider(IProviderGetter[DateStr, SepaDF]):
    """S3-based provider for SEPA data."""

    def __init__(self, s3_block: S3Bucket, prefix: str = "source_data/"):
        """
        Initialize with an S3 block.

        Args:
            s3_block: The S3 block to use
            prefix: The prefix for S3 keys
        """
        self.s3_block = s3_block
        self.prefix = prefix
        self._historical_items = {}  # date -> item

    def list_available_keys(self) -> list[DateStr]:
        """
        Return a list of available dates from S3.

        Returns:
            list[DateStr]: List of available dates in YYYY-MM-DD format
        """
        # List objects in the bucket with the given prefix
        objects = self.s3_block.list_objects(folder=self.prefix)
        if isinstance(objects, Coroutine):
            raise ValueError("S3 objects coroutines are not supported")

        file_regex = re.compile(r"sepa_(\d{4}-\d{2}-\d{2})\.zip")

        # Build items dictionary
        items = []
        for obj in objects:
            try:
                # Extract date from key (assuming format like source_data/YYYY-MM-DD/...)
                key = obj["Key"]
                date_match = file_regex.search(key)
                if not date_match:
                    continue

                date = date_match.group(1)
                item = SepaS3DataItem.create(block=self.s3_block, key=key, item_reported_date=date)
                items.append(item)
                self._historical_items[cast(DateStr, date)] = item
            except Exception as e:
                logger = get_logger_safe(__name__)
                logger.warning(f"Failed to process S3 object {obj['Key']}: {e}")
                continue

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
        return process_sepa_data(data_item=data_item, source_name="s3")


@task(name="Create SEPA S3 Provider")
def create_sepa_s3_provider(s3_block: S3Bucket, prefix: str = "source_data/") -> SepaS3Provider:
    """
    Create a SEPA S3 provider instance.

    Args:
        s3_block: The S3 block to use
        prefix: The prefix for S3 keys

    Returns:
        SepaS3Provider: The provider instance
    """
    return SepaS3Provider(s3_block=s3_block, prefix=prefix)
