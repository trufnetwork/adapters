"""
S3 provider for handling Argentina SEPA product average data.
"""

import re

from pandera.typing import DataFrame
from prefect_aws import S3Bucket

from tsn_adapters.tasks.argentina.models.sepa.sepa_models import (
    SepaWeightedAvgPriceProductModel,
)
from tsn_adapters.tasks.argentina.provider.base import SepaS3BaseProvider
from tsn_adapters.tasks.argentina.types import DateStr


class ProductAveragesProvider(SepaS3BaseProvider[DataFrame[SepaWeightedAvgPriceProductModel]]):
    """
    Handles reading and writing of processed product average data with counts
    in the 'processed/' prefix of the S3 bucket.
    """

    # Define the regex pattern to extract YYYY-MM-DD from the path
    # Example path: processed/2024-03-15/product_averages.zip
    DATE_REGEX = re.compile(r"(\d{4}-\d{2}-\d{2})/product_averages\.zip$")

    @property
    def _date_pattern(self) -> re.Pattern[str]:
        """
        Returns the compiled regex pattern to find date strings (YYYY-MM-DD)
        in the S3 keys relative to the provider's prefix.
        """
        # This pattern assumes keys like 'YYYY-MM-DD/product_averages.zip'
        # relative to the 'processed/' prefix.
        return self.DATE_REGEX

    def __init__(self, s3_block: S3Bucket):
        """
        Initializes the provider with the S3 block and sets the prefix.

        Args:
            s3_block: The Prefect S3Bucket block instance configured for access.
        """
        super().__init__(prefix="processed/", s3_block=s3_block)

    @staticmethod
    def to_product_averages_file_key(date: DateStr) -> str:
        """
        Generates the relative S3 object key for the product averages file for a given date.

        Args:
            date: The date string in 'YYYY-MM-DD' format.

        Returns:
            The relative S3 key string (e.g., 'YYYY-MM-DD/product_averages.zip').
            The base class handles prepending the 'processed/' prefix.
        """
        return f"{date}/product_averages.zip"

    def save_product_averages(self, date_str: DateStr, data: DataFrame[SepaWeightedAvgPriceProductModel]) -> None:
        """
        Saves the weighted product average DataFrame to the designated S3 location as a compressed CSV.

        Args:
            date_str: The date string ('YYYY-MM-DD') for which the data is being saved.
            data: The DataFrame containing weighted product average data conforming to SepaWeightedAvgPriceProductModel.
        """
        file_key = self.to_product_averages_file_key(date_str)
        self.write_csv(file_key, data)

    def get_product_averages_for(self, key: DateStr) -> DataFrame[SepaWeightedAvgPriceProductModel]:
        """
        Retrieves the weighted product average DataFrame from S3 for a specific date.

        Args:
            key: The date string ('YYYY-MM-DD') to retrieve data for.

        Returns:
            A DataFrame containing the weighted product average data.
        """
        file_key = self.to_product_averages_file_key(key)
        # Cast the result of read_csv to the specific Pandera DataFrame type
        return DataFrame[SepaWeightedAvgPriceProductModel](self.read_csv(file_key))

    def exists(self, key: DateStr) -> bool:
        """
        Checks if the product average data file exists in S3 for the specified date.

        Args:
            key: The date string ('YYYY-MM-DD') to check for.

        Returns:
            True if the file exists, False otherwise.
        """
        file_key = self.to_product_averages_file_key(key)
        return self.path_exists(file_key) 