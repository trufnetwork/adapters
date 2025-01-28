"""
S3-based data providers for Argentina SEPA data.
"""

import re
from typing import Optional

from prefect_aws import S3Bucket
import pandas as pd

from tsn_adapters.tasks.argentina.models.sepa import SepaS3RawDataItem
from tsn_adapters.tasks.argentina.models.sepa.s3_item import SepaS3BaseProvider
from tsn_adapters.tasks.argentina.provider.data_processor import process_sepa_data
from tsn_adapters.tasks.argentina.types import DateStr, SepaDF
from tsn_adapters.utils import deroutine


class RawDataProvider(SepaS3BaseProvider):
    """Handles raw data from source_data/ prefix"""
    def __init__(self):
        super().__init__(prefix="source_data/")


class ProcessedDataProvider(SepaS3BaseProvider):
    """Handles processed data from processed/ prefix"""
    def __init__(self):
        super().__init__(prefix="processed/")
    
    def get_processed_data(self, date_str: str) -> pd.DataFrame:
        """Get processed data for specific date"""
        key = f"{date_str}/data.csv"
        return self.read_csv(key)
    
    def save_processed_data(
        self,
        date_str: str,
        data: pd.DataFrame,
        uncategorized: pd.DataFrame,
        logs: bytes
    ) -> None:
        """Save all processed outputs for a date"""
        # Save main data
        self.write_csv(f"{date_str}/data.csv", data)
        
        # Save uncategorized products
        self.write_csv(f"{date_str}/uncategorized.csv", uncategorized)
        
        # Save compressed logs
        self.write_bytes(f"{date_str}/logs.zip", logs)


class SepaS3Provider(SepaS3BaseProvider[SepaDF]):
    """Provider for accessing SEPA data from S3."""

    def __init__(self, prefix: str = "source_data/", s3_block: Optional[S3Bucket] = None):
        """Initialize the S3 provider.
        
        Args:
            prefix: S3 prefix to use (default: 'source_data/')
            s3_block: Optional preconfigured S3 block
        """
        super().__init__(prefix=prefix, s3_block=s3_block)
        self._date_pattern = re.compile(r"sepa_(\d{4}-\d{2}-\d{2})\.zip$")
        self._historical_items: dict[DateStr, SepaS3RawDataItem] = {}

    def list_available_keys(self) -> list[DateStr]:
        """List available dates in the S3 prefix.
        
        Returns:
            List of available dates in YYYY-MM-DD format
        """
        keys = deroutine(self.s3_block.list_objects(folder=self.prefix))
        dates = []
        
        for key in keys:
            match = self._date_pattern.search(str(key))
            if match:
                date = DateStr(match.group(1))
                # Create and store the data item for later use
                self._historical_items[date] = SepaS3RawDataItem.create(
                    block=self.s3_block,
                    key=str(key),
                    item_reported_date=date
                )
                dates.append(date)
                
        return sorted(dates)

    def get_data_for(self, key: DateStr) -> SepaDF:
        """Get SEPA data for a specific date.
        
        Args:
            key: Date in YYYY-MM-DD format
            
        Returns:
            DataFrame containing the SEPA data
            
        Raises:
            ValueError: If key format is invalid
            FileNotFoundError: If ZIP file doesn't exist
            KeyError: If data for date hasn't been listed yet
        """
        if not re.match(r"\d{4}-\d{2}-\d{2}", key):
            raise ValueError(f"Invalid date format: {key}")

        if key not in self._historical_items:
            raise KeyError(f"No data available for date: {key}")
            
        data_item = self._historical_items[key]
        return process_sepa_data(data_item=data_item, source_name="s3")
