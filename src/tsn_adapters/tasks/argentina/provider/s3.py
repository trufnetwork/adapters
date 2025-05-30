"""
S3-based data providers for Argentina SEPA data.
"""

import re
from typing import Iterator

from prefect_aws import S3Bucket

from tsn_adapters.common.interfaces.provider import IProviderGetter
from tsn_adapters.tasks.argentina.provider.base import SepaS3BaseProvider
from tsn_adapters.tasks.argentina.provider.data_processor import process_sepa_zip, process_sepa_zip_streaming
from tsn_adapters.tasks.argentina.types import AggregatedPricesDF, DateStr, SepaDF, UncategorizedDF


class RawDataProvider(SepaS3BaseProvider[SepaDF]):
    """Handles raw data from source_data/ prefix"""

    @property
    def _date_pattern(self) -> re.Pattern[str]:
        return re.compile(r"sepa_(\d{4}-\d{2}-\d{2})\.zip$")

    @staticmethod
    def to_file_key(date: DateStr) -> str:
        return f"sepa_{date}.zip"

    def __init__(self, s3_block: S3Bucket):
        super().__init__(prefix="source_data/", s3_block=s3_block)

    def get_raw_data_for(self, date: DateStr) -> SepaDF:
        """Get raw data for specific date - DEPRECATED: Use stream_raw_data_for() for memory efficiency"""
        file_key = self.to_file_key(date)
        return process_sepa_zip(self.create_reader(file_key), date, "sepa")

    def stream_raw_data_for(self, date: DateStr, chunk_size: int = 100000) -> Iterator[SepaDF]:
        """
        Stream raw data for specific date in chunks for memory-efficient processing.
        
        Args:
            date: Date to process (YYYY-MM-DD)
            chunk_size: Number of rows per chunk
            
        Yields:
            SepaDF chunks of the specified size
        """
        file_key = self.to_file_key(date)
        
        # Use the new streaming processor that handles chunking at the source
        yield from process_sepa_zip_streaming(
            self.create_reader(file_key), 
            date, 
            "sepa", 
            chunk_size=chunk_size
        )

    def has_data_for(self, date: DateStr) -> bool:
        """Check if data exists for the given date without loading it."""
        file_key = self.to_file_key(date)
        return self.path_exists(file_key)


class ProcessedDataProvider(
    SepaS3BaseProvider[AggregatedPricesDF], IProviderGetter[DateStr, AggregatedPricesDF]
):
    """Handles processed data from processed/ prefix"""

    @property
    def _date_pattern(self) -> re.Pattern[str]:
        return re.compile(r"(\d{4}-\d{2}-\d{2})/data\.zip$")

    @staticmethod
    def to_data_file_key(date: DateStr) -> str:
        return f"{date}/data.zip"

    def __init__(self, s3_block: S3Bucket):
        super().__init__(prefix="processed/", s3_block=s3_block)

    def save_processed_data(
        self,
        date_str: DateStr,
        data: AggregatedPricesDF,
        uncategorized: UncategorizedDF,
        logs: bytes,
    ) -> None:
        """Save all processed outputs for a date"""
        # Save main data
        self.write_csv(self.to_data_file_key(date_str), data)

        # Save uncategorized products
        self.write_csv(f"{date_str}/uncategorized.zip", uncategorized)

        # Save compressed logs
        self.write_bytes(f"{date_str}/logs.zip", logs)

    def get_data_for(self, key: DateStr) -> AggregatedPricesDF:
        """Get processed data for specific date"""
        file_key = self.to_data_file_key(key)
        return AggregatedPricesDF(self.read_csv(file_key))

    def exists(self, key: DateStr) -> bool:
        """Check if processed data exists for specific date"""
        file_key = self.to_data_file_key(key)
        return self.path_exists(file_key)
