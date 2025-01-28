"""
S3-based data providers for Argentina SEPA data.
"""

import re

from pandera.typing import DataFrame
from prefect_aws import S3Bucket

from tsn_adapters.common.interfaces.provider import IProviderGetter
from tsn_adapters.tasks.argentina.provider.base import SepaS3BaseProvider
from tsn_adapters.tasks.argentina.types import AggregatedPricesDF, DateStr, SepaDF, UncategorizedDF


class RawDataProvider(SepaS3BaseProvider[SepaDF]):
    """Handles raw data from source_data/ prefix"""

    @property
    def _date_pattern(self) -> re.Pattern[str]:
        return re.compile(r"sepa_(\d{4}-\d{2}-\d{2})\.zip$")

    def __init__(self, s3_block: S3Bucket):
        super().__init__(prefix="source_data/", s3_block=s3_block)


class ProcessedDataProvider(
    SepaS3BaseProvider[AggregatedPricesDF], IProviderGetter[DateStr, DataFrame[AggregatedPricesDF]]
):
    """Handles processed data from processed/ prefix"""

    @property
    def _date_pattern(self) -> re.Pattern[str]:
        return re.compile(r"(\d{4}-\d{2}-\d{2})/data\.zip$")

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
        self.write_csv(f"{date_str}/data.zip", data)

        # Save uncategorized products
        self.write_csv(f"{date_str}/uncategorized.zip", uncategorized)

        # Save compressed logs
        self.write_bytes(f"{date_str}/logs.zip", logs)
