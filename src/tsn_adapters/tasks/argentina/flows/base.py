"""
Base flow controller for Argentina SEPA data processing.
"""

from prefect import get_run_logger
from prefect_aws import S3Bucket

from tsn_adapters.tasks.argentina.provider import ProcessedDataProvider, RawDataProvider
from tsn_adapters.tasks.argentina.types import DateStr


class ArgentinaFlowController:
    """Base class for flow coordination."""

    def __init__(
        self,
        s3_block: S3Bucket,
    ):
        """Initialize flow controller with providers.

        Args:
            s3_block: Optional preconfigured S3 block
        """
        self.logger = get_run_logger()
        self.raw_provider = RawDataProvider(s3_block=s3_block)
        self.processed_provider = ProcessedDataProvider(s3_block=s3_block)

    def validate_date(self, date: DateStr) -> None:
        """Validate a date string.

        Args:
            date: Date string to validate

        Raises:
            ValueError: If date format is invalid
        """
        import re

        if not re.match(r"\d{4}-\d{2}-\d{2}", date):
            raise ValueError(f"Invalid date format: {date}")
