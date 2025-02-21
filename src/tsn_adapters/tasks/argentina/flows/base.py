"""
Base flow controller for Argentina SEPA data processing.
"""

from datetime import datetime
import re

from prefect import get_run_logger
from prefect_aws import S3Bucket

from tsn_adapters.tasks.argentina.errors.errors import InvalidDateFormatError
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
            InvalidDateFormatError: If date format is invalid
        """
        # First check for separators
        if "/" in date:
            raise InvalidDateFormatError(date, "wrong_separator")
        if "-" not in date:
            raise InvalidDateFormatError(date, "no_separator")

        # Check basic format
        if not re.match(r"\d{4}-\d{2}-\d{2}", date):
            raise InvalidDateFormatError(date, "wrong_format")

        # Check date validity
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError as e:
            error_msg = str(e)
            if "month must be in 1..12" in error_msg:
                raise InvalidDateFormatError(date, "invalid_month")
            if "day is out of range for month" in error_msg or "day must be in" in error_msg:
                raise InvalidDateFormatError(date, "invalid_day")
            # Only raise wrong_format if it's not a specific month/day error
            raise InvalidDateFormatError(date, "wrong_format")
