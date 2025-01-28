"""
Base provider classes for Argentina SEPA data.
"""

from abc import ABC, abstractmethod
import io
import re
from typing import Generic, Optional, TypeVar

import pandas as pd
from pandas import DataFrame as PandasDataFrame
from pandera.typing import DataFrame
from prefect_aws import S3Bucket

from tsn_adapters.tasks.argentina.base_types import DateStr
from tsn_adapters.utils import deroutine

T = TypeVar("T")  # For return type of get_data_for


class SepaS3BaseProvider(ABC, Generic[T]):
    """Base class for S3-based SEPA data providers.

    This class provides common S3 functionality and path validation
    for SEPA data providers.
    """

    def __init__(self, prefix: str, s3_block: S3Bucket):
        """Initialize the S3 provider with a prefix and optional S3 block.

        Args:
            prefix: S3 prefix for this provider (e.g. 'source_data/')
            s3_block: Optional preconfigured S3 block. If not provided,
                     will attempt to load from Prefect.
        """
        self.prefix = self._validate_prefix(prefix)
        self.s3_block = s3_block

    @property
    @abstractmethod
    def _date_pattern(self) -> re.Pattern[str]:
        """Get the date pattern for the S3 prefix."""
        pass

    @staticmethod
    def _validate_prefix(prefix: str) -> str:
        """Validate and normalize the S3 prefix.

        Args:
            prefix: The prefix to validate

        Returns:
            Normalized prefix ending with '/'

        Raises:
            ValueError: If prefix is invalid
        """
        if not prefix:
            raise ValueError("Prefix cannot be empty")

        # Normalize the prefix to ensure it ends with /
        prefix = prefix.strip("/") + "/"

        # Basic validation
        if ".." in prefix or "~" in prefix:
            raise ValueError(f"Invalid prefix: {prefix}")

        return prefix

    def get_full_path(self, key: str) -> str:
        """Get the full S3 path for a key.

        Args:
            key: The key to get the path for

        Returns:
            Full S3 path including prefix
        """
        # Remove leading/trailing slashes from key
        key = key.strip("/")
        return f"{self.prefix}{key}"

    def read_csv(self, key: str) -> PandasDataFrame:
        """Read a CSV file from S3."""
        content_in_bytes = deroutine(self.s3_block.read_path(self.get_full_path(key)))
        buffer = io.BytesIO(content_in_bytes)
        return pd.read_csv(buffer, compression="zip")

    def write_csv(self, key: str, data: PandasDataFrame) -> None:
        """Write a CSV file to S3."""
        buffer = io.BytesIO()
        data.to_csv(buffer, compression="zip")
        buffer.seek(0)
        deroutine(self.s3_block.write_path(self.get_full_path(key), buffer.getvalue()))

    def write_bytes(self, key: str, data: bytes) -> None:
        """Write bytes to S3."""
        deroutine(self.s3_block.write_path(self.get_full_path(key), data))

    def list_available_keys(self) -> list[DateStr]:
        """List available keys in the S3 prefix.

        Returns:
            List of available keys
        """
        keys = deroutine(self.s3_block.list_objects(folder=self.prefix))
        dates = []

        for key in keys:
            match = self._date_pattern.search(str(key))
            if match:
                date = DateStr(match.group(1))
                dates.append(date)

        return sorted(dates)

    def get_data_for(self, key: DateStr) -> DataFrame[T]:
        """Get processed data for specific date"""
        return DataFrame[T](self.read_csv(key))
