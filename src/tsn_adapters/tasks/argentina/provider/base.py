"""
Base provider classes for Argentina SEPA data.
"""

from abc import ABC, abstractmethod
import io
import re
from threading import Thread
from typing import Generator, Generic, Optional, TypeVar

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

    def get_full_path(self, file_key: str) -> str:
        """Get the full S3 path for a key.

        Args:
            file_key: The key to get the path for

        Returns:
            Full S3 path including prefix
        """
        # Remove leading/trailing slashes from key
        file_key = file_key.strip("/")
        return f"{self.prefix}{file_key}"

    def read_csv(self, file_key: str) -> PandasDataFrame:
        """Read a CSV file from S3."""
        content_in_bytes = deroutine(self.s3_block.read_path(self.get_full_path(file_key)))
        buffer = io.BytesIO(content_in_bytes)
        return pd.read_csv(buffer, compression="zip")

    def write_csv(self, file_key: str, data: PandasDataFrame) -> None:
        """Write a CSV file to S3."""
        buffer = io.BytesIO()
        data.to_csv(buffer, index=False, compression="zip")
        buffer.seek(0)
        deroutine(self.s3_block.write_path(self.get_full_path(file_key), buffer.getvalue()))

    def path_exists(self, file_key: str) -> bool:
        """Check if a file exists in S3."""
        dir_objects = deroutine(self.s3_block.list_objects(folder=self.prefix))
        full_path = self.get_full_path(file_key)
        return any(full_path in obj["Key"] for obj in dir_objects)

    def create_reader(self, file_key: str) -> Generator[bytes, None, None]:
        """Create a lazy reader for a file in S3.
        
        This method returns a buffered reader that only downloads the data
        when it's actually read, helping with memory efficiency for large files.
        
        Args:
            file_key: The S3 key of the file to read
            
        Returns:
            A buffered reader object that can be used to read the file contents
        """
        content = deroutine(self.s3_block.read_path(self.get_full_path(file_key)))
        yield content

    def write_bytes(self, file_key: str, data: bytes) -> None:
        """Write bytes to S3."""
        deroutine(self.s3_block.write_path(self.get_full_path(file_key), data))

    def list_available_keys(self) -> list[DateStr]:
        """List available keys in the S3 prefix.

        Returns:
            List of available keys
        """
        items = deroutine(self.s3_block.list_objects(folder=self.prefix))
        dates = []

        for item in items:
            match = self._date_pattern.search(item["Key"])
            if match:
                date = DateStr(match.group(1))
                dates.append(date)

        return sorted(dates)