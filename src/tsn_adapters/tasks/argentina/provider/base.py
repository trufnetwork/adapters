"""
Base provider classes for Argentina SEPA data.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Generic, Optional, TypeVar

from prefect_aws import S3Bucket

from tsn_adapters.common.interfaces.provider import IProviderGetter
from tsn_adapters.tasks.argentina.base_types import DateStr
from tsn_adapters.utils import deroutine

T = TypeVar("T")  # For return type of get_data_for


class SepaS3BaseProvider(IProviderGetter[DateStr, T], ABC, Generic[T]):
    """Base class for S3-based SEPA data providers.
    
    This class provides common S3 functionality and path validation
    for SEPA data providers.
    """

    def __init__(self, prefix: str, s3_block: Optional[S3Bucket] = None):
        """Initialize the S3 provider with a prefix and optional S3 block.
        
        Args:
            prefix: S3 prefix for this provider (e.g. 'source_data/')
            s3_block: Optional preconfigured S3 block. If not provided,
                     will attempt to load from Prefect.
        """
        self.prefix = self._validate_prefix(prefix)
        self._s3_block = s3_block

    @property
    def s3_block(self) -> S3Bucket:
        """Get the S3 block, loading from Prefect if not already loaded."""
        if self._s3_block is None:
            self._s3_block = S3Bucket.load("argentina-sepa")
        return deroutine(self._s3_block)

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

    @abstractmethod
    def list_available_keys(self) -> list[DateStr]:
        """List available keys in the S3 prefix.
        
        Returns:
            List of available keys
        """
        pass

    @abstractmethod
    def get_data_for(self, key: DateStr) -> T:
        """Get data for a specific key.
        
        Args:
            key: The key to get data for
            
        Returns:
            The data for the key
        """
        pass 