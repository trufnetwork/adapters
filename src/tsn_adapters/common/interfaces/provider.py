"""
Base interfaces for data providers.
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

import pandas as pd

K = TypeVar("K")  # For key types (e.g. DateStr)
T = TypeVar("T")  # For data types (e.g. a DataFrame)


class IProviderGetter(ABC, Generic[K, T]):
    """Interface for fetching data from a source provider."""

    @abstractmethod
    def list_available_keys(self) -> list[K]:
        """
        Return a list of available keys that can be fetched.

        Returns:
            list[K]: List of available keys (e.g. list of available dates)
        """
        pass

    @abstractmethod
    def get_data_for(self, key: K) -> T:
        """
        Return data for the given key.

        Args:
            key: The key to fetch data for (e.g. date string)

        Returns:
            T: The data from the source
        """
        pass


class IStreamSourceMapFetcher(ABC):
    """Interface for fetching stream to source mapping.

    We have a defined stream for BTC. Our provider is Coinbase.
    We need to know which coinbase ID corresponds to this stream:
    {
        "stream_id": "st123...789",
        "source_id": "BTC"
    }
    """

    @abstractmethod
    def get_streams(self) -> pd.DataFrame:
        """
        Return a dataframe of stream metadata.

        Returns:
            pd.DataFrame: DataFrame containing stream metadata with columns:
                - stream_id: str
                - source_id: str
        """
        pass
