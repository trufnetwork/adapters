"""
Base interfaces for SEPA data providers.
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from tsn_adapters.tasks.argentina.types import StreamSourceMapDF

K = TypeVar("K")  # For key types (e.g. DateStr)
T = TypeVar("T")  # For data types (e.g. SepaDF)


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
    """Interface for fetching stream metadata."""

    @abstractmethod
    def get_streams(self) -> StreamSourceMapDF:
        """
        Return a dataframe of stream metadata.

        Returns:
            StreamMetadataDF: DataFrame containing stream metadata with columns:
                - stream_id: StreamId
                - source_id: str
        """
        pass
