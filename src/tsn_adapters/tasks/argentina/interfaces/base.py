"""
Base interfaces for the Argentina SEPA data ingestion pipeline components.
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

import pandas as pd

from tsn_adapters.tasks.argentina.types import NeededKeysMap, StreamId, StreamMetadataDF

T = TypeVar("T")  # For generic data types
K = TypeVar("K")  # For key types


class IStreamDetailsFetcher(ABC):
    """Interface for fetching stream metadata."""

    @abstractmethod
    def get_streams(self) -> StreamMetadataDF:
        """
        Return a dataframe of stream metadata.

        Returns:
            StreamMetadataDF: DataFrame containing stream metadata with columns:
                - stream_id: StreamId
                - source_id: str
                - available_dates: List[DateStr]
        """
        pass


class IProviderGetter(ABC, Generic[K, T]):
    """Interface for fetching data from a source provider."""

    @abstractmethod
    def get_data_for(self, key: K) -> T:
        """
        Return data for the given key.

        Args:
            key: The key to fetch data for (e.g. date string, source_id)

        Returns:
            The data from the source
        """
        pass


class ITargetGetter(ABC):
    """Interface for reading data from the target system."""

    @abstractmethod
    def get_latest(self, stream_id: StreamId, data_provider: str) -> pd.DataFrame:
        """
        Fetch existing records for the given stream from the target system.

        Args:
            stream_id: The stream ID to fetch data for
            data_provider: The data provider identifier

        Returns:
            pd.DataFrame: The existing data in the target system
        """
        pass


class ITargetSetter(ABC):
    """Interface for writing data to the target system."""

    @abstractmethod
    def insert_data(self, stream_id: StreamId, data: pd.DataFrame, data_provider: str) -> None:
        """
        Insert data into the target system.

        Args:
            stream_id: The stream ID to insert data for
            data: The data to insert
            data_provider: The data provider identifier
        """
        pass


class IReconciliationStrategy(ABC):
    """Interface for determining what data needs to be fetched."""

    @abstractmethod
    def determine_needed_keys(
        self, streams_df: StreamMetadataDF, target_getter: ITargetGetter, data_provider: str
    ) -> NeededKeysMap:
        """
        Determine which keys need to be fetched for each stream.

        Args:
            streams_df: DataFrame containing stream metadata
            target_getter: The target system getter
            data_provider: The data provider identifier

        Returns:
            Dict mapping stream_id to list of keys that need to be fetched
        """
        pass


class IDataTransformer(ABC, Generic[T]):
    """Interface for transforming data."""

    @abstractmethod
    def transform(self, data: T) -> pd.DataFrame:
        """
        Transform data from source format to target format.

        Args:
            data: The data to transform

        Returns:
            pd.DataFrame: The transformed data
        """
        pass
