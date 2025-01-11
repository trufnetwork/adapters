"""
Base interfaces for target system interactions.
"""

from abc import ABC, abstractmethod

import pandas as pd

from tsn_adapters.tasks.argentina.types import StreamId


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
