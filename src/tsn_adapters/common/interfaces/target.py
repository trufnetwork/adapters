"""
Base interfaces for target system interactions.
"""

from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

import pandas as pd
from pandera.typing import DataFrame

from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel

S = TypeVar("S")  # For stream ID types (e.g. StreamId)


class ITargetClient(ABC, Generic[S]):
    """Unified interface for reading/writing data in the target system."""

    @abstractmethod
    def get_latest(self, stream_id: S, data_provider: str) -> pd.DataFrame:
        """
        Fetch existing records for the given stream from the target system.

        Args:
            stream_id: The stream ID to fetch data for
            data_provider: The data provider identifier

        Returns:
            pd.DataFrame: The existing data in the target system
        """
        pass

    @abstractmethod
    def batch_insert_data(self, data: DataFrame[TnDataRowModel]) -> None:
        """
        Batch insert data into the target system.
        """
        pass

    @abstractmethod
    def insert_data(self, stream_id: S, data: pd.DataFrame, data_provider: Optional[str] = None) -> None:
        """
        Insert data into the target system.

        Args:
            stream_id: The stream ID to insert data for
            data: The data to insert
            data_provider: The data provider identifier
        """
        pass
