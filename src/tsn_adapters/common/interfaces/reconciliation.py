"""
Base interfaces for data reconciliation.
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

import pandas as pd

from tsn_adapters.common.interfaces.provider import IProviderGetter
from tsn_adapters.common.interfaces.target import ITargetClient

K = TypeVar("K")  # For key types (e.g. DateStr)
S = TypeVar("S")  # For stream ID types (e.g. StreamId)


class IReconciliationStrategy(ABC, Generic[K, S]):
    """Interface for determining what data needs to be fetched."""

    @abstractmethod
    def determine_needed_keys(
        self,
        streams_df: pd.DataFrame,
        provider_getter: IProviderGetter,
        target_client: ITargetClient,
        data_provider: str,
    ) -> dict[S, list[K]]:
        """
        Determine which keys need to be fetched for each stream.

        Args:
            streams_df: DataFrame containing stream metadata with columns:
                - stream_id: str
                - source_id: str
            provider_getter: The source data provider
            target_client: The target system client
            data_provider: The data provider identifier

        Returns:
            Dict mapping stream_id to list of keys that need to be fetched
        """
        pass
