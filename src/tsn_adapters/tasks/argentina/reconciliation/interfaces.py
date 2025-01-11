"""
Base interfaces for data reconciliation.
"""

from abc import ABC, abstractmethod

from tsn_adapters.tasks.argentina.provider.interfaces.base import IProviderGetter
from tsn_adapters.tasks.argentina.target.interfaces import ITargetGetter
from tsn_adapters.tasks.argentina.types import NeededKeysMap, StreamSourceMapDF


class IReconciliationStrategy(ABC):
    """Interface for determining what data needs to be fetched."""

    @abstractmethod
    def determine_needed_keys(
        self,
        streams_df: StreamSourceMapDF,
        provider_getter: IProviderGetter,
        target_getter: ITargetGetter,
        data_provider: str,
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

    