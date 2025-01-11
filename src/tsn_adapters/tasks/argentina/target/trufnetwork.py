"""
TrufNetwork target implementations for getting and setting data.
"""

import pandas as pd
from prefect import task
from prefect.utilities.asyncutils import sync_compatible

from tsn_adapters.blocks.tn_access import TNAccessBlock, task_insert_and_wait_for_tx, task_read_records
from tsn_adapters.tasks.argentina.target.interfaces import ITargetGetter, ITargetSetter
from tsn_adapters.tasks.argentina.types import StreamId


class TrufNetworkTargetGetter(ITargetGetter):
    """Gets data from TrufNetwork."""

    def __init__(self, block_name: str):
        """
        Initialize with a TrufNetwork access block name.

        Args:
            block_name: Name of the TrufNetwork access block
        """
        self._block_name = block_name
        self._state: dict[str, TNAccessBlock] = {}

    @property
    def block(self) -> TNAccessBlock:
        """Get the initialized block instance."""
        if "block" not in self._state:
            raise RuntimeError("Block not initialized. Call initialize() first.")
        return self._state["block"]

    @sync_compatible
    async def initialize(self) -> None:
        """Initialize by loading the block."""
        block = await TNAccessBlock.aload(self._block_name)
        self._state["block"] = block

    def get_latest(self, stream_id: StreamId, data_provider: str) -> pd.DataFrame:
        """
        Fetch existing records for the given stream from TrufNetwork.

        Args:
            stream_id: The stream ID to fetch data for
            data_provider: The data provider identifier

        Returns:
            pd.DataFrame: The existing data in TrufNetwork
        """
        return task_read_records(
            block=self.block,
            stream_id=stream_id,
            data_provider=data_provider,
        )


class TrufNetworkTargetSetter(ITargetSetter):
    """Sets data in TrufNetwork."""

    def __init__(self, block_name: str):
        """
        Initialize with a TrufNetwork access block name.

        Args:
            block_name: Name of the TrufNetwork access block
        """
        self._block_name = block_name
        self._state: dict[str, TNAccessBlock] = {}

    @property
    def block(self) -> TNAccessBlock:
        """Get the initialized block instance."""
        if "block" not in self._state:
            raise RuntimeError("Block not initialized. Call initialize() first.")
        return self._state["block"]

    @sync_compatible
    async def initialize(self) -> None:
        """Initialize by loading the block."""
        block = await TNAccessBlock.aload(self._block_name)
        self._state["block"] = block

    def insert_data(self, stream_id: StreamId, data: pd.DataFrame, data_provider: str) -> None:
        """
        Insert data into TrufNetwork.

        Args:
            stream_id: The stream ID to insert data for
            data: The data to insert
            data_provider: The data provider identifier
        """
        task_insert_and_wait_for_tx(
            block=self.block,
            stream_id=stream_id,
            records=data,
            data_provider=data_provider,
        )


@task(name="Create TrufNetwork Target Components")
def create_trufnetwork_components(
    block_name: str,
) -> tuple[TrufNetworkTargetGetter, TrufNetworkTargetSetter]:
    """
    Create TrufNetwork target components.

    Args:
        block_name: Name of the TrufNetwork access block

    Returns:
        tuple: (getter, setter) instances
    """
    getter = TrufNetworkTargetGetter(block_name)
    getter.initialize()

    setter = TrufNetworkTargetSetter(block_name)
    setter.initialize()

    return getter, setter
