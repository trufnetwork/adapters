"""
TrufNetwork target system implementation.
"""

import logging
from typing import Optional
import warnings

import pandas as pd
from pandera.typing import DataFrame
from prefect import get_run_logger
from prefect.utilities.asyncutils import sync_compatible

from tsn_adapters.blocks.tn_access import (
    TNAccessBlock,
    task_insert_and_wait_for_tx,
    task_read_records,
    task_split_and_insert_records,
    task_wait_for_tx,
)
from tsn_adapters.common.interfaces.target import ITargetClient
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.tasks.argentina.types import StreamId

logger = logging.getLogger(__name__)


class TrufNetworkClient(ITargetClient[StreamId]):
    """Client for interacting with TrufNetwork target system."""

    def __init__(self, block_name: str) -> None:
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
        logger = get_run_logger()
        logger.debug(f"Getting latest data for stream {stream_id} from TrufNetwork")

        # Get latest record from TrufNetwork
        df = task_read_records(
            block=self.block,
            stream_id=stream_id,
            data_provider=data_provider,
        )

        if df is None or df.empty:
            logger.info("No existing data found for stream %s", stream_id)
            return pd.DataFrame()

        # check only 1 record
        if len(df) > 1:
            raise ValueError(f"More than 1 record found for stream {stream_id}")

        logger.debug("Found existing record for stream %s", stream_id)
        return df

    def batch_insert_data(self, data: DataFrame[TnDataRowModel]) -> None:
        """
        Batch insert data into TrufNetwork.
        """
        logger = get_run_logger()
        logger.info(f"Inserting {len(data)} records into TrufNetwork")

        results = task_split_and_insert_records(
            block=self.block,
            records=data,
            is_unix=False,
            wait=True,
            has_external_created_at=True,
        )

        if results["failed_records"] is not None and not results["failed_records"].empty:
            raise ValueError("Failed to insert data into TrufNetwork")

    def insert_data(self, stream_id: StreamId, data: pd.DataFrame, data_provider: Optional[str] = None) -> None:
        """
        Insert data into TrufNetwork.

        Args:
            stream_id: The stream ID to insert data for
            data: The data to insert
            data_provider: The data provider identifier

        Deprecated: Use batch_insert_data instead.
        """
        warnings.warn("insert_data is deprecated. Use batch_insert_data instead.", DeprecationWarning)
        logger = get_run_logger()
        logger.info(f"Inserting {len(data)} records for stream {stream_id} into TrufNetwork")

        # Insert data into TrufNetwork
        task_insert_and_wait_for_tx(
            block=self.block,
            stream_id=stream_id,
            records=data,
            data_provider=data_provider,
        )

        logger.info("Data inserted successfully")


def create_trufnetwork_components(block_name: str) -> TrufNetworkClient:
    """
    Create TrufNetwork client instance.

    Args:
        block_name: The name of the TrufNetwork block to use

    Returns:
        TrufNetworkClient: The client instance
    """
    client = TrufNetworkClient(block_name)
    client.initialize()  # Initialize the block
    return client
