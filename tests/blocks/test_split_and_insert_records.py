"""
Integration tests for TNAccessBlock's split_and_insert_records functionality.

These tests verify the batch splitting and insertion behavior with real TN streams.
"""

from collections.abc import Generator
from datetime import datetime

import pandas as pd
from pandera.typing import DataFrame
import pytest
import trufnetwork_sdk_c_bindings.exports as truf_sdk
from trufnetwork_sdk_py.utils import generate_stream_id

from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel

# Default private key for testing
DEFAULT_TN_PRIVATE_KEY = "0" * 63 + "1"  # 64 zeros ending with 1


@pytest.fixture(scope="session")
def helper_contract_id(tn_block: TNAccessBlock) -> Generator[str, None, None]:
    """Create and manage the helper contract."""
    helper_stream_id = tn_block.helper_contract_stream_id
    client = tn_block.get_client()

    # Try to deploy helper contract
    try:
        # we don't need to initialize helper contracts
        client.deploy_stream(helper_stream_id, stream_type=truf_sdk.StreamTypeHelper, wait=True)
    except Exception as e:
        if "dataset exists" not in str(e) and "already exists" not in str(e):
            raise e

    yield helper_stream_id

    # Don't cleanup helper contract as it might be used by other tests


@pytest.fixture
def test_stream_ids(tn_block: TNAccessBlock, helper_contract_id: str) -> Generator[list[str], None, None]:
    """Generate unique test stream IDs and handle cleanup.

    Creates two test streams and cleans them up after the test.
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    stream_ids = [
        generate_stream_id(f"test_split_{timestamp}_1"),
        generate_stream_id(f"test_split_{timestamp}_2"),
    ]

    # Deploy streams
    client = tn_block.get_client()
    for stream_id in stream_ids:
        client.deploy_stream(stream_id, stream_type=truf_sdk.StreamTypePrimitiveUnix, wait=True)
        client.init_stream(stream_id, wait=True)

    yield stream_ids

    # Cleanup
    for stream_id in stream_ids:
        try:
            tn_block.destroy_stream(stream_id)
        except Exception as e:
            pytest.fail(f"Failed to cleanup test stream {stream_id}: {e}")


@pytest.fixture
def sample_records(test_stream_ids: list[str]) -> DataFrame[TnDataRowModel]:
    """Create sample records for testing split insertion."""
    records = []
    base_timestamp = int(datetime.now().timestamp())

    # Create 10 records for each stream
    for stream_id in test_stream_ids:
        for i in range(10):
            records.append(
                {
                    "stream_id": stream_id,
                    "date": base_timestamp + i,
                    "value": float(i * 100),
                }
            )

    return DataFrame[TnDataRowModel](pd.DataFrame(records))


class TestSplitAndInsertRecords:
    """Integration tests for split_and_insert_records functionality."""

    def test_split_and_insert_small_batches(self, tn_block: TNAccessBlock, sample_records: DataFrame[TnDataRowModel]):
        """Test splitting and inserting records with small batch size."""
        # Split into very small batches (2 records each)
        results = tn_block.split_and_insert_records(sample_records, max_batch_size=2)

        assert results is not None
        assert len(results["success_tx_hashes"]) > 0
        assert len(results["failed_records"]) == 0

        # Wait for all transactions to be confirmed
        for tx_hash in results["success_tx_hashes"]:
            tn_block.wait_for_tx(tx_hash)

        # Verify records were inserted by reading them back
        for stream_id in sample_records["stream_id"].unique():
            records = tn_block.read_records(stream_id, is_unix=True, date_from=0)
            stream_record_count = len(sample_records[sample_records["stream_id"] == stream_id])
            assert len(records) == stream_record_count

    def test_split_and_insert_single_batch(self, tn_block: TNAccessBlock, sample_records: DataFrame[TnDataRowModel]):
        """Test inserting all records in a single batch."""
        # Use batch size larger than total records
        results = tn_block.split_and_insert_records(sample_records, max_batch_size=100)

        assert results is not None
        assert len(results["success_tx_hashes"]) == 1  # Should be single batch
        assert len(results["failed_records"]) == 0

        # Wait for transaction to be confirmed
        for tx_hash in results["success_tx_hashes"]:
            tn_block.wait_for_tx(tx_hash)

        # Verify records were inserted
        for stream_id in sample_records["stream_id"].unique():
            records = tn_block.read_records(stream_id, is_unix=True, date_from=0)
            stream_record_count = len(sample_records[sample_records["stream_id"] == stream_id])
            assert len(records) == stream_record_count

    def test_split_and_insert_empty_records(self, tn_block: TNAccessBlock):
        """Test handling of empty records DataFrame."""
        empty_records = DataFrame[TnDataRowModel](pd.DataFrame(columns=["stream_id", "date", "value"]))
        results = tn_block.split_and_insert_records(empty_records)

        assert results is None  # Should return None for empty records

    def test_split_and_insert_with_failures(self, tn_block: TNAccessBlock, sample_records: DataFrame[TnDataRowModel]):
        """Test handling of insertion failures."""
        # Add some records with invalid stream IDs that will fail
        invalid_records = pd.DataFrame(
            [
                {
                    "stream_id": "invalid_stream_id",
                    "date": int(datetime.now().timestamp()),
                    "value": 100.0,
                }
            ]
        )
        mixed_records = pd.concat([sample_records, invalid_records])
        mixed_records = DataFrame[TnDataRowModel](mixed_records)

        results = tn_block.split_and_insert_records(mixed_records, max_batch_size=5)

        assert results is not None
        assert len(results["success_tx_hashes"]) > 0
        assert len(results["failed_records"]) > 0
        assert "invalid_stream_id" in results["failed_records"]["stream_id"].values
