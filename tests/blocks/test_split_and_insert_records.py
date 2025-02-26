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
from prefect import flow

from tsn_adapters.blocks.tn_access import TNAccessBlock, task_split_and_insert_records
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel


@pytest.fixture(scope="session")
def helper_contract_id(tn_block: TNAccessBlock) -> Generator[str, None, None]:
    """Create and manage the helper contract."""
    helper_stream_id = tn_block.helper_contract_stream_name
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


# Define flows outside the test class
@flow(name="helper-split-small-batches-flow")
def helper_split_small_batches_flow(tn_block: TNAccessBlock, records: DataFrame[TnDataRowModel], max_batch_size: int):
    """Flow wrapper for split_and_insert_records with small batches."""
    return task_split_and_insert_records(
        tn_block, records, max_batch_size=max_batch_size, is_unix=True, 
        filter_deployed_streams=False, wait=False
    )

@flow(name="helper-split-single-batch-flow")
def helper_split_single_batch_flow(tn_block: TNAccessBlock, records: DataFrame[TnDataRowModel], max_batch_size: int):
    """Flow wrapper for split_and_insert_records with a single batch."""
    return task_split_and_insert_records(
        tn_block, records, max_batch_size=max_batch_size, is_unix=True, 
        filter_deployed_streams=False, wait=False
    )

@flow(name="helper-split-empty-records-flow")
def helper_split_empty_records_flow(tn_block: TNAccessBlock, empty_records: DataFrame[TnDataRowModel]):
    """Flow wrapper for split_and_insert_records with empty records."""
    return task_split_and_insert_records(
        tn_block, empty_records, is_unix=True, filter_deployed_streams=False, wait=False
    )

@flow(name="helper-split-with-failures-flow")
def helper_split_with_failures_flow(tn_block: TNAccessBlock, mixed_records: DataFrame[TnDataRowModel]):
    """Flow wrapper for split_and_insert_records with failures."""
    return task_split_and_insert_records(
        tn_block, mixed_records, is_unix=True, filter_deployed_streams=False, wait=True
    )

@flow(name="helper-filter-deployed-streams-flow")
def helper_filter_deployed_streams_flow(tn_block: TNAccessBlock, all_records: DataFrame[TnDataRowModel]):
    """Flow wrapper for split_and_insert_records with filter_deployed_streams=True."""
    return task_split_and_insert_records(
        tn_block,
        all_records,
        is_unix=True,
        filter_deployed_streams=True,
        wait=True
    )

@flow(name="helper-filter-deployed-streams-empty-flow")
def helper_filter_deployed_streams_empty_flow(tn_block: TNAccessBlock, empty_records: DataFrame[TnDataRowModel]):
    """Flow wrapper for split_and_insert_records with empty records and filter_deployed_streams=True."""
    return task_split_and_insert_records(
        tn_block,
        empty_records,
        is_unix=True,
        filter_deployed_streams=True,
        wait=True
    )

class TestSplitAndInsertRecords:
    """Integration tests for split_and_insert_records functionality."""

    def test_split_and_insert_small_batches(self, tn_block: TNAccessBlock, sample_records: DataFrame[TnDataRowModel]):
        """Test splitting and inserting records with small batch size."""
        # Split into very small batches (2 records each)
        results = helper_split_small_batches_flow(tn_block, sample_records, max_batch_size=2)

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
        results = helper_split_single_batch_flow(tn_block, sample_records, max_batch_size=100)

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
        results = helper_split_empty_records_flow(tn_block, empty_records)

        if results is None:
            pytest.fail("Results should not be None")

        assert results["success_tx_hashes"] == []
        assert results["failed_records"].empty

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

        results = helper_split_with_failures_flow(tn_block, mixed_records)

        assert results is not None
        assert len(results["success_tx_hashes"]) == 0
        assert len(results["failed_records"]) > 0
        assert "invalid_stream_id" in results["failed_records"]["stream_id"].values
        assert any("invalid stream id" in reason for reason in results["failed_reasons"])

    def test_filter_deployed_streams(self, tn_block: TNAccessBlock, test_stream_ids: list[str]):
        """Test that filter_deployed_streams correctly filters out non-deployed and uninitialized streams."""
        base_timestamp = int(datetime.now().timestamp())
        
        # Create records for deployed, initialized, and non-existent streams
        deployed_records = []
        for stream_id in test_stream_ids:  # These are already deployed and initialized by the fixture
            deployed_records.append({
                "stream_id": stream_id,
                "date": base_timestamp,
                "value": 100.0,
            })
            
        # Create a stream that exists but isn't initialized
        uninitialized_stream_id = generate_stream_id("uninitialized_test_stream")
        client = tn_block.get_client()
        client.deploy_stream(uninitialized_stream_id, stream_type=truf_sdk.StreamTypePrimitiveUnix, wait=True)
        # Don't initialize it
        
        # Create records for uninitialized and non-existent streams
        non_deployed_stream_id = generate_stream_id("non_deployed_test_stream")
        other_records = [
            {
                "stream_id": uninitialized_stream_id,
                "date": base_timestamp,
                "value": 200.0,
            },
            {
                "stream_id": non_deployed_stream_id,
                "date": base_timestamp,
                "value": 300.0,
            }
        ]
        
        # Combine all records
        all_records = pd.DataFrame(deployed_records + other_records)
        all_records = DataFrame[TnDataRowModel](all_records)
        
        # Test with filter_deployed_streams=True
        results = helper_filter_deployed_streams_flow(tn_block, all_records)
        
        assert results is not None
        # Should have successfully processed only the deployed and initialized streams
        assert len(results["success_tx_hashes"]) > 0
        # No failed records since non-deployed/uninitialized streams were filtered out
        assert len(results["failed_records"]) == 0
        
        # Verify only deployed and initialized streams were processed
        for stream_id in test_stream_ids:
            records = tn_block.read_records(stream_id, is_unix=True, date_from=base_timestamp)
            assert len(records) == 1
            
        # Verify non-existent streams were not processed
        with pytest.raises(Exception):
            tn_block.read_records(non_deployed_stream_id, is_unix=True, date_from=base_timestamp)
            
        # Clean up the uninitialized stream
        try:
            tn_block.destroy_stream(uninitialized_stream_id)
        except Exception as e:
            pytest.fail(f"Failed to cleanup test stream {uninitialized_stream_id}: {e}")

    def test_filter_deployed_streams_empty_records(self, tn_block: TNAccessBlock):
        """Test that filter_deployed_streams handles empty record sets correctly."""
        # Create an empty DataFrame with the correct schema
        empty_records = DataFrame[TnDataRowModel](pd.DataFrame(columns=["stream_id", "data_provider", "date", "value"]))
        
        # Test with filter_deployed_streams=True
        results = helper_filter_deployed_streams_empty_flow(tn_block, empty_records)
        
        assert results is not None
        assert len(results["success_tx_hashes"]) == 0
        assert len(results["failed_records"]) == 0
