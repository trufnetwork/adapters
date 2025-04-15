"""
Integration tests for TNAccessBlock's split_and_insert_records functionality.

These tests verify the batch splitting and insertion behavior with real TN streams.
"""

from collections.abc import Generator
from datetime import datetime
import time

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow
import pytest
import trufnetwork_sdk_c_bindings.exports as truf_sdk
from trufnetwork_sdk_py.utils import generate_stream_id

from tsn_adapters.blocks.tn_access import TNAccessBlock, task_split_and_insert_records
from tsn_adapters.common.trufnetwork.models.tn_models import StreamLocatorModel, TnDataRowModel


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
        client.deploy_stream(stream_id, stream_type=truf_sdk.StreamTypePrimitive, wait=True)

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
        tn_block, records, max_batch_size=max_batch_size, is_unix=True, filter_deployed_streams=False, wait=False
    )


@flow(name="helper-split-single-batch-flow")
def helper_split_single_batch_flow(tn_block: TNAccessBlock, records: DataFrame[TnDataRowModel], max_batch_size: int):
    """Flow wrapper for split_and_insert_records with a single batch."""
    return task_split_and_insert_records(
        tn_block, records, max_batch_size=max_batch_size, is_unix=True, filter_deployed_streams=False, wait=False
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
    return task_split_and_insert_records(tn_block, all_records, is_unix=True, filter_deployed_streams=True, wait=True)


@flow(name="helper-filter-deployed-streams-empty-flow")
def helper_filter_deployed_streams_empty_flow(tn_block: TNAccessBlock, empty_records: DataFrame[TnDataRowModel]):
    """Flow wrapper for split_and_insert_records with empty records and filter_deployed_streams=True."""
    return task_split_and_insert_records(tn_block, empty_records, is_unix=True, filter_deployed_streams=True, wait=True)


@flow(name="helper-direct-filter-streams-flow")
def helper_direct_filter_streams_flow(
    tn_block: TNAccessBlock, records: DataFrame[TnDataRowModel], max_depth: int, max_filter_size: int
):
    """Flow wrapper for directly calling task_filter_initialized_streams."""
    from tsn_adapters.blocks.tn_access import task_filter_initialized_streams

    filter_results = task_filter_initialized_streams.with_options(
        retries=0, cache_key_fn=None, cache_expiration=None, retry_condition_fn=None
    )(block=tn_block, records=records, max_depth=max_depth, max_filter_size=max_filter_size)
    # Ensure we return a non-None value for type checking
    if filter_results is None:
        empty_df = DataFrame[StreamLocatorModel](pd.DataFrame(columns=["stream_id", "data_provider"]))
        return {"existent_streams": empty_df, "missing_streams": empty_df}
    return filter_results


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

        # Verify error messages without iterating over potentially None value
        if results.get("failed_reasons") and len(results.get("failed_reasons", [])) > 0:
            # At least one error message should contain "invalid stream id"
            found_invalid_message = False
            for reason in results.get("failed_reasons", []):
                if reason and "invalid stream id" in reason:
                    found_invalid_message = True
                    break
            assert found_invalid_message, "Expected to find 'invalid stream id' in error messages"
        else:
            assert False, "Expected failed_reasons to contain error messages"

    @pytest.mark.usefixtures("prefect_test_fixture")
    def test_filter_initialized_streams(self, tn_block: TNAccessBlock, test_stream_ids: list[str]):
        """Test the divide-and-conquer stream filtering algorithm with a variety of stream states."""
        base_timestamp = int(datetime.now().timestamp())
        client = tn_block.get_client()
        test_name = f"filter_init_{base_timestamp}"

        # 1. Set up test streams with different statuses
        # Already initialized streams from fixture
        initialized_stream_ids = test_stream_ids

        # Create uninitialized streams (deployed but not initialized)
        uninitialized_stream_ids = [
            generate_stream_id(f"{test_name}_uninit_1"),
            generate_stream_id(f"{test_name}_uninit_2"),
            generate_stream_id(f"{test_name}_uninit_3"),
        ]

        for stream_id in uninitialized_stream_ids:
            client.deploy_stream(stream_id, stream_type=truf_sdk.StreamTypePrimitive, wait=True)
            # Deliberately not initializing these streams

        # Generate non-existent stream IDs
        non_existent_stream_ids = [
            generate_stream_id(f"{test_name}_nonexistent_1"),
            generate_stream_id(f"{test_name}_nonexistent_2"),
            generate_stream_id(f"{test_name}_nonexistent_3"),
        ]

        # 2. Create records for all streams
        all_records = []

        # Add records for initialized streams
        for stream_id in initialized_stream_ids:
            all_records.append(
                {
                    "stream_id": stream_id,
                    "data_provider": tn_block.current_account,
                    "date": base_timestamp,
                    "value": 100.0,
                }
            )

        # Add records for uninitialized streams
        for stream_id in uninitialized_stream_ids:
            all_records.append(
                {
                    "stream_id": stream_id,
                    "data_provider": tn_block.current_account,
                    "date": base_timestamp,
                    "value": 200.0,
                }
            )

        # Add records for non-existent streams
        for stream_id in non_existent_stream_ids:
            all_records.append(
                {
                    "stream_id": stream_id,
                    "data_provider": tn_block.current_account,
                    "date": base_timestamp,
                    "value": 300.0,
                }
            )

        # Convert to DataFrame
        records_df = DataFrame[TnDataRowModel](pd.DataFrame(all_records))

        # 3. Test the filter function directly with different parameters

        # Test with normal parameters
        filter_results = helper_direct_filter_streams_flow(tn_block, records_df, max_depth=10, max_filter_size=5000)

        # Verify results
        assert len(filter_results["initialized_streams"]) == len(initialized_stream_ids)
        assert len(filter_results["uninitialized_streams"]) == len(uninitialized_stream_ids) + len(
            non_existent_stream_ids
        )

        # Verify each initialized stream is in the existent_streams result
        existent_stream_ids = filter_results["initialized_streams"]["stream_id"].tolist()
        for stream_id in initialized_stream_ids:
            assert stream_id in existent_stream_ids

        # Verify uninitialized and non-existent streams are in missing_streams
        missing_stream_ids = filter_results["uninitialized_streams"]["stream_id"].tolist()
        for stream_id in uninitialized_stream_ids + non_existent_stream_ids:
            assert stream_id in missing_stream_ids

        # 4. Test with small max_filter_size to force divide and conquer
        small_filter_results = helper_direct_filter_streams_flow(tn_block, records_df, max_depth=10, max_filter_size=1)

        # Results should be the same even with a small max_filter_size
        assert len(small_filter_results["initialized_streams"]) == len(initialized_stream_ids)
        assert len(small_filter_results["uninitialized_streams"]) == len(uninitialized_stream_ids) + len(
            non_existent_stream_ids
        )

        # 5. Test with small max_depth to force fallback to individual checks
        fallback_filter_results = helper_direct_filter_streams_flow(
            tn_block, records_df, max_depth=1, max_filter_size=5000
        )

        # Results should be the same even with fallback
        assert len(fallback_filter_results["initialized_streams"]) == len(initialized_stream_ids)
        assert len(fallback_filter_results["uninitialized_streams"]) == len(uninitialized_stream_ids) + len(
            non_existent_stream_ids
        )

        # 6. Clean up the uninitialized streams
        for stream_id in uninitialized_stream_ids:
            try:
                tn_block.destroy_stream(stream_id)
            except Exception as e:
                pytest.fail(f"Failed to cleanup test stream {stream_id}: {e}")

    def test_filter_initialized_streams_large(self, tn_block: TNAccessBlock, test_stream_ids: list[str]):
        """Test the divide-and-conquer algorithm with a larger number of streams to stress test performance."""
        base_timestamp = int(datetime.now().timestamp())
        client = tn_block.get_client()
        test_name = f"filter_large_{base_timestamp}"

        # Use streams from fixture as initialized streams
        initialized_stream_ids = test_stream_ids

        # Print debug info about test_stream_ids
        print("\nDEBUG: test_stream_ids fixture:")
        print(f"Type: {type(test_stream_ids)}")
        print(f"Content: {test_stream_ids}")
        print(f"Length: {len(test_stream_ids)}")

        # Create a larger set of records to test the divide-and-conquer algorithm
        # We'll use mostly non-existent streams as they don't require deployment
        num_nonexistent_streams = 20  # Adjust based on desired test size
        non_existent_stream_ids = [
            generate_stream_id(f"{test_name}_nonexist_{i}") for i in range(num_nonexistent_streams)
        ]

        # Create a few uninitialized streams
        uninitialized_stream_ids = [
            generate_stream_id(f"{test_name}_uninit_1"),
            generate_stream_id(f"{test_name}_uninit_2"),
        ]

        for stream_id in uninitialized_stream_ids:
            client.deploy_stream(stream_id, stream_type=truf_sdk.StreamTypePrimitive, wait=True)
            # Deliberately not initializing

        # Combine all stream ids
        all_stream_ids = initialized_stream_ids + uninitialized_stream_ids + non_existent_stream_ids

        # Create records for all streams
        all_records = []
        for i, stream_id in enumerate(all_stream_ids):
            all_records.append(
                {
                    "stream_id": stream_id,
                    "data_provider": tn_block.current_account,
                    "date": base_timestamp + i,
                    "value": float(i * 10),
                }
            )

        # Convert to DataFrame
        records_df = DataFrame[TnDataRowModel](pd.DataFrame(all_records))

        # --- Measure performance with different configurations ---

        # Test with default parameters
        start_time = time.time()
        filter_results = helper_direct_filter_streams_flow(tn_block, records_df, max_depth=10, max_filter_size=5000)
        default_duration = time.time() - start_time

        # Debug prints to understand the issue
        print("\nDEBUG: Filter results:")
        print(f"initialized_stream_ids: {initialized_stream_ids}")
        print(f"filter_results keys: {filter_results.keys()}")
        print(f"filter_results['initialized_streams']:\n{filter_results['initialized_streams']}")
        print(f"Number of initialized streams: {len(filter_results['initialized_streams'])}")
        print(f"Number of expected streams: {len(initialized_stream_ids)}")

        # Check for duplicates in initialized_streams
        if len(filter_results["initialized_streams"]) > 0:
            stream_id_counts = filter_results["initialized_streams"]["stream_id"].value_counts()
            print(f"Stream ID counts: {stream_id_counts}")
            duplicates = stream_id_counts[stream_id_counts > 1]
            if not duplicates.empty:
                print(f"Duplicate stream IDs found: {duplicates}")
                # Print the full rows for duplicated stream IDs
                for dup_id in duplicates.index:
                    dup_rows = filter_results["initialized_streams"][
                        filter_results["initialized_streams"]["stream_id"] == dup_id
                    ]
                    print(f"Rows for duplicate stream ID {dup_id}:\n{dup_rows}")

        # Verify correct number of streams in each category
        assert len(filter_results["initialized_streams"]) == len(initialized_stream_ids)
        assert len(filter_results["uninitialized_streams"]) == len(uninitialized_stream_ids) + len(
            non_existent_stream_ids
        )

        # Force divide and conquer with small max_filter_size
        start_time = time.time()
        small_batch_results = helper_direct_filter_streams_flow(tn_block, records_df, max_depth=10, max_filter_size=3)
        small_batch_duration = time.time() - start_time

        # Results should be consistent regardless of batch size
        assert len(small_batch_results["initialized_streams"]) == len(initialized_stream_ids)
        assert len(small_batch_results["uninitialized_streams"]) == len(uninitialized_stream_ids) + len(
            non_existent_stream_ids
        )

        # Force fallback method with small max_depth
        start_time = time.time()
        fallback_results = helper_direct_filter_streams_flow(tn_block, records_df, max_depth=1, max_filter_size=5000)
        fallback_duration = time.time() - start_time

        # Results should be consistent regardless of approach
        assert len(fallback_results["initialized_streams"]) == len(initialized_stream_ids)
        assert len(fallback_results["uninitialized_streams"]) == len(uninitialized_stream_ids) + len(
            non_existent_stream_ids
        )

        # Log performance results
        print(f"\nPerformance results for filter_initialized_streams with {len(all_stream_ids)} streams:")
        print(f"Default parameters: {default_duration:.4f} seconds")
        print(f"Small batch size: {small_batch_duration:.4f} seconds")
        print(f"Fallback method: {fallback_duration:.4f} seconds\n")

        # Clean up uninitialized streams
        for stream_id in uninitialized_stream_ids:
            try:
                tn_block.destroy_stream(stream_id)
            except Exception as e:
                pytest.fail(f"Failed to cleanup test stream {stream_id}: {e}")

    def test_filter_deployed_streams(self, tn_block: TNAccessBlock, test_stream_ids: list[str]):
        """Test that filter_deployed_streams correctly filters out non-deployed and uninitialized streams."""
        base_timestamp = int(datetime.now().timestamp())

        # Create records for deployed, initialized, and non-existent streams
        deployed_records = []
        for stream_id in test_stream_ids:  # These are already deployed and initialized by the fixture
            deployed_records.append(
                {
                    "stream_id": stream_id,
                    "date": base_timestamp,
                    "value": 100.0,
                }
            )

        # Create a stream that exists but isn't initialized
        uninitialized_stream_id = generate_stream_id("uninitialized_test_stream")
        client = tn_block.get_client()
        client.deploy_stream(uninitialized_stream_id, stream_type=truf_sdk.StreamTypePrimitive, wait=True)
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
            },
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

    def test_filter_initialized_streams_streaming(self, tn_block: TNAccessBlock, test_stream_ids: list[str]):
        """Test the streaming capability of the filter function with very large stream batches."""
        base_timestamp = int(datetime.now().timestamp())
        test_name = f"filter_stream_{base_timestamp}"

        # Use a small number of real streams (from fixture) and a large number of non-existent streams
        # to create a realistic large-scale test without creating too many real streams
        initialized_stream_ids = test_stream_ids

        # Create a large set of non-existent streams (these don't require actual deployment)
        num_streams = 50  # This would create a large batch without overwhelming the test
        non_existent_stream_ids = [generate_stream_id(f"{test_name}_nonexist_{i}") for i in range(num_streams)]

        # Combine all stream IDs
        all_stream_ids = initialized_stream_ids + non_existent_stream_ids

        # Create records for all streams
        all_records = []
        for i, stream_id in enumerate(all_stream_ids):
            all_records.append(
                {
                    "stream_id": stream_id,
                    "data_provider": tn_block.current_account,
                    "date": base_timestamp + i,
                    "value": float(i * 10),
                }
            )

        # Convert to DataFrame
        records_df = DataFrame[TnDataRowModel](pd.DataFrame(all_records))

        # Test with various max_filter_size settings to see how it affects streaming behavior
        filter_sizes = [5, 10, 20, 50, len(all_stream_ids)]

        print(f"\nStreaming performance results for {len(all_stream_ids)} streams:")
        for size in filter_sizes:
            start_time = time.time()
            results = helper_direct_filter_streams_flow(tn_block, records_df, max_depth=10, max_filter_size=size)
            duration = time.time() - start_time

            # Verify results are correct
            assert len(results["initialized_streams"]) == len(initialized_stream_ids)
            assert len(results["uninitialized_streams"]) == len(non_existent_stream_ids)

            print(f"  max_filter_size={size}: {duration:.4f} seconds")
