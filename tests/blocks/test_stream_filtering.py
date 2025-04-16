"""Unit tests for the stream filtering functionality."""

# import logging # Removed unused import
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
from prefect import flow
import pytest
from tsn_adapters.blocks.stream_filtering import (
    DivideAndConquerResult,
    StreamStatesResult,
    batch_method_filter_streams,
    combine_divide_conquer_results,
    fallback_method_filter_streams,
    task_filter_streams_divide_conquer,
    task_get_stream_states_divide_conquer,
)
from tsn_adapters.blocks.tn_access import (
    MetadataProcedureNotFoundError,
    StreamLocatorModel,
    convert_to_typed_df,
    create_empty_df,
    create_empty_stream_locator_df,
)
from tests.utils.fake_tn_access import FakeTNAccessBlock


@pytest.fixture(autouse=True)
def patch_stream_filtering_tasks(monkeypatch: Any):
    from tsn_adapters.blocks import stream_filtering

    monkeypatch.setattr(stream_filtering.task_get_stream_states_divide_conquer, "retries", 0)
    monkeypatch.setattr(stream_filtering.task_filter_batch_initialized_streams, "retries", 0)
    monkeypatch.setattr(stream_filtering.task_filter_streams_divide_conquer, "retries", 0)


class TestStreamFiltering:
    """Test cases for stream filtering functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        from tests.utils.fake_tn_access import FakeTNAccessBlock
        self.mock_block = FakeTNAccessBlock()

        # Create test data
        self.stream_data = pd.DataFrame(
            {"data_provider": ["provider1", "provider2", "provider3"], "stream_id": ["stream1", "stream2", "stream3"]}
        )
        self.stream_locators = convert_to_typed_df(self.stream_data, StreamLocatorModel)

        # Sample results
        self.empty_df = create_empty_df(["data_provider", "stream_id"])
        self.empty_typed_df = create_empty_stream_locator_df()
        self.sample_result = DivideAndConquerResult(
            initialized_streams=convert_to_typed_df(
                pd.DataFrame({"data_provider": ["provider1"], "stream_id": ["stream1"]}), StreamLocatorModel
            ),
            uninitialized_streams=convert_to_typed_df(
                pd.DataFrame({"data_provider": ["provider2"], "stream_id": ["stream2"]}), StreamLocatorModel
            ),
            depth=1,
            fallback_used=False,
        )

    @patch("tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
    def test_fallback_method_filter_streams(self, mock_batch_init: MagicMock):
        """
        Test fallback_method_filter_streams using FakeTNAccessBlock state for stream_exists,
        but mocking the task called internally.
        
        This test simulates a scenario where only 'stream1' is deployed and initialized,
        and 'stream2' does not exist. The intent is to verify that the fallback method
        correctly partitions initialized and uninitialized streams based on the block's state.
        """
        self.mock_block.reset()
        self.mock_block.set_deployed_streams({"stream1"})
        self.mock_block.set_initialized_streams({"stream1"})

        # Mock the return value of the internal task call
        mock_batch_init.return_value = convert_to_typed_df(
            pd.DataFrame([{"data_provider": "provider1", "stream_id": "stream1"}]),
            StreamLocatorModel
        )

        # Only the first two locators are processed for this test case
        result = fallback_method_filter_streams(self.mock_block, self.stream_locators.iloc[:2], 1)

        # Verify the mock was called correctly
        mock_batch_init.assert_called_once()
        called_args = mock_batch_init.call_args[0]
        assert called_args[0] == self.mock_block
        pd.testing.assert_frame_equal(called_args[1], self.stream_locators.iloc[[0]]) # Should be called only with existing stream1

        # The result should reflect that 'stream1' is initialized and 'stream2' is uninitialized
        assert not result["initialized_streams"].empty
        assert not result["uninitialized_streams"].empty
        assert result["initialized_streams"].iloc[0]["stream_id"] == "stream1"
        assert result["uninitialized_streams"].iloc[0]["stream_id"] == "stream2"
        assert result["depth"] == 1
        assert result["fallback_used"] is True

    def test_batch_method_filter_streams_success(self):
        """
        Test batch_method_filter_streams when the batch call succeeds.
        
        This test configures the FakeTNAccessBlock so that only 'stream1' is both deployed and initialized.
        The intent is to verify that the batch method correctly identifies initialized and uninitialized streams.
        """
        self.mock_block.reset()
        # Only 'stream1' is deployed and initialized; others are deployed but not initialized
        self.mock_block.set_deployed_streams({"stream1", "stream2", "stream3"})
        self.mock_block.set_initialized_streams({"stream1"})

        result = batch_method_filter_streams(self.mock_block, self.stream_locators, 1)

        # The result should reflect that only 'stream1' is initialized
        assert result is not None
        assert not result["initialized_streams"].empty
        assert not result["uninitialized_streams"].empty
        assert result["initialized_streams"].iloc[0]["stream_id"] == "stream1"
        assert result["depth"] == 1
        assert result["fallback_used"] is False

    def test_batch_method_filter_streams_metadata_error(self):
        """
        Test batch_method_filter_streams when the batch call raises MetadataProcedureNotFoundError.
        
        This test configures the FakeTNAccessBlock to raise the error on batch init, simulating a metadata error.
        The function should handle this error gracefully and return None.
        """
        self.mock_block.reset()
        self.mock_block.set_deployed_streams({"stream1", "stream2", "stream3"})
        self.mock_block.set_initialized_streams({"stream1"})
        self.mock_block.set_error("batch_init", MetadataProcedureNotFoundError("Test error"))

        result = batch_method_filter_streams(self.mock_block, self.stream_locators, 1)

        # The result should be None, as the function should handle the error gracefully
        assert result is None

    def test_combine_divide_conquer_results(self):
        """Test combine_divide_conquer_results."""
        # Create two sample results
        left_result = DivideAndConquerResult(
            initialized_streams=convert_to_typed_df(
                pd.DataFrame({"data_provider": ["provider1"], "stream_id": ["stream1"]}), StreamLocatorModel
            ),
            uninitialized_streams=convert_to_typed_df(
                pd.DataFrame({"data_provider": ["provider2"], "stream_id": ["stream2"]}), StreamLocatorModel
            ),
            depth=1,
            fallback_used=False,
        )

        right_result = DivideAndConquerResult(
            initialized_streams=convert_to_typed_df(
                pd.DataFrame({"data_provider": ["provider3"], "stream_id": ["stream3"]}), StreamLocatorModel
            ),
            uninitialized_streams=convert_to_typed_df(
                pd.DataFrame({"data_provider": ["provider4"], "stream_id": ["stream4"]}), StreamLocatorModel
            ),
            depth=2,
            fallback_used=True,
        )

        # Call the function
        result = combine_divide_conquer_results(left_result, right_result)

        # Verify the result
        assert len(result["initialized_streams"]) == 2
        assert len(result["uninitialized_streams"]) == 2
        assert result["depth"] == 2  # max of left and right
        assert result["fallback_used"] is True  # OR of left and right

    @patch("tsn_adapters.blocks.stream_filtering.fallback_method_filter_streams")
    @patch("tsn_adapters.blocks.stream_filtering.batch_method_filter_streams")
    @patch("tsn_adapters.blocks.stream_filtering.task_filter_streams_divide_conquer")
    def test_task_filter_streams_divide_conquer_empty(
        self, mock_task: MagicMock, mock_batch: MagicMock, mock_fallback: MagicMock
    ):
        """Test task_filter_streams_divide_conquer with empty input."""
        # Create empty DataFrame
        empty_locators = convert_to_typed_df(pd.DataFrame(columns=["data_provider", "stream_id"]), StreamLocatorModel)

        # Call the function, pass a MagicMock logger if needed
        result = task_filter_streams_divide_conquer.fn(self.mock_block, empty_locators, 10, 0, False, 5000)

        # Verify the result
        assert result["initialized_streams"].empty
        assert result["uninitialized_streams"].empty
        assert result["depth"] == 0
        assert result["fallback_used"] is False

        # Verify none of the methods were called
        mock_batch.assert_not_called()
        mock_fallback.assert_not_called()
        mock_task.assert_not_called()

    @patch("tsn_adapters.blocks.stream_filtering.fallback_method_filter_streams")
    def test_task_filter_streams_divide_conquer_max_depth(self, mock_fallback: MagicMock):
        """Test task_filter_streams_divide_conquer with max depth reached."""
        # Mock fallback method
        mock_fallback.return_value = self.sample_result

        # Call the function, pass a MagicMock logger if needed
        result = task_filter_streams_divide_conquer.fn(self.mock_block, self.stream_locators, 10, 10, False, 5000)

        # Verify fallback was called
        mock_fallback.assert_called_once()

        # Verify the result
        assert result == self.sample_result

    @patch("tsn_adapters.blocks.stream_filtering.batch_method_filter_streams")
    def test_task_filter_streams_divide_conquer_batch_success(self, mock_batch: MagicMock):
        """Test task_filter_streams_divide_conquer when batch method succeeds."""
        # Mock batch method
        mock_batch.return_value = self.sample_result

        # Call the function, pass a MagicMock logger if needed
        result = task_filter_streams_divide_conquer.fn(self.mock_block, self.stream_locators, 10, 0, False, 5000)

        # Verify batch was called
        mock_batch.assert_called_once_with(self.mock_block, self.stream_locators, 0)

        # Verify the result
        assert result == self.sample_result

    @patch("tsn_adapters.blocks.stream_filtering.batch_method_filter_streams")
    @patch("tsn_adapters.blocks.stream_filtering.combine_divide_conquer_results")
    @pytest.mark.usefixtures("prefect_test_fixture")
    def test_task_filter_streams_divide_conquer_recursive(self, mock_combine: MagicMock, mock_batch: MagicMock):
        """Test task_filter_streams_divide_conquer with recursive calls.

        Uses a flow wrapper to ensure the task runs in a context where .submit() works.
        """
        # Mock batch method to return None (forcing divide and conquer)
        mock_batch.return_value = None

        # Mock combine method
        combined_result = self.sample_result
        mock_combine.return_value = combined_result

        # Mock the task.submit method and its future results
        left_future = MagicMock()
        left_future.result.return_value = self.sample_result

        right_future = MagicMock()
        right_future.result.return_value = self.sample_result

        # Call the function, pass a MagicMock logger if needed
        with patch.object(task_filter_streams_divide_conquer, "submit") as mock_submit:
            mock_submit.side_effect = [left_future, right_future]

            # Wrap the .fn call in a flow
            @flow
            def recursive_test_flow():
                return task_filter_streams_divide_conquer.fn(self.mock_block, self.stream_locators, 10, 0, False, 5000)
            
            result = recursive_test_flow()

            # Verify submit was called twice (for left and right halves)
            assert mock_submit.call_count == 2

            # Verify combine was called
            mock_combine.assert_called_once_with(self.sample_result, self.sample_result)

            # Verify the result
            assert result == combined_result


# --- Tests for task_get_stream_states_divide_conquer ---


@pytest.fixture
def test_filtering_fixture() -> TestStreamFiltering:
    """Fixture to provide an instance of TestStreamFiltering for setup."""
    fixture_instance = TestStreamFiltering()
    fixture_instance.setup_method()
    return fixture_instance


@pytest.mark.usefixtures("prefect_test_fixture")
def test_task_get_stream_states_empty_input(
    test_filtering_fixture: TestStreamFiltering, caplog: pytest.LogCaptureFixture
):
    """Test task_get_stream_states_divide_conquer with empty input DataFrame."""
    result = task_get_stream_states_divide_conquer(
        block=test_filtering_fixture.mock_block,
        stream_locators=test_filtering_fixture.empty_typed_df,
        current_depth=0,  # Explicitly set depth for clarity
    )
    assert result["non_deployed"].empty
    assert result["deployed_but_not_initialized"].empty
    assert result["deployed_and_initialized"].empty
    assert result["depth"] == 0
    assert not result["fallback_used"]
    assert "Input stream_locators is empty, returning empty result." in caplog.text


# Remove the patch decorator as we will control behavior via the fake block directly
# @patch("tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
def test_task_get_stream_states_fallback_single_non_deployed(
    # mock_batch_init: MagicMock,  <- Remove mock parameter
    test_filtering_fixture: TestStreamFiltering
):
    """Test fallback: single stream that does not exist."""
    # Configure the fake block: No streams are deployed
    test_filtering_fixture.mock_block.reset() # Ensure clean state
    test_filtering_fixture.mock_block.set_deployed_streams(set())
    single_stream_df = test_filtering_fixture.stream_locators.iloc[[0]]

    # Call the function directly, forcing fallback path via flag or letting it fall naturally
    result = task_get_stream_states_divide_conquer.fn( # Use .fn to bypass Prefect task runner for direct call
        block=test_filtering_fixture.mock_block,
        stream_locators=single_stream_df,
        force_fallback=True, # Keep forcing fallback for this specific test scenario
        current_depth=10,
    )

    # Assertions remain the same, mock_batch_init call check is removed
    # mock_batch_init.assert_not_called() # No mock to check

    assert len(result["non_deployed"]) == 1
    assert result["non_deployed"].iloc[0]["stream_id"] == "stream1"
    assert result["deployed_but_not_initialized"].empty
    assert result["deployed_and_initialized"].empty
    assert result["depth"] == 10
    assert result["fallback_used"]


# Remove patch
# @patch("tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
def test_task_get_stream_states_fallback_single_deployed_not_initialized(
    # mock_batch_init: MagicMock, <- Remove mock parameter
    test_filtering_fixture: TestStreamFiltering
):
    """Test fallback: single stream that exists but is not initialized."""
    # Configure fake block: stream1 exists but is not initialized
    test_filtering_fixture.mock_block.reset()
    test_filtering_fixture.mock_block.set_deployed_streams({"stream1"})
    test_filtering_fixture.mock_block.set_initialized_streams(set()) # Explicitly empty
    # mock_batch_init.return_value = test_filtering_fixture.empty_df  <- Remove mock setup
    single_stream_df = test_filtering_fixture.stream_locators.iloc[[0]]

    # Call function directly using .fn
    result = task_get_stream_states_divide_conquer.fn(
        block=test_filtering_fixture.mock_block,
        stream_locators=single_stream_df,
        force_fallback=True,  # Keep forcing fallback for this test
        current_depth=1,
    )

    # Remove mock assertion
    # mock_batch_init.assert_called_once()
    # pd.testing.assert_frame_equal(mock_batch_init.call_args[1]["stream_locators"], single_stream_df)

    # Verify results (should be the same)
    assert result["non_deployed"].empty
    assert len(result["deployed_but_not_initialized"]) == 1
    assert result["deployed_but_not_initialized"].iloc[0]["stream_id"] == "stream1"
    assert result["deployed_and_initialized"].empty
    assert result["depth"] == 1
    assert result["fallback_used"]


# Remove patch
# @patch("tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
def test_task_get_stream_states_fallback_single_deployed_and_initialized(
    # mock_batch_init: MagicMock, <- Remove mock parameter
    test_filtering_fixture: TestStreamFiltering
):
    """Test fallback: single stream that exists and is initialized."""
    # Configure fake block: stream1 deployed and initialized
    test_filtering_fixture.mock_block.reset()
    test_filtering_fixture.mock_block.set_deployed_streams({"stream1"})
    test_filtering_fixture.mock_block.set_initialized_streams({"stream1"})
    # Batch init returns the stream
    # initialized_df = pd.DataFrame([{"data_provider": "provider1", "stream_id": "stream1"}]) <- Remove mock setup
    # mock_batch_init.return_value = initialized_df <- Remove mock setup
    single_stream_df = test_filtering_fixture.stream_locators.iloc[[0]]

    # Call function directly using .fn
    result = task_get_stream_states_divide_conquer.fn(
        block=test_filtering_fixture.mock_block,
        stream_locators=single_stream_df,
        force_fallback=True, # Keep forcing fallback for this test
        current_depth=2,
    )

    # Remove mock assertions
    # mock_batch_init.assert_called_once()
    # pd.testing.assert_frame_equal(mock_batch_init.call_args[1]["stream_locators"], single_stream_df)

    # Verify results (should be the same)
    assert result["non_deployed"].empty
    assert result["deployed_but_not_initialized"].empty
    assert len(result["deployed_and_initialized"]) == 1
    assert result["deployed_and_initialized"].iloc[0]["stream_id"] == "stream1"
    assert result["depth"] == 2
    assert result["fallback_used"]


# Remove patch
# @patch("tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
@pytest.mark.usefixtures("prefect_test_fixture")
def test_task_get_stream_states_fallback_multiple_mixed_states(
    # mock_batch_init: MagicMock, <- Remove mock parameter
    test_filtering_fixture: TestStreamFiltering
):
    """Test fallback: multiple streams with various states."""
    # Configure fake block:
    # stream1: exists, initialized
    # stream2: does not exist
    # stream3: exists, not initialized
    test_filtering_fixture.mock_block.reset()
    test_filtering_fixture.mock_block.set_deployed_streams({"stream1", "stream3"})
    test_filtering_fixture.mock_block.set_initialized_streams({"stream1"})

    # Batch init is called only on existing (stream1, stream3)
    # It returns only stream1 as initialized
    # initialized_df = pd.DataFrame([{"data_provider": "provider1", "stream_id": "stream1"}]) <- Remove mock setup
    # mock_batch_init.return_value = initialized_df <- Remove mock setup

    # Call function directly using .fn
    result = task_get_stream_states_divide_conquer.fn(
        block=test_filtering_fixture.mock_block,
        stream_locators=test_filtering_fixture.stream_locators,  # Use all 3 streams
        force_fallback=True, # Keep forcing fallback for this test
        current_depth=3,
    )

    # Remove mock assertions
    # mock_batch_init.assert_called_once()
    # expected_batch_input_df = test_filtering_fixture.stream_locators.iloc[[0, 2]].reset_index(drop=True)
    # actual_batch_input_df = mock_batch_init.call_args[1]["stream_locators"].reset_index(drop=True)
    # pd.testing.assert_frame_equal(actual_batch_input_df, expected_batch_input_df)

    # Check results (should be the same)
    assert len(result["non_deployed"]) == 1
    assert result["non_deployed"].iloc[0]["stream_id"] == "stream2"

    assert len(result["deployed_but_not_initialized"]) == 1
    assert result["deployed_but_not_initialized"].iloc[0]["stream_id"] == "stream3"

    assert len(result["deployed_and_initialized"]) == 1
    assert result["deployed_and_initialized"].iloc[0]["stream_id"] == "stream1"

    assert result["depth"] == 3
    assert result["fallback_used"]


# Remove patch, configure fake block to raise error
# @patch("tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
@pytest.mark.usefixtures("prefect_test_fixture")
def test_task_get_stream_states_batch_metadata_error_falls_through(
    # mock_batch_init: MagicMock, <- Remove mock parameter
    test_filtering_fixture: TestStreamFiltering,
    caplog: pytest.LogCaptureFixture,
):
    """Test that batch attempt failure (MetadataProcedureNotFoundError) falls through.

    This test now uses the FakeTNAccessBlock's error injection.
    The function should attempt the batch call, catch the error, log it, and then
    proceed with the divide-and-conquer/fallback logic (which we don't explicitly
    mock or check the final state for here, focusing only on the error handling).
    """
    # Configure fake block to raise MetadataProcedureNotFoundError on batch call
    test_filtering_fixture.mock_block.reset()
    # Ensure streams exist so the call doesn't fail early for non-existence
    all_stream_ids = set(test_filtering_fixture.stream_locators['stream_id'])
    test_filtering_fixture.mock_block.set_deployed_streams(all_stream_ids)
    test_filtering_fixture.mock_block.set_error(
        "batch_init", MetadataProcedureNotFoundError("Fake Batch init failed")
    )
    # mock_batch_init.side_effect = MetadataProcedureNotFoundError("Batch init failed") <- Remove mock setup

    with caplog.at_level("DEBUG"):
        @flow
        def test_flow():
            # Note: We call the actual task function here, not .fn, to test Prefect integration
            task_get_stream_states_divide_conquer(
            block=test_filtering_fixture.mock_block,
            stream_locators=test_filtering_fixture.stream_locators,
            current_depth=0,
        )
        test_flow()

    # Assert that the batch method was attempted (implicitly, by checking the log)
    # The exact number of calls isn't critical here, just that the error path was hit.
    # assert mock_batch_init.call_count >= 1 <- Remove mock assertion
    assert "Batch method failed with MetadataProcedureNotFoundError" in caplog.text


# Remove patch, configure fake block to raise error for recursion test
# @patch("tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
@pytest.mark.usefixtures("prefect_test_fixture")
def test_task_get_stream_states_recursive(
    test_filtering_fixture: TestStreamFiltering,
    caplog: pytest.LogCaptureFixture,
):
    """Test the recursive divide-and-conquer path."""
    # Configure fake block to raise MetadataProcedureNotFoundError for recursion test
    test_filtering_fixture.mock_block.reset()
    test_filtering_fixture.mock_block.set_error(
        "batch_init", MetadataProcedureNotFoundError("Force recursion")
    )

    # Define what the combine function should return
    final_combined_result = StreamStatesResult(
        non_deployed=test_filtering_fixture.stream_locators.iloc[[1]],  # stream2
        deployed_but_not_initialized=test_filtering_fixture.stream_locators.iloc[[2]],  # stream3
        deployed_and_initialized=test_filtering_fixture.stream_locators.iloc[[0]],  # stream1
        depth=1,
        fallback_used=False,
    )

    # Patch the combine function and recursive calls as before
    with patch(
        "tsn_adapters.blocks.stream_filtering._combine_stream_states_results",
        return_value=final_combined_result,
    ) as mock_combine, patch(
        "tsn_adapters.blocks.stream_filtering.task_get_stream_states_divide_conquer",
        side_effect=[
            StreamStatesResult(
                non_deployed=test_filtering_fixture.empty_typed_df,
                deployed_but_not_initialized=test_filtering_fixture.empty_typed_df,
                deployed_and_initialized=test_filtering_fixture.stream_locators.iloc[[0]],
                depth=1,
                fallback_used=False,
            ),
            StreamStatesResult(
                non_deployed=test_filtering_fixture.stream_locators.iloc[[1]],
                deployed_but_not_initialized=test_filtering_fixture.stream_locators.iloc[[2]],
                deployed_and_initialized=test_filtering_fixture.empty_typed_df,
                depth=1,
                fallback_used=False,
            ),
        ],
    ) as mock_recursive_call:
        
        # Wrap the .fn call in a flow
        @flow
        def recursive_test_flow_states():
            return task_get_stream_states_divide_conquer.fn(
                block=test_filtering_fixture.mock_block,
                stream_locators=test_filtering_fixture.stream_locators,  # Use all 3 streams
                current_depth=0,
            )
        
        result = recursive_test_flow_states()

        # Assertions
        assert mock_recursive_call.call_count == 2  # Called for left and right halves
        mock_combine.assert_called_once()
        assert result == final_combined_result


# --- Integration-style test with real TestTNAccessBlock ---
@pytest.mark.usefixtures("prefect_test_fixture")
def test_task_get_stream_states_divide_conquer_integration():
    """Integration test: Use a real FakeTNAccessBlock to check stream state partitioning."""
    # Set up a fake TNAccessBlock with two existing streams
    block = FakeTNAccessBlock()
    block.set_deployed_streams({"stream1", "stream3"})
    # Prepare DataFrame with three streams
    stream_data = pd.DataFrame(
        {"data_provider": ["provider1", "provider1", "provider1"], "stream_id": ["stream1", "stream2", "stream3"]}
    )
    stream_locators = convert_to_typed_df(stream_data, StreamLocatorModel)

    # Call the actual task (no patching/mocking)
    result = task_get_stream_states_divide_conquer.fn(
        block=block,
        stream_locators=stream_locators,
        max_depth=5,
        current_depth=0,
        force_fallback=True,  # Use fallback for deterministic behavior
    )
    # stream1 and stream3 exist, stream2 does not
    # All existing streams are not initialized (FakeTNAccessBlock does not auto-initialize)
    assert set(result["non_deployed"]["stream_id"]) == {"stream2"}
    assert set(result["deployed_but_not_initialized"]["stream_id"]) == {"stream1", "stream3"}
    assert result["deployed_and_initialized"].empty
