"""Unit tests for the stream filtering functionality."""

from unittest.mock import MagicMock, patch

import pandas as pd
from src.tsn_adapters.blocks.stream_filtering import (
    DivideAndConquerResult,
    batch_method_filter_streams,
    combine_divide_conquer_results,
    fallback_method_filter_streams,
    task_filter_streams_divide_conquer,
)
from src.tsn_adapters.blocks.tn_access import (
    MetadataProcedureNotFoundError,
    StreamLocatorModel,
    TNAccessBlock,
    convert_to_typed_df,
    create_empty_df,
)


class TestStreamFiltering:
    """Test cases for stream filtering functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_block = MagicMock(spec=TNAccessBlock)
        self.mock_logger = MagicMock()

        # Create test data
        self.stream_data = pd.DataFrame(
            {"data_provider": ["provider1", "provider2", "provider3"], "stream_id": ["stream1", "stream2", "stream3"]}
        )
        self.stream_locators = convert_to_typed_df(self.stream_data, StreamLocatorModel)

        # Sample results
        self.empty_df = create_empty_df(["data_provider", "stream_id"])
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

    @patch("src.tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
    def test_fallback_method_filter_streams(self, mock_batch_init: MagicMock):
        """Test fallback_method_filter_streams."""
        # Mock stream_exists on the block instance directly
        # Simulate first stream exists, second does not
        self.mock_block.stream_exists.side_effect = [True, False]

        # Mock the subsequent batch initialization check on the streams found to exist
        expected_df = pd.DataFrame([{"data_provider": "provider1", "stream_id": "stream1"}])
        mock_batch_init.return_value = expected_df

        # Call the function
        # Process only the first two locators for this test case
        result = fallback_method_filter_streams(self.mock_block, self.stream_locators.iloc[:2], 1, self.mock_logger)

        # Verify stream_exists was called twice
        assert self.mock_block.stream_exists.call_count == 2
        # Verify batch init was called once
        mock_batch_init.assert_called_once()

        # Check that the right streams were passed to task_filter_batch_initialized_streams
        # We don't need exact DataFrame comparison which can be tricky with conversions
        called_stream_locators = mock_batch_init.call_args[1]["stream_locators"]
        assert len(called_stream_locators) == 1
        assert called_stream_locators.iloc[0]["data_provider"] == "provider1"
        assert called_stream_locators.iloc[0]["stream_id"] == "stream1"

        # Verify the result
        assert not result["initialized_streams"].empty
        assert not result["uninitialized_streams"].empty
        assert result["initialized_streams"].iloc[0]["stream_id"] == "stream1"
        assert result["uninitialized_streams"].iloc[0]["stream_id"] == "stream2"  # Stream 2 didn't exist
        assert result["depth"] == 1
        assert result["fallback_used"] is True

    @patch("src.tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
    def test_batch_method_filter_streams_success(self, mock_batch_init: MagicMock):
        """Test batch_method_filter_streams when successful."""
        # Mock batch initialization check
        mock_batch_init.return_value = [{"data_provider": "provider1", "stream_id": "stream1"}]

        # Call the function
        result = batch_method_filter_streams(self.mock_block, self.stream_locators, 1, self.mock_logger)

        # Verify the result
        assert result is not None
        assert not result["initialized_streams"].empty
        assert not result["uninitialized_streams"].empty
        assert result["initialized_streams"].iloc[0]["stream_id"] == "stream1"
        assert result["depth"] == 1
        assert result["fallback_used"] is False

    @patch("src.tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
    def test_batch_method_filter_streams_metadata_error(self, mock_batch_init: MagicMock):
        """Test batch_method_filter_streams with metadata procedure not found error."""

        # We need to adjust how we mock this - the side_effect needs to be a function
        # that raises the exception to allow proper try/except behavior

        mock_batch_init.raiseError.side_effect = MetadataProcedureNotFoundError("Test error")

        # Call the function
        result = batch_method_filter_streams(self.mock_block, self.stream_locators, 1, self.mock_logger)

        # Verify the result is None, as the function should handle this error gracefully
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
        result = combine_divide_conquer_results(left_result, right_result, self.mock_logger)

        # Verify the result
        assert len(result["initialized_streams"]) == 2
        assert len(result["uninitialized_streams"]) == 2
        assert result["depth"] == 2  # max of left and right
        assert result["fallback_used"] is True  # OR of left and right

    @patch("src.tsn_adapters.blocks.stream_filtering.fallback_method_filter_streams")
    @patch("src.tsn_adapters.blocks.stream_filtering.batch_method_filter_streams")
    @patch("src.tsn_adapters.blocks.stream_filtering.task_filter_streams_divide_conquer")
    def test_task_filter_streams_divide_conquer_empty(
        self, mock_task: MagicMock, mock_batch: MagicMock, mock_fallback: MagicMock
    ):
        """Test task_filter_streams_divide_conquer with empty input."""
        # Create empty DataFrame
        empty_locators = convert_to_typed_df(pd.DataFrame(columns=["data_provider", "stream_id"]), StreamLocatorModel)

        # Call the function with get_run_logger mocked
        with patch("src.tsn_adapters.blocks.stream_filtering.get_run_logger") as mock_logger:
            mock_logger.return_value = self.mock_logger

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

    @patch("src.tsn_adapters.blocks.stream_filtering.fallback_method_filter_streams")
    def test_task_filter_streams_divide_conquer_max_depth(self, mock_fallback: MagicMock):
        """Test task_filter_streams_divide_conquer with max depth reached."""
        # Mock fallback method
        mock_fallback.return_value = self.sample_result

        # Call the function with get_run_logger mocked
        with patch("src.tsn_adapters.blocks.stream_filtering.get_run_logger") as mock_logger:
            mock_logger.return_value = self.mock_logger

            result = task_filter_streams_divide_conquer.fn(self.mock_block, self.stream_locators, 10, 10, False, 5000)

            # Verify fallback was called
            mock_fallback.assert_called_once_with(self.mock_block, self.stream_locators, 10, self.mock_logger)

            # Verify the result
            assert result == self.sample_result

    @patch("src.tsn_adapters.blocks.stream_filtering.batch_method_filter_streams")
    def test_task_filter_streams_divide_conquer_batch_success(self, mock_batch: MagicMock):
        """Test task_filter_streams_divide_conquer when batch method succeeds."""
        # Mock batch method
        mock_batch.return_value = self.sample_result

        # Call the function with get_run_logger mocked
        with patch("src.tsn_adapters.blocks.stream_filtering.get_run_logger") as mock_logger:
            mock_logger.return_value = self.mock_logger

            result = task_filter_streams_divide_conquer.fn(self.mock_block, self.stream_locators, 10, 0, False, 5000)

            # Verify batch was called
            mock_batch.assert_called_once_with(self.mock_block, self.stream_locators, 0, self.mock_logger)

            # Verify the result
            assert result == self.sample_result

    @patch("src.tsn_adapters.blocks.stream_filtering.batch_method_filter_streams")
    @patch("src.tsn_adapters.blocks.stream_filtering.combine_divide_conquer_results")
    def test_task_filter_streams_divide_conquer_recursive(self, mock_combine: MagicMock, mock_batch: MagicMock):
        """Test task_filter_streams_divide_conquer with recursive calls."""
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

        # Call the function with get_run_logger and task.submit mocked
        with patch("src.tsn_adapters.blocks.stream_filtering.get_run_logger") as mock_logger:
            mock_logger.return_value = self.mock_logger

            with patch.object(task_filter_streams_divide_conquer, "submit") as mock_submit:
                mock_submit.side_effect = [left_future, right_future]

                result = task_filter_streams_divide_conquer.fn(
                    self.mock_block, self.stream_locators, 10, 0, False, 5000
                )

                # Verify submit was called twice (for left and right halves)
                assert mock_submit.call_count == 2

                # Verify combine was called
                mock_combine.assert_called_once_with(self.sample_result, self.sample_result, self.mock_logger)

                # Verify the result
                assert result == combined_result
