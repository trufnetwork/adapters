"""Unit tests for the stream filtering functionality."""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from src.tsn_adapters.blocks.stream_filtering import (
    check_single_stream_existence,
    fallback_method_filter_streams,
    batch_method_filter_streams,
    combine_divide_conquer_results,
    task_filter_streams_divide_conquer,
    DivideAndConquerResult,
)
from src.tsn_adapters.blocks.tn_access import (
    TNAccessBlock,
    create_empty_df,
    convert_to_typed_df,
    StreamLocatorModel,
)


class TestStreamFiltering:
    """Test cases for stream filtering functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_block = MagicMock(spec=TNAccessBlock)
        self.mock_logger = MagicMock()
        
        # Create test data
        self.stream_data = pd.DataFrame({
            "data_provider": ["provider1", "provider2", "provider3"],
            "stream_id": ["stream1", "stream2", "stream3"]
        })
        self.stream_locators = convert_to_typed_df(self.stream_data, StreamLocatorModel)
        
        # Sample results
        self.empty_df = create_empty_df(["data_provider", "stream_id"])
        self.sample_result = DivideAndConquerResult(
            initialized_streams=convert_to_typed_df(
                pd.DataFrame({"data_provider": ["provider1"], "stream_id": ["stream1"]}),
                StreamLocatorModel
            ),
            uninitialized_streams=convert_to_typed_df(
                pd.DataFrame({"data_provider": ["provider2"], "stream_id": ["stream2"]}),
                StreamLocatorModel
            ),
            depth=1,
            fallback_used=False
        )

    def test_check_single_stream_existence_exists(self):
        """Test check_single_stream_existence when stream exists."""
        self.mock_block.stream_exists.return_value = True
        row = self.stream_data.iloc[0]
        
        existent, non_existent = check_single_stream_existence(
            self.mock_block, row, self.mock_logger
        )
        
        self.mock_block.stream_exists.assert_called_once()
        assert not existent.empty
        assert existent.iloc[0]["stream_id"] == "stream1"
        assert non_existent.empty

    def test_check_single_stream_existence_not_exists(self):
        """Test check_single_stream_existence when stream doesn't exist."""
        self.mock_block.stream_exists.return_value = False
        row = self.stream_data.iloc[0]
        
        existent, non_existent = check_single_stream_existence(
            self.mock_block, row, self.mock_logger
        )
        
        self.mock_block.stream_exists.assert_called_once()
        assert existent.empty
        assert not non_existent.empty
        assert non_existent.iloc[0]["stream_id"] == "stream1"

    def test_check_single_stream_existence_error(self):
        """Test check_single_stream_existence when an error occurs."""
        self.mock_block.stream_exists.side_effect = Exception("Test error")
        row = self.stream_data.iloc[0]
        
        existent, non_existent = check_single_stream_existence(
            self.mock_block, row, self.mock_logger
        )
        
        self.mock_block.stream_exists.assert_called_once()
        assert existent.empty
        assert not non_existent.empty
        assert non_existent.iloc[0]["stream_id"] == "stream1"
        self.mock_logger.warning.assert_called_once()
        self.mock_logger.debug.assert_called_once()

    @patch("src.tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
    def test_fallback_method_filter_streams(self, mock_batch_init):
        """Test fallback_method_filter_streams."""
        # Setup mock for check_single_stream_existence
        with patch("src.tsn_adapters.blocks.stream_filtering.check_single_stream_existence") as mock_check:
            # First stream exists, second doesn't
            mock_check.side_effect = [
                (pd.DataFrame({"data_provider": ["provider1"], "stream_id": ["stream1"]}), self.empty_df),
                (self.empty_df, pd.DataFrame({"data_provider": ["provider2"], "stream_id": ["stream2"]}))
            ]
            
            # Mock batch initialization check
            mock_batch_init.return_value = [{"data_provider": "provider1", "stream_id": "stream1"}]
            
            # Call the function
            result = fallback_method_filter_streams(
                self.mock_block, 
                self.stream_locators.iloc[:2], 
                1,
                self.mock_logger
            )
            
            # Verify the result
            assert mock_check.call_count == 2
            assert not result["initialized_streams"].empty
            assert not result["uninitialized_streams"].empty
            assert result["initialized_streams"].iloc[0]["stream_id"] == "stream1"
            assert result["uninitialized_streams"].iloc[0]["stream_id"] == "stream2"
            assert result["depth"] == 1
            assert result["fallback_used"] is True

    @patch("src.tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
    def test_batch_method_filter_streams_success(self, mock_batch_init):
        """Test batch_method_filter_streams when successful."""
        # Mock batch initialization check
        mock_batch_init.return_value = [{"data_provider": "provider1", "stream_id": "stream1"}]
        
        # Call the function
        result = batch_method_filter_streams(
            self.mock_block, 
            self.stream_locators, 
            1,
            self.mock_logger
        )
        
        # Verify the result
        assert result is not None
        assert not result["initialized_streams"].empty
        assert not result["uninitialized_streams"].empty
        assert result["initialized_streams"].iloc[0]["stream_id"] == "stream1"
        assert result["depth"] == 1
        assert result["fallback_used"] is False

    @patch("src.tsn_adapters.blocks.stream_filtering.task_filter_batch_initialized_streams")
    def test_batch_method_filter_streams_metadata_error(self, mock_batch_init):
        """Test batch_method_filter_streams with metadata procedure not found error."""
        from src.tsn_adapters.blocks.tn_access import MetadataProcedureNotFoundError
        
        # Mock batch initialization check to raise error
        mock_batch_init.side_effect = MetadataProcedureNotFoundError("Test error")
        
        # Call the function
        result = batch_method_filter_streams(
            self.mock_block, 
            self.stream_locators, 
            1,
            self.mock_logger
        )
        
        # Verify the result
        assert result is None

    def test_combine_divide_conquer_results(self):
        """Test combine_divide_conquer_results."""
        # Create two sample results
        left_result = DivideAndConquerResult(
            initialized_streams=convert_to_typed_df(
                pd.DataFrame({"data_provider": ["provider1"], "stream_id": ["stream1"]}),
                StreamLocatorModel
            ),
            uninitialized_streams=convert_to_typed_df(
                pd.DataFrame({"data_provider": ["provider2"], "stream_id": ["stream2"]}),
                StreamLocatorModel
            ),
            depth=1,
            fallback_used=False
        )
        
        right_result = DivideAndConquerResult(
            initialized_streams=convert_to_typed_df(
                pd.DataFrame({"data_provider": ["provider3"], "stream_id": ["stream3"]}),
                StreamLocatorModel
            ),
            uninitialized_streams=convert_to_typed_df(
                pd.DataFrame({"data_provider": ["provider4"], "stream_id": ["stream4"]}),
                StreamLocatorModel
            ),
            depth=2,
            fallback_used=True
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
        self, mock_task, mock_batch, mock_fallback
    ):
        """Test task_filter_streams_divide_conquer with empty input."""
        # Create empty DataFrame
        empty_locators = convert_to_typed_df(
            pd.DataFrame(columns=["data_provider", "stream_id"]), 
            StreamLocatorModel
        )
        
        # Call the function with get_run_logger mocked
        with patch("src.tsn_adapters.blocks.stream_filtering.get_run_logger") as mock_logger:
            mock_logger.return_value = self.mock_logger
            
            result = task_filter_streams_divide_conquer.fn(
                self.mock_block, empty_locators, 10, 0, False, 5000
            )
            
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
    def test_task_filter_streams_divide_conquer_max_depth(self, mock_fallback):
        """Test task_filter_streams_divide_conquer with max depth reached."""
        # Mock fallback method
        mock_fallback.return_value = self.sample_result
        
        # Call the function with get_run_logger mocked
        with patch("src.tsn_adapters.blocks.stream_filtering.get_run_logger") as mock_logger:
            mock_logger.return_value = self.mock_logger
            
            result = task_filter_streams_divide_conquer.fn(
                self.mock_block, self.stream_locators, 10, 10, False, 5000
            )
            
            # Verify fallback was called
            mock_fallback.assert_called_once_with(
                self.mock_block, self.stream_locators, 10, self.mock_logger
            )
            
            # Verify the result
            assert result == self.sample_result

    @patch("src.tsn_adapters.blocks.stream_filtering.batch_method_filter_streams")
    def test_task_filter_streams_divide_conquer_batch_success(self, mock_batch):
        """Test task_filter_streams_divide_conquer when batch method succeeds."""
        # Mock batch method
        mock_batch.return_value = self.sample_result
        
        # Call the function with get_run_logger mocked
        with patch("src.tsn_adapters.blocks.stream_filtering.get_run_logger") as mock_logger:
            mock_logger.return_value = self.mock_logger
            
            result = task_filter_streams_divide_conquer.fn(
                self.mock_block, self.stream_locators, 10, 0, False, 5000
            )
            
            # Verify batch was called
            mock_batch.assert_called_once_with(
                self.mock_block, self.stream_locators, 0, self.mock_logger
            )
            
            # Verify the result
            assert result == self.sample_result

    @patch("src.tsn_adapters.blocks.stream_filtering.batch_method_filter_streams")
    @patch("src.tsn_adapters.blocks.stream_filtering.combine_divide_conquer_results")
    def test_task_filter_streams_divide_conquer_recursive(self, mock_combine, mock_batch):
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
                mock_combine.assert_called_once_with(
                    self.sample_result, self.sample_result, self.mock_logger
                )
                
                # Verify the result
                assert result == combined_result 