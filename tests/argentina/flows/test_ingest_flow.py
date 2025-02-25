"""Tests for the Argentina SEPA ingestion flow."""

import pandas as pd
import pytest
import logging
from unittest.mock import MagicMock, patch

from tsn_adapters.tasks.argentina.flows.ingest_flow import IngestFlow
from tsn_adapters.tasks.argentina.models.stream_source import StreamSourceMetadataModel
from tsn_adapters.tasks.argentina.models.aggregated_prices import SepaAggregatedPricesModel
from pandera.typing import DataFrame
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel


@pytest.fixture
def mock_s3_bucket():
    """Mock S3 bucket."""
    return MagicMock()


@pytest.fixture
def mock_target_client():
    """Mock target client."""
    client = MagicMock()
    return client


@pytest.fixture
def mock_fetcher():
    """Mock stream fetcher."""
    fetcher = MagicMock()
    # Create a sample streams DataFrame
    streams_df = pd.DataFrame({
        "stream_id": ["stream1", "stream2"],
        "source_id": ["source1", "source2"],
        "description": ["Stream 1", "Stream 2"],
    })
    fetcher.fetch_streams.return_value = DataFrame[StreamSourceMetadataModel](streams_df)
    return fetcher


@pytest.fixture
def mock_provider_getter():
    """Mock provider getter with empty data."""
    provider = MagicMock()
    # Return empty DataFrame when get_data_for is called
    provider.get_data_for.return_value = DataFrame[SepaAggregatedPricesModel](
        pd.DataFrame(columns=["category_id", "avg_price", "date"])
    )
    return provider


@pytest.fixture
def mock_transformer():
    """Mock transformer returning empty data."""
    transformer = MagicMock()
    # Return empty DataFrame when transform is called
    transformer.transform.return_value = DataFrame[TnDataRowModel](
        pd.DataFrame(columns=["date", "value", "stream_id", "data_provider"])
    )
    return transformer


@pytest.fixture
def mock_recon_strategy():
    """Mock reconciliation strategy."""
    strategy = MagicMock()
    # Return mock needed keys
    strategy.determine_needed_keys.return_value = {
        "stream1": ["2023-01-01", "2023-01-02"],
        "stream2": ["2023-01-01"],
    }
    return strategy


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    logger = MagicMock(spec=logging.Logger)
    return logger


class TestIngestFlow:
    """Test cases for the ingest flow."""

    def test_empty_data_handling(
        self,
        mock_s3_bucket: MagicMock,
        mock_target_client: MagicMock,
        mock_fetcher: MagicMock,
        mock_provider_getter: MagicMock,
        mock_transformer: MagicMock,
        mock_recon_strategy: MagicMock,
        mock_logger: MagicMock,
    ):
        """Test that the flow handles empty data without errors."""
        # Patch the get_run_logger to avoid MissingContextError
        with patch("tsn_adapters.tasks.argentina.flows.base.get_run_logger", return_value=mock_logger), \
             patch("tsn_adapters.tasks.argentina.flows.ingest_flow.task_create_stream_fetcher", return_value=mock_fetcher), \
             patch("tsn_adapters.tasks.argentina.flows.ingest_flow.task_get_streams", return_value=mock_fetcher.fetch_streams()), \
             patch("tsn_adapters.tasks.argentina.flows.ingest_flow.create_trufnetwork_components", return_value=mock_target_client), \
             patch("tsn_adapters.tasks.argentina.flows.ingest_flow.task_create_reconciliation_strategy", return_value=mock_recon_strategy), \
             patch("tsn_adapters.tasks.argentina.flows.ingest_flow.task_create_transformer", return_value=mock_transformer), \
             patch("tsn_adapters.tasks.argentina.flows.ingest_flow.task_determine_needed_keys") as mock_determine_needed_keys, \
             patch("tsn_adapters.tasks.argentina.flows.ingest_flow.task_transform_data", return_value=mock_transformer.transform(None)), \
             patch("tsn_adapters.tasks.argentina.flows.ingest_flow.task_insert_data.submit") as submit_mock, \
             patch("tsn_adapters.tasks.argentina.flows.ingest_flow.transactions.transaction"), \
             patch("tsn_adapters.tasks.argentina.flows.ingest_flow.create_markdown_artifact"):
            
            # Create flow instance with mock objects
            flow = IngestFlow(
                source_descriptor_type="test",
                source_descriptor_block_name="test",
                trufnetwork_access_block_name="test",
                s3_block=mock_s3_bucket,
                data_provider="test",
            )
            
            # Mock internal components to return our mock objects
            flow.processed_provider = mock_provider_getter
            
            # Mock the return value for determine_needed_keys
            mock_determine_needed_keys.return_value.result.return_value = mock_recon_strategy.determine_needed_keys()
            
            # Run the flow - this should not raise any exceptions
            flow.run()
            
            # Verify the calls
            # Ensure insert_data.submit was called
            submit_mock.assert_called_once()
            
            # Get the arguments passed to submit
            _, kwargs = submit_mock.call_args
            
            # Verify that the data passed has the correct structure
            assert "data" in kwargs, "The 'data' parameter was not passed to task_insert_data.submit"
            data_df = kwargs["data"]
            
            # Check that the DataFrame has the required columns, even if empty
            required_columns = ["date", "value", "stream_id", "data_provider"]
            for column in required_columns:
                assert column in data_df.columns, f"Required column '{column}' is missing from the DataFrame"
            
            # Verify the DataFrame is empty but properly formatted (our fix ensures this works)
            assert len(data_df) == 0, "The DataFrame should be empty for this test case" 