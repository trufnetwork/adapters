"""
Integration tests for the Argentina SEPA Product Insertion Flow.
"""

# import asyncio # Removed unused import
from collections.abc import Generator
from datetime import datetime, timezone

# import json # Removed unused import
import os
from typing import Any
from unittest.mock import (
    ANY,
    AsyncMock,
    MagicMock,
    patch,
)

import boto3  # type: ignore
from moto import mock_aws
import pandas as pd
from pandera.typing import DataFrame
from prefect_aws import S3Bucket  # type: ignore
import pytest

# Import helpers
from tests.argentina.helpers import (
    make_metadata,
    upload_df_to_s3_csv_gz,
    upload_metadata_to_s3,
)

from tsn_adapters.blocks.primitive_source_descriptor import S3SourceDescriptor
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.tasks.argentina.flows.insert_products_flow import insert_argentina_products_flow
from tsn_adapters.tasks.argentina.models import ArgentinaProductStateMetadata, DynamicPrimitiveSourceModel
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.types import DateStr

# --- Constants ---
TEST_E2E_BUCKET_NAME = "test-e2e-insertion-bucket"
BASE_AGG_PATH = "aggregated"
BASE_PROC_PATH = "processed"
STATE_FILE_PATH = f"{BASE_AGG_PATH}/argentina_product_state.json"  # Matches spec and flow
DESCRIPTOR_FILE_PATH = f"{BASE_AGG_PATH}/argentina_products.csv.gz"  # Matches spec

# --- Fixtures for Mocked Blocks ---


@pytest.fixture
def mock_s3_block() -> MagicMock:
    """Fixture for a generic mocked S3Bucket block."""
    return MagicMock(spec=S3Bucket)


@pytest.fixture
def mock_tn_block() -> MagicMock:
    """Fixture for a mocked TNAccessBlock."""
    return MagicMock(spec=TNAccessBlock)


@pytest.fixture
def mock_descriptor_block() -> MagicMock:
    """Fixture for a mocked S3SourceDescriptor block."""
    mock = MagicMock(spec=S3SourceDescriptor)
    # Set the file_path attribute expected by the load task
    mock.file_path = DESCRIPTOR_FILE_PATH
    return mock


# --- Fixtures for Moto-based S3 Interaction ---


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def real_s3_bucket_block(aws_credentials: None, prefect_test_fixture: Any) -> Generator[S3Bucket, None, None]:
    """Creates a mock S3 bucket using moto and returns a real S3Bucket block instance."""
    _ = aws_credentials
    _ = prefect_test_fixture
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")  # type: ignore
        s3_client.create_bucket(Bucket=TEST_E2E_BUCKET_NAME)  # type: ignore
        s3_block = S3Bucket(bucket_name=TEST_E2E_BUCKET_NAME)
        # Attach the boto3 client for direct manipulation in tests
        s3_block._boto_client = s3_client  # type: ignore
        yield s3_block


# --- Sample Data Fixtures ---


@pytest.fixture
def sample_descriptor_df() -> DataFrame[DynamicPrimitiveSourceModel]:
    """Provides a sample descriptor DataFrame."""
    data = {
        "stream_id": ["stream-1", "stream-2"],
        "source_id": ["p1", "p2"],
        "source_type": ["arg_sepa_prod"] * 2,
        "productos_descripcion": ["Desc 1", "Desc 2"],
        "first_shown_at": ["2023-01-01"] * 2,
    }
    return DynamicPrimitiveSourceModel.validate(pd.DataFrame(data), lazy=True)


@pytest.fixture
def sample_daily_avg_df_date1() -> DataFrame[SepaAvgPriceProductModel]:
    """Sample daily averages for the first date."""
    data = {
        "id_producto": ["p1", "p2"],
        "productos_descripcion": ["Prod 1", "Prod 2"],
        "productos_precio_lista_avg": [10.5, 20.25],
        "date": ["2024-03-15"] * 2,
    }
    return SepaAvgPriceProductModel.validate(pd.DataFrame(data), lazy=True)


@pytest.fixture
def sample_transformed_df_date1() -> DataFrame[TnDataRowModel]:
    """Sample transformed data matching sample_daily_avg_df_date1."""
    # Timestamp for 2024-03-15 00:00:00 UTC
    ts = int(datetime(2024, 3, 15, 0, 0, 0, tzinfo=timezone.utc).timestamp())
    data = {
        "stream_id": ["stream-1", "stream-2"],
        "date": [ts, ts],
        "value": ["10.5", "20.25"],
        "data_provider": [None, None],  # Assuming default provider
    }
    return TnDataRowModel.validate(pd.DataFrame(data), lazy=True)


@pytest.fixture
def initial_metadata() -> ArgentinaProductStateMetadata:
    """Initial state metadata before the flow runs."""
    # Use the factory
    return make_metadata(
        last_aggregation_processed_date="2024-03-14",  # Example value
        last_product_deployment_date="2024-03-16",  # Allows processing 15th and 16th
        last_insertion_processed_date="2024-03-14",  # Process dates after this
        total_products_count=2,  # Example value
    )


# --- Helper Functions for Test Setup ---

# Removed upload_to_s3, upload_df_to_s3_csv_gz, upload_metadata_to_s3 - Use imported helpers


def setup_initial_s3_state(
    s3_block: S3Bucket,
    metadata: ArgentinaProductStateMetadata,
    descriptor_df: pd.DataFrame,
    daily_data: dict[DateStr, pd.DataFrame],  # Date string -> daily avg df
):
    """Sets up the initial S3 state for a test run."""
    # Upload initial metadata
    upload_metadata_to_s3(s3_block, STATE_FILE_PATH, metadata)

    # Upload descriptor
    upload_df_to_s3_csv_gz(s3_block, DESCRIPTOR_FILE_PATH, descriptor_df)

    # Upload daily average files
    provider = ProductAveragesProvider(s3_block=s3_block)  # Use real provider to get paths
    for date_str, df in daily_data.items():
        file_key = provider.to_product_averages_file_key(date_str)
        full_path = f"{provider.prefix}{file_key}"
        upload_df_to_s3_csv_gz(s3_block, full_path, df)


# --- Basic Integration Test Structure ---


@pytest.mark.asyncio
async def test_insert_flow_successful_run(
    prefect_test_fixture: Any,
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    initial_metadata: ArgentinaProductStateMetadata,  # Use fixture
    sample_descriptor_df: DataFrame[DynamicPrimitiveSourceModel],  # Use fixture
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],  # Use fixture
    sample_transformed_df_date1: DataFrame[TnDataRowModel],  # Use fixture
):
    """
    Test a successful run of the flow for a single date.
    Verifies task calls within the loop and state update.
    """
    # Arrange
    date_to_process = "2024-03-15"
    batch_size = 500
    original_metadata = initial_metadata.model_copy()  # Keep original for comparison

    # Patch the tasks called within the flow
    # Note: Patching the TN insertion task requires the full path including the module it's imported into.
    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_insertion_metadata", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_product_descriptor", new_callable=AsyncMock
        ) as mock_load_desc,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.determine_dates_to_insert", new_callable=AsyncMock
        ) as mock_determine_dates,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_daily_averages", new_callable=AsyncMock
        ) as mock_load_daily,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.transform_product_data", new_callable=AsyncMock
        ) as mock_transform,
        patch("tsn_adapters.tasks.argentina.flows.insert_products_flow.task_split_and_insert_records") as mock_insert,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.save_insertion_metadata", new_callable=AsyncMock
        ) as mock_save_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.create_markdown_artifact"
        ) as mock_create_artifact,
    ):

        # Configure mock return values for a successful single-date run
        mock_load_state.return_value = initial_metadata  # Start with initial state
        mock_load_desc.return_value = sample_descriptor_df
        mock_determine_dates.return_value = [date_to_process]
        mock_load_daily.return_value = sample_daily_avg_df_date1
        mock_transform.return_value = sample_transformed_df_date1

        # Act
        await insert_argentina_products_flow(
            s3_block=mock_s3_block,
            tn_block=mock_tn_block,
            descriptor_block=mock_descriptor_block,
            batch_size=batch_size,
        )

        # Assert
        # 1. Initial task calls
        mock_load_state.assert_called_once_with(s3_block=mock_s3_block, state_file_path=STATE_FILE_PATH)
        mock_load_desc.assert_called_once_with(descriptor_block=mock_descriptor_block)
        # Provider is instantiated inside the flow, need to check args passed to determine_dates
        # We can check the metadata argument
        mock_determine_dates.assert_called_once()
        assert mock_determine_dates.call_args[1]["metadata"] == initial_metadata

        # 2. Task calls within the loop (for the single date)
        mock_load_daily.assert_called_once()
        assert mock_load_daily.call_args[1]["date_str"] == date_to_process

        mock_transform.assert_called_once_with(
            daily_avg_df=sample_daily_avg_df_date1,
            descriptor_df=sample_descriptor_df,
            date_str=date_to_process,
            return_state=False,
        )

        mock_insert.assert_called_once_with(
            block=mock_tn_block, records=sample_transformed_df_date1, batch_size=batch_size
        )

        # 3. Final state save call
        mock_save_state.assert_called_once()
        # Check the metadata passed to save_state
        saved_metadata_call = mock_save_state.call_args[1]["metadata"]
        assert isinstance(saved_metadata_call, ArgentinaProductStateMetadata)
        # Verify only the insertion date was updated
        expected_saved_metadata = original_metadata.model_copy()
        expected_saved_metadata.last_insertion_processed_date = date_to_process
        assert saved_metadata_call == expected_saved_metadata
        assert mock_save_state.call_args[1]["s3_block"] == mock_s3_block
        assert mock_save_state.call_args[1]["state_file_path"] == STATE_FILE_PATH

        # 4. Reporting call
        mock_create_artifact.assert_called_once()
        # Optional: Check artifact content for basic details
        artifact_md = mock_create_artifact.call_args.kwargs.get("markdown", "")
        assert f"Processed Date Range:** {date_to_process} to {date_to_process}" in artifact_md
        assert "(1 dates)" in artifact_md
        assert f"Total Records Transformed & Submitted:** {len(sample_transformed_df_date1)}" in artifact_md


# --- Parametrization for Flow Tests (Example) ---

# Combine some basic run scenarios
# Define scenarios: (test_id, dates_to_return, expected_processed_dates, initial_metadata_override)
flow_run_scenarios = [
    (
        "single_date",
        ["2024-03-15"],
        ["2024-03-15"],
        make_metadata(last_insertion_processed_date="2024-03-14", last_product_deployment_date="2024-03-16"),
    ),
    (
        "two_dates",
        ["2024-03-15", "2024-03-16"],
        ["2024-03-15", "2024-03-16"],
        make_metadata(last_insertion_processed_date="2024-03-14", last_product_deployment_date="2024-03-16"),
    ),
    (
        "no_new_dates",
        [],  # determine_dates returns empty
        [],  # No dates should be processed
        make_metadata(last_insertion_processed_date="2024-03-16", last_product_deployment_date="2024-03-16"),
    ),
    (
        "deployment_restriction",
        ["2024-03-15"],  # Only 15th is within deployment window
        ["2024-03-15"],
        make_metadata(last_insertion_processed_date="2024-03-14", last_product_deployment_date="2024-03-15"),
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("test_id, dates_to_return, expected_processed_dates, initial_meta", flow_run_scenarios)
async def test_insert_flow_basic_scenarios(
    test_id: str,
    dates_to_return: list[DateStr],
    expected_processed_dates: list[DateStr],
    initial_meta: ArgentinaProductStateMetadata,
    prefect_test_fixture: Any,
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    sample_descriptor_df: DataFrame[DynamicPrimitiveSourceModel],
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],
    sample_transformed_df_date1: DataFrame[TnDataRowModel],
):
    """Test basic flow execution scenarios using parametrization."""
    # Arrange
    batch_size = 500
    # Use copies to avoid modification across parametrized runs
    current_metadata = initial_meta.model_copy()
    desc_df = sample_descriptor_df.copy()
    daily_avg_df = sample_daily_avg_df_date1.copy()  # Use same daily/transformed data for simplicity
    transformed_df = sample_transformed_df_date1.copy()

    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_insertion_metadata", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_product_descriptor", new_callable=AsyncMock
        ) as mock_load_desc,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.determine_dates_to_insert", new_callable=AsyncMock
        ) as mock_determine_dates,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_daily_averages", new_callable=AsyncMock
        ) as mock_load_daily,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.transform_product_data", new_callable=AsyncMock
        ) as mock_transform,
        patch("tsn_adapters.tasks.argentina.flows.insert_products_flow.task_split_and_insert_records") as mock_insert,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.save_insertion_metadata", new_callable=AsyncMock
        ) as mock_save_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.create_markdown_artifact"
        ) as mock_create_artifact,
    ):

        # Configure mocks based on scenario
        mock_load_state.return_value = current_metadata
        mock_load_desc.return_value = desc_df
        mock_determine_dates.return_value = dates_to_return
        # Simulate loading/transforming the same data for each date for simplicity
        mock_load_daily.return_value = daily_avg_df
        mock_transform.return_value = transformed_df

        # Act
        await insert_argentina_products_flow(
            s3_block=mock_s3_block,
            tn_block=mock_tn_block,
            descriptor_block=mock_descriptor_block,
            batch_size=batch_size,
        )

        # Assert
        # Initial calls
        mock_load_state.assert_called_once()
        mock_load_desc.assert_called_once()
        mock_determine_dates.assert_called_once()

        # Assert calls based on expected processed dates
        num_expected_calls = len(expected_processed_dates)
        assert mock_load_daily.call_count == num_expected_calls
        assert mock_transform.call_count == num_expected_calls
        assert mock_insert.call_count == num_expected_calls
        assert mock_save_state.call_count == num_expected_calls

        if num_expected_calls > 0:
            # Check args of the last call to save_state
            last_saved_metadata = mock_save_state.call_args_list[-1][1]["metadata"]
            assert last_saved_metadata.last_insertion_processed_date == expected_processed_dates[-1]
        else:
            mock_save_state.assert_not_called()

        # Reporting call (always called)
        mock_create_artifact.assert_called_once()
        artifact_md = mock_create_artifact.call_args.kwargs.get("markdown", "")
        if num_expected_calls > 0:
            assert f"Processed Date Range:** {expected_processed_dates[0]} to {expected_processed_dates[-1]}" in artifact_md
            assert f"({num_expected_calls} dates)" in artifact_md
            assert f"Total Records Transformed & Submitted:** {len(transformed_df) * num_expected_calls}" in artifact_md
        else:
            assert "No new dates found to process" in artifact_md


# --- Fatal Error Scenario Tests (Keep separate for clarity) ---


@pytest.mark.asyncio
async def test_insert_flow_fatal_error_load_state(
    prefect_test_fixture: Any,
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
):
    """Test flow fails if loading initial state fails."""
    # Arrange
    test_exception = OSError("S3 connection error during state load")
    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_insertion_metadata", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_product_descriptor", new_callable=AsyncMock
        ) as mock_load_desc,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.save_insertion_metadata", new_callable=AsyncMock
        ) as mock_save_state,
    ):

        mock_load_state.side_effect = test_exception

        # Act & Assert
        with pytest.raises(OSError) as exc_info:
            await insert_argentina_products_flow(
                s3_block=mock_s3_block, tn_block=mock_tn_block, descriptor_block=mock_descriptor_block
            )

        assert exc_info.value is test_exception
        mock_load_state.assert_called_once()
        mock_load_desc.assert_not_called()  # Should fail before loading descriptor
        mock_save_state.assert_not_called()  # State should not be saved


@pytest.mark.asyncio
async def test_insert_flow_fatal_error_load_descriptor(
    prefect_test_fixture: Any,
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    initial_metadata: ArgentinaProductStateMetadata,
):
    """Test flow fails if loading descriptor fails."""
    # Arrange
    from tsn_adapters.tasks.argentina.tasks.descriptor_tasks import DescriptorError  # Import specific error

    test_exception = DescriptorError("Failed to get descriptor")

    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_insertion_metadata", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_product_descriptor", new_callable=AsyncMock
        ) as mock_load_desc,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.determine_dates_to_insert", new_callable=AsyncMock
        ) as mock_determine_dates,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.save_insertion_metadata", new_callable=AsyncMock
        ) as mock_save_state,
    ):

        mock_load_state.return_value = initial_metadata
        mock_load_desc.side_effect = test_exception

        # Act & Assert
        with pytest.raises(DescriptorError) as exc_info:
            await insert_argentina_products_flow(
                s3_block=mock_s3_block, tn_block=mock_tn_block, descriptor_block=mock_descriptor_block
            )

        assert exc_info.value is test_exception
        mock_load_state.assert_called_once()
        mock_load_desc.assert_called_once()
        mock_determine_dates.assert_not_called()  # Should fail before determining dates
        mock_save_state.assert_not_called()


@pytest.mark.asyncio
async def test_insert_flow_fatal_error_load_daily(
    prefect_test_fixture: Any,
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    initial_metadata: ArgentinaProductStateMetadata,
    sample_descriptor_df: DataFrame[DynamicPrimitiveSourceModel],
):
    """Test flow fails if loading daily averages fails within the loop."""
    # Arrange
    from tsn_adapters.tasks.argentina.tasks.date_processing_tasks import (
        DailyAverageLoadingError,  # Import specific error
    )

    test_exception = DailyAverageLoadingError("Failed to load daily")
    date_to_fail = "2024-03-15"

    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_insertion_metadata", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_product_descriptor", new_callable=AsyncMock
        ) as mock_load_desc,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.determine_dates_to_insert", new_callable=AsyncMock
        ) as mock_determine_dates,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_daily_averages", new_callable=AsyncMock
        ) as mock_load_daily,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.transform_product_data", new_callable=AsyncMock
        ) as mock_transform,
        patch("tsn_adapters.tasks.argentina.flows.insert_products_flow.task_split_and_insert_records") as mock_insert,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.save_insertion_metadata", new_callable=AsyncMock
        ) as mock_save_state,
    ):

        mock_load_state.return_value = initial_metadata
        mock_load_desc.return_value = sample_descriptor_df
        mock_determine_dates.return_value = [date_to_fail]
        mock_load_daily.side_effect = test_exception

        # Act & Assert
        with pytest.raises(DailyAverageLoadingError) as exc_info:
            await insert_argentina_products_flow(
                s3_block=mock_s3_block, tn_block=mock_tn_block, descriptor_block=mock_descriptor_block
            )

        assert exc_info.value is test_exception
        mock_load_state.assert_called_once()
        mock_load_desc.assert_called_once()
        mock_determine_dates.assert_called_once()
        mock_load_daily.assert_called_once_with(
            provider=ANY, # Use ANY for the provider instance created in the flow
            date_str=date_to_fail
        )  # Check called once for the failing date
        mock_transform.assert_not_called()
        mock_insert.assert_not_called()
        mock_save_state.assert_not_called() # State should not be saved for the failed date


@pytest.mark.asyncio
async def test_insert_flow_fatal_error_transform(
    prefect_test_fixture: Any,
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    initial_metadata: ArgentinaProductStateMetadata,
    sample_descriptor_df: DataFrame[DynamicPrimitiveSourceModel],
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],
):
    """Test flow fails if transformation fails (e.g., mapping integrity)."""
    # Arrange
    from tsn_adapters.tasks.argentina.tasks.date_processing_tasks import MappingIntegrityError  # Import specific error

    test_exception = MappingIntegrityError("Missing product mapping")
    date_to_fail = "2024-03-15"

    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_insertion_metadata", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_product_descriptor", new_callable=AsyncMock
        ) as mock_load_desc,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.determine_dates_to_insert", new_callable=AsyncMock
        ) as mock_determine_dates,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_daily_averages", new_callable=AsyncMock
        ) as mock_load_daily,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.transform_product_data", new_callable=AsyncMock
        ) as mock_transform,
        patch("tsn_adapters.tasks.argentina.flows.insert_products_flow.task_split_and_insert_records") as mock_insert,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.save_insertion_metadata", new_callable=AsyncMock
        ) as mock_save_state,
    ):

        mock_load_state.return_value = initial_metadata
        mock_load_desc.return_value = sample_descriptor_df
        mock_determine_dates.return_value = [date_to_fail]
        mock_load_daily.return_value = sample_daily_avg_df_date1  # Return valid daily data
        mock_transform.side_effect = test_exception  # Simulate failure during transform

        # Act & Assert
        with pytest.raises(MappingIntegrityError) as exc_info:
            await insert_argentina_products_flow(
                s3_block=mock_s3_block, tn_block=mock_tn_block, descriptor_block=mock_descriptor_block
            )

        assert exc_info.value is test_exception
        mock_load_daily.assert_called_once()
        mock_transform.assert_called_once()
        mock_insert.assert_not_called()
        mock_save_state.assert_not_called()


@pytest.mark.asyncio
async def test_insert_flow_fatal_error_insert(
    prefect_test_fixture: Any,
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    initial_metadata: ArgentinaProductStateMetadata,
    sample_descriptor_df: DataFrame[DynamicPrimitiveSourceModel],
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],
    sample_transformed_df_date1: DataFrame[TnDataRowModel],
):
    """Test flow fails if TN insertion task fails."""
    # Arrange
    from prefect.exceptions import UpstreamTaskError  # Example exception type

    test_exception = UpstreamTaskError("TN insertion failed")
    date_to_fail = "2024-03-15"

    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_insertion_metadata", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_product_descriptor", new_callable=AsyncMock
        ) as mock_load_desc,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.determine_dates_to_insert", new_callable=AsyncMock
        ) as mock_determine_dates,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_daily_averages", new_callable=AsyncMock
        ) as mock_load_daily,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.transform_product_data", new_callable=AsyncMock
        ) as mock_transform,
        patch("tsn_adapters.tasks.argentina.flows.insert_products_flow.task_split_and_insert_records") as mock_insert,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.save_insertion_metadata", new_callable=AsyncMock
        ) as mock_save_state,
    ):

        mock_load_state.return_value = initial_metadata
        mock_load_desc.return_value = sample_descriptor_df
        mock_determine_dates.return_value = [date_to_fail]
        mock_load_daily.return_value = sample_daily_avg_df_date1
        mock_transform.return_value = sample_transformed_df_date1  # Transform succeeds
        mock_insert.side_effect = test_exception  # Insertion fails

        # Act & Assert
        with pytest.raises(UpstreamTaskError) as exc_info:
            await insert_argentina_products_flow(
                s3_block=mock_s3_block, tn_block=mock_tn_block, descriptor_block=mock_descriptor_block
            )

        assert exc_info.value is test_exception
        mock_load_daily.assert_called_once()
        mock_transform.assert_called_once()
        mock_insert.assert_called_once()
        mock_save_state.assert_not_called()  # State should not be saved


@pytest.mark.asyncio
async def test_insert_flow_fatal_error_save_state(
    prefect_test_fixture: Any,
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    initial_metadata: ArgentinaProductStateMetadata,
    sample_descriptor_df: DataFrame[DynamicPrimitiveSourceModel],
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],
    sample_transformed_df_date1: DataFrame[TnDataRowModel],  # Reused for simplicity
):
    """Test flow fails if saving state fails after successful insertion."""
    # Arrange
    test_exception = OSError("S3 connection error during state save")
    date_to_fail = "2024-03-15"

    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_insertion_metadata", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_product_descriptor", new_callable=AsyncMock
        ) as mock_load_desc,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.determine_dates_to_insert", new_callable=AsyncMock
        ) as mock_determine_dates,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_daily_averages", new_callable=AsyncMock
        ) as mock_load_daily,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.transform_product_data", new_callable=AsyncMock
        ) as mock_transform,
        patch("tsn_adapters.tasks.argentina.flows.insert_products_flow.task_split_and_insert_records") as mock_insert,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.save_insertion_metadata", new_callable=AsyncMock
        ) as mock_save_state,
    ):

        mock_load_state.return_value = initial_metadata
        mock_load_desc.return_value = sample_descriptor_df
        mock_determine_dates.return_value = [date_to_fail]
        mock_load_daily.return_value = sample_daily_avg_df_date1
        mock_transform.return_value = sample_transformed_df_date1
        mock_insert.return_value = None  # Insertion task succeeds (returns None or tx hash)
        mock_save_state.side_effect = test_exception  # Saving state fails

        # Act & Assert
        with pytest.raises(OSError) as exc_info:
            await insert_argentina_products_flow(
                s3_block=mock_s3_block, tn_block=mock_tn_block, descriptor_block=mock_descriptor_block
            )

        assert exc_info.value is test_exception
        mock_load_daily.assert_called_once()
        mock_transform.assert_called_once()
        mock_insert.assert_called_once()
        mock_save_state.assert_called_once()  # Save state was attempted


# --- Reporting Test (Keep separate) ---


@pytest.mark.asyncio
async def test_insert_flow_reporting(
    prefect_test_fixture: Any,
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    initial_metadata: ArgentinaProductStateMetadata,
    sample_descriptor_df: DataFrame[DynamicPrimitiveSourceModel],
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],
    sample_transformed_df_date1: DataFrame[TnDataRowModel],
):
    """Test that the summary artifact is created correctly."""
    # Arrange
    dates_to_process = ["2024-03-15", "2024-03-16"]
    total_records_per_date = len(sample_transformed_df_date1)
    expected_total_records = total_records_per_date * len(dates_to_process)

    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_insertion_metadata", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_product_descriptor", new_callable=AsyncMock
        ) as mock_load_desc,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.determine_dates_to_insert", new_callable=AsyncMock
        ) as mock_determine_dates,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.load_daily_averages", new_callable=AsyncMock
        ) as mock_load_daily,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.transform_product_data", new_callable=AsyncMock
        ) as mock_transform,
        patch("tsn_adapters.tasks.argentina.flows.insert_products_flow.task_split_and_insert_records") as mock_insert,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.save_insertion_metadata", new_callable=AsyncMock
        ) as mock_save_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.insert_products_flow.create_markdown_artifact"
        ) as mock_create_artifact,
    ):

        mock_load_state.return_value = initial_metadata
        mock_load_desc.return_value = sample_descriptor_df
        mock_determine_dates.return_value = dates_to_process
        # Simulate returning the same transformed data for both dates
        mock_load_daily.return_value = sample_daily_avg_df_date1
        mock_transform.return_value = sample_transformed_df_date1

        # Act
        await insert_argentina_products_flow(
            s3_block=mock_s3_block, tn_block=mock_tn_block, descriptor_block=mock_descriptor_block
        )

        # Assert
        mock_create_artifact.assert_called_once()
        artifact_md = mock_create_artifact.call_args.kwargs.get("markdown", "")
        assert f"Processed Date Range:** {dates_to_process[0]} to {dates_to_process[-1]}" in artifact_md
        assert f"({len(dates_to_process)} dates)" in artifact_md
        assert f"Total Records Transformed & Submitted:** {expected_total_records}" in artifact_md
