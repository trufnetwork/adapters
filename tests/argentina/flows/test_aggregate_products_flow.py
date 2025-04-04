"""
Integration tests for the Argentina SEPA Product Aggregation Flow.
"""

import io
from typing import Any
from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
)

import pandas as pd
from prefect_aws import S3Bucket  # type: ignore
import pytest

from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    WritableSourceDescriptorBlock,
)
from tsn_adapters.tasks.argentina.config import ArgentinaFlowVariableNames
from tsn_adapters.tasks.argentina.flows.aggregate_products_flow import aggregate_argentina_products_flow
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.types import DateStr

# --- Constants for End-to-End Tests ---

BASE_AGG_PATH = "aggregated"  # Matches flow default
BASE_PROC_PATH = "processed"  # Matches provider default
METADATA_S3_PATH = f"{BASE_AGG_PATH}/argentina_products_metadata.json"
DATA_S3_PATH = f"{BASE_AGG_PATH}/argentina_products.csv.zip"


# Fixture for mock S3Bucket
@pytest.fixture
def mock_s3_block() -> MagicMock:
    return MagicMock(spec=S3Bucket)


# Fixture for mock ProductAveragesProvider
@pytest.fixture
def mock_provider() -> MagicMock:
    return MagicMock(spec=ProductAveragesProvider)


# Fixture for mock WritableSourceDescriptorBlock
@pytest.fixture
def mock_descriptor_block() -> MagicMock:
    mock = MagicMock(spec=WritableSourceDescriptorBlock)
    # Pre-configure methods expected to be called
    mock.get_descriptor = MagicMock()
    mock.upsert_sources = MagicMock()
    return mock


# Fixture for empty aggregated data DataFrame (using the correct model)
@pytest.fixture
def empty_aggregated_df() -> pd.DataFrame:
    columns = list(PrimitiveSourceDataModel.to_schema().columns.keys())
    df = pd.DataFrame(columns=columns)
    # Attempt setting dtypes on the empty DataFrame based on the model
    for col, props in PrimitiveSourceDataModel.to_schema().columns.items():
        if col in df.columns and props.dtype:
            try:
                df[col] = df[col].astype(props.dtype.type)  # type: ignore
            except Exception:
                df[col] = df[col].astype(object)  # type: ignore
    return df


# --- Fixtures for Moto-based End-to-End Tests ---

# Fixtures aws_credentials and s3_bucket_block moved to tests/argentina/conftest.py

# Sample Daily Data Fixtures
@pytest.fixture
def daily_data_2024_03_10() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "id_producto": ["P001", "P002"],
            "productos_descripcion": ["Product One", "Product Two"],
            "productos_precio_lista_avg": [10.0, 20.0],
            "date": ["2024-03-10", "2024-03-10"],
        }
    )
    return SepaAvgPriceProductModel.validate(df)


@pytest.fixture
def daily_data_2024_03_11() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "id_producto": ["P002", "P003"],  # P002 is existing
            "productos_descripcion": ["Product Two Updated", "Product Three"],  # Desc for P002 should be ignored
            "productos_precio_lista_avg": [21.0, 30.0],
            "date": ["2024-03-11", "2024-03-11"],
        }
    )
    return SepaAvgPriceProductModel.validate(df)


@pytest.fixture
def daily_data_2024_03_12() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "id_producto": ["P004", "P001"],  # P001 is existing
            "productos_descripcion": ["Product Four", "Product One Again"],  # Desc for P001 should be ignored
            "productos_precio_lista_avg": [40.0, 11.0],
            "date": ["2024-03-12", "2024-03-12"],
        }
    )
    return SepaAvgPriceProductModel.validate(df)


# Helper Function to Upload Sample Daily Data
def upload_sample_data(s3_block: S3Bucket, date_str: DateStr, data: pd.DataFrame):
    """Uploads daily data DataFrame to mock S3 in the correct format."""
    file_key = ProductAveragesProvider.to_product_averages_file_key(date_str)
    full_path = f"{BASE_PROC_PATH}/{file_key}"  # Manually construct full path

    buffer = io.BytesIO()
    data.to_csv(buffer, index=False, 
                compression="zip",
                encoding="utf-8")
    buffer.seek(0)
    s3_block._boto_client.put_object(  # type: ignore
        Bucket=s3_block.bucket_name, Key=full_path, Body=buffer.getvalue()
    )


# --- Fixtures for Mocked Flow Test ---
@pytest.fixture
def expected_df_day1() -> pd.DataFrame:
    """Static expected DataFrame after processing the first date in the mock test."""
    df = pd.DataFrame(
        {
            "stream_id": ["arg_sepa_prod_1"],
            "source_id": ["1"],
            "source_type": ["argentina_sepa_product"],
        }
    )
    return PrimitiveSourceDataModel.validate(df, lazy=True)


@pytest.fixture
def expected_df_day2(expected_df_day1: pd.DataFrame) -> pd.DataFrame:
    """Static expected DataFrame after processing the second date in the mock test."""
    df = pd.DataFrame(
        {
            "stream_id": ["arg_sepa_prod_1", "arg_sepa_prod_2"],
            "source_id": ["1", "2"],
            "source_type": ["argentina_sepa_product", "argentina_sepa_product"],
        }
    )
    # Re-validate to ensure schema compliance
    return PrimitiveSourceDataModel.validate(df, lazy=True)


# --- Mocked Flow Tests ---


# Updated integration test to include state saving and artifacts using static returns
@pytest.mark.asyncio
@pytest.mark.usefixtures("prefect_test_fixture")
async def test_aggregate_flow_with_state_and_artifacts_simplified_mock(
    mock_s3_block: MagicMock,
    mock_provider: MagicMock,
    mock_descriptor_block: MagicMock,
    empty_aggregated_df: pd.DataFrame,
    expected_df_day1: pd.DataFrame,
    expected_df_day2: pd.DataFrame,
):
    """
    Tests the aggregation flow with simplified mocking for process_single_date_products.

    Verifies orchestration logic (task calls, state saving, artifact creation)
    using pre-defined return values for the processing task.
    """
    dates_to_process = ["2024-01-01", "2024-01-02"]

    # Calculate the new products expected for day 2
    new_products_day2 = pd.concat([expected_df_day1, expected_df_day2]).drop_duplicates(keep=False)

    # Configure the mock for process_single_date_products to return NEW products per day
    mock_process_task = AsyncMock(
        # Return expected_df_day1 for the first call, and just the *new* ones for the second
        side_effect=[expected_df_day1, new_products_day2] 
    )

    # Mock all dependent tasks
    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.determine_aggregation_dates",
            return_value=(dates_to_process, "1970-01-01", "1970-01-01"),
        ) as mock_date_range_task,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.ProductAveragesProvider",
            return_value=mock_provider,
        ),
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.process_single_date_products", mock_process_task
        ),
        patch("tsn_adapters.tasks.argentina.flows.aggregate_products_flow.create_markdown_artifact") as mock_artifact,
    ):
        # Configure the mocked descriptor block methods
        mock_descriptor_block.get_descriptor.return_value = empty_aggregated_df

        # Run the flow, adding the descriptor_block argument
        await aggregate_argentina_products_flow(
            s3_block=mock_s3_block,
            descriptor_block=mock_descriptor_block,
            force_reprocess=False,
        )

        # --- Assertions ---

        # Verify initial tasks called once
        mock_descriptor_block.get_descriptor.assert_called_once()
        mock_date_range_task.assert_called_once()

        # Verify process_single_date_products was called for each date
        assert mock_process_task.call_count == len(dates_to_process)
        
        # Check first call parameters
        first_call_kwargs = mock_process_task.call_args_list[0].kwargs
        assert first_call_kwargs['date_to_process'] == "2024-01-01"
        assert first_call_kwargs['product_averages_provider'] == mock_provider
        pd.testing.assert_frame_equal(first_call_kwargs['current_aggregated_data'], empty_aggregated_df)
        
        # Check second call parameters
        second_call_kwargs = mock_process_task.call_args_list[1].kwargs
        assert second_call_kwargs['date_to_process'] == "2024-01-02"
        assert second_call_kwargs['product_averages_provider'] == mock_provider
        pd.testing.assert_frame_equal(second_call_kwargs['current_aggregated_data'], expected_df_day1)

        # Verify descriptor_block.upsert_sources calls within the loop
        # Upsert should be called with the *new* products returned by process_single_date_products
        assert mock_descriptor_block.upsert_sources.call_count == len(dates_to_process)
        # Check first upsert call arguments (should be expected_df_day1 as it contains only new products)
        first_upsert_call_args = mock_descriptor_block.upsert_sources.call_args_list[0].args[0]
        pd.testing.assert_frame_equal(first_upsert_call_args, expected_df_day1)

        # Check second upsert call arguments (should be only the *new* product from day 2)
        # Need to calculate the diff between day2 and day1 expected results - CALCULATION MOVED UP
        # new_products_day2 = pd.concat([expected_df_day1, expected_df_day2]).drop_duplicates(keep=False)
        second_upsert_call_args = mock_descriptor_block.upsert_sources.call_args_list[1].args[0]
        # Now the second upsert call should receive the correctly mocked *new* products for day 2
        pd.testing.assert_frame_equal(second_upsert_call_args, new_products_day2)

        # Check artifact call (happens after the loop)
        mock_artifact.assert_called_once()
        _, artifact_call_kwargs = mock_artifact.call_args  # Use _ for unused variable
        final_report_string = artifact_call_kwargs["markdown"]

        # Verify content of the final report artifact based on actual format
        assert f"({len(dates_to_process)} dates processed)" in final_report_string  # Check for (N dates)
        assert f"New Products Added:** {len(expected_df_day2)}" in final_report_string  # Check for New Products Added
        assert (
            f"Total Unique Products:** {len(expected_df_day2)}" in final_report_string
        )  # Check for Total Unique Products


@pytest.mark.usefixtures("prefect_test_fixture")
@pytest.mark.asyncio
async def test_aggregate_flow_no_dates_to_process_with_artifact(
    mock_s3_block: MagicMock,
    mock_provider: MagicMock,
    mock_descriptor_block: MagicMock,
    empty_aggregated_df: pd.DataFrame,
):
    """
    Tests the flow behavior when determine_aggregation_dates returns an empty list.
    """
    # --- Mock Setup ---
    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.ProductAveragesProvider",
            return_value=mock_provider,
        ) as mock_provider_init,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.determine_aggregation_dates"
        ) as mock_determine_range,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.process_single_date_products"
        ) as mock_process_single_date,
        # Mock artifact creation - patch where it's used in the flow module
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.create_markdown_artifact",
            new_callable=MagicMock,
        ) as mock_create_artifact,
    ):
        # mock_load_state.return_value = (empty_aggregated_df, default_metadata)
        mock_determine_range.return_value = ([], "1970-01-01", "1970-01-01")  # Return empty list in 3-tuple format

        # --- Flow Execution ---
        await aggregate_argentina_products_flow(
            s3_block=mock_s3_block,
            descriptor_block=mock_descriptor_block,
            force_reprocess=False,
        )

        # --- Assertions ---
        mock_provider_init.assert_called_once()
        mock_determine_range.assert_called_once()

        # Assert that the processing loop and save state were NOT entered
        mock_process_single_date.assert_not_called()

        # Assert that the artifact was created with the 'no dates' message
        mock_create_artifact.assert_called_once()
        assert mock_create_artifact.call_args is not None
        artifact_call_args = mock_create_artifact.call_args.kwargs
        assert artifact_call_args["key"] == "argentina-product-aggregation-summary"
        assert "No new dates found to process" in artifact_call_args["markdown"]

# --- Moto-based End-to-End Tests ---


@pytest.mark.usefixtures(
    "prefect_test_fixture", "aws_credentials", "show_prefect_logs_fixture"
)  # Ensure Prefect context and AWS creds
@pytest.mark.asyncio
async def test_aggregate_flow_end_to_end_cold_start(
    s3_bucket_block: S3Bucket,
    mock_descriptor_block: MagicMock,
    empty_aggregated_df: pd.DataFrame,
    daily_data_2024_03_10: pd.DataFrame,
    daily_data_2024_03_11: pd.DataFrame,
    daily_data_2024_03_12: pd.DataFrame,
):
    """
    Tests the flow end-to-end with moto, starting with no pre-existing state.
    """
    # Arrange: Upload sample daily data
    upload_sample_data(s3_bucket_block, DateStr("2024-03-10"), daily_data_2024_03_10)
    upload_sample_data(s3_bucket_block, DateStr("2024-03-11"), daily_data_2024_03_11)
    upload_sample_data(s3_bucket_block, DateStr("2024-03-12"), daily_data_2024_03_12)

    # Mock descriptor interactions for the E2E test
    mock_descriptor_block.get_descriptor.return_value = empty_aggregated_df

    # Mock Prefect Variables needed by determine_aggregation_dates
    with patch("tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks.variables.Variable.get") as mock_variable_get:
        
        def get_variable_side_effect(name: str, default: Any = None) -> str:
            if name == ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE:
                # Set a date that allows our test dates (2024-03-10 to 12) to be processed
                return "2024-03-12" 
            elif name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE:
                # For cold start, use the default (no previous aggregation)
                return ArgentinaFlowVariableNames.DEFAULT_DATE
            return default # Should not happen for these vars, but good practice
            
        mock_variable_get.side_effect = get_variable_side_effect

        # Act: Run the flow (first time)
        await aggregate_argentina_products_flow(
            s3_block=s3_bucket_block,
            descriptor_block=mock_descriptor_block,
            force_reprocess=False,
        )

    # --- Verify Final State (Verify MOCK upsert calls) ---
    # Verify the sequence of upsert calls
    assert mock_descriptor_block.upsert_sources.call_count == 3

    # Call 1 (Date 10): P001, P002
    upsert_call_1 = mock_descriptor_block.upsert_sources.call_args_list[0].args[0]
    assert isinstance(upsert_call_1, pd.DataFrame)
    assert set(upsert_call_1["source_id"]) == {"P001", "P002"}
    assert len(upsert_call_1) == 2

    # Call 2 (Date 11): P003 (P002 already exists)
    upsert_call_2 = mock_descriptor_block.upsert_sources.call_args_list[1].args[0]
    assert isinstance(upsert_call_2, pd.DataFrame)
    assert set(upsert_call_2["source_id"]) == {"P003"}
    assert len(upsert_call_2) == 1

    # Call 3 (Date 12): P004 (P001 already exists)
    upsert_call_3 = mock_descriptor_block.upsert_sources.call_args_list[2].args[0]
    assert isinstance(upsert_call_3, pd.DataFrame)
    assert set(upsert_call_3["source_id"]) == {"P004"}
    assert len(upsert_call_3) == 1

    # Verify final get_descriptor call (although not strictly necessary for this test logic)
    mock_descriptor_block.get_descriptor.assert_called_once()


@pytest.mark.usefixtures("prefect_test_fixture", "aws_credentials")
@pytest.mark.asyncio
async def test_aggregate_flow_end_to_end_resume(
    s3_bucket_block: S3Bucket,
    mock_descriptor_block: MagicMock,
    daily_data_2024_03_10: pd.DataFrame,
    daily_data_2024_03_11: pd.DataFrame,
    daily_data_2024_03_12: pd.DataFrame,
):
    """Tests resuming aggregation from a pre-existing state."""
    # --- Setup Initial State (Only in Mock Descriptor) ---
    initial_products = [
        {
            "stream_id": "arg_sepa_prod_P001",
            "source_id": "P001",
            "source_type": "argentina_sepa_product",
        },
        {
            "stream_id": "arg_sepa_prod_P002",
            "source_id": "P002",
            "source_type": "argentina_sepa_product",
        },
    ]
    initial_data_df = pd.DataFrame(initial_products)
    # Validate initial state before mocking
    initial_data_df = PrimitiveSourceDataModel.validate(
        initial_data_df[list(PrimitiveSourceDataModel.to_schema().columns.keys())], lazy=True
    )

    # Mock the initial descriptor state
    mock_descriptor_block.get_descriptor.return_value = initial_data_df

    # Upload only the data for dates *after* the initial state date used in gating (e.g., 2024-03-10)
    upload_sample_data(s3_bucket_block, DateStr("2024-03-11"), daily_data_2024_03_11)
    upload_sample_data(s3_bucket_block, DateStr("2024-03-12"), daily_data_2024_03_12)

    # Mock Prefect Variables for gating (assuming resume after 2024-03-10)
    with patch("prefect.variables.Variable.get") as mock_variable_get:
        # Configure side effect for different variable names
        def get_side_effect(name: str, default: Any = None) -> Any:
            if name == ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE:
                return "2024-03-12"  # Allow processing up to 12th
            if name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE:
                return "2024-03-10"  # Resume after 10th
            return default

        mock_variable_get.side_effect = get_side_effect

        # Act: Run the flow (should resume from 2024-03-11)
        await aggregate_argentina_products_flow(
            s3_block=s3_bucket_block,
            descriptor_block=mock_descriptor_block,
            force_reprocess=False,
        )

    # --- Verify Final State (Verify MOCK upsert calls) ---
    # Expected state includes initial products + new ones from 11th and 12th
    # Verify the sequence of upsert calls (should process 11th and 12th)
    assert mock_descriptor_block.upsert_sources.call_count == 2

    # Call 1 (Date 11): P003 (P002 already exists)
    upsert_call_1 = mock_descriptor_block.upsert_sources.call_args_list[0].args[0]
    assert isinstance(upsert_call_1, pd.DataFrame)
    assert set(upsert_call_1["source_id"]) == {"P003"}
    assert len(upsert_call_1) == 1

    # Call 2 (Date 12): P004 (P001 already exists)
    upsert_call_2 = mock_descriptor_block.upsert_sources.call_args_list[1].args[0]
    assert isinstance(upsert_call_2, pd.DataFrame)
    assert set(upsert_call_2["source_id"]) == {"P004"}
    assert len(upsert_call_2) == 1

    # Verify initial descriptor load was called
    mock_descriptor_block.get_descriptor.assert_called_once()


@pytest.mark.usefixtures("prefect_test_fixture", "aws_credentials")
@pytest.mark.asyncio
async def test_aggregate_flow_end_to_end_force_reprocess(
    s3_bucket_block: S3Bucket,
    mock_descriptor_block: MagicMock,
    daily_data_2024_03_10: pd.DataFrame,
    daily_data_2024_03_11: pd.DataFrame,
    daily_data_2024_03_12: pd.DataFrame,
):
    """
    Tests the flow end-to-end with moto, with force_reprocess=True, ignoring initial state date.
    """
    # Simulate existing state in descriptor that will be overwritten/ignored by reprocessing
    initial_products = [
        {
            "stream_id": "arg_sepa_prod_OLD",
            "source_id": "OLD",
            "source_type": "argentina_sepa_product",
        }
    ]
    initial_data_df = pd.DataFrame(initial_products)
    initial_data_df = PrimitiveSourceDataModel.validate(
        initial_data_df[list(PrimitiveSourceDataModel.to_schema().columns.keys())], lazy=True
    )

    # Mock the initial descriptor state
    mock_descriptor_block.get_descriptor.return_value = initial_data_df

    # Upload all daily data, including the date covered by "initial state" date (10th)
    upload_sample_data(s3_bucket_block, DateStr("2024-03-10"), daily_data_2024_03_10)
    upload_sample_data(s3_bucket_block, DateStr("2024-03-11"), daily_data_2024_03_11)
    upload_sample_data(s3_bucket_block, DateStr("2024-03-12"), daily_data_2024_03_12)

    # Mock Prefect Variables for gating (force_reprocess=True means LAST_AGGREGATION not fetched)
    with patch("prefect.variables.Variable.get") as mock_variable_get:
        # Only need to mock PREPROCESS date
        def get_side_effect(name: str, default: Any = None) -> str | None:
            if name == ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE:
                return "2024-03-12"  # Allow processing up to 12th
            return default

        mock_variable_get.side_effect = get_side_effect

        # Act: Run the flow with force_reprocess=True
        await aggregate_argentina_products_flow(
            s3_block=s3_bucket_block,
            descriptor_block=mock_descriptor_block,
            force_reprocess=True,
        )

    # --- Verify Final State (Verify MOCK upsert calls) ---
    # Expected state is the same as cold start, should process 10, 11, 12
    assert mock_descriptor_block.upsert_sources.call_count == 3

    # Call 1 (Date 10): P001, P002
    upsert_call_1 = mock_descriptor_block.upsert_sources.call_args_list[0].args[0]
    assert isinstance(upsert_call_1, pd.DataFrame)
    assert set(upsert_call_1["source_id"]) == {"P001", "P002"}
    assert len(upsert_call_1) == 2

    # Call 2 (Date 11): P003
    upsert_call_2 = mock_descriptor_block.upsert_sources.call_args_list[1].args[0]
    assert isinstance(upsert_call_2, pd.DataFrame)
    assert set(upsert_call_2["source_id"]) == {"P003"}
    assert len(upsert_call_2) == 1

    # Call 3 (Date 12): P004
    upsert_call_3 = mock_descriptor_block.upsert_sources.call_args_list[2].args[0]
    assert isinstance(upsert_call_3, pd.DataFrame)
    assert set(upsert_call_3["source_id"]) == {"P004"}
    assert len(upsert_call_3) == 1

    # Verify initial descriptor load was called
    mock_descriptor_block.get_descriptor.assert_called_once()


