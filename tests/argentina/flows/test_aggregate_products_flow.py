"""
Integration tests for the Argentina SEPA Product Aggregation Flow.
"""

import asyncio
from collections.abc import Generator
import gzip
import io
import json
import os
from typing import Any
from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
)

import boto3  # type: ignore
from moto import mock_aws
import pandas as pd
from prefect_aws import S3Bucket  # type: ignore
import pytest

from tsn_adapters.tasks.argentina.flows.aggregate_products_flow import aggregate_argentina_products_flow
from tsn_adapters.tasks.argentina.models import DynamicPrimitiveSourceModel, ArgentinaProductStateMetadata
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks import (
    _generate_argentina_product_stream_id,  # type: ignore
)
from tsn_adapters.tasks.argentina.types import DateStr

# --- Constants for End-to-End Tests ---
TEST_E2E_BUCKET_NAME = "test-e2e-aggregation-bucket"
BASE_AGG_PATH = "aggregated"  # Matches flow default
BASE_PROC_PATH = "processed"  # Matches provider default
METADATA_S3_PATH = f"{BASE_AGG_PATH}/argentina_products_metadata.json"
DATA_S3_PATH = f"{BASE_AGG_PATH}/argentina_products.csv.gz"


# Fixture for mock S3Bucket
@pytest.fixture
def mock_s3_block() -> MagicMock:
    return MagicMock(spec=S3Bucket)


# Fixture for mock ProductAveragesProvider
@pytest.fixture
def mock_provider() -> MagicMock:
    return MagicMock(spec=ProductAveragesProvider)


# Fixture for empty aggregated data DataFrame
@pytest.fixture
def empty_aggregated_df() -> pd.DataFrame:
    columns = list(DynamicPrimitiveSourceModel.to_schema().columns.keys())
    df = pd.DataFrame(columns=columns)
    # Attempt setting dtypes on the empty DataFrame based on the model
    for col, props in DynamicPrimitiveSourceModel.to_schema().columns.items():
        if col in df.columns and props.dtype:
            try:
                df[col] = df[col].astype(props.dtype.type)  # type: ignore
            except Exception:
                df[col] = df[col].astype(object)  # type: ignore
    return df


# Fixture for default metadata
@pytest.fixture
def default_metadata() -> ArgentinaProductStateMetadata:
    return ArgentinaProductStateMetadata()


# --- Fixtures for Moto-based End-to-End Tests ---


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
    """Creates a mock S3 bucket using moto and returns a real S3Bucket block."""
    _ = aws_credentials  # Ensure credentials are set
    _ = prefect_test_fixture  # Ensure Prefect context if needed
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1")  # type: ignore
        s3_client.create_bucket(Bucket=TEST_E2E_BUCKET_NAME)  # type: ignore
        # Instantiate the Prefect block targeting the moto bucket
        s3_block = S3Bucket(bucket_name=TEST_E2E_BUCKET_NAME)
        # Provide access to the underlying boto3 client for direct manipulation if needed
        s3_block._boto_client = s3_client  # type: ignore
        yield s3_block


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
    # Use gzip directly for consistent compression with task logic
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
        data.to_csv(io.TextIOWrapper(gz_file, "utf-8"), index=False)
    buffer.seek(0)
    s3_block._boto_client.put_object(  # type: ignore
        Bucket=s3_block.bucket_name, Key=full_path, Body=buffer.getvalue()
    )


# Helper Function to Upload Initial State
def upload_initial_state(s3_block: S3Bucket, metadata: ArgentinaProductStateMetadata, data: pd.DataFrame):
    """Uploads initial aggregated state (metadata JSON, data CSV.gz) to mock S3."""
    # Upload Metadata
    metadata_json = metadata.model_dump_json().encode("utf-8")
    s3_block._boto_client.put_object(  # type: ignore
        Bucket=s3_block.bucket_name, Key=METADATA_S3_PATH, Body=metadata_json
    )

    # Upload Data
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
        data.to_csv(io.TextIOWrapper(gz_file, "utf-8"), index=False)
    buffer.seek(0)
    s3_block._boto_client.put_object(  # type: ignore
        Bucket=s3_block.bucket_name, Key=DATA_S3_PATH, Body=buffer.getvalue()
    )


# Helper Function to Verify Final State
def verify_final_state(
    s3_block: S3Bucket,
    expected_last_date: str,
    expected_total_products: int,
    expected_products: list[dict[str, Any]],  # List of dicts representing expected rows
):
    """Downloads and verifies the final state from mock S3."""
    # Verify Metadata
    try:
        metadata_obj = s3_block._boto_client.get_object(Bucket=s3_block.bucket_name, Key=METADATA_S3_PATH)  # type: ignore
        metadata_content = metadata_obj["Body"].read().decode("utf-8")  # type: ignore
        loaded_metadata = ArgentinaProductStateMetadata(**json.loads(metadata_content))  # type: ignore
        assert loaded_metadata.last_aggregation_processed_date == expected_last_date
        assert loaded_metadata.total_products_count == expected_total_products
    except s3_block._boto_client.exceptions.NoSuchKey:  # type: ignore
        pytest.fail(f"Metadata file not found at {METADATA_S3_PATH}")

    # Verify Data
    try:
        data_obj = s3_block._boto_client.get_object(Bucket=s3_block.bucket_name, Key=DATA_S3_PATH)  # type: ignore
        data_content = data_obj["Body"].read()  # type: ignore
        buffer = io.BytesIO(data_content)  # type: ignore
        loaded_data_df = pd.read_csv(buffer, compression="gzip", keep_default_na=False, na_values=[""])  # type: ignore
        # Validate schema compliance
        loaded_data_df = DynamicPrimitiveSourceModel.validate(loaded_data_df, lazy=True)

        assert len(loaded_data_df) == expected_total_products

        # Convert expected products (which MUST now be complete dicts) to DataFrame
        expected_data_df = pd.DataFrame(expected_products)

        # Ensure column order and types match for comparison
        expected_data_df = expected_data_df[
            list(DynamicPrimitiveSourceModel.to_schema().columns.keys())
        ]  # Order columns
        try:
            expected_data_df = DynamicPrimitiveSourceModel.validate(expected_data_df, lazy=True)  # Ensure types
        except Exception as e:
            pytest.fail(f"Provided expected_products data failed validation: {e}\nData: {expected_products}")

        # Sort both dataframes for consistent comparison
        loaded_data_df_sorted = loaded_data_df.sort_values(by="source_id").reset_index(drop=True)  # type: ignore
        expected_data_df_sorted = expected_data_df.sort_values(by="source_id").reset_index(drop=True)  # type: ignore

        pd.testing.assert_frame_equal(
            loaded_data_df_sorted, expected_data_df_sorted, check_dtype=True
        )  # Dtype check can be strict

    except s3_block._boto_client.exceptions.NoSuchKey:  # type: ignore
        pytest.fail(f"Data file not found at {DATA_S3_PATH}")
    except Exception as e:
        pytest.fail(f"Error verifying final data state: {e}")


# --- Fixtures for Mocked Flow Test ---
@pytest.fixture
def expected_df_day1() -> pd.DataFrame:
    """Static expected DataFrame after processing the first date in the mock test."""
    df = pd.DataFrame(
        {
            "stream_id": ["arg_sepa_prod_1"],
            "source_id": ["1"],
            "source_type": ["argentina_sepa_product"],
            "productos_descripcion": ["Product One"],
            "first_shown_at": ["2024-01-01"],
        }
    )
    return DynamicPrimitiveSourceModel.validate(df, lazy=True)


@pytest.fixture
def expected_df_day2(expected_df_day1: pd.DataFrame) -> pd.DataFrame:
    """Static expected DataFrame after processing the second date in the mock test."""
    df = pd.DataFrame(
        {
            "stream_id": ["arg_sepa_prod_1", "arg_sepa_prod_2"],
            "source_id": ["1", "2"],
            "source_type": ["argentina_sepa_product", "argentina_sepa_product"],
            "productos_descripcion": ["Product One", "Product Two"],
            "first_shown_at": ["2024-01-01", "2024-01-02"],
        }
    )
    # Re-validate to ensure schema compliance
    return DynamicPrimitiveSourceModel.validate(df, lazy=True)


# --- Mocked Flow Tests ---


# Updated integration test to include state saving and artifacts using static returns
@pytest.mark.asyncio
@pytest.mark.usefixtures("prefect_test_fixture")
async def test_aggregate_flow_with_state_and_artifacts_simplified_mock(
    mock_s3_block: MagicMock,
    mock_provider: MagicMock,
    empty_aggregated_df: pd.DataFrame,
    default_metadata: ArgentinaProductStateMetadata,
    expected_df_day1: pd.DataFrame,
    expected_df_day2: pd.DataFrame,
):
    """
    Tests the aggregation flow with simplified mocking for process_single_date_products.

    Verifies orchestration logic (task calls, state saving, artifact creation)
    using pre-defined return values for the processing task.
    """
    dates_to_process = ["2024-01-01", "2024-01-02"]

    # Configure the mock for process_single_date_products to return static DFs
    mock_process_task = AsyncMock(
        # No spec needed here as we control the return value completely
        side_effect=[expected_df_day1, expected_df_day2]
    )

    # Mock all dependent tasks
    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.load_aggregation_state",
            AsyncMock(return_value=(empty_aggregated_df, default_metadata)),
        ) as mock_load_task,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.determine_aggregation_dates",
            return_value=dates_to_process,
        ) as mock_date_range_task,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.ProductAveragesProvider",
            return_value=mock_provider,
        ),
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.process_single_date_products", mock_process_task
        ),
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.save_aggregation_state", new_callable=AsyncMock
        ) as mock_save_task,
        patch("tsn_adapters.tasks.argentina.flows.aggregate_products_flow.create_markdown_artifact") as mock_artifact,
    ):
        # Run the flow
        await aggregate_argentina_products_flow(
            s3_block=mock_s3_block,
            force_reprocess=False,
        )

        # --- Assertions ---

        # Verify initial tasks called once
        mock_load_task.assert_awaited_once()
        mock_date_range_task.assert_called_once()

        # Verify process_single_date_products was called for each date
        assert mock_process_task.call_count == len(dates_to_process)
        mock_process_task.assert_any_await(
            date_to_process="2024-01-01",
            current_aggregated_data=empty_aggregated_df,  # Initial call uses empty DF
            product_averages_provider=mock_provider,
        )
        mock_process_task.assert_any_await(
            date_to_process="2024-01-02",
            current_aggregated_data=expected_df_day1,  # Second call uses result from first
            product_averages_provider=mock_provider,
        )

        # Verify save_aggregation_state calls within the loop
        assert mock_save_task.call_count == len(dates_to_process)
        # Check first save call arguments
        first_save_call_args = mock_save_task.call_args_list[0].kwargs
        pd.testing.assert_frame_equal(first_save_call_args["aggregated_data"], expected_df_day1)
        assert first_save_call_args["metadata"] is default_metadata  # Should pass the original metadata object

        # Check second save call arguments
        second_save_call_args = mock_save_task.call_args_list[1].kwargs
        pd.testing.assert_frame_equal(second_save_call_args["aggregated_data"], expected_df_day2)
        assert second_save_call_args["metadata"] is default_metadata  # Still passes the original metadata object

        # Check artifact call (happens after the loop)
        mock_artifact.assert_called_once()
        _, artifact_call_kwargs = mock_artifact.call_args  # Use _ for unused variable
        final_report_string = artifact_call_kwargs["markdown"]

        # Verify content of the final report artifact based on actual format
        assert f"({len(dates_to_process)} dates)" in final_report_string  # Check for (N dates)
        assert f"New Products Added:** {len(expected_df_day2)}" in final_report_string  # Check for New Products Added
        assert (
            f"Total Unique Products:** {len(expected_df_day2)}" in final_report_string
        )  # Check for Total Unique Products


@pytest.mark.usefixtures("prefect_test_fixture")
def test_aggregate_flow_no_dates_to_process_with_artifact(
    mock_s3_block: MagicMock,
    mock_provider: MagicMock,
    empty_aggregated_df: pd.DataFrame,
    default_metadata: ArgentinaProductStateMetadata,
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
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.load_aggregation_state", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.determine_aggregation_dates"
        ) as mock_determine_range,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.process_single_date_products"
        ) as mock_process_single_date,
        # Mock save state (should not be called)
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.save_aggregation_state", new_callable=AsyncMock
        ) as mock_save_state,
        # Mock artifact creation - patch where it's used in the flow module
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.create_markdown_artifact",
            new_callable=MagicMock,
        ) as mock_create_artifact,
    ):
        mock_load_state.return_value = (empty_aggregated_df, default_metadata)
        mock_determine_range.return_value = []  # Return empty list

        # --- Flow Execution ---
        asyncio.run(aggregate_argentina_products_flow(s3_block=mock_s3_block, force_reprocess=False))

        # --- Assertions ---
        mock_provider_init.assert_called_once()
        mock_load_state.assert_awaited_once()
        mock_determine_range.assert_called_once()

        # Assert that the processing loop and save state were NOT entered
        mock_process_single_date.assert_not_called()
        mock_save_state.assert_not_called()

        # Assert that the artifact was created with the 'no dates' message
        mock_create_artifact.assert_called_once()
        assert mock_create_artifact.call_args is not None
        artifact_call_args = mock_create_artifact.call_args.kwargs
        assert artifact_call_args["key"] == "argentina-product-aggregation-summary"
        assert "No new dates found to process." in artifact_call_args["markdown"]


# --- Moto-based End-to-End Tests ---


@pytest.mark.usefixtures(
    "prefect_test_fixture", "aws_credentials", "show_prefect_logs_fixture"
)  # Ensure Prefect context and AWS creds
@pytest.mark.asyncio
async def test_aggregate_flow_end_to_end_cold_start(
    real_s3_bucket_block: S3Bucket,
    daily_data_2024_03_10: pd.DataFrame,
    daily_data_2024_03_11: pd.DataFrame,
    daily_data_2024_03_12: pd.DataFrame,
):
    """
    Tests the flow end-to-end with moto, starting with no pre-existing state.
    """
    # Arrange: Upload sample daily data
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-10"), daily_data_2024_03_10)
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-11"), daily_data_2024_03_11)
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-12"), daily_data_2024_03_12)

    # Act: Run the flow (first time)
    await aggregate_argentina_products_flow(
        s3_block=real_s3_bucket_block,
        force_reprocess=False,
    )

    # --- Verify Final State ---
    expected_final_products = [
        {
            "stream_id": _generate_argentina_product_stream_id("P001"),
            "source_id": "P001",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product One",
            "first_shown_at": "2024-03-10",
        },
        {
            "stream_id": _generate_argentina_product_stream_id("P002"),
            "source_id": "P002",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product Two",
            "first_shown_at": "2024-03-10",
        },
        {
            "stream_id": _generate_argentina_product_stream_id("P003"),
            "source_id": "P003",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product Three",
            "first_shown_at": "2024-03-11",
        },
        {
            "stream_id": _generate_argentina_product_stream_id("P004"),
            "source_id": "P004",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product Four",
            "first_shown_at": "2024-03-12",
        },
    ]

    verify_final_state(
        s3_block=real_s3_bucket_block,
        expected_last_date="2024-03-12",
        expected_total_products=4,
        expected_products=expected_final_products,
    )


@pytest.mark.usefixtures("prefect_test_fixture", "aws_credentials")
@pytest.mark.asyncio
async def test_aggregate_flow_end_to_end_resume(
    real_s3_bucket_block: S3Bucket,
    daily_data_2024_03_10: pd.DataFrame,
    daily_data_2024_03_11: pd.DataFrame,
    daily_data_2024_03_12: pd.DataFrame,
    empty_aggregated_df: pd.DataFrame,
):
    """Tests resuming aggregation from a pre-existing state."""
    # --- Setup Initial State ---
    initial_metadata = ArgentinaProductStateMetadata(last_aggregation_processed_date="2024-03-10", total_products_count=2)
    initial_products = [
        {
            "stream_id": "arg_sepa_prod_P001",
            "source_id": "P001",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product One Initial",  # Desc should not change
            "first_shown_at": "2024-03-09",  # Earlier date
        },
        {
            "stream_id": "arg_sepa_prod_P002",
            "source_id": "P002",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product Two Initial",  # Desc should not change
            "first_shown_at": "2024-03-10",
        },
    ]
    initial_data_df = pd.DataFrame(initial_products)
    # Validate initial state before uploading
    initial_data_df = DynamicPrimitiveSourceModel.validate(
        initial_data_df[list(DynamicPrimitiveSourceModel.to_schema().columns.keys())], lazy=True
    )

    upload_initial_state(real_s3_bucket_block, initial_metadata, initial_data_df)

    # Upload only the data for dates *after* the initial state
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-11"), daily_data_2024_03_11)
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-12"), daily_data_2024_03_12)

    # Act: Run the flow (should resume from 2024-03-11)
    await aggregate_argentina_products_flow(
        s3_block=real_s3_bucket_block,
        force_reprocess=False,
    )

    # --- Verify Final State ---
    # Expected state includes initial products + new ones from 11th and 12th
    expected_final_products = [
        # Initial products (description/first_shown_at should remain unchanged)
        {
            "stream_id": "arg_sepa_prod_P001",
            "source_id": "P001",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product One Initial",
            "first_shown_at": "2024-03-09",
        },
        {
            "stream_id": "arg_sepa_prod_P002",
            "source_id": "P002",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product Two Initial",
            "first_shown_at": "2024-03-10",
        },
        # New product from 11th
        {
            "stream_id": _generate_argentina_product_stream_id("P003"),
            "source_id": "P003",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product Three",
            "first_shown_at": "2024-03-11",
        },
        # New product from 12th
        {
            "stream_id": _generate_argentina_product_stream_id("P004"),
            "source_id": "P004",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product Four",
            "first_shown_at": "2024-03-12",
        },
    ]

    verify_final_state(
        s3_block=real_s3_bucket_block,
        expected_last_date="2024-03-12",
        expected_total_products=4,
        expected_products=expected_final_products,
    )


@pytest.mark.usefixtures("prefect_test_fixture", "aws_credentials")
@pytest.mark.asyncio
async def test_aggregate_flow_end_to_end_force_reprocess(
    real_s3_bucket_block: S3Bucket,
    daily_data_2024_03_10: pd.DataFrame,
    daily_data_2024_03_11: pd.DataFrame,
    daily_data_2024_03_12: pd.DataFrame,
):
    """
    Tests the flow end-to-end with moto, with force_reprocess=True, ignoring initial state date.
    """
    # Simulate existing state that will be overwritten
    initial_metadata = ArgentinaProductStateMetadata(last_aggregation_processed_date="2024-03-10", total_products_count=1)
    initial_products = [
        {
            "stream_id": "arg_sepa_prod_OLD",
            "source_id": "OLD",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Old Product",
            "first_shown_at": "2024-03-01",
        }
    ]
    initial_data_df = pd.DataFrame(initial_products)
    initial_data_df = DynamicPrimitiveSourceModel.validate(
        initial_data_df[list(DynamicPrimitiveSourceModel.to_schema().columns.keys())], lazy=True
    )
    upload_initial_state(real_s3_bucket_block, initial_metadata, initial_data_df)

    # Upload all daily data, including the date covered by initial state
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-10"), daily_data_2024_03_10)
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-11"), daily_data_2024_03_11)
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-12"), daily_data_2024_03_12)

    # Act: Run the flow with force_reprocess=True
    await aggregate_argentina_products_flow(
        s3_block=real_s3_bucket_block,
        force_reprocess=True,
    )

    # --- Verify Final State ---
    # Expected state is the same as cold start, ignoring the initial state
    expected_final_products = [
        {
            "stream_id": _generate_argentina_product_stream_id("P001"),
            "source_id": "P001",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product One",
            "first_shown_at": "2024-03-10",
        },
        {
            "stream_id": _generate_argentina_product_stream_id("P002"),
            "source_id": "P002",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product Two",
            "first_shown_at": "2024-03-10",
        },
        {
            "stream_id": _generate_argentina_product_stream_id("P003"),
            "source_id": "P003",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product Three",
            "first_shown_at": "2024-03-11",
        },
        {
            "stream_id": _generate_argentina_product_stream_id("P004"),
            "source_id": "P004",
            "source_type": "argentina_sepa_product",
            "productos_descripcion": "Product Four",
            "first_shown_at": "2024-03-12",
        },
    ]

    verify_final_state(
        s3_block=real_s3_bucket_block,
        expected_last_date="2024-03-12",
        expected_total_products=4,
        expected_products=expected_final_products,
    )
