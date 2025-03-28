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
from tsn_adapters.tasks.argentina.models import DynamicPrimitiveSourceModel, ProductAggregationMetadata
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks import _generate_argentina_product_stream_id
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
def default_metadata() -> ProductAggregationMetadata:
    return ProductAggregationMetadata()


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
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=TEST_E2E_BUCKET_NAME)
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
def upload_initial_state(s3_block: S3Bucket, metadata: ProductAggregationMetadata, data: pd.DataFrame):
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
        metadata_content = metadata_obj["Body"].read().decode("utf-8")
        loaded_metadata = ProductAggregationMetadata(**json.loads(metadata_content))
        assert loaded_metadata.last_processed_date == expected_last_date
        assert loaded_metadata.total_products_count == expected_total_products
    except s3_block._boto_client.exceptions.NoSuchKey:  # type: ignore
        pytest.fail(f"Metadata file not found at {METADATA_S3_PATH}")

    # Verify Data
    try:
        data_obj = s3_block._boto_client.get_object(Bucket=s3_block.bucket_name, Key=DATA_S3_PATH)  # type: ignore
        data_content = data_obj["Body"].read()
        buffer = io.BytesIO(data_content)
        loaded_data_df = pd.read_csv(buffer, compression="gzip", keep_default_na=False, na_values=[""])
        # Validate schema compliance
        loaded_data_df = DynamicPrimitiveSourceModel.validate(loaded_data_df, lazy=True)

        assert len(loaded_data_df) == expected_total_products

        # Convert expected products to DataFrame for easier comparison
        # Ensure stream_id generation matches the logic used in the task
        for prod in expected_products:
            if "stream_id" not in prod:
                prod["stream_id"] = _generate_argentina_product_stream_id(prod["source_id"])
            if "source_type" not in prod:
                prod["source_type"] = "argentina_sepa_product"  # Default source type

        expected_data_df = pd.DataFrame(expected_products)
        # Ensure column order and types match for comparison
        expected_data_df = expected_data_df[
            list(DynamicPrimitiveSourceModel.to_schema().columns.keys())
        ]  # Order columns
        expected_data_df = DynamicPrimitiveSourceModel.validate(expected_data_df, lazy=True)  # Ensure types

        # Sort both dataframes for consistent comparison
        loaded_data_df_sorted = loaded_data_df.sort_values(by="source_id").reset_index(drop=True)
        expected_data_df_sorted = expected_data_df.sort_values(by="source_id").reset_index(drop=True)

        pd.testing.assert_frame_equal(
            loaded_data_df_sorted, expected_data_df_sorted, check_dtype=False
        )  # Dtype check can be strict

    except s3_block._boto_client.exceptions.NoSuchKey:  # type: ignore
        pytest.fail(f"Data file not found at {DATA_S3_PATH}")
    except Exception as e:
        pytest.fail(f"Error verifying final data state: {e}")


# --- Mocked Flow Tests ---


# Updated integration test to include state saving and artifacts
@pytest.mark.usefixtures("prefect_test_fixture")
def test_aggregate_flow_with_state_and_artifacts(
    mock_s3_block: MagicMock,
    mock_provider: MagicMock,
    empty_aggregated_df: pd.DataFrame,
    default_metadata: ProductAggregationMetadata,
):
    """
    Tests the aggregation flow including state saving within the loop and artifact creation.

    Mocks tasks and helpers, verifies calls to save_aggregation_state and create_markdown_artifact.
    Simulates adding new products.
    """
    dates_to_process = ["2024-01-01", "2024-01-02"]
    # Simulate finding 1 new product on the first date, 2 on the second
    product_counts_after = [1, 3]

    # --- Mock Setup ---
    # Mock ProductAveragesProvider instantiation
    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.ProductAveragesProvider",
            return_value=mock_provider,
        ) as mock_provider_init,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.load_aggregation_state", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.determine_date_range_to_process"
        ) as mock_determine_range,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.process_single_date_products"
        ) as mock_process_single_date,
        # Mock the save state task
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.save_aggregation_state", new_callable=AsyncMock
        ) as mock_save_state,
        # Mock the artifact creation function - patch where it's used in the flow module
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.create_markdown_artifact",
            new_callable=MagicMock,
        ) as mock_create_artifact,
    ):
        # Configure mock return values
        mock_load_state.return_value = (empty_aggregated_df.copy(), default_metadata.model_copy(deep=True))
        mock_determine_range.return_value = dates_to_process

        # Simulate process_single_date_products returning a modified DataFrame
        # We need a mutable object to track the state across calls
        current_df_state = {"df": empty_aggregated_df.copy()}
        current_count_state = {"count": 0}

        def mock_process_side_effect(
            date_to_process: str,
            current_aggregated_data: pd.DataFrame,
            product_averages_provider: ProductAveragesProvider,
        ) -> pd.DataFrame:
            if date_to_process == "2024-01-01":
                new_count = product_counts_after[0]
                # Simulate adding rows - actual content doesn't matter for this mock
                new_rows = pd.DataFrame([{"source_id": f"prod_{i}"} for i in range(new_count)])
            elif date_to_process == "2024-01-02":
                new_count = product_counts_after[1]
                new_rows = pd.DataFrame([{"source_id": f"prod_{i}"} for i in range(new_count)])
            else:
                new_rows = pd.DataFrame()
                new_count = len(current_aggregated_data)

            # Update the state for the next call
            current_df_state["df"] = new_rows  # Simplified: just return the expected final shape
            current_count_state["count"] = new_count
            return current_df_state["df"]

        mock_process_single_date.side_effect = mock_process_side_effect

        # --- Flow Execution ---
        asyncio.run(aggregate_argentina_products_flow(s3_block=mock_s3_block, force_reprocess=False))

        # --- Assertions ---
        # 1. Provider Instantiation
        mock_provider_init.assert_called_once_with(s3_block=mock_s3_block)

        # 2. Load State Call
        mock_load_state.assert_awaited_once()
        # Check args if necessary, e.g., mock_load_state.assert_awaited_once_with(s3_block=mock_s3_block, ...) # wait_for needs care

        # 3. Determine Date Range Call
        mock_determine_range.assert_called_once()
        # Check args if necessary

        # 4. Process Single Date Calls
        assert mock_process_single_date.call_count == len(dates_to_process)

        # 5. Save State Calls (Inside the loop)
        assert mock_save_state.await_count == len(dates_to_process)

        # Verify the arguments of the LAST call match the final expected state
        final_date = dates_to_process[-1]
        final_count = product_counts_after[-1]
        expected_final_metadata = default_metadata.model_copy(deep=True)
        expected_final_metadata.last_processed_date = final_date
        expected_final_metadata.total_products_count = final_count
        # Reconstruct the final expected DataFrame based on the mock logic
        expected_final_df = pd.DataFrame([{"source_id": f"prod_{j}"} for j in range(final_count)])

        last_call_args = mock_save_state.await_args_list[-1].kwargs
        assert last_call_args["s3_block"] == mock_s3_block
        assert last_call_args["metadata"] == expected_final_metadata
        pd.testing.assert_frame_equal(last_call_args["aggregated_data"], expected_final_df, check_dtype=False)

        # 6. Create Artifact Call (After the loop)
        mock_create_artifact.assert_called_once()
        # Add assertion to satisfy linter
        assert mock_create_artifact.call_args is not None
        artifact_call_args = mock_create_artifact.call_args.kwargs
        assert artifact_call_args["key"] == "argentina-product-aggregation-summary"

        assert "**Processed Date Range:** 2024-01-01 to 2024-01-02" in artifact_call_args["markdown"]
        assert "**New Products Added:** 3" in artifact_call_args["markdown"]  # 1 + 2
        assert f"**Total Unique Products:** {product_counts_after[-1]}" in artifact_call_args["markdown"]
        assert "**Force Reprocess Flag:** False" in artifact_call_args["markdown"]


@pytest.mark.usefixtures("prefect_test_fixture")
def test_aggregate_flow_no_dates_to_process_with_artifact(
    mock_s3_block: MagicMock,
    mock_provider: MagicMock,
    empty_aggregated_df: pd.DataFrame,
    default_metadata: ProductAggregationMetadata,
):
    """
    Tests the flow behavior when determine_date_range_to_process returns an empty list.
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
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.determine_date_range_to_process"
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
        # Add assertion to satisfy linter
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

    # Act: Run the flow
    # Use await directly as the flow is async
    await aggregate_argentina_products_flow(
        s3_block=real_s3_bucket_block,
        force_reprocess=False,
    )

    # Assert: Verify the final state in mock S3
    expected_products = [
        {"source_id": "P001", "productos_descripcion": "Product One", "first_shown_at": "2024-03-10"},
        {"source_id": "P002", "productos_descripcion": "Product Two", "first_shown_at": "2024-03-10"},
        {"source_id": "P003", "productos_descripcion": "Product Three", "first_shown_at": "2024-03-11"},
        {"source_id": "P004", "productos_descripcion": "Product Four", "first_shown_at": "2024-03-12"},
    ]
    verify_final_state(
        s3_block=real_s3_bucket_block,
        expected_last_date="2024-03-12",
        expected_total_products=4,
        expected_products=expected_products,
    )


@pytest.mark.usefixtures("prefect_test_fixture", "aws_credentials")
@pytest.mark.asyncio
async def test_aggregate_flow_end_to_end_resume(
    real_s3_bucket_block: S3Bucket,
    daily_data_2024_03_10: pd.DataFrame,
    daily_data_2024_03_11: pd.DataFrame,
    daily_data_2024_03_12: pd.DataFrame,
):
    """
    Tests the flow end-to-end with moto, resuming from pre-existing state.
    """
    # Arrange: Upload initial state (processed up to 2024-03-10)
    initial_metadata = ProductAggregationMetadata(last_processed_date="2024-03-10", total_products_count=2)
    initial_data = pd.DataFrame(
        [
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
        ]
    )
    initial_data = DynamicPrimitiveSourceModel.validate(initial_data, lazy=True)  # Ensure type compliance
    upload_initial_state(real_s3_bucket_block, initial_metadata, initial_data)

    # Arrange: Upload sample daily data for all dates
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-10"), daily_data_2024_03_10)
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-11"), daily_data_2024_03_11)
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-12"), daily_data_2024_03_12)

    # Act: Run the flow (should resume from 2024-03-11)
    await aggregate_argentina_products_flow(s3_block=real_s3_bucket_block, force_reprocess=False)

    # Assert: Verify the final state (should include P003 and P004)
    expected_products = [
        # From initial state
        {"source_id": "P001", "productos_descripcion": "Product One", "first_shown_at": "2024-03-10"},
        {"source_id": "P002", "productos_descripcion": "Product Two", "first_shown_at": "2024-03-10"},
        # Newly added
        {"source_id": "P003", "productos_descripcion": "Product Three", "first_shown_at": "2024-03-11"},
        {"source_id": "P004", "productos_descripcion": "Product Four", "first_shown_at": "2024-03-12"},
    ]
    verify_final_state(
        s3_block=real_s3_bucket_block,
        expected_last_date="2024-03-12",  # Processed up to the last available date
        expected_total_products=4,
        expected_products=expected_products,
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
    # Arrange: Upload initial state (processed up to 2024-03-10) - same as resume test
    initial_metadata = ProductAggregationMetadata(last_processed_date="2024-03-10", total_products_count=2)
    initial_data = pd.DataFrame(
        [
            {
                "stream_id": _generate_argentina_product_stream_id("P001"),
                "source_id": "P001",
                "source_type": "argentina_sepa_product",
                "productos_descripcion": "Product One Old Desc",
                "first_shown_at": "2024-03-09",
            },  # Older date/desc
            {
                "stream_id": _generate_argentina_product_stream_id("P002"),
                "source_id": "P002",
                "source_type": "argentina_sepa_product",
                "productos_descripcion": "Product Two",
                "first_shown_at": "2024-03-10",
            },
        ]
    )
    initial_data = DynamicPrimitiveSourceModel.validate(initial_data, lazy=True)
    upload_initial_state(real_s3_bucket_block, initial_metadata, initial_data)

    # Arrange: Upload sample daily data for all dates
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-10"), daily_data_2024_03_10)
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-11"), daily_data_2024_03_11)
    upload_sample_data(real_s3_bucket_block, DateStr("2024-03-12"), daily_data_2024_03_12)

    # Act: Run the flow with force_reprocess=True
    await aggregate_argentina_products_flow(
        s3_block=real_s3_bucket_block,
        force_reprocess=True,  # Force reprocessing from the beginning
    )

    # Assert: Verify the final state (should be identical to cold start, preserving original first_shown_at/desc)
    # The logic should find P001/P002 on 2024-03-10 again, but NOT update their first_shown_at or description.
    expected_products = [
        {
            "source_id": "P001",
            "productos_descripcion": "Product One",
            "first_shown_at": "2024-03-10",
        },  # Should take desc/date from 2024-03-10 data
        {"source_id": "P002", "productos_descripcion": "Product Two", "first_shown_at": "2024-03-10"},
        {"source_id": "P003", "productos_descripcion": "Product Three", "first_shown_at": "2024-03-11"},
        {"source_id": "P004", "productos_descripcion": "Product Four", "first_shown_at": "2024-03-12"},
    ]
    verify_final_state(
        s3_block=real_s3_bucket_block,
        expected_last_date="2024-03-12",  # Processed up to the last available date
        expected_total_products=4,
        expected_products=expected_products,
    )
