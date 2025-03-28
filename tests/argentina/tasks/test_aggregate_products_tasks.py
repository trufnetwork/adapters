"""
Unit tests for Argentina SEPA product aggregation tasks.
"""

from collections.abc import Generator
import gzip
import io
import json
import logging
from typing import Any
from unittest.mock import MagicMock

from _pytest.logging import LogCaptureFixture
from moto import mock_aws
import pandas as pd
from pandera.typing import DataFrame
from prefect.logging.loggers import disable_run_logger
from prefect_aws import S3Bucket
import pytest

from tsn_adapters.tasks.argentina.models.aggregate_products_models import (
    DynamicPrimitiveSourceModel,
    ProductAggregationMetadata,
)
from tsn_adapters.tasks.argentina.provider.product_averages import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks import (
    determine_date_range_to_process,
    load_aggregation_state,
    save_aggregation_state,
)
from tsn_adapters.tasks.argentina.types import DateStr

# --- Fixtures ---

TEST_BUCKET_NAME = "test-aggregation-bucket"
BASE_PATH = "test_agg"
METADATA_PATH = f"{BASE_PATH}/argentina_products_metadata.json"
DATA_PATH = f"{BASE_PATH}/argentina_products.csv.gz"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"  # Default region for moto


@pytest.fixture(scope="function")
def s3_bucket_block(prefect_test_fixture: Any) -> Generator[S3Bucket, None, None]:
    """Creates a mock S3 bucket and returns an S3Bucket block instance."""
    _ = prefect_test_fixture
    with mock_aws():
        import boto3  # type: ignore

        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=TEST_BUCKET_NAME)
        # Instantiate the Prefect block
        s3_block = S3Bucket(bucket_name=TEST_BUCKET_NAME)
        yield s3_block  # Use yield to ensure cleanup if needed, though moto handles it


@pytest.fixture
def valid_metadata() -> ProductAggregationMetadata:
    """Valid ProductAggregationMetadata instance."""
    return ProductAggregationMetadata(last_processed_date="2024-03-10", total_products_count=25)


@pytest.fixture
def valid_metadata_json(valid_metadata: ProductAggregationMetadata) -> bytes:
    """Valid metadata serialized to JSON bytes."""
    return valid_metadata.model_dump_json().encode("utf-8")


@pytest.fixture
def invalid_metadata_json() -> bytes:
    """Invalid JSON bytes."""
    return b'{"last_processed_date": "2024-03-11", "total_products_count": "abc"}'  # count is not int


@pytest.fixture
def corrupted_metadata_json() -> bytes:
    """Corrupted (non-parseable) JSON bytes."""
    return b'{"last_processed_date": "2024-03-12", total_products_count: 50'  # Missing closing brace


@pytest.fixture
def valid_agg_data() -> DataFrame[DynamicPrimitiveSourceModel]:
    """Valid aggregated data DataFrame."""
    df = pd.DataFrame(
        {
            "stream_id": ["arg_sepa_prod_001", "arg_sepa_prod_002"],
            "source_id": ["001", "002"],
            "source_type": ["argentina_sepa_product", "argentina_sepa_product"],
            "productos_descripcion": ["Test Product 1", "Test Product 2"],
            "first_shown_at": ["2024-03-01", "2024-03-05"],
        }
    )
    # Ensure dtypes match the model for consistency before validation/saving
    return DynamicPrimitiveSourceModel.validate(df, lazy=True)


@pytest.fixture
def valid_agg_data_csv_gz(valid_agg_data: DataFrame[DynamicPrimitiveSourceModel]) -> bytes:
    """Valid aggregated data DataFrame serialized to gzipped CSV bytes."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
        valid_agg_data.to_csv(io.TextIOWrapper(gz_file, "utf-8"), index=False)
    return buffer.getvalue()


@pytest.fixture
def invalid_agg_data_csv_gz() -> bytes:
    """Invalid aggregated data (missing column) as gzipped CSV bytes."""
    df = pd.DataFrame(
        {
            "stream_id": ["arg_sepa_prod_003"],
            "source_id": ["003"],
            # Missing source_type
            "productos_descripcion": ["Test Product 3"],
            "first_shown_at": ["2024-03-08"],
        }
    )
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
        df.to_csv(io.TextIOWrapper(gz_file, "utf-8"), index=False)
    return buffer.getvalue()


@pytest.fixture
def corrupted_agg_data_csv_gz() -> bytes:
    """Corrupted (non-CSV) gzipped bytes."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
        gz_file.write(b"this is not a csv file")
    return buffer.getvalue()


# --- Test Cases for load_aggregation_state ---


@pytest.mark.asyncio
async def test_load_state_both_exist_valid(
    s3_bucket_block: S3Bucket,
    valid_metadata: ProductAggregationMetadata,
    valid_metadata_json: bytes,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    valid_agg_data_csv_gz: bytes,
):
    """Test loading state when both files exist and are valid."""
    # Arrange: Upload valid files to mock S3
    await s3_bucket_block.awrite_path(METADATA_PATH, valid_metadata_json)
    await s3_bucket_block.awrite_path(DATA_PATH, valid_agg_data_csv_gz)

    # Act
    loaded_data, loaded_metadata = await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)

    # Assert
    assert loaded_metadata == valid_metadata
    pd.testing.assert_frame_equal(loaded_data, valid_agg_data, check_dtype=False)  # Dtype check can be tricky


@pytest.mark.asyncio
async def test_load_state_neither_exist(s3_bucket_block: S3Bucket):
    """Test loading state when neither file exists."""
    # Arrange: No files uploaded

    # Act
    loaded_data, loaded_metadata = await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)

    # Assert
    assert loaded_metadata == ProductAggregationMetadata()  # Defaults
    assert loaded_data.empty
    # Check columns match the model schema
    assert list(loaded_data.columns) == list(DynamicPrimitiveSourceModel.to_schema().columns.keys())


@pytest.mark.asyncio
async def test_load_state_only_metadata_exists(
    s3_bucket_block: S3Bucket,
    valid_metadata: ProductAggregationMetadata,
    valid_metadata_json: bytes,
):
    """Test loading state when only the metadata file exists."""
    # Arrange: Upload only metadata
    await s3_bucket_block.awrite_path(METADATA_PATH, valid_metadata_json)

    # Act
    loaded_data, loaded_metadata = await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)

    # Assert
    assert loaded_metadata == valid_metadata
    assert loaded_data.empty
    assert list(loaded_data.columns) == list(DynamicPrimitiveSourceModel.to_schema().columns.keys())


@pytest.mark.asyncio
async def test_load_state_only_data_exists(
    s3_bucket_block: S3Bucket,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    valid_agg_data_csv_gz: bytes,
):
    """Test loading state when only the data file exists."""
    # Arrange: Upload only data
    await s3_bucket_block.awrite_path(DATA_PATH, valid_agg_data_csv_gz)

    # Act
    loaded_data, loaded_metadata = await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)

    # Assert
    assert loaded_metadata == ProductAggregationMetadata()  # Defaults
    pd.testing.assert_frame_equal(loaded_data, valid_agg_data, check_dtype=False)


@pytest.mark.asyncio
async def test_load_state_invalid_metadata_json(
    s3_bucket_block: S3Bucket,
    invalid_metadata_json: bytes,
    valid_agg_data_csv_gz: bytes,
):
    """Test loading state with invalid (but parseable) metadata JSON content."""
    # Arrange: Upload invalid metadata JSON and valid data
    await s3_bucket_block.awrite_path(METADATA_PATH, invalid_metadata_json)
    await s3_bucket_block.awrite_path(DATA_PATH, valid_agg_data_csv_gz)

    # Act & Assert
    with pytest.raises(Exception):  # Pydantic's ValidationError inherits from Exception
        await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)


@pytest.mark.asyncio
async def test_load_state_corrupted_metadata_json(
    s3_bucket_block: S3Bucket,
    corrupted_metadata_json: bytes,
    valid_agg_data_csv_gz: bytes,
):
    """Test loading state with corrupted (non-parseable) metadata JSON."""
    # Arrange: Upload corrupted metadata and valid data
    await s3_bucket_block.awrite_path(METADATA_PATH, corrupted_metadata_json)
    await s3_bucket_block.awrite_path(DATA_PATH, valid_agg_data_csv_gz)

    # Act & Assert
    with pytest.raises(json.JSONDecodeError):
        await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)


@pytest.mark.asyncio
async def test_load_state_invalid_agg_data_schema(
    s3_bucket_block: S3Bucket,
    valid_metadata_json: bytes,
    invalid_agg_data_csv_gz: bytes,  # Missing column 'source_type'
):
    """Test loading state with data that fails Pandera validation."""
    # Arrange: Upload valid metadata and invalid data
    await s3_bucket_block.awrite_path(METADATA_PATH, valid_metadata_json)
    await s3_bucket_block.awrite_path(DATA_PATH, invalid_agg_data_csv_gz)

    # Act & Assert
    with pytest.raises(Exception):
        await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)


@pytest.mark.asyncio
async def test_load_state_corrupted_agg_data(
    s3_bucket_block: S3Bucket,
    valid_metadata_json: bytes,
    corrupted_agg_data_csv_gz: bytes,  # Not a CSV
):
    """Test loading state with corrupted (non-CSV) data."""
    # Arrange: Upload valid metadata and corrupted data
    await s3_bucket_block.awrite_path(METADATA_PATH, valid_metadata_json)
    await s3_bucket_block.awrite_path(DATA_PATH, corrupted_agg_data_csv_gz)

    # Act & Assert
    with pytest.raises(Exception):
        await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)


# --- Test Cases for save_aggregation_state ---


@pytest.mark.asyncio
async def test_save_state_valid(
    s3_bucket_block: S3Bucket,
    valid_metadata: ProductAggregationMetadata,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
):
    """Test saving valid metadata and aggregated data."""
    # Arrange: Prepare valid data and metadata objects
    # No need to pre-upload anything for saving test

    # Act
    await save_aggregation_state(
        s3_block=s3_bucket_block, aggregated_data=valid_agg_data, metadata=valid_metadata, base_path=BASE_PATH
    )

    # Assert: Read back the files from mock S3 and verify content

    # Verify Metadata
    metadata_bytes = await s3_bucket_block.aread_path(METADATA_PATH)
    loaded_metadata_dict = json.loads(metadata_bytes.decode("utf-8"))
    reloaded_metadata = ProductAggregationMetadata(**loaded_metadata_dict)
    assert reloaded_metadata == valid_metadata

    # Verify Data
    data_bytes_gz = await s3_bucket_block.aread_path(DATA_PATH)
    # Decompress and read CSV
    with gzip.open(io.BytesIO(data_bytes_gz), "rt", encoding="utf-8") as f:
        # Specify dtypes during read based on the model to ensure consistency
        dtypes = {
            col: props.dtype.type if props.dtype else object
            for col, props in DynamicPrimitiveSourceModel.to_schema().columns.items()
        }
        for col in DynamicPrimitiveSourceModel.to_schema().columns.keys():
            if col not in dtypes:
                dtypes[col] = object

        reloaded_df = pd.read_csv(f, dtype=dtypes, keep_default_na=False, na_values=[""])

    # Validate reloaded data schema
    reloaded_validated_df = DynamicPrimitiveSourceModel.validate(reloaded_df, lazy=True)

    # Compare DataFrames
    pd.testing.assert_frame_equal(reloaded_validated_df, valid_agg_data, check_dtype=False)


@pytest.mark.asyncio
async def test_save_state_overwrites_existing(
    s3_bucket_block: S3Bucket,
    valid_metadata: ProductAggregationMetadata,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    corrupted_metadata_json: bytes,  # Some dummy initial content
    corrupted_agg_data_csv_gz: bytes,  # Some dummy initial content
):
    """Test that saving overwrites existing files."""
    # Arrange: Upload some initial dummy/corrupted content
    await s3_bucket_block.awrite_path(METADATA_PATH, corrupted_metadata_json)
    await s3_bucket_block.awrite_path(DATA_PATH, corrupted_agg_data_csv_gz)

    # Act: Save the new valid state
    await save_aggregation_state(
        s3_block=s3_bucket_block, aggregated_data=valid_agg_data, metadata=valid_metadata, base_path=BASE_PATH
    )

    # Assert: Read back and verify the *new* content
    metadata_bytes = await s3_bucket_block.aread_path(METADATA_PATH)
    reloaded_metadata = ProductAggregationMetadata.model_validate_json(metadata_bytes)
    assert reloaded_metadata == valid_metadata

    data_bytes_gz = await s3_bucket_block.aread_path(DATA_PATH)
    with gzip.open(io.BytesIO(data_bytes_gz), "rt", encoding="utf-8") as f:
        dtypes = {
            col: props.dtype.type if props.dtype else object
            for col, props in DynamicPrimitiveSourceModel.to_schema().columns.items()
        }
        for col in DynamicPrimitiveSourceModel.to_schema().columns.keys():
            if col not in dtypes:
                dtypes[col] = object
        reloaded_df = pd.read_csv(f, dtype=dtypes, keep_default_na=False, na_values=[""])
    reloaded_validated_df = DynamicPrimitiveSourceModel.validate(reloaded_df, lazy=True)
    pd.testing.assert_frame_equal(reloaded_validated_df, valid_agg_data, check_dtype=False)


# --- Test Cases for determine_date_range_to_process ---


@pytest.fixture
def mock_provider() -> MagicMock:
    """Creates a mock ProductAveragesProvider."""
    provider = MagicMock(spec=ProductAveragesProvider)
    # Mock the SYNCHRONOUS method
    provider.list_available_keys = MagicMock()
    return provider


def test_determine_dates_no_prior_state(mock_provider: MagicMock):
    """Test when no prior state exists (default metadata), should return all dates."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11"), DateStr("2024-03-12")]
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ProductAggregationMetadata()
    force_reprocess = False

    # Act
    # Call the synchronous task's function directly using .fn()
    result = determine_date_range_to_process.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == available_dates
    # Use assert_called_once for synchronous mock
    mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_force_reprocess(mock_provider: MagicMock):
    """Test when force_reprocess is True, should return all dates."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11"), DateStr("2024-03-12")]
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ProductAggregationMetadata(last_processed_date="2024-03-11", total_products_count=10)
    force_reprocess = True

    # Act
    result = determine_date_range_to_process.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == available_dates
    mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_prior_state_exists(mock_provider: MagicMock):
    """Test resuming from a previous state (last_processed_date)."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11"), DateStr("2024-03-12"), DateStr("2024-03-13")]
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ProductAggregationMetadata(last_processed_date="2024-03-11", total_products_count=10)
    force_reprocess = False
    expected_dates = [DateStr("2024-03-12"), DateStr("2024-03-13")]

    # Act
    result = determine_date_range_to_process.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == expected_dates
    mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_no_new_dates(mock_provider: MagicMock):
    """Test when prior state exists, but no new dates are available."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11")]
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ProductAggregationMetadata(last_processed_date="2024-03-11", total_products_count=10)
    force_reprocess = False
    expected_dates: list[DateStr] = []

    # Act
    result = determine_date_range_to_process.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == expected_dates
    mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_gaps_in_available_dates(mock_provider: MagicMock):
    """Test correct handling when there are gaps in available dates."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-12"), DateStr("2024-03-13")]
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ProductAggregationMetadata(last_processed_date="2024-03-10", total_products_count=5)
    force_reprocess = False
    expected_dates = [DateStr("2024-03-12"), DateStr("2024-03-13")]

    # Act
    result = determine_date_range_to_process.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == expected_dates
    mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_no_available_dates(mock_provider: MagicMock):
    """Test when the provider returns an empty list of dates."""
    # Arrange
    available_dates: list[DateStr] = []
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ProductAggregationMetadata()
    force_reprocess = False
    expected_dates: list[DateStr] = []

    # Act
    result = determine_date_range_to_process.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == expected_dates
    mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_invalid_metadata_date(mock_provider: MagicMock, caplog: LogCaptureFixture):
    """Test when metadata contains an invalid date format, should process all."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11")]
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ProductAggregationMetadata()
    metadata.last_processed_date = "invalid-date"
    metadata.total_products_count=10
    force_reprocess = False
    expected_dates = available_dates

    # Act
    with caplog.at_level(logging.WARNING): # Use logging.WARNING
        result = determine_date_range_to_process.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == expected_dates
    mock_provider.list_available_keys.assert_called_once()
    assert "Invalid last_processed_date 'invalid-date' in metadata" in caplog.text
