"""
Unit tests for Argentina SEPA product aggregation tasks.
"""

from collections.abc import Generator
import gzip
import io
import json
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from _pytest.logging import LogCaptureFixture
from moto import mock_aws
import pandas as pd
from pandera.typing import DataFrame
from prefect_aws import S3Bucket
import pytest
from botocore.exceptions import ClientError

from tsn_adapters.tasks.argentina.models.aggregate_products_models import (
    DynamicPrimitiveSourceModel,
    ProductAggregationMetadata,
)
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider.product_averages import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks import (
    _generate_argentina_product_stream_id,  # type: ignore
    create_empty_aggregated_data,
    determine_date_range_to_process,
    load_aggregation_state,
    process_single_date_products,
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


@pytest.fixture
def empty_agg_data() -> DataFrame[DynamicPrimitiveSourceModel]:
    """Empty but validated aggregated data DataFrame."""
    df = create_empty_aggregated_data()
    return DynamicPrimitiveSourceModel.validate(df, lazy=True)


@pytest.fixture
def sample_daily_data_1() -> DataFrame[SepaAvgPriceProductModel]:
    """Sample daily product data for testing (date 1)."""
    df = pd.DataFrame(
        {
            "id_producto": ["001", "101", "102"],
            "productos_descripcion": ["Existing Prod 1 Updated Desc", "New Prod 1", "New Prod 2"],
            "productos_precio_lista_avg": [10.5, 20.0, 30.5],
            "date": ["2024-03-11", "2024-03-11", "2024-03-11"],
        }
    )
    return SepaAvgPriceProductModel.validate(df)


@pytest.fixture
def sample_daily_data_2() -> DataFrame[SepaAvgPriceProductModel]:
    """Sample daily product data for testing (date 2). Includes duplicates."""
    df = pd.DataFrame(
        {
            "id_producto": ["102", "103", "103", "104"],
            "productos_descripcion": ["New Prod 2 Again", "New Prod 3", "New Prod 3 Duplicate", "New Prod 4"],
            "productos_precio_lista_avg": [31.0, 40.0, 40.1, 50.0],
            "date": ["2024-03-12", "2024-03-12", "2024-03-12", "2024-03-12"],
        }
    )
    return SepaAvgPriceProductModel.validate(df)


@pytest.fixture
def sample_daily_data_invalid() -> DataFrame[SepaAvgPriceProductModel]:
    """Sample daily product data with invalid records."""
    df = pd.DataFrame(
        {
            "id_producto": ["105", None, "106", ""],  # Invalid IDs
            "productos_descripcion": ["Valid New 5", "Invalid Prod No ID", "Valid New 6", "Invalid Prod Empty ID"],
            "productos_precio_lista_avg": [60.0, 70.0, 80.0, 90.0],
            "date": ["2024-03-13", "2024-03-13", "2024-03-13", "2024-03-13"],
        }
    )
    # Note: Pandera validation happens *inside* the tested function for loaded data
    # We return it unvalidated to simulate loading raw data
    return df  # type: ignore


# --- Helper function to convert DataFrame to gzipped CSV bytes ---
def _df_to_csv_gz_bytes(df: pd.DataFrame) -> bytes:
    """Converts a pandas DataFrame to gzipped CSV bytes."""
    # Write CSV to an in-memory text buffer
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    # Encode the CSV string to bytes
    csv_bytes = csv_buffer.getvalue().encode("utf-8")

    # Compress the bytes using gzip
    gzip_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gzip_buffer, mode="wb") as gz_file:
        gz_file.write(csv_bytes)
    gzip_buffer.seek(0)
    return gzip_buffer.getvalue()


@pytest.fixture
def mock_provider() -> MagicMock:
    """Creates a mock ProductAveragesProvider with a mock S3 block."""
    provider = MagicMock(spec=ProductAveragesProvider)
    # Mock the S3 block within the provider
    provider.s3_block = AsyncMock(spec=S3Bucket)

    # Define helper for side effect with type hint
    def _to_key(date: DateStr) -> str:
        return f"{date}/product_averages.zip"

    # Define helper for side effect with type hint
    def _get_path(key: str) -> str:
        return f"processed/{key}"  # Assuming default prefix

    # Mock the methods using the helper functions
    provider.to_product_averages_file_key = MagicMock(side_effect=_to_key)
    provider.get_full_path = MagicMock(side_effect=_get_path)

    # Also mock list_available_keys for other tests using this fixture
    provider.list_available_keys = MagicMock()
    return provider


@pytest.fixture
def setup_mock_aread_path(mock_provider: MagicMock):
    """Fixture providing a function to configure mock_provider.s3_block.aread_path."""

    def _setup(date_str: DateStr, data: pd.DataFrame | None = None, side_effect: Exception | None = None):
        """
        Configures aread_path mock for a specific date, data, or side effect.

        Args:
            date_str: The date to mock for.
            data: The DataFrame to return (will be converted to gzipped CSV bytes).
                  If None and side_effect is None, implies no data found (raises FileNotFoundError).
            side_effect: An exception instance to raise when aread_path is called.
        """
        expected_file_key = mock_provider.to_product_averages_file_key(date_str)
        expected_full_path = mock_provider.get_full_path(expected_file_key)

        if side_effect:
            mock_provider.s3_block.aread_path.side_effect = side_effect
        elif data is not None:
            mock_provider.s3_block.aread_path.return_value = _df_to_csv_gz_bytes(data)
            mock_provider.s3_block.aread_path.side_effect = None
        else:
            # Simulate S3 'Not Found' using ClientError
            error_response = {
                'Error': {'Code': 'NoSuchKey', 'Message': 'The specified key does not exist.'},
                'ResponseMetadata': {'HTTPStatusCode': 404}
            }
            operation_name = 'GetObject'
            mock_provider.s3_block.aread_path.side_effect = ClientError(error_response, operation_name)

        return expected_full_path

    return _setup


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

    # Assert: Read back files and verify
    # Verify Metadata
    metadata_bytes = await s3_bucket_block.aread_path(METADATA_PATH)
    assert json.loads(metadata_bytes.decode("utf-8")) == valid_metadata.model_dump()

    # Verify Data
    data_bytes = await s3_bucket_block.aread_path(DATA_PATH)
    data_buffer: io.BytesIO = io.BytesIO(data_bytes)  # Add explicit type hint
    # Specify dtype explicitly based on model for robustness
    dtypes = {
        col: props.dtype.type if props.dtype else object
        for col, props in DynamicPrimitiveSourceModel.to_schema().columns.items()
    }
    loaded_df = pd.read_csv(
        data_buffer,
        compression="gzip",
        dtype=dtypes,
        keep_default_na=False,  # Match how data is loaded in tasks
        na_values=[""],  # Standard Python list with empty string
    )
    # Use check_dtype=True for stricter comparison, requires careful dtype handling
    pd.testing.assert_frame_equal(loaded_df, valid_agg_data, check_dtype=True)


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
    metadata.total_products_count = 10
    force_reprocess = False
    expected_dates = available_dates

    # Act
    with caplog.at_level(logging.WARNING):  # Use logging.WARNING
        result = determine_date_range_to_process.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == expected_dates
    mock_provider.list_available_keys.assert_called_once()
    assert "Invalid last_processed_date 'invalid-date' in metadata" in caplog.text


# --- Test Cases for process_single_date_products ---


@pytest.mark.asyncio
async def test_process_only_new_products(
    mock_provider: MagicMock,
    empty_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    sample_daily_data_1: DataFrame[SepaAvgPriceProductModel],
    setup_mock_aread_path: Any,
):
    """Test processing a date with only new products starting from empty state."""
    # Arrange
    date_to_process = DateStr("2024-03-11")
    expected_full_path = setup_mock_aread_path(date_to_process, data=sample_daily_data_1)
    initial_data = empty_agg_data

    # Act
    result_df = await process_single_date_products(date_to_process, initial_data, mock_provider)

    # Assert
    mock_provider.s3_block.aread_path.assert_awaited_once_with(expected_full_path)
    assert len(result_df) == 3
    # Explicitly convert to list of strings before creating set
    assert set(result_df["source_id"].astype(str).tolist()) == {"001", "101", "102"}

    # Check details of a new product
    new_prod_101 = result_df[result_df["source_id"] == "101"].iloc[0]  # type: ignore
    # Cast values accessed from the row Series
    assert str(new_prod_101["productos_descripcion"]) == "New Prod 1"
    assert str(new_prod_101["first_shown_at"]) == date_to_process
    assert str(new_prod_101["stream_id"]) == _generate_argentina_product_stream_id("101")
    assert str(new_prod_101["source_type"]) == "argentina_sepa_product"


@pytest.mark.asyncio
async def test_process_only_existing_products(
    mock_provider: MagicMock,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],  # Has 001, 002
    sample_daily_data_1: DataFrame[SepaAvgPriceProductModel],  # Has 001, 101, 102
    setup_mock_aread_path: Any,
):
    """Test processing a date where only previously seen products appear."""
    # Arrange
    date_to_process = DateStr("2024-03-11")
    expected_full_path = setup_mock_aread_path(
        date_to_process, data=sample_daily_data_1[sample_daily_data_1["id_producto"] == "001"].copy()
    )
    initial_data = valid_agg_data.copy()  # Has 001, 002
    initial_length = len(initial_data)

    # Act
    result_df = await process_single_date_products(date_to_process, initial_data, mock_provider)

    # Assert
    mock_provider.s3_block.aread_path.assert_awaited_once_with(expected_full_path)
    assert len(result_df) == initial_length  # No new products added
    # Explicitly convert to list of strings before creating set
    assert set(result_df["source_id"].astype(str).tolist()) == {"001", "002"}

    # Verify existing product wasn't modified (check first_shown_at)
    existing_prod_001 = result_df[result_df["source_id"] == "001"].iloc[0]
    original_prod_001 = initial_data[initial_data["source_id"] == "001"].iloc[0]
    # Cast values accessed from the row Series
    assert str(existing_prod_001["first_shown_at"]) == str(original_prod_001["first_shown_at"])
    assert str(existing_prod_001["productos_descripcion"]) == str(
        original_prod_001["productos_descripcion"]
    )  # Should not update


@pytest.mark.asyncio
async def test_process_mix_new_and_existing(
    mock_provider: MagicMock,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],  # Has 001, 002
    sample_daily_data_1: DataFrame[SepaAvgPriceProductModel],  # Has 001, 101, 102
    setup_mock_aread_path: Any,
):
    """Test processing a date with a mix of new and existing products."""
    # Arrange
    date_to_process = DateStr("2024-03-11")
    expected_full_path = setup_mock_aread_path(date_to_process, data=sample_daily_data_1)
    initial_data = valid_agg_data.copy()
    initial_length = len(initial_data)

    # Act
    result_df = await process_single_date_products(date_to_process, initial_data, mock_provider)

    # Assert
    mock_provider.s3_block.aread_path.assert_awaited_once_with(expected_full_path)
    assert len(result_df) == initial_length + 2  # Added 101, 102
    # Explicitly convert to list of strings before creating set
    assert set(result_df["source_id"].astype(str).tolist()) == {"001", "002", "101", "102"}

    # Check new product details
    new_prod_102 = result_df[result_df["source_id"] == "102"].iloc[0]
    # Cast values accessed from the row Series
    assert str(new_prod_102["productos_descripcion"]) == "New Prod 2"
    assert str(new_prod_102["first_shown_at"]) == date_to_process

    # Check existing product wasn't modified
    existing_prod_001 = result_df[result_df["source_id"] == "001"].iloc[0]
    original_prod_001 = initial_data[initial_data["source_id"] == "001"].iloc[0]
    # Cast values accessed from the row Series
    assert str(existing_prod_001["first_shown_at"]) == str(original_prod_001["first_shown_at"])


@pytest.mark.asyncio
async def test_process_duplicates_in_daily_file(
    mock_provider: MagicMock,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],  # Has 001, 002
    sample_daily_data_2: DataFrame[SepaAvgPriceProductModel],  # Has 102 (dup), 103 (dup), 104
    setup_mock_aread_path: Any,
):
    """Test processing a daily file containing duplicate product IDs."""
    # Arrange
    date_to_process = DateStr("2024-03-12")
    expected_full_path = setup_mock_aread_path(date_to_process, data=sample_daily_data_2)
    initial_data = valid_agg_data.copy()
    initial_length = len(initial_data)

    # Act
    result_df = await process_single_date_products(date_to_process, initial_data, mock_provider)

    # Assert
    mock_provider.s3_block.aread_path.assert_awaited_once_with(expected_full_path)
    # Should add 102, 103, 104 only once each
    assert len(result_df) == initial_length + 3
    # Explicitly convert to list of strings before creating set
    assert set(result_df["source_id"].astype(str).tolist()) == {"001", "002", "102", "103", "104"}

    # Check details of product 103 (added once)
    new_prod_103 = result_df[result_df["source_id"] == "103"]
    assert len(new_prod_103) == 1
    # Cast values accessed from the row Series (via .iloc[0])
    assert str(new_prod_103.iloc[0]["productos_descripcion"]) == "New Prod 3"  # Takes first description
    assert str(new_prod_103.iloc[0]["first_shown_at"]) == date_to_process


@pytest.mark.asyncio
async def test_process_missing_daily_file(
    mock_provider: MagicMock,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    caplog: LogCaptureFixture,
    setup_mock_aread_path: Any,
):
    """Test processing when the daily product file is missing."""
    # Arrange
    date_to_process = DateStr("2024-03-15")
    expected_full_path = setup_mock_aread_path(date_to_process, data=None)
    initial_data = valid_agg_data.copy()
    initial_length = len(initial_data)

    # Act
    with caplog.at_level(logging.WARNING):
        result_df = await process_single_date_products(date_to_process, initial_data, mock_provider)

    # Assert
    mock_provider.s3_block.aread_path.assert_awaited_once_with(expected_full_path)
    assert len(result_df) == initial_length  # Should return original data
    pd.testing.assert_frame_equal(result_df, initial_data)
    # Check for the WARNING message when ClientError(NoSuchKey/404) is caught
    assert f"Product averages file not found for date: {date_to_process}. Skipping date." in caplog.text
    # Ensure the ERROR message is NOT present for this specific case
    assert f"Unexpected error loading daily data for {date_to_process}" not in caplog.text
    # Ensure the subsequent WARNING about returning current data is present
    assert f"Failed to load or parse daily data for {date_to_process}. Returning current aggregated data." in caplog.text


@pytest.mark.asyncio
async def test_process_invalid_records_in_daily_file(
    mock_provider: MagicMock,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    sample_daily_data_invalid: pd.DataFrame,  # Raw DF with invalid records
    caplog: LogCaptureFixture,
    setup_mock_aread_path: Any,
):
    """Test processing a daily file with invalid product records (missing/empty IDs)."""
    # Arrange
    date_to_process = DateStr("2024-03-13")
    expected_full_path = setup_mock_aread_path(date_to_process, data=sample_daily_data_invalid)
    initial_data = valid_agg_data.copy()
    initial_length = len(initial_data)

    # Act
    with caplog.at_level(logging.WARNING):
        result_df = await process_single_date_products(date_to_process, initial_data, mock_provider)

    # Assert
    mock_provider.s3_block.aread_path.assert_awaited_once_with(expected_full_path)
    # Should add only valid new products (105, 106)
    assert len(result_df) == initial_length + 2
    # Explicitly convert to list of strings before creating set
    assert set(result_df["source_id"].astype(str).tolist()) == {"001", "002", "105", "106"}

    # Check logs for warnings about filtered records (new behavior after refactor)
    assert f"Filtered out 2 records with invalid 'id_producto' for date {date_to_process}" in caplog.text
    # Ensure the old message is *not* present
    assert "Skipping record with" not in caplog.text  # This might still appear if description is missing

    # Check details of successfully added product
    new_prod_105 = result_df[result_df["source_id"] == "105"].iloc[0]
    # Cast values accessed from the row Series
    assert str(new_prod_105["productos_descripcion"]) == "Valid New 5"
    assert str(new_prod_105["first_shown_at"]) == date_to_process
