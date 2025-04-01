"""
Unit tests for Argentina SEPA product aggregation tasks.
"""

from collections.abc import Generator
import gzip
import io
import json
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from _pytest.logging import LogCaptureFixture
from botocore.exceptions import ClientError  # type: ignore
from moto import mock_aws
from mypy_boto3_s3 import S3Client
import pandas as pd
from pandera.errors import SchemaError
from pandera.typing import DataFrame
from prefect_aws import S3Bucket  # type: ignore
from pydantic import ValidationError
from pydantic_core import SchemaError
import pytest
from tests.argentina.helpers import (
    make_metadata,
    read_s3_csv_gz,
    read_s3_metadata,
    upload_df_to_s3_csv_gz,
    upload_metadata_to_s3,
    upload_to_s3,
)

from tsn_adapters.tasks.argentina.models.aggregate_products_models import (
    ArgentinaProductStateMetadata,
    DynamicPrimitiveSourceModel,
)
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider.product_averages import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks import (
    _generate_argentina_product_stream_id,  # type: ignore
    create_empty_aggregated_data,
    determine_aggregation_dates,
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
        # Instantiate the Prefect block
        s3_block = S3Bucket(bucket_name=TEST_BUCKET_NAME)
        s3_client: S3Client = s3_block._get_s3_client()
        s3_client.create_bucket(Bucket=TEST_BUCKET_NAME)
        # Attach the boto3 client for direct manipulation by helpers
        s3_block._boto_client = s3_client  # type: ignore
        yield s3_block  # Use yield to ensure cleanup if needed, though moto handles it


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
def empty_agg_data() -> DataFrame[DynamicPrimitiveSourceModel]:
    """Empty but validated aggregated data DataFrame."""
    df = create_empty_aggregated_data()
    # Validate against the correct model: DynamicPrimitiveSourceModel
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
            side_effect: An exception to raise instead of returning data.
        """
        expected_file_key = mock_provider.to_product_averages_file_key(date_str)
        expected_full_path = mock_provider.get_full_path(expected_file_key)

        # Create a new AsyncMock for aread_path if it doesn't exist or needs resetting
        if not hasattr(mock_provider.s3_block, "aread_path") or not isinstance(
            mock_provider.s3_block.aread_path, AsyncMock
        ):
            mock_provider.s3_block.aread_path = AsyncMock()

        # Configure the mock based on the path
        config = {}
        if side_effect:
            config["side_effect"] = side_effect
        elif data is not None:
            # Convert DataFrame to gzipped CSV bytes for the mock return value
            buffer = io.BytesIO()
            with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
                data.to_csv(io.TextIOWrapper(gz_file, "utf-8"), index=False, encoding="utf-8")
            config["return_value"] = buffer.getvalue()
        else:  # Assume file not found if no data and no specific error
            config["side_effect"] = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        # Use a side_effect function to handle path-specific mocking
        original_side_effect = mock_provider.s3_block.aread_path.side_effect
        side_effect_map = getattr(original_side_effect, "_path_map", {}) if callable(original_side_effect) else {}
        side_effect_map[expected_full_path] = config

        async def path_specific_side_effect(path: str):
            if path in side_effect_map:
                config = side_effect_map[path]
                if "side_effect" in config:
                    raise config["side_effect"]
                return config.get("return_value")
            # Default behavior if path not mocked (e.g., raise not found)
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

        # Store the map for future calls to _setup
        path_specific_side_effect._path_map = side_effect_map  # type: ignore
        mock_provider.s3_block.aread_path.side_effect = path_specific_side_effect

    return _setup


# --- Test Cases for load_aggregation_state ---


@pytest.mark.asyncio
async def test_load_aggregation_state_success(
    s3_bucket_block: S3Bucket,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
):
    """Test successful loading of existing metadata and data."""
    # Arrange
    # Use factory for metadata
    initial_metadata = make_metadata(last_aggregation_processed_date="2024-03-10", total_products_count=2)
    # Use helpers to upload
    upload_metadata_to_s3(s3_bucket_block, METADATA_PATH, initial_metadata)
    upload_df_to_s3_csv_gz(s3_bucket_block, DATA_PATH, valid_agg_data)

    # Act
    loaded_data, loaded_metadata = await load_aggregation_state.fn(s3_block=s3_bucket_block, base_path=BASE_PATH)

    # Assert
    assert loaded_metadata == initial_metadata
    pd.testing.assert_frame_equal(loaded_data, valid_agg_data)


@pytest.mark.asyncio
async def test_load_aggregation_state_no_files(
    s3_bucket_block: S3Bucket, empty_agg_data: DataFrame[DynamicPrimitiveSourceModel], caplog: LogCaptureFixture
):
    """Test loading when metadata and data files are missing (returns defaults/empty)."""
    # Arrange: No files uploaded
    caplog.set_level(logging.WARNING)
    default_metadata = make_metadata()  # Default values

    # Act
    loaded_data, loaded_metadata = await load_aggregation_state.fn(s3_block=s3_bucket_block, base_path=BASE_PATH)

    # Assert
    assert loaded_metadata == default_metadata
    pd.testing.assert_frame_equal(loaded_data, empty_agg_data)
    assert "Metadata file not found. Using defaults" in caplog.text
    assert "Data file not found. Returning empty DataFrame" in caplog.text


@pytest.mark.asyncio
async def test_load_aggregation_state_only_metadata_exists(
    s3_bucket_block: S3Bucket, empty_agg_data: DataFrame[DynamicPrimitiveSourceModel], caplog: LogCaptureFixture
):
    """Test loading when only metadata exists (returns metadata and empty data)."""
    # Arrange
    caplog.set_level(logging.WARNING)
    initial_metadata = make_metadata(last_aggregation_processed_date="2024-01-01", total_products_count=5)
    upload_metadata_to_s3(s3_bucket_block, METADATA_PATH, initial_metadata)
    # Data file does not exist

    # Act
    loaded_data, loaded_metadata = await load_aggregation_state.fn(s3_block=s3_bucket_block, base_path=BASE_PATH)

    # Assert
    assert loaded_metadata == initial_metadata
    pd.testing.assert_frame_equal(loaded_data, empty_agg_data)
    assert "Metadata file not found" not in caplog.text  # Metadata was found
    assert "Data file not found. Returning empty DataFrame" in caplog.text


@pytest.mark.asyncio
async def test_load_aggregation_state_only_data_exists(
    s3_bucket_block: S3Bucket, valid_agg_data: DataFrame[DynamicPrimitiveSourceModel], caplog: LogCaptureFixture
):
    """Test loading when only data exists (returns default metadata and data)."""
    # Arrange
    caplog.set_level(logging.WARNING)
    default_metadata = make_metadata()  # Expect defaults
    upload_df_to_s3_csv_gz(s3_bucket_block, DATA_PATH, valid_agg_data)
    # Metadata file does not exist

    # Act
    loaded_data, loaded_metadata = await load_aggregation_state.fn(s3_block=s3_bucket_block, base_path=BASE_PATH)

    # Assert
    assert loaded_metadata == default_metadata
    pd.testing.assert_frame_equal(loaded_data, valid_agg_data)
    assert "Metadata file not found. Using defaults" in caplog.text
    assert "Data file not found" not in caplog.text  # Data was found


@pytest.mark.asyncio
async def test_load_aggregation_state_invalid_metadata_json(
    s3_bucket_block: S3Bucket, valid_agg_data: DataFrame[DynamicPrimitiveSourceModel]
):
    """Test loading state with invalid metadata JSON."""
    # Arrange
    # Use helper to upload valid data
    upload_df_to_s3_csv_gz(s3_bucket_block, DATA_PATH, valid_agg_data)
    # Upload invalid json bytes directly
    invalid_json_bytes = b'{"last_aggregation_processed_date": "2024-03-11", "total_products_count": "abc"}'
    upload_to_s3(s3_bucket_block, METADATA_PATH, invalid_json_bytes)

    # Act & Assert
    # Expect Pydantic ValidationError during model parsing
    with pytest.raises(ValidationError):
        await load_aggregation_state.fn(s3_block=s3_bucket_block, base_path=BASE_PATH)


@pytest.mark.asyncio
async def test_load_aggregation_state_corrupted_metadata_json(
    s3_bucket_block: S3Bucket, valid_agg_data: DataFrame[DynamicPrimitiveSourceModel]
):
    """Test loading state with corrupted (non-parseable) metadata JSON."""
    # Arrange
    upload_df_to_s3_csv_gz(s3_bucket_block, DATA_PATH, valid_agg_data)
    corrupted_json_bytes = b'{"last_aggregation_processed_date": "2024-03-12", total_products_count: 50'
    upload_to_s3(s3_bucket_block, METADATA_PATH, corrupted_json_bytes)

    # Act & Assert
    with pytest.raises(json.JSONDecodeError):
        await load_aggregation_state.fn(s3_block=s3_bucket_block, base_path=BASE_PATH)


@pytest.mark.asyncio
async def test_load_aggregation_state_invalid_data_schema(
    s3_bucket_block: S3Bucket,
):
    """Test loading state with data CSV that fails Pandera validation."""
    # Arrange
    initial_metadata = make_metadata()
    upload_metadata_to_s3(s3_bucket_block, METADATA_PATH, initial_metadata)
    # Create invalid data (missing required column)
    invalid_df = pd.DataFrame(
        {
            "stream_id": ["arg_sepa_prod_003"],
            "source_id": ["003"],
            # Missing source_type
            "productos_descripcion": ["Test Product 3"],
            "first_shown_at": ["2024-03-08"],
        }
    )
    upload_df_to_s3_csv_gz(s3_bucket_block, DATA_PATH, invalid_df)

    # Act & Assert
    # Expect SchemaError from Pandera validation
    with pytest.raises(Exception):
        await load_aggregation_state.fn(s3_block=s3_bucket_block, base_path=BASE_PATH)


@pytest.mark.asyncio
async def test_load_aggregation_state_corrupted_data_csv(
    s3_bucket_block: S3Bucket,
):
    """Test loading state with corrupted (non-CSV) data file."""
    # Arrange
    initial_metadata = make_metadata()
    upload_metadata_to_s3(s3_bucket_block, METADATA_PATH, initial_metadata)
    corrupted_bytes = gzip.compress(b"this is not csv")
    upload_to_s3(s3_bucket_block, DATA_PATH, corrupted_bytes)

    # Act & Assert
    with pytest.raises(Exception):
        await load_aggregation_state.fn(s3_block=s3_bucket_block, base_path=BASE_PATH)


# --- Test Cases for save_aggregation_state ---


@pytest.mark.asyncio
async def test_save_aggregation_state_success(
    s3_bucket_block: S3Bucket,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
):
    """Test successfully saving metadata and data."""
    # Arrange
    metadata_to_save = make_metadata(last_aggregation_processed_date="2024-03-15", total_products_count=10)

    # Act
    await save_aggregation_state.fn(
        s3_block=s3_bucket_block,
        aggregated_data=valid_agg_data,
        metadata=metadata_to_save,
        base_path=BASE_PATH,
    )

    # Assert
    # Use helpers to read back and verify
    saved_metadata = read_s3_metadata(s3_bucket_block, METADATA_PATH)
    saved_data = read_s3_csv_gz(s3_bucket_block, DATA_PATH)

    assert saved_metadata == metadata_to_save
    # Validate DataFrame structure after reading (read_csv might change dtypes)
    validated_saved_data = DynamicPrimitiveSourceModel.validate(saved_data, lazy=True)
    pd.testing.assert_frame_equal(validated_saved_data, valid_agg_data)


@pytest.mark.asyncio
async def test_save_aggregation_state_no_prior_files(
    s3_bucket_block: S3Bucket,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
):
    """Test saving state when no files exist initially."""
    # Arrange
    metadata_to_save = make_metadata(total_products_count=len(valid_agg_data))
    # No files exist initially

    # Act
    await save_aggregation_state.fn(
        s3_block=s3_bucket_block,
        aggregated_data=valid_agg_data,
        metadata=metadata_to_save,
        base_path=BASE_PATH,
    )

    # Assert: Check files were created and content is correct
    saved_metadata = read_s3_metadata(s3_bucket_block, METADATA_PATH)
    saved_data = read_s3_csv_gz(s3_bucket_block, DATA_PATH)
    assert saved_metadata == metadata_to_save
    validated_saved_data = DynamicPrimitiveSourceModel.validate(saved_data, lazy=True)
    pd.testing.assert_frame_equal(validated_saved_data, valid_agg_data)


@pytest.mark.asyncio
async def test_save_aggregation_state_overwrites_existing(
    s3_bucket_block: S3Bucket,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    empty_agg_data: DataFrame[DynamicPrimitiveSourceModel],
):
    """Test that saving overwrites existing metadata and data files."""
    # Arrange: Upload initial different state
    initial_metadata = make_metadata(last_aggregation_processed_date="2023-01-01", total_products_count=0)
    upload_metadata_to_s3(s3_bucket_block, METADATA_PATH, initial_metadata)
    upload_df_to_s3_csv_gz(s3_bucket_block, DATA_PATH, empty_agg_data)

    # New state to save
    metadata_to_save = make_metadata(
        last_aggregation_processed_date="2024-03-15", total_products_count=len(valid_agg_data)
    )

    # Act
    await save_aggregation_state.fn(
        s3_block=s3_bucket_block,
        aggregated_data=valid_agg_data,  # Save the valid data
        metadata=metadata_to_save,
        base_path=BASE_PATH,
    )

    # Assert: Check the files now contain the new state
    saved_metadata = read_s3_metadata(s3_bucket_block, METADATA_PATH)
    saved_data = read_s3_csv_gz(s3_bucket_block, DATA_PATH)
    assert saved_metadata == metadata_to_save  # Metadata should be updated
    validated_saved_data = DynamicPrimitiveSourceModel.validate(saved_data, lazy=True)
    pd.testing.assert_frame_equal(validated_saved_data, valid_agg_data)  # Data should be updated


# --- Test Cases for determine_date_range_to_process ---


def test_determine_dates_no_prior_state(mock_provider: MagicMock):
    """Test when no prior state exists (default metadata), should return all dates."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11"), DateStr("2024-03-12")]
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ArgentinaProductStateMetadata()
    force_reprocess = False

    # Act
    # Call the synchronous task's function directly using .fn()
    result = determine_aggregation_dates.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == available_dates
    # Use assert_called_once for synchronous mock
    mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_force_reprocess(mock_provider: MagicMock):
    """Test when force_reprocess is True, should return all dates."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11"), DateStr("2024-03-12")]
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ArgentinaProductStateMetadata(last_aggregation_processed_date="2024-03-11", total_products_count=10)
    force_reprocess = True

    # Act
    result = determine_aggregation_dates.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == available_dates
    mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_prior_state_exists(mock_provider: MagicMock):
    """Test resuming from a previous state (last_processed_date)."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11"), DateStr("2024-03-12"), DateStr("2024-03-13")]
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ArgentinaProductStateMetadata(last_aggregation_processed_date="2024-03-11", total_products_count=10)
    force_reprocess = False
    expected_dates = [DateStr("2024-03-12"), DateStr("2024-03-13")]

    # Act
    result = determine_aggregation_dates.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == expected_dates
    mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_no_new_dates(mock_provider: MagicMock):
    """Test when prior state exists, but no new dates are available."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11")]
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ArgentinaProductStateMetadata(last_aggregation_processed_date="2024-03-11", total_products_count=10)
    force_reprocess = False
    expected_dates: list[DateStr] = []

    # Act
    result = determine_aggregation_dates.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == expected_dates
    mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_gaps_in_available_dates(mock_provider: MagicMock):
    """Test correct handling when there are gaps in available dates."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-12"), DateStr("2024-03-13")]
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ArgentinaProductStateMetadata(last_aggregation_processed_date="2024-03-10", total_products_count=5)
    force_reprocess = False
    expected_dates = [DateStr("2024-03-12"), DateStr("2024-03-13")]

    # Act
    result = determine_aggregation_dates.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == expected_dates
    mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_no_available_dates(mock_provider: MagicMock):
    """Test when the provider returns an empty list of dates."""
    # Arrange
    available_dates: list[DateStr] = []
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ArgentinaProductStateMetadata()
    force_reprocess = False
    expected_dates: list[DateStr] = []

    # Act
    result = determine_aggregation_dates.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == expected_dates
    mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_invalid_metadata_date(mock_provider: MagicMock, caplog: LogCaptureFixture):
    """Test when metadata contains an invalid date format, should process all."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11")]
    mock_provider.list_available_keys.return_value = available_dates
    metadata = ArgentinaProductStateMetadata()
    metadata.last_aggregation_processed_date = "invalid-date"
    metadata.total_products_count = 10
    force_reprocess = False
    expected_dates = available_dates

    # Act
    with caplog.at_level(logging.WARNING):  # Use logging.WARNING
        result = determine_aggregation_dates.fn(mock_provider, metadata, force_reprocess)

    # Assert
    assert result == expected_dates
    mock_provider.list_available_keys.assert_called_once()
    assert "Invalid last_aggregation_processed_date 'invalid-date' in metadata" in caplog.text


# --- Test Cases for process_single_date_products ---


@pytest.mark.asyncio
async def test_process_single_date_new_products(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    empty_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    sample_daily_data_1: DataFrame[SepaAvgPriceProductModel],
):
    """Test processing a date when aggregated data is initially empty."""
    # Arrange
    date_str = "2024-03-11"
    setup_mock_aread_path(date_str, data=sample_daily_data_1)

    # Act
    result_df = await process_single_date_products.fn(
        date_to_process=DateStr(date_str),
        current_aggregated_data=empty_agg_data,
        product_averages_provider=mock_provider,
    )

    # Assert
    # Expect all products from daily data to be added
    assert len(result_df) == len(sample_daily_data_1)
    assert set(result_df["source_id"]) == set(sample_daily_data_1["id_producto"])
    # Check stream_id generation (simple check)
    assert result_df["stream_id"].iloc[0].startswith("st")
    assert all(result_df["first_shown_at"] == date_str)


@pytest.mark.asyncio
async def test_process_single_date_existing_and_new(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    sample_daily_data_1: DataFrame[SepaAvgPriceProductModel],
):
    """Test processing a date with a mix of existing and new products."""
    # Arrange
    date_str = "2024-03-11"
    setup_mock_aread_path(date_str, data=sample_daily_data_1)
    # valid_agg_data has IDs '001', '002'
    # sample_daily_data_1 has IDs '001', '101', '102'

    # Act
    result_df = await process_single_date_products.fn(
        date_to_process=DateStr(date_str),
        current_aggregated_data=valid_agg_data,
        product_averages_provider=mock_provider,
    )

    # Assert
    # Expect initial count + new products ('101', '102')
    expected_count = len(valid_agg_data) + 2
    assert len(result_df) == expected_count
    assert set(result_df["source_id"]) == {"001", "002", "101", "102"}
    # Check first_shown_at for new products
    assert result_df[result_df["source_id"] == "101"]["first_shown_at"].iloc[0] == date_str
    assert result_df[result_df["source_id"] == "102"]["first_shown_at"].iloc[0] == date_str
    # Check first_shown_at for existing product (should not change)
    assert (
        result_df[result_df["source_id"] == "001"]["first_shown_at"].iloc[0]
        == valid_agg_data[valid_agg_data["source_id"] == "001"]["first_shown_at"].iloc[0]
    )


@pytest.mark.asyncio
async def test_process_single_date_duplicates_in_daily(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    sample_daily_data_2: DataFrame[SepaAvgPriceProductModel],
):
    """Test processing with duplicate product IDs within the daily file."""
    # Arrange
    date_str = "2024-03-12"
    setup_mock_aread_path(date_str, data=sample_daily_data_2)
    # valid_agg_data has IDs '001', '002'
    # sample_daily_data_2 has IDs '102', '103', '103', '104'

    # Act
    result_df = await process_single_date_products.fn(
        date_to_process=DateStr(date_str),
        current_aggregated_data=valid_agg_data,
        product_averages_provider=mock_provider,
    )

    # Assert
    # Expect initial count + new unique products ('102', '103', '104')
    expected_count = len(valid_agg_data) + 3
    assert len(result_df) == expected_count
    assert set(result_df["source_id"]) == {"001", "002", "102", "103", "104"}
    # Check first_shown_at for the new ones
    assert result_df[result_df["source_id"] == "102"]["first_shown_at"].iloc[0] == date_str
    assert result_df[result_df["source_id"] == "103"]["first_shown_at"].iloc[0] == date_str
    assert result_df[result_df["source_id"] == "104"]["first_shown_at"].iloc[0] == date_str


@pytest.mark.asyncio
async def test_process_single_date_invalid_daily_records(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    sample_daily_data_invalid: DataFrame[SepaAvgPriceProductModel],
    caplog: LogCaptureFixture,
):
    """Test that invalid records (None ID, empty ID) in daily data are filtered out."""
    # Arrange
    date_str = "2024-03-13"
    caplog.set_level(logging.WARNING)
    # Use the unvalidated invalid data fixture
    setup_mock_aread_path(date_str, data=sample_daily_data_invalid)
    # valid_agg_data has IDs '001', '002'
    # sample_daily_data_invalid *should* only add '105', '106' after filtering

    # Act
    result_df = await process_single_date_products.fn(
        date_to_process=DateStr(date_str),
        current_aggregated_data=valid_agg_data,
        product_averages_provider=mock_provider,
    )

    # Assert
    # Expect initial count + valid new products ('105', '106')
    expected_count = len(valid_agg_data) + 2
    assert len(result_df) == expected_count
    assert set(result_df["source_id"]) == {"001", "002", "105", "106"}
    assert "Filtered out 2 records with invalid 'id_producto'" in caplog.text


@pytest.mark.asyncio
async def test_process_single_date_daily_file_not_found(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    caplog: LogCaptureFixture,
):
    """Test processing when the daily data file is not found (should return original data)."""
    # Arrange
    date_str = "2024-03-14"
    caplog.set_level(logging.WARNING)
    # Simulate file not found by not setting up data for the date
    setup_mock_aread_path(date_str, side_effect=ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject"))

    # Act
    result_df = await process_single_date_products.fn(
        date_to_process=DateStr(date_str),
        current_aggregated_data=valid_agg_data,
        product_averages_provider=mock_provider,
    )

    # Assert
    # Expect the original aggregated data to be returned unchanged
    pd.testing.assert_frame_equal(result_df, valid_agg_data)
    assert f"Product averages file not found for date: {date_str}. Skipping date." in caplog.text


@pytest.mark.asyncio
async def test_process_single_date_daily_file_load_error(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    caplog: LogCaptureFixture,
):
    """Test processing when loading the daily data file causes an S3 error."""
    # Arrange
    date_str = "2024-03-15"
    caplog.set_level(logging.ERROR)
    # Simulate a different S3 error
    s3_error = ClientError({"Error": {"Code": "InternalError"}}, "GetObject")
    setup_mock_aread_path(date_str, side_effect=s3_error)

    # Act
    result_df = await process_single_date_products.fn(
        date_to_process=DateStr(date_str),
        current_aggregated_data=valid_agg_data,
        product_averages_provider=mock_provider,
    )

    # Assert
    # Expect original data returned, and error logged
    pd.testing.assert_frame_equal(result_df, valid_agg_data)
    assert f"S3 Error loading product averages for {date_str}" in caplog.text
    assert "InternalError" in caplog.text


@pytest.mark.asyncio
async def test_process_single_date_daily_file_parse_error(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    valid_agg_data: DataFrame[DynamicPrimitiveSourceModel],
    caplog: LogCaptureFixture,
):
    """Test processing when the daily data file is corrupted (parse error)."""
    # Arrange
    date_str = "2024-03-16"
    caplog.set_level(logging.ERROR)
    # Simulate parse error by setting up corrupted data
    mock_provider.s3_block.aread_path = AsyncMock(return_value=gzip.compress(b"col1|col2\ninvalid,data"))

    # Act
    # Mock pd.read_csv within the task's context to raise ParserError
    with patch("tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks.pd.read_csv", side_effect=pd.errors.ParserError("Mock parse error")):
        result_df = await process_single_date_products.fn(
            date_to_process=DateStr(date_str),
            current_aggregated_data=valid_agg_data,
            product_averages_provider=mock_provider,
        )

    # Assert
    # Expect original data returned, and error logged
    pd.testing.assert_frame_equal(result_df, valid_agg_data)
    assert f"CSV parsing error for date {date_str}" in caplog.text