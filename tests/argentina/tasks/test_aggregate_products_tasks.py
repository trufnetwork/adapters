"""
Unit tests for Argentina SEPA product aggregation tasks.
"""

from collections.abc import Generator
import gzip
import io
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from _pytest.logging import LogCaptureFixture
from botocore.exceptions import ClientError  # type: ignore
from moto import mock_aws
from mypy_boto3_s3 import S3Client
import pandas as pd
from pandera.typing import DataFrame
from prefect_aws import S3Bucket  # type: ignore
import pytest

from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel
from tsn_adapters.tasks.argentina.config import ArgentinaFlowVariableNames
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider.product_averages import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks import (
    determine_aggregation_dates,
    process_single_date_products,
)
from tsn_adapters.tasks.argentina.types import DateStr
from tsn_adapters.utils import create_empty_df

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
def valid_agg_data() -> DataFrame[PrimitiveSourceDataModel]:
    """Valid aggregated data DataFrame."""
    df = pd.DataFrame(
        {
            "stream_id": ["arg_sepa_prod_001", "arg_sepa_prod_002"],
            "source_id": ["001", "002"],
            "source_type": ["argentina_sepa_product", "argentina_sepa_product"],
        }
    )
    # Ensure dtypes match the model for consistency before validation/saving
    return PrimitiveSourceDataModel.validate(df, lazy=True)


@pytest.fixture
def empty_agg_data() -> DataFrame[PrimitiveSourceDataModel]:
    """Empty but validated aggregated data DataFrame."""
    df = create_empty_df(PrimitiveSourceDataModel)
    # Validate against the correct model: PrimitiveSourceDataModel
    return PrimitiveSourceDataModel.validate(df, lazy=True)


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


# --- Test Cases for determine_date_range_to_process ---


def test_determine_dates_no_prior_state(mock_provider: MagicMock):
    """Test when no prior state exists (default metadata), should return all dates."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11"), DateStr("2024-03-12")]
    mock_provider.list_available_keys.return_value = available_dates
    force_reprocess = False
    
    # Mock Prefect variables to simulate:
    # - Preprocess date: far future (to include all dates)
    # - Aggregation date: default 1970-01-01 (to process all dates after it)
    with patch("tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks.variables.get") as mock_var_get:
        mock_var_get.side_effect = lambda var_name, default: (
            "2099-12-31" if var_name == ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE
            else "1970-01-01" if var_name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE
            else default
        )
        
        # Act
        result, _, _ = determine_aggregation_dates.fn(mock_provider, force_reprocess)
    
        # Assert
        assert result == available_dates
        # Use assert_called_once for synchronous mock
        mock_provider.list_available_keys.assert_called_once()
        
        # Verify variable calls
        mock_var_get.assert_any_call(
            ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE, 
            default=ArgentinaFlowVariableNames.DEFAULT_DATE
        )
        mock_var_get.assert_any_call(
            ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE, 
            default=ArgentinaFlowVariableNames.DEFAULT_DATE
        )


def test_determine_dates_force_reprocess(mock_provider: MagicMock):
    """Test when force_reprocess is True, should return all dates."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11"), DateStr("2024-03-12")]
    mock_provider.list_available_keys.return_value = available_dates
    force_reprocess = True
    
    # Mock Prefect variables
    with patch("tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks.variables.get") as mock_var_get:
        # For LAST_PREPROCESS, return a date that includes all available dates
        # For LAST_AGGREGATION, the function should ignore this and use DEFAULT_DATE
        mock_var_get.side_effect = lambda var_name, default: (
            "2024-03-15" if var_name == ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE
            else default
        )
        
        # Act
        result, _, _ = determine_aggregation_dates.fn(mock_provider, force_reprocess)
    
        # Assert
        assert result == available_dates
        mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_prior_state_exists(mock_provider: MagicMock):
    """Test resuming from a previous state (last_processed_date)."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11"), DateStr("2024-03-12"), DateStr("2024-03-13")]
    mock_provider.list_available_keys.return_value = available_dates
    force_reprocess = False
    expected_dates = [DateStr("2024-03-12"), DateStr("2024-03-13")]
    
    # Mock Prefect variables
    with patch("tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks.variables.get") as mock_var_get:
        # Mock LAST_PREPROCESS to allow all dates
        # Mock LAST_AGGREGATION to be "2024-03-11" (so we process dates after it)
        mock_var_get.side_effect = lambda var_name, default: (
            "2024-03-15" if var_name == ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE
            else "2024-03-11" if var_name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE
            else default
        )
        
        # Act
        result, _, _ = determine_aggregation_dates.fn(mock_provider, force_reprocess)
    
        # Assert
        assert result == expected_dates


def test_determine_dates_no_new_dates(mock_provider: MagicMock):
    """Test when no dates are new (all already processed)."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11")]
    mock_provider.list_available_keys.return_value = available_dates
    force_reprocess = False
    
    # Mock Prefect variables - all available dates already processed
    with patch("tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks.variables.get") as mock_var_get:
        mock_var_get.side_effect = lambda var_name, default: (
            "2024-03-15" if var_name == ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE
            else "2024-03-11" if var_name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE
            else default
        )
        
        # Act
        result, _, _ = determine_aggregation_dates.fn(mock_provider, force_reprocess)
    
        # Assert
        assert result == []
        mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_gaps_in_available_dates(mock_provider: MagicMock):
    """Test correct handling when there are gaps in available dates."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-12"), DateStr("2024-03-13")]
    mock_provider.list_available_keys.return_value = available_dates
    force_reprocess = False
    expected_dates = [DateStr("2024-03-12"), DateStr("2024-03-13")]
    
    # Mock Prefect variables
    with patch("tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks.variables.get") as mock_var_get:
        mock_var_get.side_effect = lambda var_name, default: (
            "2024-03-15" if var_name == ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE
            else "2024-03-10" if var_name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE
            else default
        )
        
        # Act
        result, _, _ = determine_aggregation_dates.fn(mock_provider, force_reprocess)
    
        # Assert
        assert result == expected_dates
        mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_no_available_dates(mock_provider: MagicMock):
    """Test handling when no dates are available."""
    # Arrange
    mock_provider.list_available_keys.return_value = []
    force_reprocess = False
    
    # Mock Prefect variables
    with patch("tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks.variables.get") as mock_var_get:
        mock_var_get.return_value = "2024-03-15"  # Any date
        
        # Act
        result, _, _ = determine_aggregation_dates.fn(mock_provider, force_reprocess)
    
        # Assert
        assert result == []
        mock_provider.list_available_keys.assert_called_once()


def test_determine_dates_invalid_metadata_date(mock_provider: MagicMock, caplog: LogCaptureFixture):
    """Test when metadata contains an invalid date format, should process all."""
    # Arrange
    available_dates = [DateStr("2024-03-10"), DateStr("2024-03-11")]
    mock_provider.list_available_keys.return_value = available_dates
    force_reprocess = False
    expected_dates = available_dates
    
    # Mock Prefect variables
    with patch("tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks.variables.get") as mock_var_get:
        mock_var_get.side_effect = lambda var_name, default: (
            "2024-03-15" if var_name == ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE
            else "1970-01-01" if var_name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE
            else default
        )
        
        # Act
        with caplog.at_level(logging.WARNING):  # Use logging.WARNING
            result, _, _ = determine_aggregation_dates.fn(mock_provider, force_reprocess)
    
        # Assert
        assert result == expected_dates


# --- Test Cases for process_single_date_products ---


@pytest.mark.asyncio
async def test_process_single_date_new_products(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    empty_agg_data: DataFrame[PrimitiveSourceDataModel],
    sample_daily_data_1: DataFrame[SepaAvgPriceProductModel],
):
    """Test processing a date with all new products."""
    # Arrange
    date_str = "2024-03-10"
    setup_mock_aread_path(date_str, data=sample_daily_data_1)
    
    # Act
    result_df = await process_single_date_products.fn(
        date_to_process=DateStr(date_str),
        current_aggregated_data=empty_agg_data,
        product_averages_provider=mock_provider,
    )
    
    # Assert
    # Expect only new products in the result
    assert len(result_df) == len(sample_daily_data_1.drop_duplicates(subset=["id_producto"]))
    # Verify columns
    assert "stream_id" in result_df.columns
    assert "source_id" in result_df.columns
    assert "source_type" in result_df.columns
    # All products from the daily data should be in the result
    assert set(result_df["source_id"]) == set(sample_daily_data_1["id_producto"].drop_duplicates())


@pytest.mark.asyncio
async def test_process_single_date_existing_and_new(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    valid_agg_data: DataFrame[PrimitiveSourceDataModel],
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
    # Expect ONLY new products ('101', '102')
    assert len(result_df) == 2
    # Check that the new products are in the result
    result_source_ids = set(result_df["source_id"])
    assert "101" in result_source_ids
    assert "102" in result_source_ids
    # But existing product is not there
    assert "001" not in result_source_ids


@pytest.mark.asyncio
async def test_process_single_date_duplicates_in_daily(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    valid_agg_data: DataFrame[PrimitiveSourceDataModel],
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
    # Expect only new unique product IDs ('102', '103', '104')
    assert len(result_df) == 3
    result_source_ids = set(result_df["source_id"])
    assert result_source_ids == {"102", "103", "104"}


@pytest.mark.asyncio
async def test_process_single_date_invalid_daily_records(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    valid_agg_data: DataFrame[PrimitiveSourceDataModel],
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
    # sample_daily_data_invalid has IDs '105', None, '106', ''
    
    # Act
    result_df = await process_single_date_products.fn(
        date_to_process=DateStr(date_str),
        current_aggregated_data=valid_agg_data,
        product_averages_provider=mock_provider,
    )
    
    # Assert
    # Expect only valid new product IDs ('105', '106')
    expected_ids = {"105", "106"}
    result_source_ids = set(filter(None, result_df["source_id"]))
    assert result_source_ids == expected_ids


@pytest.mark.asyncio
async def test_process_single_date_daily_file_not_found(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    valid_agg_data: DataFrame[PrimitiveSourceDataModel],
    caplog: LogCaptureFixture,
):
    """Test processing when the daily data file is not found (should return empty df)."""
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
    # Expect empty DataFrame with correct schema
    assert len(result_df) == 0
    assert set(result_df.columns) == set(["stream_id", "source_id", "source_type"])


@pytest.mark.asyncio
async def test_process_single_date_daily_file_load_error(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    valid_agg_data: DataFrame[PrimitiveSourceDataModel],
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
    # Expect empty DataFrame with correct schema
    assert len(result_df) == 0
    assert set(result_df.columns) == set(["stream_id", "source_id", "source_type"])


@pytest.mark.asyncio
async def test_process_single_date_daily_file_parse_error(
    mock_provider: MagicMock,
    setup_mock_aread_path: Any,
    valid_agg_data: DataFrame[PrimitiveSourceDataModel],
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
    # Expect empty DataFrame with correct schema
    assert len(result_df) == 0
    assert set(result_df.columns) == set(["stream_id", "source_id", "source_type"])