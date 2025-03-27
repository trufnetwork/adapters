"""
Unit tests for Argentina SEPA product aggregation tasks.
"""

import gzip
import io
import json
from typing import Any, Generator

from moto import mock_aws
import pandas as pd
from pandera.typing import DataFrame
from prefect_aws import S3Bucket
from pydantic_core import SchemaError
import pytest

from tsn_adapters.tasks.argentina.models.aggregate_products_models import (
    DynamicPrimitiveSourceModel,
    ProductAggregationMetadata,
)
from tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks import load_aggregation_state

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
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1" # Default region for moto

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
        yield s3_block # Use yield to ensure cleanup if needed, though moto handles it

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
    return b'{"last_processed_date": "2024-03-11", "total_products_count": "abc"}' # count is not int

@pytest.fixture
def corrupted_metadata_json() -> bytes:
    """Corrupted (non-parseable) JSON bytes."""
    return b'{"last_processed_date": "2024-03-12", total_products_count: 50' # Missing closing brace

@pytest.fixture
def valid_agg_data() -> DataFrame[DynamicPrimitiveSourceModel]:
    """Valid aggregated data DataFrame."""
    df = pd.DataFrame({
        "stream_id": ["arg_sepa_prod_001", "arg_sepa_prod_002"],
        "source_id": ["001", "002"],
        "source_type": ["argentina_sepa_product", "argentina_sepa_product"],
        "productos_descripcion": ["Test Product 1", "Test Product 2"],
        "first_shown_at": ["2024-03-01", "2024-03-05"],
    })
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
    df = pd.DataFrame({
        "stream_id": ["arg_sepa_prod_003"],
        "source_id": ["003"],
        # Missing source_type
        "productos_descripcion": ["Test Product 3"],
        "first_shown_at": ["2024-03-08"],
    })
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
    pd.testing.assert_frame_equal(loaded_data, valid_agg_data, check_dtype=False) # Dtype check can be tricky

@pytest.mark.asyncio
async def test_load_state_neither_exist(s3_bucket_block: S3Bucket):
    """Test loading state when neither file exists."""
    # Arrange: No files uploaded

    # Act
    loaded_data, loaded_metadata = await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)

    # Assert
    assert loaded_metadata == ProductAggregationMetadata() # Defaults
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
    assert loaded_metadata == ProductAggregationMetadata() # Defaults
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
    with pytest.raises(Exception): # Pydantic's ValidationError inherits from Exception
        await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)

@pytest.mark.asyncio
async def test_load_state_corrupted_metadata_json(
    s3_bucket_block: S3Bucket,
    corrupted_metadata_json: bytes,
    valid_agg_data_csv_gz: bytes,
):
    """Test loading state with corrupted (unparseable) metadata JSON."""
    # Arrange: Upload corrupted metadata JSON and valid data
    await s3_bucket_block.awrite_path(METADATA_PATH, corrupted_metadata_json)
    await s3_bucket_block.awrite_path(DATA_PATH, valid_agg_data_csv_gz)

    # Act & Assert
    with pytest.raises(json.JSONDecodeError):
        await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)

@pytest.mark.asyncio
async def test_load_state_invalid_data_schema(
    s3_bucket_block: S3Bucket,
    valid_metadata_json: bytes,
    invalid_agg_data_csv_gz: bytes,
):
    """Test loading state with data that fails Pandera validation."""
    # Arrange: Upload valid metadata and invalid data (schema mismatch)
    await s3_bucket_block.awrite_path(METADATA_PATH, valid_metadata_json)
    await s3_bucket_block.awrite_path(DATA_PATH, invalid_agg_data_csv_gz)

    # Act & Assert
    with pytest.raises(Exception):
        await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)


@pytest.mark.asyncio
async def test_load_state_corrupted_data_file(
    s3_bucket_block: S3Bucket,
    valid_metadata_json: bytes,
    corrupted_agg_data_csv_gz: bytes,
):
    """Test loading state with a corrupted (non-CSV) data file."""
    # Arrange: Upload valid metadata and corrupted data
    await s3_bucket_block.awrite_path(METADATA_PATH, valid_metadata_json)
    await s3_bucket_block.awrite_path(DATA_PATH, corrupted_agg_data_csv_gz)

    # Act & Assert
    with pytest.raises(Exception) as excinfo: # Catch broader errors during read_csv
        await load_aggregation_state(s3_bucket_block, base_path=BASE_PATH)