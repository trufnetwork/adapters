"""
Shared helper functions for Argentina-related tests.
"""

import gzip
import io
import json
from typing import Any

from mypy_boto3_s3 import S3Client
import pandas as pd
from pandera.typing import DataFrame
from prefect_aws import S3Bucket  # type: ignore
from tsn_adapters.tasks.argentina.models import ArgentinaProductStateMetadata


# --- S3 Interaction Helpers ---


def upload_to_s3(s3_block: S3Bucket, path: str, content: bytes):
    """Helper to upload bytes content to the mock S3 bucket."""
    # Assumes s3_block._boto_client is attached in the test fixture
    s3_block._boto_client.put_object(Bucket=s3_block.bucket_name, Key=path, Body=content)  # type: ignore


def upload_df_to_s3_csv_gz(s3_block: S3Bucket, path: str, df: pd.DataFrame):
    """Helper to upload a DataFrame as gzipped CSV to mock S3."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
        # Ensure consistent encoding
        df.to_csv(io.TextIOWrapper(gz_file, "utf-8"), index=False, encoding="utf-8")
    buffer.seek(0)
    upload_to_s3(s3_block, path, buffer.getvalue())


def upload_metadata_to_s3(s3_block: S3Bucket, path: str, metadata: ArgentinaProductStateMetadata):
    """Helper to upload metadata model as JSON to mock S3."""
    metadata_json = metadata.model_dump_json()
    upload_to_s3(s3_block, path, metadata_json.encode("utf-8"))


def read_s3_metadata(s3_block: S3Bucket, path: str) -> ArgentinaProductStateMetadata:
    """Helper to read and parse metadata JSON from mock S3."""
    s3_client: S3Client = s3_block._get_s3_client()
    obj = s3_client.get_object(Bucket=s3_block.bucket_name, Key=path)  # type: ignore
    metadata_bytes = obj["Body"].read()
    return ArgentinaProductStateMetadata.model_validate(json.loads(metadata_bytes.decode("utf-8")))


def read_s3_csv_gz(s3_block: S3Bucket, path: str) -> pd.DataFrame:
    """Helper to read a gzipped CSV from mock S3 into a DataFrame."""
    s3_client: S3Client = s3_block._get_s3_client() # type: ignore
    try:
        obj = s3_client.get_object(Bucket=s3_block.bucket_name, Key=path)
        body = obj.get('Body')
        csv_bytes: bytes = body.read() if body else b'' # type: ignore
    except s3_client.exceptions.NoSuchKey: # type: ignore
        # Handle file not found explicitly
        # This case might be handled by the caller, but good to be explicit
        # Depending on usage, might return empty DF or re-raise
        # For load_aggregation_state, empty DF is expected if file missing
        return pd.DataFrame()
    except AttributeError:
        csv_bytes = b''

    if not csv_bytes:
        return pd.DataFrame() # Return empty DataFrame if no content

    buffer = io.BytesIO(csv_bytes)
    # Use pandas read_csv with gzip compression
    try:
        # Specify dtype=str to prevent pandas from inferring numeric types for IDs
        # Alternatively, specify dtypes based on a schema if available
        return pd.read_csv(buffer, compression='gzip', dtype=str)
    except Exception as e:
        # Catch potential pandas parsing errors or other read errors
        raise IOError(f"Failed to read or parse gzipped CSV from {path}: {e}") from e


# --- Data Generation Helpers ---


def make_metadata(**overrides: Any) -> ArgentinaProductStateMetadata:
    """
    Factory function to create ArgentinaProductStateMetadata instances with optional overrides.

    Args:
        **overrides: Keyword arguments to override default metadata fields.

    Returns:
        An instance of ArgentinaProductStateMetadata.
    """
    defaults = {
        "last_aggregation_processed_date": "1970-01-01",
        "last_product_deployment_date": "1970-01-01",
        "last_insertion_processed_date": "1970-01-01",
        "total_products_count": 0,
    }
    # Ensure overrides only contain valid keys for the model
    valid_keys = ArgentinaProductStateMetadata.model_fields.keys()
    filtered_overrides = {k: v for k, v in overrides.items() if k in valid_keys}
    # Merge defaults and overrides
    data = {**defaults, **filtered_overrides}
    return ArgentinaProductStateMetadata(**data)


# --- Constants (optional, can be defined here or in tests) ---
# Example: TEST_BUCKET_NAME = "test-my-bucket"
