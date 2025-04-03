"""
Shared helper functions for Argentina-related tests.
"""

import io

from mypy_boto3_s3 import S3Client
import pandas as pd
from prefect_aws import S3Bucket  # type: ignore

# --- S3 Interaction Helpers ---


def upload_to_s3(s3_block: S3Bucket, path: str, content: bytes):
    """Helper to upload bytes content to the mock S3 bucket."""
    # Assumes s3_block._boto_client is attached in the test fixture
    s3_block._boto_client.put_object(Bucket=s3_block.bucket_name, Key=path, Body=content)  # type: ignore


def upload_df_to_s3_csv_zip(s3_block: S3Bucket, path: str, df: pd.DataFrame):
    """Helper to upload a DataFrame as zipped CSV to mock S3."""
    buffer = io.BytesIO()
    # Write DataFrame to the binary buffer, letting pandas handle zipping
    df.to_csv(buffer, index=False, encoding="utf-8", compression="zip")
    buffer.seek(0)
    # Upload the raw bytes from the buffer
    upload_to_s3(s3_block, path, buffer.getvalue())


def read_s3_csv_zip(s3_block: S3Bucket, path: str) -> pd.DataFrame:
    """Helper to read a zipped CSV from mock S3 into a DataFrame."""
    s3_client: S3Client = s3_block._get_s3_client()  # type: ignore
    try:
        obj = s3_client.get_object(Bucket=s3_block.bucket_name, Key=path)
        body = obj.get("Body")
        csv_bytes: bytes = body.read() if body else b""  # type: ignore
    except s3_client.exceptions.NoSuchKey:  # type: ignore
        # Handle file not found explicitly
        # This case might be handled by the caller, but good to be explicit
        # Depending on usage, might return empty DF or re-raise
        # For load_aggregation_state, empty DF is expected if file missing
        return pd.DataFrame()
    except AttributeError:
        csv_bytes = b""

    if not csv_bytes:
        return pd.DataFrame()  # Return empty DataFrame if no content

    buffer = io.BytesIO(csv_bytes)
    # Use pandas read_csv with zip compression
    try:
        # Specify dtype=str to prevent pandas from inferring numeric types for IDs
        # Alternatively, specify dtypes based on a schema if available
        return pd.read_csv(buffer, compression="zip", dtype=str)
    except Exception as e:
        # Catch potential pandas parsing errors or other read errors
        raise OSError(f"Failed to read or parse zipped CSV from {path}: {e}") from e
