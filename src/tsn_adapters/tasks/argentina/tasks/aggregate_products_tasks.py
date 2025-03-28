"""
Prefect tasks for loading Argentina SEPA product aggregation state from S3.

Handles state loading (metadata JSON, aggregated products CSV) including
validation and default creation if files are missing.
"""

import io
import json
from typing import Any, TypeVar
from datetime import date, timedelta, datetime

from botocore.exceptions import ClientError  # type: ignore
import pandas as pd
from pandera.errors import SchemaError, SchemaDefinitionError
from pandera.typing import DataFrame
from prefect import task
from prefect_aws import S3Bucket  # type: ignore
import gzip # Add gzip import

from tsn_adapters.tasks.argentina.models.aggregate_products_models import (
    DynamicPrimitiveSourceModel,
    ProductAggregationMetadata,
)
from tsn_adapters.tasks.argentina.provider.product_averages import ProductAveragesProvider
from tsn_adapters.tasks.argentina.types import DateStr
from tsn_adapters.utils.logging import get_logger_safe

T = TypeVar("T")


# --- State Loading Helper Functions ---

def _is_client_error_not_found(exception: ClientError) -> bool:
    """Checks if a botocore ClientError is an S3 'Not Found' (404 or NoSuchKey)."""
    # Safely access potentially untyped response dictionary elements
    error_dict: dict[str, Any] = exception.response
    error_code = error_dict.get("Error", {}).get("Code", "")
    status_code_str = error_dict.get("ResponseMetadata", {}).get("HTTPStatusCode")
    status_code = int(status_code_str) if status_code_str is not None else None
    return error_code == "NoSuchKey" or status_code == 404


async def _load_metadata_from_s3(
    s3_block: S3Bucket, metadata_path: str
) -> ProductAggregationMetadata:
    """
    Loads ProductAggregationMetadata from JSON in S3.

    Returns default metadata if the file is not found (404/NoSuchKey).

    Args:
        s3_block: S3Bucket block for S3 access.
        metadata_path: Path to the metadata JSON file in the bucket.

    Returns:
        Loaded or default ProductAggregationMetadata.

    Raises:
        ClientError: For non-404 S3 errors.
        json.JSONDecodeError: For invalid JSON.
        Exception: For other unexpected errors.
    """
    logger = get_logger_safe(__name__)
    try:
        logger.info(f"Loading metadata: {metadata_path}")
        metadata_bytes = await s3_block.aread_path(metadata_path)
        metadata = ProductAggregationMetadata(**json.loads(metadata_bytes.decode("utf-8")))
        logger.info("Metadata loaded successfully.")
        return metadata
    except ClientError as e:
        if _is_client_error_not_found(e):
            default_metadata = ProductAggregationMetadata()
            logger.warning(f"Metadata file not found. Using defaults: {default_metadata}")
            return default_metadata
        else:
            logger.error(f"S3 Error loading metadata: {e}", exc_info=True)
            raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in metadata file: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading metadata: {e}", exc_info=True)
        raise


def _create_empty_aggregated_data() -> pd.DataFrame:
    """Creates an empty DataFrame matching DynamicPrimitiveSourceModel columns."""
    columns = list(DynamicPrimitiveSourceModel.to_schema().columns.keys())
    df = pd.DataFrame(columns=columns)
    # Attempt setting dtypes on the empty DataFrame based on the model
    for col, props in DynamicPrimitiveSourceModel.to_schema().columns.items():
        if col in df.columns and props.dtype:
            try:
                df[col] = df[col].astype(props.dtype.type) # type: ignore
            except Exception:
                df[col] = df[col].astype(object) # type: ignore
    return df


async def _load_data_from_s3(
    s3_block: S3Bucket, data_path: str
) -> DataFrame[DynamicPrimitiveSourceModel]:
    """
    Loads and validates aggregated product data from a gzipped CSV in S3.

    Returns an empty, validated DataFrame if the file is not found.
    Validates loaded data using Pandera, passing `lazy=True` as required by model config.

    Args:
        s3_block: S3Bucket block for S3 access.
        data_path: Path to the gzipped CSV data file in the bucket.

    Returns:
        Validated DataFrame matching DynamicPrimitiveSourceModel (can be empty).

    Raises:
        ClientError: For non-404 S3 errors.
        SchemaError, SchemaDefinitionError: For Pandera validation issues.
        pd.errors.ParserError: For malformed CSV.
        Exception: For other unexpected errors.
    """
    logger = get_logger_safe(__name__)
    try:
        logger.info(f"Loading aggregated data: {data_path}")
        data_bytes = await s3_block.aread_path(data_path)
        data_buffer = io.BytesIO(data_bytes)

        # Prepare dtypes based on Pandera model for robust parsing
        dtypes = {
            col: props.dtype.type if props.dtype else object
            for col, props in DynamicPrimitiveSourceModel.to_schema().columns.items()
        }
        for col in DynamicPrimitiveSourceModel.to_schema().columns.keys():
             if col not in dtypes: dtypes[col] = object

        # Note: type checker might flag read_csv with buffer due to complex overloads
        loaded_df: pd.DataFrame = pd.read_csv( # type: ignore[call-overload]
            data_buffer,
            compression="gzip",
            dtype=dtypes,
            keep_default_na=False,
            na_values=[""]
        )

        logger.info("Validating loaded data...")
        # Validate with lazy=True (required as model uses strict="filter")
        validated_df = DynamicPrimitiveSourceModel.validate(loaded_df, lazy=True)
        logger.info(f"Data loaded and validated ({len(validated_df)} records).")
        return validated_df

    except ClientError as e:
        if _is_client_error_not_found(e):
            logger.warning("Data file not found. Returning empty DataFrame.")
            empty_df = _create_empty_aggregated_data()
            try:
                # Must validate empty DF with lazy=True due to model config
                return DynamicPrimitiveSourceModel.validate(empty_df, lazy=True)
            except SchemaDefinitionError as sde:
                 logger.error(f"Pandera definition error validating empty DF: {sde}", exc_info=True)
                 raise sde # Indicates a code/model definition issue
        else:
            logger.error(f"S3 Error loading data: {e}", exc_info=True)
            raise
    except (SchemaError, SchemaDefinitionError) as e:
        logger.error(f"Pandera validation failed for data: {e}", exc_info=True)
        raise
    except pd.errors.ParserError as e:
        logger.error(f"CSV parsing error for data: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading/validating data: {e}", exc_info=True)
        raise


# --- Main Aggregation Task ---

@task(name="Load Aggregation State from S3")
async def load_aggregation_state(
    s3_block: S3Bucket, base_path: str = "aggregated"
) -> tuple[DataFrame[DynamicPrimitiveSourceModel], ProductAggregationMetadata]:
    """
    Loads aggregation state (metadata and data) from S3.

    Orchestrates calls to helper functions for loading and validation.

    Args:
        s3_block: Configured Prefect S3Bucket block.
        base_path: Base S3 directory path for aggregation files.

    Returns:
        Tuple: (validated product DataFrame, loaded metadata).

    Raises:
        Exception: Propagated from helper functions on failure.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"Starting state load from S3 path: {base_path}")

    metadata_path = f"{base_path}/argentina_products_metadata.json"
    data_path = f"{base_path}/argentina_products.csv.gz"

    # Load metadata (or defaults)
    metadata = await _load_metadata_from_s3(s3_block, metadata_path)

    # Load data (or empty) and validate
    aggregated_data = await _load_data_from_s3(s3_block, data_path)

    logger.info("Aggregation state load completed.")
    return aggregated_data, metadata

# --- State Saving Task ---

@task(name="Save Aggregation State to S3")
async def save_aggregation_state(
    s3_block: S3Bucket,
    aggregated_data: DataFrame[DynamicPrimitiveSourceModel],
    metadata: ProductAggregationMetadata,
    base_path: str = "aggregated",
) -> None:
    """
    Saves the aggregation state (metadata JSON and data CSV.gz) to S3.

    Args:
        s3_block: Configured Prefect S3Bucket block.
        aggregated_data: DataFrame conforming to DynamicPrimitiveSourceModel containing the products.
        metadata: ProductAggregationMetadata object containing the latest state.
        base_path: Base S3 directory path for aggregation files.

    Raises:
        Exception: If S3 write operations fail.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"Starting state save to S3 path: {base_path}")

    metadata_path = f"{base_path}/argentina_products_metadata.json"
    data_path = f"{base_path}/argentina_products.csv.gz"

    # Save Metadata
    try:
        logger.info(f"Saving metadata to: {metadata_path}")
        metadata_json = metadata.model_dump_json()
        metadata_bytes = metadata_json.encode("utf-8")
        # Note: Prefect's S3Bucket.write_path handles conditional writes (ETag/versioning)
        # based on the underlying S3 client configuration if versioning is enabled on the bucket.
        # Explicit ETag checking would require using boto3 directly or extending S3Bucket.
        await s3_block.awrite_path(path=metadata_path, content=metadata_bytes)
        logger.info("Metadata saved successfully.")
    except Exception as e:
        logger.error(f"Failed to save metadata to {metadata_path}: {e}", exc_info=True)
        raise # Re-raise after logging

    # Save Data
    try:
        logger.info(f"Saving aggregated data to: {data_path}")
        # Convert DataFrame to CSV bytes
        csv_buffer = io.StringIO()
        aggregated_data.to_csv(csv_buffer, index=False, encoding="utf-8")
        csv_bytes = csv_buffer.getvalue().encode("utf-8")
        csv_buffer.close()

        # Compress CSV bytes using gzip
        compressed_bytes = gzip.compress(csv_bytes)

        # Write compressed bytes to S3
        await s3_block.awrite_path(path=data_path, content=compressed_bytes)
        logger.info(f"Aggregated data saved successfully ({len(aggregated_data)} records).")
    except Exception as e:
        logger.error(f"Failed to save aggregated data to {data_path}: {e}", exc_info=True)
        raise # Re-raise after logging

    logger.info("Aggregation state save completed.")


# --- Date Range Determination Task ---

@task(name="Determine Date Range to Process")
def determine_date_range_to_process(
    product_averages_provider: ProductAveragesProvider,
    metadata: ProductAggregationMetadata,
    force_reprocess: bool = False,
) -> list[DateStr]:
    """
    Determines the list of dates for which product data needs processing.

    Uses the provider to list available dates and filters them based on the
    last processed date from metadata and the force_reprocess flag.

    Args:
        product_averages_provider: Provider instance to access product average data keys.
        metadata: Current aggregation metadata containing the last processed date.
        force_reprocess: If True, ignore metadata and process all available dates.

    Returns:
        Sorted list of date strings (YYYY-MM-DD) to process.

    Raises:
        Exception: Propagated from provider's list_available_keys on failure.
    """
    logger = get_logger_safe(__name__)
    logger.info("Determining date range to process...")

    try:
        # Fetch available dates using the corrected provider method
        available_dates_str = product_averages_provider.list_available_keys()
        logger.info(f"Found {len(available_dates_str)} available dates in S3.")
        if not available_dates_str:
            logger.info("No available dates found to process.")
            return []

        # Sort dates chronologically (string sort works for YYYY-MM-DD)
        available_dates_str.sort()

    except Exception as e:
        logger.error(f"Error listing available keys: {e}", exc_info=True)
        raise # Re-raise critical errors

    # Determine the start date for processing
    start_processing_from: date | None = None
    if force_reprocess:
        logger.info("`force_reprocess` is True. Processing all available dates.")
        start_processing_from = datetime.strptime(available_dates_str[0], "%Y-%m-%d").date()
    elif metadata.last_processed_date and metadata.last_processed_date != "1970-01-01":
        try:
            last_processed_dt = datetime.strptime(metadata.last_processed_date, "%Y-%m-%d").date()
            # Start processing from the day *after* the last processed date
            start_processing_from = last_processed_dt + timedelta(days=1)
            logger.info(f"Resuming processing from {start_processing_from.isoformat()} (day after {last_processed_dt.isoformat()}).")
        except ValueError:
            logger.warning(f"Invalid last_processed_date '{metadata.last_processed_date}' in metadata. Processing all dates.")
            start_processing_from = datetime.strptime(available_dates_str[0], "%Y-%m-%d").date()
    else:
        logger.info("No valid last processed date found in metadata. Processing all available dates.")
        start_processing_from = datetime.strptime(available_dates_str[0], "%Y-%m-%d").date()

    # Filter available dates
    dates_to_process: list[DateStr] = []
    for date_str in available_dates_str:
        try:
            current_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            if start_processing_from and current_date >= start_processing_from:
                dates_to_process.append(DateStr(date_str))
        except ValueError:
            logger.warning(f"Skipping invalid date format found in available keys: {date_str}")
            continue # Skip malformed dates

    if dates_to_process:
        logger.info(f"Determined {len(dates_to_process)} dates to process: "
                    f"from {dates_to_process[0]} to {dates_to_process[-1]}.")
    else:
        logger.info("No new dates to process based on the start date criteria.")

    return dates_to_process
