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
from trufnetwork_sdk_py.utils import generate_stream_id # type: ignore

from tsn_adapters.tasks.argentina.models.aggregate_products_models import (
    DynamicPrimitiveSourceModel,
    ProductAggregationMetadata,
)
from tsn_adapters.tasks.argentina.provider.product_averages import ProductAveragesProvider
from tsn_adapters.tasks.argentina.types import DateStr
from tsn_adapters.utils.logging import get_logger_safe

T = TypeVar("T")

STREAM_NAME_PREFIX = "arg_sepa_prod_"

# --- State Loading Helper Functions ---

def _is_client_error_not_found(exception: ClientError) -> bool:
    """Checks if a botocore ClientError is an S3 'Not Found' (404 or NoSuchKey)."""
    # Safely access potentially untyped response dictionary elements
    error_dict: dict[str, Any] = exception.response
    error_code = error_dict.get("Error", {}).get("Code", "")
    status_code_str = error_dict.get("ResponseMetadata", {}).get("HTTPStatusCode")
    status_code = int(status_code_str) if status_code_str is not None else None
    return error_code == "NoSuchKey" or status_code == 404


@task(name="Load Metadata from S3")
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


def create_empty_aggregated_data() -> pd.DataFrame:
    """Creates an empty DataFrame matching DynamicPrimitiveSourceModel columns and dtypes."""
    logger = get_logger_safe(__name__)
    schema = DynamicPrimitiveSourceModel.to_schema()
    columns = list(schema.columns.keys())
    
    # Create empty DataFrame with columns derived from the schema
    df = pd.DataFrame(columns=columns)
    
    try:
        # Validate the empty DataFrame against the schema
        validated_df = DynamicPrimitiveSourceModel.validate(df, lazy=True)
        return validated_df
    except (SchemaError, SchemaDefinitionError) as e:
        logger.error(f"Failed to validate empty DataFrame schema: {e}")
        # Return unvalidated dataframe as fallback if validation fails (indicates schema issue)
        return df


@task(name="Load Data from S3")
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
            empty_df = create_empty_aggregated_data()
            try:
                # Must validate empty DF with lazy=True due to model config
                return DynamicPrimitiveSourceModel.validate(empty_df, lazy=True)
            except SchemaDefinitionError as sde:
                 logger.error(f"Pandera definition error validating empty DF: {sde}", exc_info=True)
                 raise sde # Indicates a code/model definition issue
        else:
            logger.error(f"S3 Error loading data: {e}", exc_info=True)
            raise # Re-raise the original S3 error instead of returning non-existent data
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


# --- Single Date Processing Helpers ---

async def _load_and_parse_daily_data(
    provider: ProductAveragesProvider, date_to_process: DateStr
) -> pd.DataFrame | None:
    """
    Loads and parses the daily product average data for a specific date from S3.

    Args:
        provider: The ProductAveragesProvider instance.
        date_to_process: The date string (YYYY-MM-DD).

    Returns:
        A pandas DataFrame containing the daily product data, or None if not found or error occurs.
    """
    logger = get_logger_safe(__name__)
    file_key = provider.to_product_averages_file_key(date_to_process)
    full_path = provider.get_full_path(file_key)
    logger.debug(f"Attempting to load daily data from: {full_path}")

    try:
        content_in_bytes = await provider.s3_block.aread_path(full_path)
        buffer = io.BytesIO(content_in_bytes)
        daily_products_df = pd.read_csv( # type: ignore[call-overload]
            buffer,
            compression="gzip",
            dtype={'id_producto': str},  # Ensure ID is read as string
            keep_default_na=False,       # Treat empty strings as strings
            na_values=[],                # Define which values are treated as NA (empty list means only standard NAs)
        )
        logger.info(f"Loaded {len(daily_products_df)} product records for {date_to_process}.")
        return daily_products_df
    except ClientError as e:
        if _is_client_error_not_found(e):
            logger.warning(f"Product averages file not found for date: {date_to_process}. Skipping date.")
        else:
            logger.error(f"S3 Error loading product averages for {date_to_process}: {e}", exc_info=True)
        return None
    except pd.errors.ParserError as e:
        logger.error(f"CSV parsing error for date {date_to_process}: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Unexpected error loading daily data for {date_to_process}: {e}", exc_info=True)
        return None


def _filter_valid_daily_products(daily_df: pd.DataFrame, date_str: DateStr) -> pd.DataFrame:
    """
    Filters the daily DataFrame to keep only rows with valid 'id_producto'.

    Ensures 'id_producto' is a non-empty string and not 'None' (case-insensitive).

    Args:
        daily_df: The raw daily product DataFrame.
        date_str: The date string (YYYY-MM-DD) for logging purposes.

    Returns:
        A DataFrame containing only rows with valid product IDs.
    """
    logger = get_logger_safe(__name__)
    initial_daily_count = len(daily_df)

    # Ensure 'id_producto' exists and handle potential missing column more gracefully
    if 'id_producto' not in daily_df.columns:
        logger.error(f"Missing 'id_producto' column in daily data for {date_str}. Returning empty DataFrame.")
        return pd.DataFrame() # Return empty df if column is missing

    # Filter based on non-empty string criteria for id_producto
    # The column is already read as string due to dtype={'id_producto': str} in _load_and_parse_daily_data
    valid_daily_df = daily_df[
        (daily_df['id_producto'].notna()) &  # Ensure it's not NaN/NaT
        (daily_df['id_producto'] != "") &   # Ensure it's not an empty string
        (daily_df['id_producto'].astype(str).str.lower() != "none") # type: ignore[union-attr] # Ensure it's not the string "None" (case-insensitive)
    ].copy() # Use .copy() to avoid SettingWithCopyWarning

    filtered_count = initial_daily_count - len(valid_daily_df)
    if filtered_count > 0:
        logger.warning(f"Filtered out {filtered_count} records with invalid 'id_producto' for date {date_str}.")

    return valid_daily_df


def _extract_existing_ids(aggregated_df: DataFrame[DynamicPrimitiveSourceModel]) -> set[str]:
    """
    Extracts the set of unique source_id values from the aggregated DataFrame.

    Handles cases where the DataFrame might be empty or lack the 'source_id' column.

    Args:
        aggregated_df: The current aggregated product data.

    Returns:
        A set containing unique existing product source IDs.
    """
    if 'source_id' in aggregated_df.columns and not aggregated_df.empty:
        # Ensure correct conversion to string before getting unique values
        unique_ids_list = list(aggregated_df['source_id'].astype(str).unique()) # type: ignore[no-untyped-call]
        return set(unique_ids_list) # type: ignore[arg-type]
    else:
        return set()


def _generate_argentina_product_stream_id(source_id: str) -> str:
    """
    Generates a stream ID specific to Argentina SEPA products using the SDK utility.

    Args:
        source_id: The product ID (id_producto). Must be non-empty.

    Returns:
        The generated stream ID (e.g., "arg_sepa_prod_123").

    Raises:
        ValueError: If the source_id is empty or invalid.
    """
    if not source_id:
        raise ValueError("Invalid source_id (empty) provided for generating stream ID.")
    stream_name = f"{STREAM_NAME_PREFIX}{source_id}"
    try:
        # Use the SDK's generate_stream_id function
        return generate_stream_id(stream_name)
    except Exception as e:
        # Catch potential errors from the SDK utility if necessary
        raise ValueError(f"Failed to generate stream ID for source_id '{source_id}': {e}")


def _identify_and_build_new_records(
    valid_daily_df: pd.DataFrame, existing_ids: set[str], date_to_process: DateStr
) -> tuple[list[dict[str, Any]], set[str]]:
    """
    Iterates through valid daily products, identifies new ones, and builds their records.

    Args:
        valid_daily_df: DataFrame containing valid products for the day.
        existing_ids: Set of product IDs already present in the aggregated data.
        date_to_process: The date string (YYYY-MM-DD).

    Returns:
        A tuple containing:
            - A list of dictionaries, each representing a new product record.
            - The updated set of existing IDs (including the newly added ones for this date).
    """
    logger = get_logger_safe(__name__)
    new_product_records: list[dict[str, Any]] = []
    processed_daily_ids: set[str] = set() # Track IDs processed within this daily file to handle duplicates

    # Ensure required columns exist before iterating
    required_cols = {'id_producto', 'productos_descripcion'}
    if not required_cols.issubset(valid_daily_df.columns):
         missing_cols = required_cols - set(valid_daily_df.columns)
         logger.error(f"Daily data for {date_to_process} missing required columns: {missing_cols}. Cannot identify new products.")
         return [], existing_ids # Return empty list and original IDs

    updated_existing_ids = existing_ids.copy()

    for _, product_row in valid_daily_df.iterrows(): # type: ignore[assignment]
        try:
            # Ensure data types before comparison and usage
            product_id = str(product_row['id_producto']) # type: ignore[arg-type]
            description = str(product_row['productos_descripcion']) # type: ignore[arg-type]


            # Check if product is new (not in aggregated set AND not already processed today)
            if product_id not in updated_existing_ids and product_id not in processed_daily_ids:
                try:
                    # Generate Stream ID
                    stream_id = _generate_argentina_product_stream_id(product_id)

                    # Create new record dictionary
                    new_record = {
                        "stream_id": stream_id,
                        "source_id": product_id,
                        "source_type": "argentina_sepa_product",
                        "productos_descripcion": description,
                        "first_shown_at": date_to_process,
                    }
                    new_product_records.append(new_record)

                    # Add to sets to prevent re-adding
                    updated_existing_ids.add(product_id)
                    processed_daily_ids.add(product_id)

                except ValueError as ve:
                    logger.error(f"Error generating stream ID for product {product_id}: {ve}", exc_info=True)
                    # Skip this product if ID generation fails
                except Exception as e:
                    logger.error(f"Unexpected error processing product {product_id} on {date_to_process}: {e}", exc_info=True)
                    # Skip this product on unexpected errors

        except KeyError as ke:
            # Log if unexpected missing columns are encountered despite check
            logger.error(f"Unexpected KeyError processing row for {date_to_process}: {ke}")
            continue # Skip row on error

    return new_product_records, updated_existing_ids


def _prepare_new_products_dataframe(new_product_records: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Creates a DataFrame from the list of new product records, applying expected string types.

    Args:
        new_product_records: A list of dictionaries representing new products.

    Returns:
        A pandas DataFrame containing the new products.
    """
    logger = get_logger_safe(__name__)
    if not new_product_records:
        return pd.DataFrame() # Return empty DataFrame if no new records

    try:
        new_products_df = pd.DataFrame(new_product_records)

        # Define expected types (using pandas string dtype for clarity)
        type_mapping = {
            'stream_id': 'string',
            'source_id': 'string',
            'source_type': 'string',
            'productos_descripcion': 'string',
            'first_shown_at': 'string'
        }

        # Apply types explicitly
        for col, dtype in type_mapping.items():
            if col in new_products_df.columns:
                try:
                    new_products_df[col] = new_products_df[col].astype(dtype) # type: ignore[assignment]
                except Exception as e:
                    logger.warning(f"Failed to set dtype '{dtype}' for column '{col}' in new products DataFrame: {e}")
            else:
                 logger.warning(f"Expected column '{col}' not found in new product records.")

        return new_products_df
    except Exception as e:
        logger.error(f"Failed to create DataFrame from new product records: {e}", exc_info=True)
        return pd.DataFrame() # Return empty on failure


def _concatenate_and_validate(
    current_aggregated_data: DataFrame[DynamicPrimitiveSourceModel],
    new_products_df: pd.DataFrame,
    date_str: DateStr,
) -> DataFrame[DynamicPrimitiveSourceModel]:
    """
    Concatenates new products with existing data, aligns columns to the schema,
    and validates the final result using the DynamicPrimitiveSourceModel.

    Args:
        current_aggregated_data: The existing aggregated data (assumed validated).
        new_products_df: DataFrame containing the newly identified products.
        date_str: The date string (YYYY-MM-DD) for logging.

    Returns:
        The updated and validated aggregated DataFrame. Returns original data if validation fails.
    """
    logger = get_logger_safe(__name__)

    if new_products_df.empty:
        logger.info(f"No new products DataFrame to concatenate for {date_str}.")
        # Still need to return validated data, especially if current was empty
        if current_aggregated_data.empty:
             try:
                 # Ensure the empty DF conforms to the schema
                 return DynamicPrimitiveSourceModel.validate(create_empty_aggregated_data(), lazy=True)
             except (SchemaError, SchemaDefinitionError) as e:
                 logger.error(f"Pandera validation failed for initial empty DataFrame: {e}", exc_info=True)
                 raise # Critical if even the empty schema is wrong
        else:
             # Assume current_aggregated_data is already validated from previous step or initial load
             return current_aggregated_data


    logger.info(f"Concatenating {len(new_products_df)} new products with {len(current_aggregated_data)} existing records for {date_str}.")

    # Ensure columns align before concat, especially if current_aggregated_data might be empty
    # Use the schema to define the canonical set of columns and order
    schema_columns = list(DynamicPrimitiveSourceModel.to_schema().columns.keys())

    # Reindex both dataframes to match the schema columns before concatenation
    try:
        # Assume current_aggregated_data is already validated (even if empty) when passed in.
        # The initial load guarantees this.
        validated_current = current_aggregated_data

        # Ensure new_products_df has all necessary columns, adding missing ones as None/NA
        for col in schema_columns:
            if col not in new_products_df.columns:
                 new_products_df[col] = pd.NA # Use pandas NA for missing values

        # Reindex both to ensure consistent column order and presence
        aligned_current = validated_current.reindex(columns=schema_columns) # type: ignore[no-untyped-call]
        aligned_new = new_products_df.reindex(columns=schema_columns) # type: ignore[no-untyped-call]

        # Perform concatenation
        updated_aggregated_data = pd.concat([aligned_current, aligned_new], ignore_index=True)
        logger.debug(f"Concatenated DataFrame has {len(updated_aggregated_data)} rows.")

    except (SchemaError, SchemaDefinitionError) as e:
         logger.error(f"Error validating/preparing empty DataFrame before concatenation: {e}", exc_info=True)
         return current_aggregated_data # Return original on error
    except Exception as e:
         logger.error(f"Error reindexing or concatenating DataFrames for {date_str}: {e}", exc_info=True)
         return current_aggregated_data # Return original on error

    # Final Validation
    logger.info(f"Performing final validation on {len(updated_aggregated_data)} records for date {date_str}...")
    try:
        # Explicitly check for nulls in required columns before Pandera validation for better logging
        for col_name, col_props in DynamicPrimitiveSourceModel.to_schema().columns.items():
            if not col_props.nullable and col_name in updated_aggregated_data.columns:
                null_count = updated_aggregated_data[col_name].isna().sum() # type: ignore[no-untyped-call]
                if null_count > 0:
                    logger.warning(f"Column '{col_name}' is required but has {null_count} null/NA values before final validation for {date_str}.")

        # Perform the final Pandera validation
        validated_updated_data = DynamicPrimitiveSourceModel.validate(updated_aggregated_data, lazy=True)
        logger.info(f"Final validation successful for {date_str}. Total product count: {len(validated_updated_data)}")
        logger.debug(f"Final validated data dtypes: {validated_updated_data.dtypes}") # type: ignore[assignment]
        return validated_updated_data

    except (SchemaError, SchemaDefinitionError) as final_err:
        logger.error(f"Final Pandera validation failed for {date_str}: {final_err}")
        # Log details about the failure if possible (e.g., failure cases from error object)
        if isinstance(final_err, SchemaError) and final_err.failure_cases is not None: # type: ignore[attr-defined]
             try:
                 failure_cases_str = final_err.failure_cases.to_string() # type: ignore[attr-defined]
                 logger.error(f"Pandera failure cases:\n{failure_cases_str}")
             except Exception as log_err:
                 logger.error(f"Additionally, failed to log Pandera failure cases: {log_err}")
        # Consider logging snippets of failing rows if feasible without exposing sensitive data
        logger.error("Returning previously aggregated data due to validation failure.")
        return current_aggregated_data # Return original data to prevent saving corrupted state
    except Exception as e:
        logger.error(f"Unexpected error during final validation for {date_str}: {e}", exc_info=True)
        return current_aggregated_data # Return original on general error


# --- Main Processing Function (Refactored) ---

async def process_single_date_products(
    date_to_process: DateStr,
    current_aggregated_data: DataFrame[DynamicPrimitiveSourceModel],
    product_averages_provider: ProductAveragesProvider,
) -> DataFrame[DynamicPrimitiveSourceModel]:
    """
    Orchestrates the loading, filtering, identification, and concatenation
    of product data for a single date using helper functions.

    Args:
        date_to_process: The date (YYYY-MM-DD) to process.
        current_aggregated_data: DataFrame with existing aggregated product data.
        product_averages_provider: Provider to fetch daily product average data.

    Returns:
        Updated DataFrame containing both existing and newly added products.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"---- Starting processing for date: {date_to_process} ----")

    # 1. Load and Parse Daily Data
    daily_products_df = await _load_and_parse_daily_data(product_averages_provider, date_to_process)
    if daily_products_df is None:
        logger.warning(f"Failed to load or parse daily data for {date_to_process}. Returning current aggregated data.")
        return current_aggregated_data # Return original data if loading failed

    # 2. Filter for Valid Product IDs
    valid_daily_df = _filter_valid_daily_products(daily_products_df, date_to_process)
    if valid_daily_df.empty:
        logger.info(f"No valid products found in daily data for {date_to_process} after filtering.")
        # No new products possible, return current (potentially validated empty) data
        return _concatenate_and_validate(current_aggregated_data, pd.DataFrame(), date_to_process)


    # 3. Get Existing Product IDs
    existing_ids = _extract_existing_ids(current_aggregated_data)
    logger.debug(f"Found {len(existing_ids)} existing unique product IDs.")

    # 4. Identify New Products and Build Records
    new_product_records, _ = _identify_and_build_new_records(
        valid_daily_df, existing_ids, date_to_process
    ) # We don't need the updated set of IDs here

    if not new_product_records:
        logger.info(f"No new products identified for {date_to_process}.")
        # No changes, return current (potentially validated empty) data
        return _concatenate_and_validate(current_aggregated_data, pd.DataFrame(), date_to_process)

    logger.info(f"Identified {len(new_product_records)} new product records for {date_to_process}.")

    # 5. Prepare DataFrame for New Products
    new_products_df = _prepare_new_products_dataframe(new_product_records)
    if new_products_df.empty and new_product_records: # Check if DF creation failed despite having records
         logger.error(f"Failed to create DataFrame for new products on {date_to_process}. Returning current data.")
         return current_aggregated_data

    # 6. Concatenate and Validate
    final_aggregated_data = _concatenate_and_validate(
        current_aggregated_data, new_products_df, date_to_process
    )

    logger.info(f"---- Finished processing for date: {date_to_process}. Final product count: {len(final_aggregated_data)} ----")
    return final_aggregated_data
