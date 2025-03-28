"""
Prefect tasks for loading Argentina SEPA product aggregation state from S3.

Handles state loading (metadata JSON, aggregated products CSV) including
validation and default creation if files are missing.
"""

import io
import json
from typing import Any, TypeVar, cast
from datetime import date, timedelta, datetime

from botocore.exceptions import ClientError  # type: ignore
import pandas as pd
from pandera.errors import SchemaError, SchemaDefinitionError
from pandera.typing import DataFrame
from prefect import task
from prefect_aws import S3Bucket  # type: ignore
import gzip # Add gzip import
from trufnetwork_sdk_py.utils import generate_stream_id # Use SDK function

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


# --- Single Date Processing Helper ---

def _generate_argentina_product_stream_id(source_id: str) -> str:
    """
    Generates a stream ID specific to Argentina SEPA products.

    Args:
        source_id: The product ID (id_producto).

    Returns:
        The generated stream ID (e.g., "arg_sepa_prod_123").
    """
    # Basic validation to prevent malformed IDs
    if not source_id:
        raise ValueError("Invalid source_id for generating stream ID.")
    # Use the SDK's generate_stream_id function with the required prefix
    stream_name = f"{STREAM_NAME_PREFIX}{source_id}"
    return generate_stream_id(stream_name)


async def process_single_date_products(
    date_to_process: DateStr,
    current_aggregated_data: DataFrame[DynamicPrimitiveSourceModel],
    product_averages_provider: ProductAveragesProvider,
) -> DataFrame[DynamicPrimitiveSourceModel]:
    """
    Processes product data for a single date, adding new products to the aggregated set.

    Args:
        date_to_process: The date (YYYY-MM-DD) to process.
        current_aggregated_data: DataFrame with existing aggregated product data.
        product_averages_provider: Provider to fetch daily product average data.

    Returns:
        Updated DataFrame containing both existing and newly added products.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"Processing products for date: {date_to_process}")
    
    # Log initial state for debugging
    if current_aggregated_data.empty:
        logger.info("Current aggregated data is empty - starting with fresh state")
    else:
        logger.info(f"Current aggregated data: {len(current_aggregated_data)} rows.")

    try:
        # 1. Load daily product data - access to S3 is now properly awaited
        # Use S3 bucket methods directly to avoid deroutine issues
        file_key = product_averages_provider.to_product_averages_file_key(date_to_process)
        full_path = product_averages_provider.get_full_path(file_key)
        
        try:
            logger.debug(f"Reading S3 file: {full_path}")
            content_in_bytes = await product_averages_provider.s3_block.aread_path(full_path)
            buffer = io.BytesIO(content_in_bytes)
            # Explicitly set dtype and NA handling to prevent conversion issues
            daily_products_df = pd.read_csv(
                buffer,
                compression="gzip",
                dtype={'id_producto': str},
                keep_default_na=False, # Treat empty strings as strings, not NaN
            )
            logger.info(f"Loaded {len(daily_products_df)} product records for {date_to_process}.")
            
        except Exception as e:
            logger.error(f"Error loading product averages for {date_to_process}: {e}", exc_info=True)
            return current_aggregated_data
            
    except FileNotFoundError:
        logger.warning(f"Product averages file not found for date: {date_to_process}. Skipping date.")
        return current_aggregated_data
    except Exception as e:
        logger.error(f"Error loading product averages for {date_to_process}: {e}", exc_info=True)
        # Depending on desired robustness, could return current_aggregated_data or re-raise
        return current_aggregated_data

    # 2. Get existing product IDs for quick lookup
    # Ensure 'source_id' column exists before accessing
    existing_product_ids: set[str]
    if 'source_id' in current_aggregated_data.columns and not current_aggregated_data.empty:
        # Convert unique IDs to a list of strings first, then to a set
        unique_ids_list = list(current_aggregated_data['source_id'].astype(str).unique())
        existing_product_ids = set(unique_ids_list)
    else:
        existing_product_ids = set()

    # --- Pre-filter daily data for valid product IDs ---
    initial_daily_count = len(daily_products_df)
    
    # The column is already read as string with dtype=str in read_csv
    # daily_products_df['id_producto'] = daily_products_df['id_producto'].astype(str)
    valid_daily_df = daily_products_df[
        # Rely on non-empty string check now that read_csv handles NA correctly
        (daily_products_df['id_producto'] != "") & \
        (daily_products_df['id_producto'] != "None") # Also explicitly filter the string "None"
    ].copy() # Use .copy() to avoid SettingWithCopyWarning

    filtered_count = initial_daily_count - len(valid_daily_df)
    if filtered_count > 0:
        logger.warning(f"Filtered out {filtered_count} records with invalid 'id_producto' for date {date_to_process}.")
    # --------------------------------------------------

    new_product_records: list[dict[str, Any]] = []
    processed_daily_ids: set[str] = set() # Track IDs processed within this daily file

    # 3. Iterate through VALID daily products
    for index, product_row in valid_daily_df.iterrows(): # {Use valid_daily_df directly}
        # Try dictionary-style access - Should be safer now after filtering
        try:
            product_id = str(product_row['id_producto']) # Already validated as non-empty/non-None string
            description = str(product_row['productos_descripcion'])
        except KeyError as ke:
            # This is less likely now but kept for robustness
            logger.error(f"Unexpected KeyError processing supposedly valid row on {date_to_process} at index {index}: {ke}")
            continue

        # Basic Validation for description (ID is already pre-validated)
        if not description:
             logger.warning(f"Skipping record with missing 'productos_descripcion' for ID {product_id} on {date_to_process}.")
             continue

        # 5. Check if product is new and not already processed today
        # No need to check 'if not product_id' as it's pre-filtered
        if product_id not in existing_product_ids and product_id not in processed_daily_ids:
            try:
                # 6. Generate Stream ID
                stream_id = _generate_argentina_product_stream_id(product_id)

                # 7. Create new record dictionary
                new_record = {
                    "stream_id": stream_id,
                    "source_id": product_id,
                    "source_type": "argentina_sepa_product", # Constant as per spec
                    "productos_descripcion": description,
                    "first_shown_at": date_to_process,
                }
                new_product_records.append(new_record)

                # Add to sets to prevent re-adding
                existing_product_ids.add(product_id)
                processed_daily_ids.add(product_id)
            except ValueError as ve:
                 logger.error(f"Error generating stream ID for product {product_id}: {ve}", exc_info=True)
                 continue # Skip this product if ID generation fails
            except Exception as e:
                logger.error(f"Unexpected error processing product {product_id} on {date_to_process}: {e}", exc_info=True)
                continue # Skip this product on unexpected errors

    # 8. Concatenate new products if any were found
    if new_product_records:
        logger.info(f"Found {len(new_product_records)} new products to add for date {date_to_process}.")
        
        # Create DataFrame with explicit types where possible
        try:
            # Create new products DataFrame
            new_products_df = pd.DataFrame(new_product_records)
            
            # Apply types explicitly to avoid potential issues
            type_mapping = {
                'stream_id': 'string',
                'source_id': 'string',
                'source_type': 'string',
                'productos_descripcion': 'string',
                'first_shown_at': 'string'
            }
            
            for col, dtype in type_mapping.items():
                if col in new_products_df.columns:
                    try:
                        new_products_df[col] = new_products_df[col].astype(dtype)
                    except Exception as e:
                        logger.warning(f"Failed to set dtype for '{col}' in new products DataFrame: {e}")
            
            # Check and log any missing columns that might be required by the model
            schema_columns = set(DynamicPrimitiveSourceModel.to_schema().columns.keys())
            df_columns = set(new_products_df.columns)
            missing_cols = schema_columns - df_columns
            if missing_cols:
                logger.warning(f"New products DataFrame is missing columns required by schema: {missing_cols}")
                # Add missing columns with None values
                for col in missing_cols:
                    new_products_df[col] = None

            # Ensure the new DataFrame conforms to the model before concatenating
            updated_aggregated_data = None # Initialize here to ensure it's always bound
            try:
                # Use lazy=True as per model config, and handle potential empty df case
                validated_new_df = DynamicPrimitiveSourceModel.validate(new_products_df, lazy=True) if not new_products_df.empty else new_products_df
                logger.debug("New products DataFrame validated successfully")

                # Ensure columns align before concat, crucial if current_aggregated_data is empty
                if current_aggregated_data.empty:
                     logger.info("Current aggregated data is empty, creating properly typed empty DataFrame")
                     # Create and validate empty frame
                     try:
                         empty_validated_df = create_empty_aggregated_data()
                         logger.debug(f"Empty DataFrame created with dtypes: {empty_validated_df.dtypes}")
                         
                         # Validate the empty frame immediately to catch definition errors early
                         empty_validated_df = DynamicPrimitiveSourceModel.validate(empty_validated_df, lazy=True)
                         logger.debug("Empty DataFrame validated successfully")
                         
                         logger.debug("Concatenating empty DataFrame with new products...")
                         updated_aggregated_data = pd.concat([empty_validated_df, validated_new_df], ignore_index=True)
                     except Exception as e:
                         logger.error(f"Error preparing empty DataFrame for concatenation: {e}", exc_info=True)
                         return current_aggregated_data
                else:
                     logger.debug("Concatenating existing data with new products...")
                     updated_aggregated_data = pd.concat([current_aggregated_data, validated_new_df], ignore_index=True)
                
                logger.debug(f"Concatenated DataFrame has {len(updated_aggregated_data)} rows and columns: {updated_aggregated_data.columns.tolist()}")
                logger.debug(f"Concatenated DataFrame dtypes: {updated_aggregated_data.dtypes}")

                # Final validation of the combined DataFrame
                logger.info(f"Performing final validation on {len(updated_aggregated_data)} records for date {date_to_process}...")
                
                # Check for any nulls in required columns before validation
                for col_name, col_props in DynamicPrimitiveSourceModel.to_schema().columns.items():
                    if not col_props.nullable and col_name in updated_aggregated_data.columns:
                        null_count = updated_aggregated_data[col_name].isna().sum()
                        if null_count > 0:
                            logger.warning(f"Column '{col_name}' is required but has {null_count} null values before validation")
                
                # Perform the final validation
                validated_updated_data = DynamicPrimitiveSourceModel.validate(updated_aggregated_data, lazy=True)
                logger.info(f"Final validation successful. Total product count: {len(validated_updated_data)}")
                logger.debug(f"Final validated data dtypes: {validated_updated_data.dtypes}")
                return validated_updated_data
            except (SchemaError, SchemaDefinitionError) as final_err:
                logger.error(f"Final validation failed: {final_err}")
                # More detailed analysis of the failure
                schema = DynamicPrimitiveSourceModel.to_schema()
                for col_name, col_props in schema.columns.items():
                    if col_name in updated_aggregated_data.columns:
                        actual_dtype = updated_aggregated_data[col_name].dtype
                        if col_props.dtype:
                            expected_dtype = col_props.dtype.type if hasattr(col_props.dtype, 'type') else object
                            expected_name = expected_dtype.__name__ if hasattr(expected_dtype, '__name__') else str(expected_dtype)
                            logger.error(f"Column '{col_name}': expected={expected_name}, actual={actual_dtype}")
                    else:
                        logger.error(f"Required column '{col_name}' is missing from DataFrame")
                
                # Return original data to prevent saving corrupted state
                return current_aggregated_data

        except Exception as e:
             # General exception for anything not caught above
             logger.error(f"Unexpected error during concatenation or validation for {date_to_process}: {e}", exc_info=True)
             logger.info(f"Returning previous state with {len(current_aggregated_data)} records due to the error.")
             return current_aggregated_data

    else:
        logger.info(f"No new products found for date {date_to_process}.")
        # Return the original dataframe, ensuring it's validated if it was empty initially
        if current_aggregated_data.empty:
             try:
                 validated_empty = DynamicPrimitiveSourceModel.validate(create_empty_aggregated_data(), lazy=True)
                 return validated_empty
             except (SchemaError, SchemaDefinitionError) as e:
                 logger.error(f"Pandera validation failed for empty DataFrame: {e}", exc_info=True)
                 raise # Re-raise if validating the empty state fails - indicates model issue
        else:
             return current_aggregated_data
