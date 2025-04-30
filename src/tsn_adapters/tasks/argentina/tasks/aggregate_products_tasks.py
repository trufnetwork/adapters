"""
Prefect tasks for loading Argentina SEPA product aggregation state from S3.

Handles state loading (metadata JSON, aggregated products CSV) including
validation and default creation if files are missing.
"""

import io
from typing import Any, TypeVar

from botocore.exceptions import ClientError  # type: ignore
import pandas as pd
from pandera.errors import SchemaDefinitionError, SchemaError
from pandera.typing import DataFrame
from prefect import task
import prefect.variables as variables  # Import prefect variables
from trufnetwork_sdk_py.utils import generate_stream_id  # type: ignore

from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel
from tsn_adapters.tasks.argentina.config import ArgentinaFlowVariableNames  # Import config
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import (
    SepaAvgPriceProductModel,
)
from tsn_adapters.tasks.argentina.provider.product_averages import ProductAveragesProvider
from tsn_adapters.tasks.argentina.types import DateStr
from tsn_adapters.utils import create_empty_df
from tsn_adapters.utils.logging import get_logger_safe

T = TypeVar("T")

STREAM_NAME_PREFIX = "arg_sepa_prod_"

# --- Helper Functions ---


def _is_client_error_not_found(e: ClientError) -> bool:
    """Check if a boto3 ClientError is a 404/NoSuchKey."""
    # Safely access potentially untyped response dictionary elements
    error_dict: dict[str, Any] = e.response
    error_code = error_dict.get("Error", {}).get("Code", "")
    status_code_str = error_dict.get("ResponseMetadata", {}).get("HTTPStatusCode")
    status_code = int(status_code_str) if status_code_str is not None else None
    return error_code == "NoSuchKey" or status_code == 404


@task(name="Determine Aggregation Dates")
def determine_aggregation_dates(
    product_averages_provider: ProductAveragesProvider,
    force_reprocess: bool = False,
) -> tuple[list[DateStr], str, str]:
    """
    Determines the range of dates to aggregate based on available data and gating variables.

    Uses the provider to list available dates and filters them based on:
    - LAST_PREPROCESS_SUCCESS_DATE (Process only dates <= this variable)
    - LAST_AGGREGATION_SUCCESS_DATE (Process only dates > this variable)
    - force_reprocess flag (If True, ignore LAST_AGGREGATION_SUCCESS_DATE but still respect LAST_PREPROCESS_SUCCESS_DATE)

    Args:
        product_averages_provider: Provider instance to access product average data keys.
        force_reprocess: If True, ignore the last aggregation date but still respect the last preprocess date.

    Returns:
        Tuple containing:
            - Sorted list of date strings (YYYY-MM-DD) to process.
            - The last aggregation success date used for filtering.
            - The last preprocess success date used for filtering.

    Raises:
        Exception: Propagated from provider's list_available_keys or variable access.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"Determining date range to process... Force reprocess: {force_reprocess}")

    # 2. Fetch Gating Variables (Fetch PREPROCESS always, AGGREGATION conditionally)
    try:
        last_preprocess_success_date = variables.Variable.get(
            ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE, default=ArgentinaFlowVariableNames.DEFAULT_DATE
        )
        assert isinstance(last_preprocess_success_date, str)
        logger.info(f"Using {ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE}: {last_preprocess_success_date}")

        if force_reprocess:
            # If forcing, ignore the actual last aggregation date, start from default
            last_aggregation_success_date = ArgentinaFlowVariableNames.DEFAULT_DATE
            logger.warning(
                f"force_reprocess=True. Using default start date {last_aggregation_success_date} instead of actual last aggregation date."
            )
        else:
            # If not forcing, use the actual last aggregation date
            last_aggregation_success_date = variables.Variable.get(
                ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE,
                default=ArgentinaFlowVariableNames.DEFAULT_DATE,
            )
            assert isinstance(last_aggregation_success_date, str)
            logger.info(
                f"Using {ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE}: {last_aggregation_success_date}"
            )
    except Exception as e:
        logger.error(f"Failed to retrieve gating Prefect variables: {e}", exc_info=True)
        raise  # Cannot proceed without gating info

    # 1. Fetch available dates
    try:
        available_dates_str = product_averages_provider.list_available_keys()
        logger.debug(f"Found {len(available_dates_str)} available dates in S3 provider.")
        if not available_dates_str:
            logger.info("No available dates found in provider.")
            # Need to fetch variables anyway to return context
            return [], str(last_aggregation_success_date), str(last_preprocess_success_date)
        available_dates_str.sort()  # Ensure chronological order
    except Exception as e:
        logger.error(f"Error listing available keys from provider: {e}", exc_info=True)
        raise

    # 3. Filter Dates based on Gating Variables and Preprocess Limit
    dates_to_process = [
        date_str
        for date_str in available_dates_str
        # Always respect the preprocess limit
        if date_str <= last_preprocess_success_date
        # Use the determined aggregation start date (actual or default)
        and date_str > last_aggregation_success_date
    ]

    # 4. Log Skipped Dates Information (using the *actual* variables fetched or used)
    skipped_preprocess = sum(1 for d in available_dates_str if d > last_preprocess_success_date)
    skipped_aggregation = sum(
        1
        for d in available_dates_str
        # Count dates before or equal to the aggregation date *used* for filtering
        if d <= last_aggregation_success_date
        # And also ensure they are within the preprocess limit we considered
        and d <= last_preprocess_success_date
    )

    if skipped_preprocess > 0:
        logger.info(
            f"Skipped {skipped_preprocess} dates later than {last_preprocess_success_date} ({ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE})."
        )
    # Use the value of last_aggregation_success_date determined in step 2 for logging
    if skipped_aggregation > 0:
        logger.info(
            f"Skipped {skipped_aggregation} dates up to and including {last_aggregation_success_date} (based on {ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE} or default if forced)."
        )

    # 5. Log Final Result and Return with Context
    if dates_to_process:
        logger.info(
            f"Determined {len(dates_to_process)} dates to process after gating: "
            f"{dates_to_process[0]} to {dates_to_process[-1]}."
        )
    else:
        logger.info("No new dates to process after applying gating logic.")

    # Return the list and the context dates used
    return dates_to_process, last_aggregation_success_date, last_preprocess_success_date


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
        daily_products_df = pd.read_csv(  # type: ignore[call-overload]
            buffer,
            compression="zip",
            dtype={"id_producto": str},  # Ensure ID is read as string
            keep_default_na=False,  # Treat empty strings as strings
            na_values=[],  # Define which values are treated as NA (empty list means only standard NAs)
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


def _extract_existing_ids(aggregated_df: DataFrame[PrimitiveSourceDataModel]) -> set[str]:
    """Extracts unique 'source_id' values from the aggregated data."""
    if aggregated_df.empty:
        return set()
    # Use 'source_id' column now
    return set(aggregated_df["source_id"].astype(str).unique())


def _generate_stream_id(source_id: str) -> str:
    """Generates a unique stream ID based on the source product ID."""
    # Simple prefixing for now, ensure uniqueness if needed
    return generate_stream_id(f"{STREAM_NAME_PREFIX}{source_id}")


def _prepare_new_products_df(
    new_products: pd.DataFrame,  # Input is raw daily data subset
    # date_str: DateStr # Date no longer needed here
) -> DataFrame[PrimitiveSourceDataModel]:  # Return Primitive model
    """Constructs the DataFrame for new products in PrimitiveSourceDataModel format.

    Input DataFrame expected to have 'id_producto' (as source_id) and
    optionally 'productos_descripcion' (which is ignored).
    """
    logger = get_logger_safe(__name__)
    if new_products.empty:
        # Return empty DataFrame matching PrimitiveSourceDataModel schema
        return create_empty_df(PrimitiveSourceDataModel)

    # Expect 'id_producto' and 'productos_descripcion' for display name
    new_products_output = new_products[["id_producto", "productos_descripcion"]].copy()

    # Rename columns to match PrimitiveSourceDataModel fields
    new_products_output.rename(
        columns={
            "id_producto": "source_id",
            "productos_descripcion": "source_display_name",
        },
        inplace=True,
    )

    # Generate stream_id
    new_products_output["stream_id"] = new_products_output["source_id"].apply(_generate_stream_id)

    # Add source_type (Assuming constant for now)
    new_products_output["source_type"] = "argentina_sepa_product"

    # Ensure only PrimitiveSourceDataModel columns remain and in correct order
    schema_columns = list(PrimitiveSourceDataModel.to_schema().columns.keys())
    new_products_output = new_products_output.reindex(columns=schema_columns)

    try:
        # Validate the structure
        validated_df = PrimitiveSourceDataModel.validate(new_products_output, lazy=True)
        logger.info(f"Prepared {len(validated_df)} new product rows for upsert.")
        return validated_df
    except (SchemaError, SchemaDefinitionError) as e:
        logger.error(f"Pandera validation failed preparing new products: {e}", exc_info=True)
        # Raise error, as returning unvalidated could break upsert
        raise ValueError("Failed to prepare valid PrimitiveSourceDataModel for new products") from e


# --- Process Single Date Task (Refactored) ---
@task(name="Process Single Date for Aggregation")
async def process_single_date_products(
    date_to_process: DateStr,
    current_aggregated_data: DataFrame[PrimitiveSourceDataModel],  # Use Primitive model
    product_averages_provider: ProductAveragesProvider,
) -> DataFrame[PrimitiveSourceDataModel]:  # Return only *new* products as Primitive model
    """
    Loads daily data, identifies new products compared to the current descriptor,
    and returns a DataFrame of *only the new* products in PrimitiveSourceDataModel format.

    Args:
        date_to_process: The date string (YYYY-MM-DD) to process.
        current_aggregated_data: DataFrame of existing products (PrimitiveSourceDataModel).
        product_averages_provider: Provider to load daily average data.

    Returns:
        DataFrame containing *only the new* products found, formatted as PrimitiveSourceDataModel.
        Returns an empty DataFrame if no new products are found or if errors occur.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"Processing date {date_to_process} to find new products...")

    # 1. Load and Validate Daily Data
    # Use helper _load_and_parse_daily_data which handles parsing & basic validation
    daily_products_df = await _load_and_parse_daily_data(product_averages_provider, date_to_process)

    if daily_products_df is None or daily_products_df.empty:
        logger.warning(f"No valid daily data loaded for {date_to_process}. Returning empty DataFrame.")
        return create_empty_df(PrimitiveSourceDataModel)

    # Validate daily data against SepaAvgPriceProductModel to ensure required columns exist
    try:
        daily_products_df_validated = SepaAvgPriceProductModel.validate(daily_products_df, lazy=True)
    except (SchemaError, SchemaDefinitionError):
        logger.warning(
            f"Daily data for {date_to_process} failed SepaAvgPriceProductModel validation. Skipping.", exc_info=True
        )
        return create_empty_df(PrimitiveSourceDataModel)

    # 2. Identify New Products
    existing_source_ids = _extract_existing_ids(current_aggregated_data)
    # Ensure daily IDs are strings for comparison
    daily_source_ids = set(daily_products_df_validated["id_producto"].astype(str).unique())
    new_source_ids = daily_source_ids - existing_source_ids

    if not new_source_ids:
        logger.info(f"No new products found for date {date_to_process}.")
        return create_empty_df(PrimitiveSourceDataModel)

    logger.info(f"Found {len(new_source_ids)} new product(s) for date {date_to_process}.")

    # 3. Prepare DataFrame for New Products
    # Filter the validated daily DataFrame to get rows corresponding to new products
    # Only need 'id_producto' for _prepare_new_products_df now
    new_products_details = daily_products_df_validated[
        daily_products_df_validated["id_producto"].astype(str).isin(new_source_ids)
    ].drop_duplicates(subset=["id_producto"])

    # Prepare the DataFrame in the target PrimitiveSourceDataModel format
    new_products_primitive_df = _prepare_new_products_df(new_products_details)

    return new_products_primitive_df
