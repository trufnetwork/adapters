"""
Tasks related to processing dates for Argentina SEPA data insertion.
"""

from datetime import date, datetime
from typing import List

import pandas as pd
from pandera.typing import DataFrame
from prefect import task

from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.types import DateStr
from tsn_adapters.utils import convert_date_str_series_to_unix_ts
from tsn_adapters.utils.create_empty_df import create_empty_df
from tsn_adapters.utils.logging import get_logger_safe  # Re-using logger helper


@task(name="Determine Dates to Insert")
async def determine_dates_to_insert(
    provider: ProductAveragesProvider,
) -> List[DateStr]:
    """
    Determines the list of dates to insert based on available data and metadata.

    Filters available dates based on the last successfully inserted date and
    the last product deployment date recorded in the metadata.

    Args:
        provider: The provider instance to list available daily average keys.

    Returns:
        A sorted list of date strings ('YYYY-MM-DD') to be processed.

    Raises:
        Exception: If listing available keys from the provider fails.
    """
    logger = get_logger_safe(__name__)
    logger.warning("Metadata-based date filtering removed from determine_dates_to_insert. Returning all available dates. Proper gating requires Prefect Variable integration.")

    # 1. Get available dates from provider
    try:
        available_dates_str: List[DateStr] = provider.list_available_keys()
        if not available_dates_str:
            logger.warning("No available dates found in the provider.")
            return []
        logger.info(f"Provider listed {len(available_dates_str)} available dates.")
        # Sort initially to ensure chronological processing if needed later
        available_dates_str.sort()
        # Return all dates for now; gating logic will be added in Step 8
        return available_dates_str
    except Exception as e:
        logger.error(f"Failed to list available keys from provider: {e}", exc_info=True)
        # Propagate error as this is critical
        raise


class DailyAverageLoadingError(Exception):
    """Custom exception for daily average loading failures."""

    pass


@task(name="Load Daily Averages for Insertion")
async def load_daily_averages(
    provider: ProductAveragesProvider,
    date_str: DateStr,
) -> DataFrame[SepaAvgPriceProductModel]:
    """
    Loads the daily product average data for a specific date using the provider.

    Treats any failure during loading (file not found, parsing error, etc.)
    as a fatal error for the flow run by re-raising an exception.

    Args:
        provider: The provider instance to fetch daily data.
        date_str: The date ('YYYY-MM-DD') for which to load data.

    Returns:
        A DataFrame containing the daily product averages conforming to SepaAvgPriceProductModel.

    Raises:
        DailyAverageLoadingError: If the provider fails to load or parse the data for the given date.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"Attempting to load daily averages for date: {date_str}")

    try:
        daily_avg_df = provider.get_product_averages_for(date_str)
        # Provider returns DataFrame[SepaAvgPriceProductModel]
        logger.info(f"Successfully loaded {len(daily_avg_df)} daily average records for {date_str}.")
        return daily_avg_df
    except Exception as e:
        msg = f"Fatal error loading daily averages for date {date_str}: {e}"
        logger.error(msg, exc_info=True)
        # Re-raise as a specific error type for clarity in flow error handling
        raise DailyAverageLoadingError(msg) from e


class MappingIntegrityError(ValueError):
    """Custom exception for product mapping failures during transformation."""
    pass


@task(name="Transform Product Data for Insertion")
async def transform_product_data(
    daily_avg_df: DataFrame[SepaAvgPriceProductModel],
    descriptor_df: DataFrame[PrimitiveSourceDataModel],
    date_str: DateStr,  # Add date_str for logging
) -> DataFrame[TnDataRowModel]:  # Update return type
    """
    Transforms daily average product data into the format needed for TN insertion.

    Joins daily averages with the descriptor to get stream IDs, performs
    a mapping integrity check, converts data types (timestamp, value),
    and validates the final structure against TnDataRowModel.

    Args:
        daily_avg_df: DataFrame containing daily product averages (SepaAvgPriceProductModel).
        descriptor_df: DataFrame containing the product descriptor (PrimitiveSourceDataModel).
        date_str: The date string ('YYYY-MM-DD') being processed (for logging).

    Returns:
        A DataFrame containing the joined data (intermediate step).

    Raises:
        MappingIntegrityError: If any product ID (`id_producto`) from the daily
            average data is not found in the descriptor.
        ValueError: If timestamp conversion fails.
        pandera.errors.SchemaError: If the final DataFrame fails validation against TnDataRowModel.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"Transforming data for date: {date_str}. Input records: {len(daily_avg_df)}")

    if daily_avg_df.empty:
        logger.info(f"Input daily average DataFrame for {date_str} is empty. Returning empty DataFrame.")
        # Return an empty DataFrame conforming to the target model
        return create_empty_df(TnDataRowModel)

    # 1. Prepare descriptor for join (keep only necessary columns)
    descriptor_join = descriptor_df[["stream_id", "source_id"]].copy()

    # 2. Perform Inner Join
    # Use suffixes to avoid potential column name collisions if needed later
    joined_df = pd.merge(
        daily_avg_df,
        descriptor_join,
        left_on="id_producto",
        right_on="source_id",
        how="inner",
        suffixes=["_avg", "_desc"],
    )

    logger.info(f"Joined daily averages with descriptor for {date_str}. Resulting records: {len(joined_df)}")

    # 3. Mapping Integrity Check
    initial_product_ids = set(daily_avg_df["id_producto"].unique())
    mapped_product_ids = set(joined_df["id_producto"].unique())

    if initial_product_ids != mapped_product_ids:
        missing_ids = initial_product_ids - mapped_product_ids
        error_msg = (
            f"Mapping integrity check failed for date {date_str}! "
            f"{len(missing_ids)} product IDs from daily averages were not found in the descriptor. "
            f"Missing IDs sample: {list(missing_ids)[:10]}..."
        )
        logger.error(error_msg)
        raise MappingIntegrityError(error_msg)
    else:
        logger.info(f"Mapping integrity check passed for date {date_str}.")

    # --- Conversion & Final Model ---
    logger.info(f"Starting data conversion and final model structuring for {date_str}.")

    # 4. Select and Rename Columns for TnDataRowModel
    # We need: stream_id (from join), date (string from join), productos_precio_lista_avg (from join)
    try:
        transformed_df = joined_df[[
            "stream_id",
            "date",  # Keep original date string for conversion
            "productos_precio_lista_avg",
        ]].copy() # Use copy to avoid SettingWithCopyWarning

        # 5. Timestamp Conversion
        # Convert 'YYYY-MM-DD' date column to UTC midnight Unix timestamps (integer seconds)
        # Explicitly cast to str series to satisfy linter before passing to conversion util
        date_str_series = transformed_df["date"].astype(str) 
        int_timestamps = convert_date_str_series_to_unix_ts(date_str_series)
        # Convert integer timestamps back to string for TN
        transformed_df["date"] = int_timestamps.astype(str)
        logger.debug(f"Converted date strings to string Unix timestamps for {date_str}.")

        # 6. Value Conversion
        # Rename 'productos_precio_lista_avg' to 'value' and convert to string
        transformed_df.rename(columns={"productos_precio_lista_avg": "value"}, inplace=True)
        transformed_df["value"] = transformed_df["value"].astype(str)
        logger.debug(f"Converted average prices to string values for {date_str}.")

        # 7. Add missing 'data_provider' column (default to None/null if not available)
        # TnDataRowModel expects this column
        if "data_provider" not in transformed_df.columns:
             transformed_df["data_provider"] = None # Pandera handles None for nullable str

    except KeyError as e:
        logger.error(f"Missing expected column during transformation for {date_str}: {e}", exc_info=True)
        raise ValueError(f"Transformation failed due to missing column: {e}") from e
    except ValueError as e:
        # Catch errors from convert_date_str_series_to_unix_ts
        logger.error(f"Error during data conversion (timestamp/value) for {date_str}: {e}", exc_info=True)
        raise # Re-raise the conversion error
    except Exception as e:
        logger.error(f"Unexpected error during data selection/conversion for {date_str}: {e}", exc_info=True)
        raise ValueError(f"Unexpected transformation error: {e}") from e

    # 8. Validate Final DataFrame against TnDataRowModel
    try:
        # The model itself has strict="filter" and coerce=True, add_missing_columns=True
        validated_df = TnDataRowModel.validate(transformed_df, lazy=True)
        logger.info(f"Successfully transformed and validated {len(validated_df)} records for {date_str}.")
        return validated_df
    except Exception as e: # Catch Pandera SchemaError or other validation issues
        logger.error(f"Final DataFrame validation failed for {date_str}: {e}", exc_info=True)
        raise
