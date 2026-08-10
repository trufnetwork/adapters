"""
Tasks related to processing dates for Argentina SEPA data insertion.
"""

from datetime import datetime, timedelta
from typing import List, Optional, cast

import pandas as pd
from pandera.typing import DataFrame, Series
from prefect import task
import prefect.variables as variables  # Import prefect variables

from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel
from tsn_adapters.tasks.argentina.config import ArgentinaFlowVariableNames
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.types import DateStr
from tsn_adapters.utils import convert_date_str_series_to_unix_ts, force_sync
from tsn_adapters.utils.create_empty_df import create_empty_df
from tsn_adapters.utils.logging import get_logger_safe  # Re-using logger helper


@task(name="Determine Dates to Insert")
async def determine_dates_to_insert(
    provider: ProductAveragesProvider,
) -> List[DateStr]:
    """
    Determines the list of dates to insert based on available data and state variables.

    Filters available dates based on the last successfully inserted date and the
    last successfully aggregated date stored in Prefect Variables. Dates to insert
    must be AFTER the last insertion and ON or BEFORE the last aggregation.

    Args:
        provider: The provider instance to list available daily average keys.

    Returns:
        A sorted list of date strings ('YYYY-MM-DD') to be processed.

    Raises:
        Exception: If listing available keys from the provider fails or variable access fails.
    """
    logger = get_logger_safe(__name__)

    # 1. Get available dates from provider
    try:
        available_dates_str: List[DateStr] = provider.list_available_keys()
        if not available_dates_str:
            logger.warning("No available dates found in the provider.")
            return []
        logger.info(f"Provider listed {len(available_dates_str)} available dates.")

        # Sort initially to ensure chronological processing if needed later
        available_dates_str.sort()

        # 2. Fetch Prefect Variables for date filtering
        try:
            # Get last insertion date
            last_insertion_processed_date = force_sync(variables.Variable.get)(
                ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE,
                default=ArgentinaFlowVariableNames.DEFAULT_DATE
            )
            assert isinstance(last_insertion_processed_date, str)

            # Get last *aggregation* date (corrected logic)
            last_aggregation_processed_date = force_sync(variables.Variable.get)(
                ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE, # Correct variable
                default=ArgentinaFlowVariableNames.DEFAULT_DATE
            )
            assert isinstance(last_aggregation_processed_date, str)

            logger.info(
                f"Using gating dates: Last Insertion = {last_insertion_processed_date}, "
                f"Last Aggregation = {last_aggregation_processed_date}" # Corrected log
            )
        except Exception as e:
            logger.error(f"Failed to retrieve Prefect variables: {e}", exc_info=True)
            # Raise the error to halt the flow if variables can't be accessed
            raise RuntimeError("Failed to retrieve Prefect gating variables") from e

        # 3. Filter dates based on state variables
        # Convert date strings to datetime objects for comparison
        try:
            insertion_dt = datetime.strptime(last_insertion_processed_date, "%Y-%m-%d").date()
            aggregation_dt = datetime.strptime(last_aggregation_processed_date, "%Y-%m-%d").date() # Corrected variable name

            # Check if aggregation date is somehow before insertion date (shouldn't happen in normal flow)
            if aggregation_dt < insertion_dt:
                logger.warning(
                    f"last_aggregation_processed_date {aggregation_dt} is before "
                    f"last_insertion_processed_date {insertion_dt}. Returning empty list."
                )
                return []

            # Filter dates that are:
            # 1. After the last insertion date (>)
            # 2. On or before the last aggregation date (<=)
            filtered_dates = []
            for date_str in available_dates_str:
                try:
                    date_dt = datetime.strptime(date_str, "%Y-%m-%d").date()
                    if date_dt > insertion_dt and date_dt <= aggregation_dt: # Corrected logic check
                        filtered_dates.append(date_str)
                except ValueError:
                    logger.warning(f"Skipping available key '{date_str}' due to invalid date format.")
                    continue

            logger.info(
                f"After filtering: {len(filtered_dates)} dates to process "
                f"(from {len(available_dates_str)} available)."
            )
            return filtered_dates

        except ValueError as e:
            logger.error(f"Invalid date format in Prefect variables: {e}", exc_info=True)
            # Raise error if dates in variables are corrupt
            raise ValueError("Invalid date format found in Prefect state variables") from e

    except Exception as e:
        logger.error(f"Failed to list available keys from provider: {e}", exc_info=True)
        # Propagate error as this is critical
        raise # Re-raise other provider errors


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


def previous_day(date_str: DateStr) -> DateStr:
    """Return the calendar day before `date_str`, in the same 'YYYY-MM-DD' shape."""
    day_before = datetime.strptime(date_str, "%Y-%m-%d").date() - timedelta(days=1)
    return cast(DateStr, day_before.isoformat())


@task(name="Load Previous Daily Averages")
async def load_previous_daily_averages(
    provider: ProductAveragesProvider,
    date_str: DateStr,
) -> Optional[DataFrame[SepaAvgPriceProductModel]]:
    """
    Load the daily averages for the day before `date_str`, or None if there are none.

    A missing predecessor is ordinary rather than exceptional: the first date SEPA
    ever published has none, and the upstream pipeline skips a day now and then.
    Callers read None as "treat every product as changed", so the cost of a missing
    file is redundant writes, never missing ones.

    Args:
        provider: The provider instance to fetch daily data.
        date_str: The date ('YYYY-MM-DD') whose predecessor should be loaded.

    Returns:
        The previous day's averages, or None when that day has no data.

    Raises:
        DailyAverageLoadingError: If the file exists but cannot be loaded or parsed.
    """
    logger = get_logger_safe(__name__)
    prior_date = previous_day(date_str)

    if not provider.exists(prior_date):
        logger.warning(
            f"No daily averages available for {prior_date}, the day before {date_str}. "
            f"Every product for {date_str} will be treated as changed."
        )
        return None

    return await load_daily_averages(provider=provider, date_str=prior_date)


@task(name="Filter Unchanged Products")
def filter_unchanged_products(
    daily_avg_df: DataFrame[SepaAvgPriceProductModel],
    previous_avg_df: Optional[DataFrame[SepaAvgPriceProductModel]],
    date_str: DateStr,
) -> DataFrame[SepaAvgPriceProductModel]:
    """
    Drop the products whose average price is identical to the previous day's.

    TN carries the last observation forward: a query for a date with no record
    resolves to the newest record at or before it, for primitive and composed
    streams alike. Re-broadcasting a price that did not move therefore costs a row
    and a transaction fee without telling a reader anything new.

    The comparison is against the previous day's *source* file rather than against
    what was published, which is what lets this work without storing any state.
    When a price is unchanged we skip the write, so the value left standing on
    chain is the one from the last day the price actually moved — and that is the
    same number the previous day's source file holds. A product missing from that
    file breaks the chain, so it is published: the cost of a gap in the source is a
    redundant write, never a missing one.

    Args:
        daily_avg_df: The averages for `date_str`.
        previous_avg_df: The averages for the day before, or None if unavailable.
        date_str: The date being processed (for logging).

    Returns:
        The subset of `daily_avg_df` whose prices differ from the previous day's.
    """
    logger = get_logger_safe(__name__)

    if daily_avg_df.empty or previous_avg_df is None or previous_avg_df.empty:
        logger.info(f"No previous day to compare {date_str} against. Keeping all {len(daily_avg_df)} products.")
        return daily_avg_df

    # A product listed twice on the previous day has no single previous price;
    # keeping one row makes the join incapable of duplicating today's rows.
    previous_prices = (
        previous_avg_df[["id_producto", "productos_precio_lista_avg"]]
        .drop_duplicates(subset="id_producto", keep="first")
        .rename(columns={"productos_precio_lista_avg": "previous_price"})
    )

    # merge preserves the left frame's row order because the right key is unique,
    # so the mask lines up positionally with daily_avg_df.
    compared = daily_avg_df.merge(previous_prices, on="id_producto", how="left")
    unchanged = (
        compared["previous_price"].notna() & (compared["productos_precio_lista_avg"] == compared["previous_price"])
    ).to_numpy()

    changed_df = daily_avg_df[~unchanged].reset_index(drop=True)
    logger.info(
        f"{date_str}: {len(changed_df)} of {len(daily_avg_df)} products moved since the previous day; "
        f"skipping {int(unchanged.sum())} unchanged."
    )
    return changed_df


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
        date_str_series: Series[str] = cast(Series[str], transformed_df["date"].astype(str)) # Fix type hint issue
        int_timestamps = convert_date_str_series_to_unix_ts(date_str_series)
        # Convert integer timestamps back to string for TN
        transformed_df["date"] = int_timestamps.astype(str)
        logger.debug(f"Converted date strings to string Unix timestamps for {date_str}.")

        # 6. Value Conversion
        # Rename 'productos_precio_lista_avg' to 'value' and convert to string
        transformed_df.rename(columns={"productos_precio_lista_avg": "value"}, inplace=True)
        transformed_df["value"] = transformed_df["value"].astype(str)
        logger.debug(f"Converted average prices to string values for {date_str}.")

        # 7. Add missing 'data_provider' column (nullable); Pandera Field(nullable=True) will allow None
        if "data_provider" not in transformed_df.columns:
            transformed_df["data_provider"] = None

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
