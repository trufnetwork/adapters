"""
Tasks related to processing dates for Argentina SEPA data insertion.
"""

from datetime import date, datetime
from typing import List

from pandera.typing import DataFrame
from prefect import task

from tsn_adapters.tasks.argentina.models import ArgentinaProductStateMetadata
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.types import DateStr
from tsn_adapters.utils.logging import get_logger_safe  # Re-using logger helper


@task(name="Determine Dates to Insert")
async def determine_dates_to_insert(
    metadata: ArgentinaProductStateMetadata,
    provider: ProductAveragesProvider,
) -> List[DateStr]:
    """
    Determines the list of dates to insert based on available data and metadata.

    Filters available dates based on the last successfully inserted date and
    the last product deployment date recorded in the metadata.

    Args:
        metadata: The current state metadata for Argentina products.
        provider: The provider instance to list available daily average keys.

    Returns:
        A sorted list of date strings ('YYYY-MM-DD') to be processed.

    Raises:
        Exception: If listing available keys from the provider fails.
    """
    logger = get_logger_safe(__name__)
    logger.info(
        f"Determining dates to process based on state: "
        f"last_insertion='{metadata.last_insertion_processed_date}', "
        f"last_deployment='{metadata.last_product_deployment_date}'"
    )

    # 1. Get available dates from provider
    try:
        available_dates_str: List[DateStr] = provider.list_available_keys()
        if not available_dates_str:
            logger.warning("No available dates found in the provider.")
            return []
        logger.info(f"Provider listed {len(available_dates_str)} available dates.")
        # Sort initially to ensure chronological processing if needed later
        available_dates_str.sort()
    except Exception as e:
        logger.error(f"Failed to list available keys from provider: {e}", exc_info=True)
        # Propagate error as this is critical
        raise

    # 2. Parse boundary dates from metadata (with fallbacks)
    try:
        last_insertion_date_obj = datetime.strptime(metadata.last_insertion_processed_date, "%Y-%m-%d").date()
    except ValueError:
        logger.warning(
            f"Invalid last_insertion_processed_date '{metadata.last_insertion_processed_date}' in metadata. Using 1970-01-01."
        )
        last_insertion_date_obj = date(1970, 1, 1)

    try:
        last_deployment_date_obj = datetime.strptime(metadata.last_product_deployment_date, "%Y-%m-%d").date()
    except ValueError:
        logger.warning(
            f"Invalid last_product_deployment_date '{metadata.last_product_deployment_date}' in metadata. Using 1970-01-01."
        )
        # Using a very early date effectively removes the upper bound if deployment date is invalid
        last_deployment_date_obj = date(1970, 1, 1)

    # Ensure deployment date isn't nonsensically early if insertion date is later
    if last_deployment_date_obj < last_insertion_date_obj:
        logger.warning(
            f"last_product_deployment_date ({last_deployment_date_obj}) is before "
            f"last_insertion_processed_date ({last_insertion_date_obj}). No dates can be processed."
        )
        return []

    # 3. Filter dates
    dates_to_process: List[DateStr] = []
    skipped_deployment_count = 0
    for date_str in available_dates_str:
        try:
            current_date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()

            # Apply filtering conditions
            if current_date_obj <= last_insertion_date_obj:
                continue  # Skip dates already processed or on the boundary date

            if current_date_obj > last_deployment_date_obj:
                skipped_deployment_count += 1
                continue  # Skip dates after the deployment cut-off

            # If passes filters, add to list
            dates_to_process.append(date_str)

        except ValueError:
            logger.warning(f"Skipping available key '{date_str}' due to invalid date format.")
            continue

    if skipped_deployment_count > 0:
        logger.info(
            f"Skipped {skipped_deployment_count} dates because they are later than "
            f"the last deployment date ({last_deployment_date_obj})."
        )

    # 4. Sort and Return (already sorted, but ensuring)
    dates_to_process.sort()
    logger.info(f"Found {len(dates_to_process)} dates to process after filtering.")

    return dates_to_process


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
