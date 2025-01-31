"""
Category price aggregation logic for SEPA data.

This module contains functions for aggregating product prices by category from the
SEPA dataset.
"""

from tsn_adapters.tasks.argentina.aggregate.uncategorized import get_uncategorized_products
from tsn_adapters.tasks.argentina.errors import (
    EmptyCategoryMapError,
    ErrorAccumulator,
    UncategorizedProductsError,
)
from tsn_adapters.tasks.argentina.types import AggregatedPricesDF, AvgPriceDF, CategoryMapDF, UncategorizedDF
from tsn_adapters.utils.logging import get_logger_safe

logger = get_logger_safe(__name__)


def aggregate_prices_by_category(
    product_category_map_df: CategoryMapDF,
    avg_price_product_df: AvgPriceDF,
) -> tuple[AggregatedPricesDF, UncategorizedDF]:
    """
    Aggregate product prices by category over time.

    This function combines product category mapping data with average price data
    to compute category-level price averages for each date.

    Parameters
    ----------
    product_category_map_df : CategoryMapDF
        DataFrame containing the mapping between products and their categories
    avg_price_product_df : AvgPriceDF
        DataFrame containing average prices per product over time

    Returns
    -------
    tuple[AggregatedPricesDF, UncategorizedDF]
        DataFrame containing average prices per category over time

    Notes
    -----
    The aggregation process:
    1. Merges product prices with their category assignments
    2. Groups the data by category and date
    3. Computes the mean price for each category-date combination

    Raises
    ------
    EmptyCategoryMapError
        If the product category mapping DataFrame is empty
    """
    # Input validation and logging
    if product_category_map_df.empty:
        logger.error("Product category mapping DataFrame is empty")
        raise EmptyCategoryMapError(url="<unknown>")

    if avg_price_product_df.empty:
        logger.warning("Average price product DataFrame is empty")
        return AggregatedPricesDF(), UncategorizedDF()

    # Log input data stats
    logger.info(
        f"Starting aggregation with {len(product_category_map_df)} category mappings "
        f"and {len(avg_price_product_df)} price records"
    )

    # Merge the product categories with prices
    merged_df = avg_price_product_df.merge(
        product_category_map_df,
        how="inner",
        left_on="id_producto",
        right_on="id_producto",
    )

    # Log merge results
    if len(merged_df) < len(avg_price_product_df):
        logger.warning(
            f"Dropped {len(avg_price_product_df) - len(merged_df)} price records " "due to missing category mappings"
        )

    # Group by category_id and date, and aggregate the avg price
    aggregated_df = merged_df.groupby(["category_id", "date"]).agg(
        avg_price=("productos_precio_lista_avg", "mean"),
    )

    # Reset index to convert groupby result to regular columns
    aggregated_df = aggregated_df.reset_index()

    # Log final results
    logger.info(f"Successfully aggregated prices into {len(aggregated_df)} category-date combinations")

    uncategorized_df = get_uncategorized_products(avg_price_product_df, product_category_map_df)

    if not uncategorized_df.empty:
        logger.warning(f"Found {len(uncategorized_df)} uncategorized products")
        accumulator = ErrorAccumulator.get_or_create_from_context()
        accumulator.add_error(UncategorizedProductsError(
            count=len(uncategorized_df),
            date=avg_price_product_df["date"].iloc[0],
            store_id=avg_price_product_df["id_comercio"].iloc[0]
        ))

    return AggregatedPricesDF(aggregated_df), uncategorized_df
