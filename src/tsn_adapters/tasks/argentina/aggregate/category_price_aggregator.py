"""
Category price aggregation logic for SEPA data.

This module contains functions for aggregating product prices by category from the
SEPA dataset.
"""

from pandera.typing import DataFrame as paDataFrame

from tsn_adapters.tasks.argentina.models import (
    SepaAggregatedPricesModel,
    SepaAvgPriceProductModel,
    SepaProductCategoryMapModel,
)
from tsn_adapters.utils.logging import get_logger_safe

logger = get_logger_safe(__name__)


def aggregate_prices_by_category(
    product_category_map_df: paDataFrame[SepaProductCategoryMapModel],
    avg_price_product_df: paDataFrame[SepaAvgPriceProductModel],
) -> paDataFrame[SepaAggregatedPricesModel]:
    """
    Aggregate product prices by category over time.

    This function combines product category mapping data with average price data
    to compute category-level price averages for each date.

    Parameters
    ----------
    product_category_map_df : paDataFrame[SepaProductCategoryMapModel]
        DataFrame containing the mapping between products and their categories
    avg_price_product_df : paDataFrame[SepaAvgPriceProductModel]
        DataFrame containing average prices per product over time

    Returns
    -------
    paDataFrame[SepaAggregatedPricesModel]
        DataFrame containing average prices per category over time

    Notes
    -----
    The aggregation process:
    1. Merges product prices with their category assignments
    2. Groups the data by category and date
    3. Computes the mean price for each category-date combination
    """
    # Input validation and logging
    if product_category_map_df.empty:
        logger.error("Product category mapping DataFrame is empty")
        raise ValueError("Cannot aggregate prices: product category mapping is empty")

    if avg_price_product_df.empty:
        logger.warning("Average price product DataFrame is empty")
        return paDataFrame[SepaAggregatedPricesModel](avg_price_product_df)

    # Log input data stats
    logger.info(
        f"Starting aggregation with {len(product_category_map_df)} category mappings "
        f"and {len(avg_price_product_df)} price records"
    )

    # Check for products without categories before merge
    unique_products_with_prices = set(avg_price_product_df["id_producto"].unique())
    unique_products_with_categories = set(product_category_map_df["id_producto"].unique())

    products_without_categories = unique_products_with_prices - unique_products_with_categories
    if products_without_categories:
        logger.warning(
            f"Found {len(products_without_categories)} products with prices but no category mapping. "
            "These will be excluded from aggregation."
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

    return paDataFrame[SepaAggregatedPricesModel](aggregated_df)
