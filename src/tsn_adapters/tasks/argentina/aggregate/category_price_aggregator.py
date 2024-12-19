"""
Category price aggregation logic for SEPA data.

This module contains functions for aggregating product prices by category from the
SEPA dataset.
"""

from pandera.typing import DataFrame as paDataFrame
from tsn_adapters.tasks.argentina.models import (
    SepaProductCategoryMapModel,
    SepaAggregatedPricesModel,
    SepaAvgPriceProductModel,
)


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
    # Merge the product categories with prices
    merged_df = avg_price_product_df.merge(
        product_category_map_df,
        how="inner",
        left_on="id_producto",
        right_on="id_producto",
    )

    # Group by category_id and date, and aggregate the avg price
    aggregated_df = merged_df.groupby(["category_id", "date"]).agg(
        avg_price=("productos_precio_lista_avg", "mean"),
    )

    # Reset index to convert groupby result to regular columns
    aggregated_df = aggregated_df.reset_index()

    return paDataFrame[SepaAggregatedPricesModel](aggregated_df) 