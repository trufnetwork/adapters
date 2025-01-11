"""
Data transformers for SEPA data.
"""

from typing import cast

import pandas as pd
from prefect import task

from tsn_adapters.tasks.argentina.aggregate.category_price_aggregator import aggregate_prices_by_category
from tsn_adapters.tasks.argentina.models.aggregated_prices import sepa_aggregated_prices_to_tn_records
from tsn_adapters.tasks.argentina.models.category_map import get_uncategorized_products
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.transformers.interfaces import IDataTransformer
from tsn_adapters.tasks.argentina.types import (
    AggregatedPricesDF,
    AvgPriceDF,
    CategoryMapDF,
    SepaDF,
    SourceId,
    StreamIdMap,
)


class SepaDataTransformer(IDataTransformer[SepaDF]):
    """Transforms SEPA data into TrufNetwork records."""

    def __init__(self, product_category_map_df: CategoryMapDF, stream_id_map: StreamIdMap):
        """
        Initialize with product category mapping and stream ID mapping.

        Args:
            product_category_map_df: DataFrame mapping products to categories
            stream_id_map: Dict mapping category IDs to stream IDs
        """
        self.product_category_map_df = product_category_map_df
        self.stream_id_map = stream_id_map

    def transform(self, data: SepaDF) -> pd.DataFrame:
        """
        Transform SEPA data into TrufNetwork records.

        Args:
            data: The SEPA data to transform

        Returns:
            pd.DataFrame: The transformed data ready for TrufNetwork
        """
        # Calculate average prices
        avg_price_by_product_df = cast(AvgPriceDF, SepaAvgPriceProductModel.from_sepa_product_data(data))

        # Aggregate by category
        aggregated_prices = cast(
            AggregatedPricesDF,
            aggregate_prices_by_category(self.product_category_map_df, avg_price_by_product_df),
        )

        # Convert to TN records
        tn_records = sepa_aggregated_prices_to_tn_records(
            aggregated_prices,
            lambda category_id: self.stream_id_map[cast(SourceId, category_id)],
        )

        return tn_records

    def get_uncategorized(self, data: SepaDF) -> SepaDF:
        """
        Get products that don't have a category mapping.

        Args:
            data: The SEPA data to check

        Returns:
            DataFrame: The uncategorized products
        """
        return cast(SepaDF, get_uncategorized_products(data, self.product_category_map_df))


@task(name="Create SEPA Data Transformer")
def create_sepa_transformer(product_category_map_df: CategoryMapDF, stream_id_map: StreamIdMap) -> SepaDataTransformer:
    """
    Create a SEPA data transformer instance.

    Args:
        product_category_map_df: DataFrame mapping products to categories
        stream_id_map: Dict mapping category IDs to stream IDs

    Returns:
        SepaDataTransformer: The transformer instance
    """
    return SepaDataTransformer(product_category_map_df, stream_id_map)
