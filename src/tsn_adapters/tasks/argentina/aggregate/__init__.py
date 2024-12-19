"""
Price aggregation functionality for the Argentina SEPA data.

This package provides functions for aggregating and transforming price data
from the SEPA dataset.
"""

from tsn_adapters.tasks.argentina.aggregate.category_price_aggregator import aggregate_prices_by_category

__all__ = ["aggregate_prices_by_category"]
