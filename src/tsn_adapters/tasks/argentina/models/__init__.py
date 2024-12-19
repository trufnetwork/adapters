"""
Models for the Argentina SEPA data processing pipeline.

This package contains Pandera models for validating and typing data structures
used in processing SEPA data.
"""

from tsn_adapters.tasks.argentina.models.category_map import SepaProductCategoryMapModel
from tsn_adapters.tasks.argentina.models.aggregated_prices import SepaAggregatedPricesModel
from tsn_adapters.tasks.argentina.models.sepa_models import SepaAvgPriceProductModel

__all__ = [
    "SepaProductCategoryMapModel",
    "SepaAggregatedPricesModel",
    "SepaAvgPriceProductModel",
]