"""
Tasks for Argentina SEPA data processing.
"""

from .aggregate_products_tasks import load_aggregation_state, save_aggregation_state, determine_aggregation_dates
from .descriptor_tasks import load_product_descriptor
from .date_processing_tasks import determine_dates_to_insert, load_daily_averages

__all__ = [
    "load_aggregation_state",
    "save_aggregation_state",
    "load_product_descriptor",
    "determine_dates_to_insert",
    "determine_aggregation_dates",
    "load_daily_averages",
]
