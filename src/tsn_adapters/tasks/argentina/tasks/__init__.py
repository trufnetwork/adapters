"""
Tasks for Argentina SEPA data processing.
"""

from .aggregate_products_tasks import (
    process_single_date_products,
    determine_aggregation_dates,
)
from .descriptor_tasks import load_product_descriptor
from .date_processing_tasks import determine_dates_to_insert, load_daily_averages, transform_product_data

__all__ = [
    "load_product_descriptor",
    "determine_dates_to_insert",
    "load_daily_averages",
    "transform_product_data",
    "process_single_date_products",
    "determine_aggregation_dates",
]
