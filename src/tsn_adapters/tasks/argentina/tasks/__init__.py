"""
Tasks for Argentina SEPA data processing.
"""

from .aggregate_products_tasks import (
    create_empty_aggregated_data,
    load_aggregation_state,
    load_insertion_metadata,
    process_single_date_products,
    save_aggregation_state,
    save_insertion_metadata,
)
from .descriptor_tasks import load_product_descriptor
from .date_processing_tasks import determine_dates_to_insert, load_daily_averages, transform_product_data

__all__ = [
    "load_aggregation_state",
    "save_aggregation_state",
    "load_product_descriptor",
    "determine_dates_to_insert",
    "load_daily_averages",
    "transform_product_data",
    "load_insertion_metadata",
    "save_insertion_metadata",
    "create_empty_aggregated_data",
    "process_single_date_products",
]
