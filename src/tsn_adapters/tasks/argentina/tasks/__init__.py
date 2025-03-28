"""
Tasks for Argentina SEPA data processing.
"""

# Expose tasks from this module
from .aggregate_products_tasks import load_aggregation_state, save_aggregation_state, determine_date_range_to_process

__all__ = [
    "load_aggregation_state",
    "save_aggregation_state",
    "determine_date_range_to_process",
]
