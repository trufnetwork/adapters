"""
Tasks for Argentina SEPA data processing.
"""

# Expose tasks from this module
from .aggregate_products_tasks import load_aggregation_state

__all__ = [
    "load_aggregation_state",
] 