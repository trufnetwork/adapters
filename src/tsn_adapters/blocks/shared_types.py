"""
Shared type definitions for TN Access and Stream Filtering modules.

This module contains shared type definitions used by various modules
to help break circular dependencies.
"""

from typing import TypedDict
from pandera.typing import DataFrame
from tsn_adapters.common.trufnetwork.models.tn_models import StreamLocatorModel


class DivideAndConquerResult(TypedDict):
    """Results from the divide and conquer stream filtering algorithm."""

    initialized_streams: DataFrame[StreamLocatorModel]
    uninitialized_streams: DataFrame[StreamLocatorModel]
    depth: int
    fallback_used: bool 