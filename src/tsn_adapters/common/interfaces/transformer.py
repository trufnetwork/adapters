"""
Base interfaces for data transformation.
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

import pandas as pd

T = TypeVar("T")  # For input data types


class IDataTransformer(ABC, Generic[T]):
    """Interface for transforming data."""

    @abstractmethod
    def transform(self, data: T) -> pd.DataFrame:
        """
        Transform data from source format to target format.

        Args:
            data: The data to transform

        Returns:
            pd.DataFrame: The transformed data
        """
        pass
