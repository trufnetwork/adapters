"""
Base interfaces for data transformation.
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pandera.typing import DataFrame

from ..trufnetwork.models.tn_models import TnDataRowModel

T = TypeVar("T")  # For input data types


class IDataTransformer(ABC, Generic[T]):
    """Interface for transforming data."""

    @abstractmethod
    def transform(self, data: T) -> DataFrame[TnDataRowModel]:
        """
        Transform data from source format to target format.

        Args:
            data: The data to transform

        Returns:
            DataFrame[TnDataRowModel]: The transformed data
        """
        pass
