"""
Error handling for Argentina SEPA processing.
"""

from tsn_adapters.tasks.argentina.errors.accumulator import ErrorAccumulator
from tsn_adapters.tasks.argentina.errors.errors import (
    AccountableRole,
    ArgentinaSEPAError,
    DateMismatchError,
    EmptyCategoryMapError,
    InvalidCategorySchemaError,
    InvalidCSVSchemaError,
    InvalidDateFormatError,
    InvalidStructureZIPError,
    MissingProductIDError,
    MissingProductosCSVError,
    UncategorizedProductsError,
)

__all__ = [
    "ErrorAccumulator",
    "AccountableRole",
    "ArgentinaSEPAError",
    "DateMismatchError",
    "EmptyCategoryMapError",
    "InvalidCategorySchemaError",
    "InvalidCSVSchemaError",
    "InvalidDateFormatError",
    "InvalidStructureZIPError",
    "MissingProductIDError",
    "MissingProductosCSVError",
    "UncategorizedProductsError",
] 