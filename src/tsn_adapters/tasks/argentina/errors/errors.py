"""
Structured error handling for Argentina SEPA processing.
"""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel

# NEW: Import the global error context helper
from tsn_adapters.tasks.argentina.errors.context_helper import ArgentinaErrorContext


class AccountableRole(Enum):
    """Roles responsible for different types of errors."""

    DATA_PROVIDER = "Data Provider"
    DATA_ENGINEERING = "Data Engineering"
    DEVELOPMENT = "Development"
    SYSTEM = "System"


class ArgentinaSEPAErrorData(BaseModel):
    """Data model for Argentina SEPA errors."""

    code: str
    message: str
    responsibility: AccountableRole
    context: dict[str, Any] = {}


class ArgentinaSEPAError(Exception):
    """Base class for all Argentina SEPA processing errors."""

    def __init__(
        self, code: str, message: str, responsibility: AccountableRole, context: Optional[dict[str, Any]] = None
    ):
        super().__init__(message)
        self.data = ArgentinaSEPAErrorData(
            code=code,
            message=message,
            responsibility=responsibility,
            context=context or {},
        )

    @property
    def code(self) -> str:
        return self.data.code

    @property
    def message(self) -> str:
        return self.data.message

    @property
    def responsibility(self) -> AccountableRole:
        return self.data.responsibility

    @property
    def context(self) -> dict[str, Any]:
        return self.data.context

    def to_dict(self) -> dict[str, Any]:
        """Convert error to dictionary format."""
        return {
            "code": self.code,
            "message": self.message,
            "responsibility": self.responsibility,
            "context": self.context,
        }


# --------------------------------------------------
# Input Validation Errors (100-199)
# --------------------------------------------------
class InvalidStructureZIPError(ArgentinaSEPAError):
    """Invalid ZIP file structure during extraction"""

    def __init__(self, error: str):
        ctx = ArgentinaErrorContext()
        super().__init__(
            code="ARG-100",
            message="Invalid ZIP file structure - cannot extract files",
            responsibility=AccountableRole.DATA_PROVIDER,
            context={
                "date": ctx.date,
                "error": error,
            },
        )


class InvalidDateFormatError(ArgentinaSEPAError):
    """Invalid date format in flow input"""

    def __init__(self, date_str: str):
        super().__init__(
            code="ARG-101",
            message=f"Invalid date format: {date_str} - must be YYYY-MM-DD",
            responsibility=AccountableRole.SYSTEM,
            context={"invalid_date": date_str},
        )


class MissingProductosCSVError(ArgentinaSEPAError):
    """Missing productos.csv in ZIP file"""

    def __init__(self, directory: str, available_files: list[str]):
        super().__init__(
            code="ARG-102",
            message="Missing productos.csv in ZIP archive",
            responsibility=AccountableRole.DATA_PROVIDER,
            context={"directory": directory, "available_files": ", ".join(available_files)},
        )


# --------------------------------------------------
# Data Processing Errors (200-299)
# --------------------------------------------------
class DateMismatchError(ArgentinaSEPAError):
    """Filename vs content date mismatch"""

    def __init__(self, internal_date: str):
        ctx = ArgentinaErrorContext()
        super().__init__(
            code="ARG-200",
            message=f"Date mismatch: Reported {ctx.date} vs Actual {internal_date}",
            responsibility=AccountableRole.DATA_PROVIDER,
            context={
                "external_date": ctx.date,
                "internal_date": internal_date,
            },
        )


class InvalidCSVSchemaError(ArgentinaSEPAError):
    """Missing required columns in RAW data"""

    def __init__(self, error: str):
        ctx = ArgentinaErrorContext()
        super().__init__(
            code="ARG-201",
            message="Missing required columns",
            responsibility=AccountableRole.DATA_PROVIDER,
            context={
                "date": ctx.date,
                "store_id": ctx.store_id,
                "error": error,
            },
        )


class InvalidProductsError(ArgentinaSEPAError):
    """Null/empty product IDs found in RAW data"""

    def __init__(self, invalid_indexes: list[int]):
        ctx = ArgentinaErrorContext()
        invalid_indexes_str = ", ".join(str(idx) for idx in invalid_indexes)
        super().__init__(
            code="ARG-202",
            message=f"{len(invalid_indexes)} products with invalid IDs",
            responsibility=AccountableRole.DEVELOPMENT,
            context={"invalid_indexes": invalid_indexes_str, "date": ctx.date, "store_id": ctx.store_id},
        )


# --------------------------------------------------
# Category Mapping Errors (300-399)
# --------------------------------------------------
class EmptyCategoryMapError(ArgentinaSEPAError):
    """Empty category mapping DataFrame"""

    def __init__(self, url: str):
        super().__init__(
            code="ARG-300",
            message="Category mapping is empty",
            responsibility=AccountableRole.DATA_ENGINEERING,
            context={"url": url},
        )


class UncategorizedProductsError(ArgentinaSEPAError):
    """Products without category mapping"""

    def __init__(self, count: int):
        ctx = ArgentinaErrorContext()
        super().__init__(
            code="ARG-301",
            message=f"{count} uncategorized products found",
            responsibility=AccountableRole.DATA_ENGINEERING,
            context={"uncategorized_count": str(count), "date": ctx.date},
        )


class InvalidCategorySchemaError(ArgentinaSEPAError):
    """Invalid category mapping schema"""

    def __init__(self, error: str, url: str):
        super().__init__(
            code="ARG-302",
            message="Invalid category mapping schema",
            responsibility=AccountableRole.DATA_ENGINEERING,
            context={"error": error, "url": url},
        )


all_errors = [
    InvalidStructureZIPError,
    InvalidDateFormatError,
    MissingProductosCSVError,
    DateMismatchError,
    InvalidCSVSchemaError,
    InvalidProductsError,
    EmptyCategoryMapError,
    UncategorizedProductsError,
    InvalidCategorySchemaError,
]
