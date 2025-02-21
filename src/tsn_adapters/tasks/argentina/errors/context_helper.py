"""
Helper class to store known context attributes for error management in Argentina flows.

This class encapsulates common context attributes (e.g., store_id, date, file_key)
which can be used during error reporting. When an error is raised, you can pass
the dictionary produced by this helper to automatically include this extra metadata.
"""

from typing import Any, Optional

from tsn_adapters.tasks.argentina.errors.context import get_error_context, set_error_context


class ContextProperty:
    """Descriptor for managing context properties in a DRY and type-safe manner."""

    def __init__(self, key: str) -> None:
        self.key = key

    def __get__(self, instance: Any, owner: Any) -> Optional[str]:
        return get_error_context().get(self.key)

    def __set__(self, instance: Any, value: str) -> None:
        set_error_context(self.key, value)
        # Optionally update the instance dictionary if needed
        instance.__dict__[self.key] = value


class ArgentinaErrorContext:
    """
    Helper class for managing known context attributes in Argentina flows.

    Attributes
    ----------
    store_id : Optional[str]
        The identifier for the store, if applicable.
    date : Optional[str]
        The relevant date (e.g., report date or flow execution date) in ISO format (YYYY-MM-DD).
    file_key : Optional[str]
        The file key or resource identifier involved in the error.
    """

    # DRY properties using the descriptor:
    store_id: ContextProperty = ContextProperty("store_id")
    date: ContextProperty = ContextProperty("date")
    file_key: ContextProperty = ContextProperty("file_key")
