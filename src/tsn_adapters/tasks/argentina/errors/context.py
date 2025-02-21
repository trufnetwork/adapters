from collections.abc import Generator
from contextlib import contextmanager
import contextvars
from typing import Any

# A context variable to hold a dictionary of key/value pairs for error context.
_error_context_var = contextvars.ContextVar("error_context", default={})


def set_error_context(key: str, value: Any) -> None:
    """
    Set a key/value pair in the global error context.
    """
    current = _error_context_var.get().copy()
    current[key] = value
    _error_context_var.set(current)


def get_error_context() -> dict[str, Any]:
    """
    Retrieve the current global error context.
    """
    return _error_context_var.get()


def clear_error_context() -> None:
    """
    Clear the global error context.
    """
    _error_context_var.set({})


@contextmanager
def error_context(**kwargs: Any) -> Generator[None, None, None]:
    """
    Context manager to temporarily update the error context.
    Example:
        with error_context(store_id="STORE-123", user_id="USER-456"):
            ...your logic...
    """
    current = get_error_context().copy()
    current.update(kwargs)
    token = _error_context_var.set(current)
    try:
        yield
    finally:
        _error_context_var.reset(token)
