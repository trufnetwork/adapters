"""
Logging utilities for TSN adapters.

This module provides logging utilities that work both in Prefect flows/tasks and in test environments.
"""

import logging
from typing import Any

from prefect import get_run_logger


def get_logger_safe(name: str = __name__) -> Any:
    """
    Get a logger that works both in Prefect flows/tasks and in test environments.

    This function attempts to get a Prefect logger first, and falls back to standard
    Python logging if no Prefect context is available.

    Parameters
    ----------
    name : str
        The name to use for the logger if falling back to standard logging.
        Defaults to the module name.

    Returns
    -------
    Logger
        Either a Prefect logger or a standard Python logger.
    """
    try:
        return get_run_logger()
    except Exception:
        return logging.getLogger(name)
