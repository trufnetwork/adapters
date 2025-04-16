"""
Logging utilities for TSN adapters.

This module provides logging utilities that work both in Prefect flows/tasks and in test environments.
"""

import logging
from typing import Any

from prefect import get_run_logger
from prefect.context import FlowRunContext, TaskRunContext


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
    # Check for existing contexts
    # we do like this, because we don't want to disable logs at tests
    task_run_context = TaskRunContext.get()
    flow_run_context = FlowRunContext.get()
    if task_run_context or flow_run_context:
        return get_run_logger()
    else:
        return logging.getLogger(name)
