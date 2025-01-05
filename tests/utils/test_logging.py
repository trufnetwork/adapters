"""Tests for logging utilities."""

import logging
from unittest.mock import patch

import pytest
from prefect import flow, task

from tsn_adapters.utils.logging import get_logger_safe


def test_get_logger_safe_outside_flow():
    """Test that get_logger_safe returns a standard logger outside a flow."""
    logger = get_logger_safe("test_logger")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test_logger"


def test_get_logger_safe_with_default_name():
    """Test that get_logger_safe uses the module name where it's called."""
    logger = get_logger_safe()
    assert isinstance(logger, logging.Logger)
    # When called without a name, it should use the module name where get_logger_safe is defined
    assert logger.name == "tsn_adapters.utils.logging"


@flow
def example_flow():
    """Example flow to test logging in a Prefect context."""
    logger = get_logger_safe()
    return logger


def test_get_logger_safe_inside_flow():
    """Test that get_logger_safe returns a Prefect logger inside a flow."""
    logger = example_flow()
    # The exact type might vary by Prefect version, but it should not be a standard logger
    assert not isinstance(logger, logging.Logger)


def test_get_logger_safe_handles_errors():
    """Test that get_logger_safe handles errors gracefully."""
    with patch("tsn_adapters.utils.logging.get_run_logger", side_effect=Exception("Test error")):
        logger = get_logger_safe("test_error_logger")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test_error_logger" 