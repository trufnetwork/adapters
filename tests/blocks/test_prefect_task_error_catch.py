"""
Minimal test to verify a Prefect task catches an exception from a mock and emits a log message.
"""
from unittest.mock import patch
from prefect import task
import logging
import pytest

class MyCustomError(Exception):
    pass

# The function to be mocked

@task(retries=1)
def may_raise():
    return "ok"

# The Prefect task that catches the error
@task(retries=1)
def task_catch_error():
    logger = logging.getLogger("test_logger")
    try:
        return may_raise()
    except MyCustomError:
        logger.info("Caught MyCustomError inside the task!")
        return "caught"

@pytest.mark.usefixtures("prefect_test_fixture")
def test_prefect_task_catches_error_and_logs(caplog: pytest.LogCaptureFixture):
    with patch(__name__ + ".may_raise", side_effect=MyCustomError("fail")):
        with caplog.at_level(logging.INFO, logger="test_logger"):
            result = task_catch_error()
            assert result == "caught"
            assert any("Caught MyCustomError inside the task!" in r.getMessage() for r in caplog.records) 