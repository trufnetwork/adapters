from typing import Any, Callable, cast
from unittest.mock import MagicMock, patch

from prefect.client.schemas.objects import StateDetails, StateType
from prefect.types._datetime import DateTime
from pydantic import SecretStr
import pytest
from trufnetwork_sdk_py.client import TNClient

from tsn_adapters.blocks.tn_access import (
    TNAccessBlock,
    TNNodeNetworkError,
    task_wait_for_tx,
    tn_special_retry_condition,
)


# --- Dummy TN Client to simulate behavior ---
class DummyTNClient(TNClient):
    def __init__(self):
        pass

    def get_first_record(self, *args: Any, **kwargs: Any) -> dict[str, str | float] | None:
        # Simulate a successful call
        return {"dummy_record": 1.0}

    def get_network_error(self, *args: Any, **kwargs: Any):
        # Simulate a TN network error
        raise RuntimeError("http post failed: dial tcp 1.2.3.4:8484: connect: connection refused")

    def get_other_error(self, *args: Any, **kwargs: Any):
        # Simulate any other error
        raise ValueError("Some other error")


# --- Tests for TNNodeNetworkError helper methods ---


def test_is_tn_node_network_error():
    error = RuntimeError("http post failed: dial tcp 1.2.3.4:8484: connect: connection refused")
    assert TNNodeNetworkError.is_tn_node_network_error(error)

    error2 = ValueError("Some other error")
    assert not TNNodeNetworkError.is_tn_node_network_error(error2)


# --- Dummy objects for testing tn_retry_condition ---

# For our tests we need dummy Task, TaskRun, and State objects.
# We use minimal dummy implementations.
from prefect import Task  # Ensure that the correct Task object is imported per your Prefect version.
from prefect.client.schemas.objects import State, TaskRun


class DummyTask(Task[Any, Any]):
    def __init__(self, fn: Callable[[], Any]):
        super().__init__(fn)


class DummyTaskRun(TaskRun):
    def __init__(self, run_count: int):
        super().__init__(run_count=run_count, task_key="dummy", dynamic_key="dummy")


class DummyState(State[Any]):
    def __init__(self, result_func: Callable[[], Any]):
        super().__init__(
            type=StateType.COMPLETED,
            message="dummy",
            data={},
            state_details=StateDetails(),
            timestamp=DateTime.now("UTC"),
        )
        self._result_func = result_func

    def result(self, raise_on_failure: bool = True, fetch: bool = True, retry_result_failure: bool = True) -> Any:
        return self._result_func()


def dummy_success():
    return "ok"


def dummy_network_error():
    raise TNNodeNetworkError("Simulated TN network error")


def dummy_value_error():
    raise ValueError("Simulated non-network error")


def test_retry_condition_network_error():
    # Always retry if a TN node network error is encountered.
    condition = tn_special_retry_condition(3)
    dummy_task = DummyTask(dummy_success)
    dummy_task_run = DummyTaskRun(run_count=5)  # run_count value is irrelevant for network errors
    dummy_state = DummyState(dummy_network_error)
    assert condition(dummy_task, dummy_task_run, dummy_state) is True


def test_retry_condition_non_network_below_max():
    condition = tn_special_retry_condition(3)
    dummy_task = DummyTask(dummy_success)
    dummy_task_run = DummyTaskRun(run_count=2)  # below max (3)
    dummy_state = DummyState(dummy_value_error)
    assert condition(dummy_task, dummy_task_run, dummy_state) is True


def test_retry_condition_non_network_at_max():
    condition = tn_special_retry_condition(3)
    dummy_task = DummyTask(dummy_success)
    dummy_task_run = DummyTaskRun(run_count=4)  # equals max -> no further retries allowed
    dummy_state = DummyState(dummy_value_error)
    assert condition(dummy_task, dummy_task_run, dummy_state) is False


def test_retry_condition_success_no_retry():
    condition = tn_special_retry_condition(3)
    dummy_task = DummyTask(dummy_success)
    dummy_task_run = DummyTaskRun(run_count=1)
    dummy_state = DummyState(dummy_success)
    # When no exception is raised, no retry is needed.
    assert condition(dummy_task, dummy_task_run, dummy_state) is False


# --- Test for a TNAccessBlock when the provider really doesn't exist ---
# Here we subclass TNAccessBlock to override its client with our dummy client.
class DummyTNAccessBlock(TNAccessBlock):
    @property
    def client(self) -> TNClient:
        # Always return our DummyTNClient that simulates a network error.
        return DummyTNClient()


def test_tn_access_block_network_error():
    block = DummyTNAccessBlock(tn_provider="nonexistent", tn_private_key=SecretStr("dummy"))
    # When invoking any client method via safe_client, expect TNNodeNetworkError.
    with pytest.raises(TNNodeNetworkError) as excinfo:
        cast(DummyTNClient, block.client).get_network_error()
    assert "http" in str(excinfo.value) or "connect" in str(excinfo.value)


@pytest.mark.integration
def test_real_tn_client_unexistent_provider():
    """
    Test the real TN client with an unexistent provider.

    This test creates a TNAccessBlock using an invalid provider URL and attempts to call
    get_first_record. The safe client is expected to detect the underlying network
    error and re-raise it as a TNNodeNetworkError.
    """
    invalid_provider = "http://nonexistent-provider.invalid"
    # Instantiate the block and attempt to get the first record.
    # Since the provider is invalid, the client creation itself should trigger a TN network error.
    with pytest.raises(TNNodeNetworkError) as excinfo:
        block = TNAccessBlock(
            tn_provider=invalid_provider,
            tn_private_key=SecretStr("0000000000000000000000000000000000000000000000000000000000000012"),
        )
        _ = block.get_first_record("dummy_stream")
    # Check that the error message indicates a connection issue.
    assert "http" in str(excinfo.value) or "connect" in str(excinfo.value)


# --- Tests for task_wait_for_tx --- #


@patch("tsn_adapters.blocks.tn_access.get_run_logger")
def test_task_wait_for_tx_success(mock_logger: MagicMock):
    """Test task_wait_for_tx calls block.wait_for_tx for a valid hash."""
    mock_block = MagicMock(spec=TNAccessBlock)
    tx_hash = "0x123abc"

    task_wait_for_tx.fn(block=mock_block, tx_hash=tx_hash)

    mock_block.wait_for_tx.assert_called_once_with(tx_hash)
    mock_logger.return_value.debug.assert_any_call(f"Waiting for transaction: {tx_hash}")


@patch("tsn_adapters.blocks.tn_access.get_run_logger")
def test_task_wait_for_tx_already_existed(mock_logger: MagicMock):
    """Test task_wait_for_tx skips when tx_hash is ALREADY_EXISTED."""
    mock_block = MagicMock(spec=TNAccessBlock)
    tx_hash = "ALREADY_EXISTED"

    task_wait_for_tx.fn(block=mock_block, tx_hash=tx_hash)

    mock_block.wait_for_tx.assert_not_called()
    mock_logger.return_value.debug.assert_called_once_with(
        f"Skipping wait for tx: Operation indicated prior existence or no tx generated (tx_hash='{tx_hash}')."
    )
