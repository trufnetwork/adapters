from datetime import datetime, timezone
import logging
from typing import Any, Optional
from unittest.mock import MagicMock

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow
from prefect.futures import PrefectFuture
from pydantic import ConfigDict
import pytest
from pytest import FixtureRequest, LogCaptureFixture, MonkeyPatch
from tests.utils.fake_tn_access import FakeTNAccessBlock

from tsn_adapters.blocks.deployment_state import DeploymentStateBlock, DeploymentStateModel
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)
from tsn_adapters.blocks.shared_types import StreamLocatorModel
from tsn_adapters.blocks.stream_filtering import StreamStatesResult
from tsn_adapters.blocks.tn_access import StreamAlreadyExistsError
from tsn_adapters.flows.stream_deploy_flow import (
    deploy_streams_flow,
    mark_batch_deployed_task,
    task_check_deployment_state,
    task_check_exist_deploy_init,
)


class LoggingFakeTNAccessBlock(FakeTNAccessBlock):
    def __init__(
        self,
        existing_streams: set[str] | None = None,
        initialized_streams: set[str] | None = None,
        error_on: dict[str, Any] | None = None,
    ):
        print("[TEST LOG] LoggingFakeTNAccessBlock.__init__ called")
        super().__init__(existing_streams=existing_streams, initialized_streams=initialized_streams, error_on=error_on)

    def deploy_stream(self, stream_id: str, stream_type: str = "primitive", wait: bool = True, **kwargs: object) -> str:
        print(f"[TEST LOG] FakeTNAccessBlock.deploy_stream called for: {stream_id}")
        return super().deploy_stream(stream_id, stream_type, wait=wait, **kwargs)

    def init_stream(self, stream_id: str, wait: bool = True, **kwargs: object) -> str:
        print(f"[TEST LOG] FakeTNAccessBlock.init_stream called for: {stream_id}")
        return super().init_stream(stream_id, wait=wait, **kwargs)

    def wait_for_tx(self, tx_hash: str, **kwargs: object) -> None:
        print(f"[TEST LOG] FakeTNAccessBlock.wait_for_tx called for: {tx_hash}")
        super().wait_for_tx(tx_hash, **kwargs)


class ErrorBlock(LoggingFakeTNAccessBlock):
    def __init__(
        self,
        existing_streams: Optional[set[str]] = None,
        initialized_streams: Optional[set[str]] = None,
        error_on: Optional[dict[str, Any]] = None,
    ):
        print("[TEST LOG] ErrorBlock.__init__ called")
        super().__init__(existing_streams=existing_streams, initialized_streams=initialized_streams, error_on=error_on)

    def deploy_stream(self, stream_id: str, stream_type: str = "primitive", wait: bool = True, **kwargs: object) -> str:
        print(f"[TEST LOG] ErrorBlock.deploy_stream called for: {stream_id}")
        raise StreamAlreadyExistsError(stream_id)

    def init_stream(self, stream_id: str, wait: bool = True, **kwargs: object) -> str:
        print(f"[TEST LOG] ErrorBlock.init_stream called for: {stream_id}")
        return "init_tx_hash"


# --- Helper Functions ---


# Helper to create a mock PrefectFuture that returns a value or raises an exception
def create_mock_prefect_future(return_value: Any = None, side_effect: Optional[Exception] = None) -> PrefectFuture[Any]:
    """Creates a mock PrefectFuture that resolves to a given value or raises an exception."""
    mock_future = MagicMock(spec=PrefectFuture)
    if side_effect:
        mock_future.result.side_effect = side_effect
    else:
        mock_future.result.return_value = return_value
    return mock_future


# Helper to create a mock StreamStatesResult
def create_mock_stream_states_result(
    non_deployed: list[str],
    deployed_but_not_initialized: list[str],
    deployed_and_initialized: list[str],
    account: str = "dummy_account",
) -> StreamStatesResult:
    """Creates a mock StreamStatesResult TypedDict for testing."""

    def _create_df(stream_ids: list[str]) -> DataFrame[StreamLocatorModel]:
        if not stream_ids:
            return DataFrame[StreamLocatorModel](pd.DataFrame(columns=["stream_id", "data_provider"]))
        return DataFrame[StreamLocatorModel](pd.DataFrame({"stream_id": stream_ids, "data_provider": account}))

    return StreamStatesResult(
        non_deployed=_create_df(non_deployed),
        deployed_but_not_initialized=_create_df(deployed_but_not_initialized),
        deployed_and_initialized=_create_df(deployed_and_initialized),
        depth=0,
        fallback_used=False,
    )


# Helper to create a mock future for the batch filter task
def create_mock_filter_future(
    existing_stream_ids: list[str], account: str = "dummy_account"
) -> PrefectFuture[DataFrame[StreamLocatorModel]]:
    """Creates a mock PrefectFuture that resolves to a StreamLocatorModel DataFrame."""
    mock_future = MagicMock(spec=PrefectFuture)
    result_df = DataFrame[StreamLocatorModel](
        pd.DataFrame({"stream_id": existing_stream_ids, "data_provider": account})
    )
    mock_future.result.return_value = result_df
    return mock_future


class TestPrimitiveSourcesDescriptor(PrimitiveSourcesDescriptorBlock):
    """Test implementation of PrimitiveSourcesDescriptorBlock that can generate a configurable number of streams."""

    model_config = ConfigDict(ignored_types=(object,))
    num_streams: int = 3

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        data = {
            "stream_id": [f"stream_{i}" for i in range(self.num_streams)],
            "source_id": [f"src_{i}" for i in range(self.num_streams)],
            "source_type": ["type1" for _ in range(self.num_streams)],
        }
        return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(data))


class TestDeploymentStateBlock(DeploymentStateBlock):
    """Test implementation of DeploymentStateBlock for testing."""

    model_config = ConfigDict(ignored_types=(object,), extra="allow")

    def __init__(self, predeployed_streams: set[str] | None = None):
        super().__init__()
        # Store which streams have been marked as deployed
        self._deployed_streams: dict[str, bool] = (
            {} if predeployed_streams is None else {s: True for s in predeployed_streams}
        )
        self._marked_streams: list[tuple[list[str], datetime]] = []

    def reset(self) -> None:
        """Reset the internal state of the block for testing."""
        self._deployed_streams = {}
        self._marked_streams = []

    def has_been_deployed(self, stream_id: str) -> bool:
        """Check if a stream has been marked as deployed."""
        return self._deployed_streams.get(stream_id, False)

    def check_multiple_streams(self, stream_ids: list[str]) -> dict[str, bool]:
        """Check deployment status for multiple streams."""
        return {stream_id: self._deployed_streams.get(stream_id, False) for stream_id in stream_ids}

    def mark_as_deployed(self, stream_id: str, timestamp: datetime) -> None:
        """Mark a stream as deployed."""
        self._deployed_streams[stream_id] = True
        self._marked_streams.append(([stream_id], timestamp))

    def mark_multiple_as_deployed(self, stream_ids: list[str], timestamp: datetime) -> None:
        """Mark multiple streams as deployed with a single timestamp."""
        for stream_id in stream_ids:
            self._deployed_streams[stream_id] = True
        self._marked_streams.append((stream_ids, timestamp))

    def get_deployment_states(self) -> DataFrame[DeploymentStateModel]:
        """Not implemented for testing."""
        raise NotImplementedError("This method is not implemented for testing")

    def update_deployment_states(self, states: DataFrame[DeploymentStateModel]) -> None:
        """Not implemented for testing."""
        raise NotImplementedError("This method is not implemented for testing")

    @property
    def deployed_streams(self) -> dict[str, bool]:
        """Get the dictionary of deployed streams."""
        return self._deployed_streams

    @deployed_streams.setter
    def deployed_streams(self, value: dict[str, bool]) -> None:
        """Set the dictionary of deployed streams."""
        self._deployed_streams = value

    @property
    def marked_streams(self) -> list[tuple[list[str], datetime]]:
        """Get the list of marked streams."""
        return self._marked_streams


@pytest.fixture
def tn_access_block() -> LoggingFakeTNAccessBlock:
    """Fixture that provides a FakeTNAccessBlock with no existing streams and logging."""
    return LoggingFakeTNAccessBlock(existing_streams=set())


@pytest.fixture
def primitive_descriptor(request: FixtureRequest) -> TestPrimitiveSourcesDescriptor:
    """Fixture that provides a TestPrimitiveSourcesDescriptor with configurable number of streams."""
    num_streams = getattr(request, "param", 3)  # Default to 3 streams if not parameterized
    return TestPrimitiveSourcesDescriptor(num_streams=num_streams)


@pytest.fixture
def deployment_state_block(request: FixtureRequest) -> TestDeploymentStateBlock:
    """Fixture that provides a TestDeploymentStateBlock with configurable predeployed streams."""
    predeployed_streams: set[str] = getattr(request, "param", set[str]())  # Default to no predeployed streams
    return TestDeploymentStateBlock(predeployed_streams=predeployed_streams)


@pytest.mark.usefixtures("prefect_test_fixture")
def test_deploy_streams_flow_all_new(
    tn_access_block: LoggingFakeTNAccessBlock,
    primitive_descriptor: TestPrimitiveSourcesDescriptor,
    # No monkeypatch needed
) -> None:
    """Test that all streams are deployed when none exist, relying on FakeTNAccessBlock."""
    tn_access_block.reset()
    # Use an empty TestDeploymentStateBlock because mark_batch_deployed_task needs it
    deployment_state_block = TestDeploymentStateBlock()

    # --- NO MOCKS for flow tasks --- #

    # Run flow within a test flow context
    @flow
    def run_deployment_flow():
        print("[TEST LOG] Starting run_deployment_flow...")
        results = deploy_streams_flow(
            psd_block=primitive_descriptor,
            tna_block=tn_access_block,
            deployment_state=deployment_state_block,  # Pass fake state block
            check_stream_state=False,  # Explicitly use the simple path
        )
        print(f"[TEST LOG] deploy_streams_flow returned: {results}")
        return results

    results = run_deployment_flow()
    print(f"[TEST LOG] Final results: {results}")
    print(
        f"[TEST LOG] Final tn_access_block state: deployed={tn_access_block.deployed_streams}, initialized={tn_access_block.initialized_streams}"
    )

    # All three streams should be deployed
    assert results["deployed_count"] == 3
    assert results["skipped_count"] == 0

    # Verify the actual streams that were deployed using the fake block
    assert tn_access_block.deployed_streams == {"stream_0", "stream_1", "stream_2"}
    assert set(tn_access_block.deploy_calls) == {"stream_0", "stream_1", "stream_2"}
    assert tn_access_block.initialized_streams == {"stream_0", "stream_1", "stream_2"}
    # Verify state block was updated
    assert len(deployment_state_block.marked_streams) > 0
    marked_ids = set()
    for streams, _ in deployment_state_block.marked_streams:
        marked_ids.update(streams)
    assert marked_ids == {"stream_0", "stream_1", "stream_2"}


@pytest.mark.usefixtures("prefect_test_fixture")
def test_deploy_streams_flow_with_existing(
    # No monkeypatch needed
) -> None:
    """Test that only non-existing streams are deployed when some already exist, relying on FakeTNAccessBlock."""
    existing_streams = {"stream_0", "stream_2"}
    # Use LoggingFakeTNAccessBlock if you want logs
    tn_access_block = LoggingFakeTNAccessBlock(existing_streams=existing_streams)
    tn_access_block.set_deployed_streams(existing_streams)  # Initial state matches existing
    # Need a deployment state block for mark task
    deployment_state_block = TestDeploymentStateBlock()
    primitive_descriptor = TestPrimitiveSourcesDescriptor(num_streams=3)

    # --- NO MOCKS for flow tasks --- #

    # Run flow within a test flow context
    from prefect import flow

    @flow
    def run_flow():
        return deploy_streams_flow(
            psd_block=primitive_descriptor,
            tna_block=tn_access_block,
            deployment_state=deployment_state_block,
            check_stream_state=False,  # Use simple path
        )

    results = run_flow()

    # When check_stream_state=False, the flow attempts deploy/init for all.
    # Existing streams (0, 2) will raise StreamAlreadyExistsError on deploy,
    # but task_check_exist_deploy_init will proceed to init them.
    # Stream 1 will be deployed and initialized.
    assert results["deployed_count"] == 3  # All streams are processed (1 D+I, 2 I)
    assert results["skipped_count"] == 0  # No streams skipped by state check

    # Verify the state of the fake block
    assert tn_access_block.deploy_calls == ["stream_1"]  # Only stream_1 deploy attempted
    assert tn_access_block.deployed_streams == {
        "stream_0",
        "stream_1",
        "stream_2",
    }  # All end up deployed (0, 2 existed, 1 added)
    assert tn_access_block.initialized_streams == {"stream_0", "stream_1", "stream_2"}  # All end up initialized

    # Verify state block was updated for all processed streams
    marked_ids = set()
    for streams, _ in deployment_state_block.marked_streams:
        marked_ids.update(streams)
    assert marked_ids == {"stream_0", "stream_1", "stream_2"}


@pytest.mark.usefixtures("prefect_test_fixture")
# Remove all patches
def test_deploy_streams_flow_with_deployment_state(
    # Remove mocks
    tn_access_block: FakeTNAccessBlock,  # Use fixture
    primitive_descriptor: TestPrimitiveSourcesDescriptor,
    # Remove monkeypatch
) -> None:
    """Test that the flow uses deployment state to filter and mark streams, relying on fakes."""
    # Set up initial state
    tn_access_block.reset()
    deployment_state_block = TestDeploymentStateBlock(predeployed_streams={"stream_0"})
    # Configure tn_access_block to match state (stream_0 exists)
    tn_access_block.set_deployed_streams({"stream_0"})

    # --- NO MOCKS for flow tasks --- #

    # --- Run the flow ---
    results = deploy_streams_flow(
        psd_block=primitive_descriptor,
        tna_block=tn_access_block,  # Pass the fake block
        deployment_state=deployment_state_block,  # Pass the configured fake state block
        check_stream_state=False,  # Use simple path
    )

    # Assertions
    # stream_0 skipped by state
    # stream_1, stream_2 processed
    assert results["deployed_count"] == 2
    assert results["skipped_count"] == 1  # stream_0 filtered by state

    # Verify the final state of FakeTNAccessBlock
    assert tn_access_block.deploy_calls == ["stream_1", "stream_2"]
    assert tn_access_block.deployed_streams == {"stream_0", "stream_1", "stream_2"}
    assert tn_access_block.initialized_streams == {"stream_1", "stream_2"}  # stream_0 was never initialized

    # Verify that the state marking task was submitted for the deployed streams
    assert len(deployment_state_block.marked_streams) > 0
    marked_ids = set()
    for streams, _ in deployment_state_block.marked_streams:
        marked_ids.update(streams)
    assert marked_ids == {"stream_1", "stream_2"}


@pytest.mark.usefixtures("prefect_test_fixture")
def test_deploy_streams_flow_all_deployed_in_state(
    tn_access_block: LoggingFakeTNAccessBlock,
    primitive_descriptor: TestPrimitiveSourcesDescriptor,
    monkeypatch: MonkeyPatch,
) -> None:
    """Test that if all streams are already deployed in the state, none are deployed."""
    # Create a deployment state with all streams already deployed
    deployment_state_block = TestDeploymentStateBlock(predeployed_streams={"stream_0", "stream_1", "stream_2"})
    # Ensure tn_access_block state matches for consistency (optional but good practice)
    tn_access_block.reset()
    tn_access_block.set_deployed_streams({"stream_0", "stream_1", "stream_2"})

    # Run flow within a test flow context
    from prefect import flow

    @flow
    def run_flow():
        # Run flow with deployment state (check_state=True implicitly or explicitly if default changed)
        # Let's assume check_state=True is needed here to test state block filtering
        return deploy_streams_flow(
            psd_block=primitive_descriptor,
            tna_block=tn_access_block,
            deployment_state=deployment_state_block,
            check_stream_state=True,  # Test the state checking path
        )

    results = run_flow()

    # All streams should be skipped due to deployment state
    assert results["deployed_count"] == 0
    assert results["skipped_count"] == 3

    # Verify that no streams were deployed or initialized in the fake block
    assert len(tn_access_block.deploy_calls) == 0
    assert len(tn_access_block.initialized_streams) == 0
    assert tn_access_block.deployed_streams == {"stream_0", "stream_1", "stream_2"}  # Should remain unchanged

    # Verify that no streams were marked as deployed in the deployment state block
    assert len(deployment_state_block.marked_streams) == 0


@pytest.mark.usefixtures("prefect_test_fixture")
def test_deploy_streams_flow_backward_compatibility(
    tn_access_block: LoggingFakeTNAccessBlock, primitive_descriptor: TestPrimitiveSourcesDescriptor
) -> None:
    """Test that the flow still works without a deployment state (backward compatibility)."""
    tn_access_block.reset()
    # Run flow without deployment state within a test flow context
    from prefect import flow

    @flow
    def run_flow():
        # Pass deployment_state=None
        return deploy_streams_flow(
            psd_block=primitive_descriptor,
            tna_block=tn_access_block,
            deployment_state=None,
            check_stream_state=False,  # Ensure simple path when no state block
        )

    results = run_flow()

    # All three streams should be deployed
    assert results["deployed_count"] == 3
    assert results["skipped_count"] == 0

    # Verify the actual streams that were deployed
    assert tn_access_block.deployed_streams == {"stream_0", "stream_1", "stream_2"}


@pytest.mark.usefixtures("prefect_test_fixture")
def test_deploy_streams_flow_catastrophic_failure(caplog: LogCaptureFixture):
    """Test that the flow handles catastrophic tn_access_block failure gracefully."""
    primitive_descriptor = TestPrimitiveSourcesDescriptor(num_streams=2)
    # Use LoggingFakeTNAccessBlock for logs
    tn_access_block = LoggingFakeTNAccessBlock()
    tn_access_block.set_deployed_streams(set())

    # Inject error into the fake block
    tn_access_block.set_error("deploy_stream", RuntimeError("Catastrophic TN failure!"))

    # Run flow within a test flow context
    from prefect import flow

    with caplog.at_level(logging.ERROR):

        @flow
        def run_flow():
            return deploy_streams_flow(
                psd_block=primitive_descriptor,
                tna_block=tn_access_block,
                check_stream_state=False,  # Simple path
            )

        state = run_flow(return_state=True)

    assert state.is_failed()

    assert "Catastrophic TN failure!" in caplog.text


@pytest.mark.usefixtures("prefect_test_fixture")
def test_deploy_streams_flow_retry_logic(caplog: LogCaptureFixture):
    """Test that the flow retries task_check_exist_deploy_init on transient failure and eventually succeeds."""
    primitive_descriptor = TestPrimitiveSourcesDescriptor(num_streams=1)
    # Use LoggingFakeTNAccessBlock for logs
    tn_access_block = LoggingFakeTNAccessBlock()
    tn_access_block.reset()

    # Simulate task_check_exist_deploy_init failing once via the fake block error injection
    call_count = {"count": 0}
    original_deploy = tn_access_block.deploy_stream

    def flaky_deploy(*args: object, **kwargs: object) -> str:
        # Get stream_id carefully from either args or kwargs
        if args:
            stream_id = str(args[0])
            other_kwargs = kwargs
        else:
            stream_id = str(kwargs.get("stream_id", "unknown"))
            other_kwargs = {k: v for k, v in kwargs.items() if k != "stream_id"}

        if call_count["count"] == 0:
            call_count["count"] += 1
            raise RuntimeError("Transient failure!")
        # Call original correctly, avoiding duplicate stream_id
        return original_deploy(stream_id=stream_id, **other_kwargs)  # type: ignore

    tn_access_block.deploy_stream = flaky_deploy  # type: ignore[assignment]

    # Run flow within a test flow context
    from prefect import flow

    with caplog.at_level(logging.ERROR):

        @flow
        def run_flow():
            return deploy_streams_flow(
                psd_block=primitive_descriptor,
                tna_block=tn_access_block,
                check_stream_state=False,  # Simple path
            )

        results = run_flow()

    # Should succeed after retry
    assert results["deployed_count"] == 1
    assert results["skipped_count"] == 0
    assert "stream_0" in tn_access_block.deployed_streams

    # Should have logged an error for the transient failure
    assert "Transient failure!" in caplog.text


@pytest.mark.usefixtures("prefect_test_fixture")
def test_mark_batch_deployed_task() -> None:
    """Test that mark_batch_deployed_task correctly marks streams as deployed."""
    # Create a deployment state
    deployment_state = TestDeploymentStateBlock()

    # Create a timestamp (ensure it's UTC)
    timestamp = datetime.now(timezone.utc)

    # Mark streams as deployed
    mark_batch_deployed_task(
        stream_ids=["stream_1", "stream_2"], deployment_state=deployment_state, timestamp=timestamp
    )

    # Verify that the streams were marked as deployed
    assert deployment_state.deployed_streams.get("stream_1", False)
    assert deployment_state.deployed_streams.get("stream_2", False)

    # Verify that mark_multiple_as_deployed was called with the correct arguments
    assert len(deployment_state.marked_streams) == 1
    marked_stream_ids, marked_timestamp = deployment_state.marked_streams[0]
    assert set(marked_stream_ids) == {"stream_1", "stream_2"}
    assert marked_timestamp == timestamp


@pytest.mark.usefixtures("prefect_test_fixture")
def test_mark_batch_deployed_task_empty_list() -> None:
    """Test that mark_batch_deployed_task handles empty stream list correctly."""
    # Create a deployment state
    deployment_state = TestDeploymentStateBlock()

    # Create a timestamp
    timestamp = datetime.now()

    # Mark empty list of streams as deployed
    mark_batch_deployed_task(stream_ids=[], deployment_state=deployment_state, timestamp=timestamp)

    # Verify that no streams were marked as deployed
    assert len(deployment_state.marked_streams) == 0


# --- Tests for Internal Tasks --- #


@pytest.mark.usefixtures("prefect_test_fixture")
def test_task_check_deployment_state(
    deployment_state_block: TestDeploymentStateBlock,
    monkeypatch: MonkeyPatch,
):
    """Test the task_check_deployment_state wrapper task."""
    # Set up the mock deployment state
    predeployed = {"stream_0": True, "stream_2": True}
    deployment_state_block.deployed_streams = predeployed
    stream_ids_to_check = ["stream_0", "stream_1", "stream_2", "stream_3"]

    # Mock get_run_logger
    mock_logger = MagicMock()
    monkeypatch.setattr("tsn_adapters.flows.stream_deploy_flow.get_run_logger", lambda: mock_logger)

    # Call the task's function directly
    result = task_check_deployment_state.fn(deployment_state=deployment_state_block, stream_ids=stream_ids_to_check)

    # Assert the result matches the expected state
    expected_result = {
        "stream_0": True,  # Predeployed
        "stream_1": False,
        "stream_2": True,  # Predeployed
        "stream_3": False,
    }
    assert result == expected_result
    mock_logger.debug.assert_any_call(f"Checking deployment state for {len(stream_ids_to_check)} streams.")


@pytest.mark.usefixtures("prefect_test_fixture")
def test_task_check_deployment_state_empty_input(
    deployment_state_block: TestDeploymentStateBlock,
    monkeypatch: MonkeyPatch,
):
    """Test task_check_deployment_state with empty input list."""
    mock_logger = MagicMock()
    monkeypatch.setattr("tsn_adapters.flows.stream_deploy_flow.get_run_logger", lambda: mock_logger)

    result = task_check_deployment_state.fn(deployment_state=deployment_state_block, stream_ids=[])

    assert result == {}
    mock_logger.debug.assert_any_call("No stream IDs provided to check deployment state.")


@pytest.mark.usefixtures("prefect_test_fixture")
def test_task_check_deployment_state_exception(
    deployment_state_block: TestDeploymentStateBlock,
    monkeypatch: MonkeyPatch,
):
    """Test task_check_deployment_state exception handling."""
    mock_logger = MagicMock()
    monkeypatch.setattr("tsn_adapters.flows.stream_deploy_flow.get_run_logger", lambda: mock_logger)

    # Make the mock block raise an error
    mock_check = MagicMock(side_effect=ValueError("State check failed"))
    deployment_state_block.check_multiple_streams = mock_check

    with pytest.raises(ValueError, match="State check failed"):
        task_check_deployment_state.fn(deployment_state=deployment_state_block, stream_ids=["stream_0"])

    mock_check.assert_called_once_with(["stream_0"])
    mock_logger.error.assert_called_once()


# --- Tests for task_check_exist_deploy_init --- #


@pytest.mark.usefixtures("prefect_test_fixture")
def test_task_check_exist_deploy_init_deploys_and_inits(
    tn_access_block: FakeTNAccessBlock,  # Use the fixture
):
    """Test task_check_exist_deploy_init when stream doesn't exist, using fakes and no patching."""
    stream_id = "new_stream"
    tn_access_block.reset()  # Ensure clean state
    tn_access_block.set_deployed_streams(set())  # Ensure stream doesn't exist initially

    # Call the task's function using .submit() to ensure Prefect context
    @flow
    def test_flow():
        return task_check_exist_deploy_init(stream_id=stream_id, tna_block=tn_access_block)

    status = test_flow()

    # Assert the final status
    assert status == "deployed"
    # Assert the state of the fake block
    assert tn_access_block.deploy_calls == [stream_id]
    assert stream_id in tn_access_block.deployed_streams
    assert stream_id in tn_access_block.initialized_streams


@pytest.mark.usefixtures("prefect_test_fixture")
def test_task_check_exist_deploy_init_skips_existing(
    tn_access_block: LoggingFakeTNAccessBlock,  # Use base fixture
):
    """Test task_check_exist_deploy_init when deploy raises StreamAlreadyExistsError, using fakes and no patching."""
    stream_id = "existing_stream"
    # Create the specific ErrorBlock for this test
    error_block = ErrorBlock(existing_streams=set())

    from prefect import flow

    @flow
    def test_flow():
        # Pass the error_block instance to the task
        return task_check_exist_deploy_init(stream_id=stream_id, tna_block=error_block)

    status = test_flow()

    assert status == "initialized"


@pytest.mark.usefixtures("prefect_test_fixture")
def test_task_check_exist_deploy_init_already_exists_error(
    tn_access_block: LoggingFakeTNAccessBlock,  # Use base fixture
):
    """Test task_check_exist_deploy_init handles StreamAlreadyExistsError from deploy, using fakes and no patching."""
    stream_id = "error_stream"
    # Create the specific ErrorBlock for this test
    error_block = ErrorBlock(existing_streams=set())

    from prefect import flow

    @flow
    def test_flow():
        # Pass the error_block instance to the task
        return task_check_exist_deploy_init(stream_id=stream_id, tna_block=error_block)

    status = test_flow()

    assert status == "initialized"


@pytest.mark.usefixtures("prefect_test_fixture")
# Remove all patches
def test_deploy_streams_flow_custom_batch_size(
    # Remove mocks
    tn_access_block: FakeTNAccessBlock,
    primitive_descriptor: TestPrimitiveSourcesDescriptor,
    deployment_state_block: TestDeploymentStateBlock,  # Use fixture
    # Remove monkeypatch
) -> None:
    """Test flow with a custom batch size, relying on fakes."""
    # Setup: All streams are new
    tn_access_block.reset()
    deployment_state_block.reset()

    # --- NO MOCKS for flow tasks --- #

    # --- Run the flow ---
    results = deploy_streams_flow(
        psd_block=primitive_descriptor,
        tna_block=tn_access_block,
        deployment_state=deployment_state_block,
        batch_size=1,  # Test custom batch size
        check_stream_state=False,
    )

    # Assertions: All 3 streams should be deployed
    assert results["deployed_count"] == 3
    assert results["skipped_count"] == 0

    # Verify TN Access Block state
    assert tn_access_block.deploy_calls == ["stream_0", "stream_1", "stream_2"]
    assert tn_access_block.deployed_streams == {"stream_0", "stream_1", "stream_2"}
    assert tn_access_block.initialized_streams == {"stream_0", "stream_1", "stream_2"}

    # Verify Deployment State Block state (batches should be size 1)
    assert len(deployment_state_block.marked_streams) == 3
    marked_ids = set()
    for streams, _ in deployment_state_block.marked_streams:
        assert len(streams) == 1  # Check batch size
        marked_ids.update(streams)
    assert marked_ids == {"stream_0", "stream_1", "stream_2"}


@pytest.mark.usefixtures("prefect_test_fixture")
def test_deploy_streams_flow_start_from_batch(
    tn_access_block: LoggingFakeTNAccessBlock,
):
    """Test that the flow respects the start_from_batch parameter, using fakes and no patching."""
    num_streams = 5
    batch_size = 2
    start_batch = 1  # Start from the second batch (index 1)
    primitive_descriptor = TestPrimitiveSourcesDescriptor(num_streams=num_streams)
    deployment_state = TestDeploymentStateBlock()
    tn_access_block.reset()

    # Run flow starting from batch 1 within a test flow context
    from prefect import flow

    @flow
    def run_flow():
        return deploy_streams_flow(
            psd_block=primitive_descriptor,
            tna_block=tn_access_block,
            deployment_state=deployment_state,
            # Let's try check_stream_state=False first, as start_from_batch
            # might work correctly even on the simple path. If not, we can change it.
            check_stream_state=False,
            batch_size=batch_size,
            start_from_batch=start_batch,
        )

    results = run_flow()

    # The first batch ([0, 1]) should be skipped, only [2, 3] and [4] processed
    # So only streams 2, 3, 4 should be deployed/initialized
    assert results["deployed_count"] == 3
    # Streams skipped by start_from_batch aren't counted in skipped_count
    assert results["skipped_count"] == 0
    assert tn_access_block.deploy_calls == ["stream_2", "stream_3", "stream_4"]
    assert tn_access_block.deployed_streams == {"stream_2", "stream_3", "stream_4"}
    assert tn_access_block.initialized_streams == {"stream_2", "stream_3", "stream_4"}
    # Verify state block was updated for only the processed streams
    marked_ids = set()
    for streams, _ in deployment_state.marked_streams:
        marked_ids.update(streams)
    assert marked_ids == {"stream_2", "stream_3", "stream_4"}


@pytest.mark.usefixtures("prefect_test_fixture")
def test_deploy_streams_flow_mixed_states_check_stream_state():
    """Test deploy_streams_flow with check_stream_state=True and mixed stream states."""

    # Arrange: Set up 4 streams with mixed states
    class MixedStateDescriptor(TestPrimitiveSourcesDescriptor):
        num_streams = 4

        def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
            data = {
                "stream_id": [f"stream_{i}" for i in range(self.num_streams)],
                "source_id": [f"src_{i}" for i in range(self.num_streams)],
                "source_type": ["type1" for _ in range(self.num_streams)],
            }
            return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(data))

    # stream_0: not deployed
    # stream_1: deployed but not initialized
    # stream_2: deployed and initialized
    # stream_3: not deployed
    deployed = {"stream_1", "stream_2"}
    initialized = {"stream_2"}
    tn_access_block = LoggingFakeTNAccessBlock(existing_streams=deployed, initialized_streams=initialized)
    tn_access_block.reset()
    tn_access_block.set_deployed_streams(deployed)
    tn_access_block.set_initialized_streams(initialized)
    deployment_state_block = TestDeploymentStateBlock()
    primitive_descriptor = MixedStateDescriptor()

    # Act: Run the flow with check_stream_state=True
    @flow
    def run_flow():
        return deploy_streams_flow(
            psd_block=primitive_descriptor,
            tna_block=tn_access_block,
            deployment_state=deployment_state_block,
            check_stream_state=True,
        )

    results = run_flow()

    # Assert: Only stream_0 and stream_3 are deployed+initialized, stream_1 is only initialized, stream_2 is skipped
    assert results["deployed_count"] == 3  # stream_0, stream_1, stream_3 processed (stream_2 skipped)
    assert results["skipped_count"] == 1  # stream_2 skipped (already deployed+initialized)

    # Check deploy calls: should be for stream_0 and stream_3 only
    assert set(tn_access_block.deploy_calls) == {"stream_0", "stream_3"}
    # All processed streams should be initialized
    assert tn_access_block.initialized_streams == {"stream_0", "stream_1", "stream_2", "stream_3"}
    # Deployed streams should include all except stream_1 (which was only initialized)
    assert tn_access_block.deployed_streams == {"stream_0", "stream_1", "stream_2", "stream_3"}

    # Deployment state block should be updated for all processed streams except the skipped one
    marked_ids = set()
    for streams, _ in deployment_state_block.marked_streams:
        marked_ids.update(streams)
    assert marked_ids == {"stream_0", "stream_1", "stream_3"}


if __name__ == "__main__":
    pytest.main([__file__])
