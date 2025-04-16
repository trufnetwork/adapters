from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock

import pandas as pd
from pandera.typing import DataFrame
from prefect.futures import PrefectFuture
from pydantic import ConfigDict, SecretStr
import pytest
from pytest import FixtureRequest
import trufnetwork_sdk_c_bindings.exports as truf_sdk  # type: ignore
import trufnetwork_sdk_py.client as tn_client  # type: ignore

from tsn_adapters.blocks.deployment_state import DeploymentStateBlock, DeploymentStateModel
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)
from tsn_adapters.blocks.shared_types import StreamLocatorModel
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.flows.stream_deploy_flow import (
    deploy_streams_flow,
    mark_batch_deployed_task,
)


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


class TestTNClient(tn_client.TNClient):
    """Test implementation of TNClient that tracks deployed streams and can be initialized with existing streams."""

    def __init__(self, existing_streams: set[str] | None = None):
        # We don't call super().__init__ to avoid real client initialization
        if existing_streams is None:
            existing_streams = set()
        self.existing_streams = existing_streams
        self.deployed_streams: list[str] = []
        # Set a dummy client to satisfy TNClient's expectations
        self.client = object()

    def get_current_account(self) -> str:
        return "dummy_account"

    def stream_exists(self, stream_id: str, data_provider: Optional[str] = None) -> bool:
        # Check if the stream exists in either existing_streams or deployed_streams
        return stream_id in self.existing_streams

    def deploy_stream(self, stream_id: str, stream_type: str = truf_sdk.StreamTypePrimitive, wait: bool = True) -> str:
        # Instead of calling the real SDK, we just track that the stream was deployed
        self.deployed_streams.append(stream_id)
        self.existing_streams.add(stream_id)
        return "dummy_tx_hash"

    def wait_for_tx(self, tx_hash: str) -> None:
        if tx_hash == "failed_tx_hash":
            raise Exception("Failed to deploy stream")
        pass


class TestTNAccessBlock(TNAccessBlock):
    """Test implementation of TNAccessBlock for testing."""

    def __init__(self, existing_streams: set[str] | None = None):
        super().__init__(
            tn_provider="", tn_private_key=SecretStr(""), helper_contract_name="", helper_contract_deployer=None
        )
        self._test_client = TestTNClient(existing_streams=existing_streams)

    def set_existing_streams(self, existing_streams: set[str]) -> None:
        """Set the streams that exist in TN for testing."""
        self._test_client = TestTNClient(existing_streams=existing_streams)

    @property
    def client(self) -> tn_client.TNClient:
        """Override client property to return our test client instead of creating a real one."""
        return self._test_client


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
def tn_access_block() -> TestTNAccessBlock:
    """Fixture that provides a TestTNAccessBlock with no existing streams."""
    return TestTNAccessBlock(existing_streams=set())


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
    tn_access_block: TestTNAccessBlock, primitive_descriptor: TestPrimitiveSourcesDescriptor
) -> None:
    """Test that all streams are deployed when none exist."""
    results = deploy_streams_flow(psd_block=primitive_descriptor, tna_block=tn_access_block)

    # All three streams should be deployed
    assert results["deployed_count"] == 3
    assert results["skipped_count"] == 0

    # Verify the actual streams that were deployed
    client = tn_access_block.client
    assert isinstance(client, TestTNClient)  # Type check for mypy
    assert set(client.deployed_streams) == {"stream_0", "stream_1", "stream_2"}


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


if __name__ == "__main__":
    pytest.main([__file__])
