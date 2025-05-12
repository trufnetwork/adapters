from collections.abc import Generator
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pandas as pd
from pandera.typing import DataFrame
from prefect.futures import PrefectFuture
from pydantic import ConfigDict
import pytest
from pytest import FixtureRequest
from tests.utils.fake_tn_access import FakeInternalTNClient, FakeTNAccessBlock
import trufnetwork_sdk_py.client as tn_client

from tsn_adapters.blocks.deployment_state import DeploymentStateBlock, DeploymentStateModel
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)
from tsn_adapters.common.trufnetwork.models.tn_models import StreamLocatorModel
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
def tn_access_block() -> Generator[FakeTNAccessBlock, None, None]:
    """Fixture that provides a FakeTNAccessBlock with no existing streams."""
    tn_access_block = FakeTNAccessBlock(existing_streams=set())
    yield tn_access_block
    tn_access_block.reset()


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
    tn_access_block: FakeTNAccessBlock, primitive_descriptor: TestPrimitiveSourcesDescriptor
) -> None:
    """Test that all streams are deployed when none exist initially and no state block is used."""
    # Ensure the client is of the expected type and check its specific property
    the_client = tn_access_block.client
    assert isinstance(the_client, FakeInternalTNClient)
    assert len(the_client.existing_streams) == 0

    num_expected_streams = primitive_descriptor.num_streams
    expected_stream_ids = {f"stream_{i}" for i in range(num_expected_streams)}

    # Run the flow without a deployment state block
    results = deploy_streams_flow(psd_block=primitive_descriptor, tna_block=tn_access_block, deployment_state=None)

    # Assertions: All streams should be deployed, none skipped
    assert results["deployed_count"] == num_expected_streams
    assert results["skipped_count"] == 0

    # Verify the correct stream definitions were passed to batch_deploy_streams
    deployed_definitions = the_client.deployed_stream_definitions_history
    deployed_ids_from_mock = {defn["stream_id"] for defn in deployed_definitions}

    assert len(deployed_definitions) == num_expected_streams
    assert deployed_ids_from_mock == expected_stream_ids
    # Check stream type (assuming all are primitive)
    assert all(d["stream_type"] == tn_client.STREAM_TYPE_PRIMITIVE for d in deployed_definitions)

    # Verify the TestTNClient internal state reflects deployment
    assert the_client.existing_streams == expected_stream_ids


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


@pytest.mark.usefixtures("prefect_test_fixture")
@pytest.mark.parametrize("primitive_descriptor", [5], indirect=True)  # Test with 5 streams
def test_deploy_streams_flow_some_exist_tn(
    tn_access_block: FakeTNAccessBlock, primitive_descriptor: TestPrimitiveSourcesDescriptor
) -> None:
    """Test that streams already existing on TN are skipped, others deployed."""
    # Setup: streams 1 and 3 already exist on TN
    initial_existing_streams = {"stream_1", "stream_3"}
    tn_access_block.set_deployed_streams(initial_existing_streams)

    test_client = tn_access_block.client
    assert isinstance(test_client, FakeInternalTNClient)
    assert test_client.existing_streams == initial_existing_streams

    expected_to_deploy_ids = {"stream_0", "stream_2", "stream_4"}
    expected_skipped_ids = initial_existing_streams

    # Run the flow without a deployment state block
    results = deploy_streams_flow(psd_block=primitive_descriptor, tna_block=tn_access_block, deployment_state=None)

    # Assertions: 3 deployed, 2 skipped
    assert results["deployed_count"] == len(expected_to_deploy_ids)
    assert results["skipped_count"] == len(expected_skipped_ids)

    # Verify the correct stream definitions were deployed
    test_client_deploy = tn_access_block.client
    assert isinstance(test_client_deploy, FakeInternalTNClient)
    deployed_definitions = test_client_deploy.deployed_stream_definitions_history
    deployed_ids_from_mock = {defn["stream_id"] for defn in deployed_definitions}
    assert deployed_ids_from_mock == expected_to_deploy_ids

    # Verify the final state of existing streams in the mock TN client
    final_existing_streams = initial_existing_streams.union(expected_to_deploy_ids)

    test_client_final = tn_access_block.client
    assert isinstance(test_client_final, FakeInternalTNClient)
    assert test_client_final.existing_streams == final_existing_streams


@pytest.mark.usefixtures("prefect_test_fixture")
@pytest.mark.parametrize("primitive_descriptor", [5], indirect=True)  # Test with 5 streams
@pytest.mark.parametrize(
    "deployment_state_block", [{"stream_0", "stream_4"}], indirect=True
)  # Streams 0 and 4 already in state
def test_deploy_streams_flow_some_in_state_block(
    tn_access_block: FakeTNAccessBlock,
    primitive_descriptor: TestPrimitiveSourcesDescriptor,
    deployment_state_block: TestDeploymentStateBlock,
) -> None:
    """Test streams marked in DeploymentStateBlock are skipped before checking TN.
    Also test that TN existing streams not in state block are skipped later.
    """
    # Setup:
    # - stream_0, stream_4 in state block
    # - stream_2 already exists on TN (but not in state block)
    initial_existing_streams_tn = {"stream_2"}
    tn_access_block.set_deployed_streams(initial_existing_streams_tn)

    expected_to_deploy_ids = {"stream_1", "stream_3"}  # Only these are not in state and not on TN initially
    expected_skipped_by_state = {"stream_0", "stream_4"}
    expected_skipped_by_tn_check = {"stream_2"}
    expected_total_skipped = len(expected_skipped_by_state) + len(expected_skipped_by_tn_check)  # 2 + 1 = 3

    # Run the flow WITH the deployment state block
    results = deploy_streams_flow(
        psd_block=primitive_descriptor, tna_block=tn_access_block, deployment_state=deployment_state_block
    )

    # Assertions: 2 deployed, 3 skipped
    assert results["deployed_count"] == len(expected_to_deploy_ids)
    assert results["skipped_count"] == expected_total_skipped

    # Verify the correct stream definitions were deployed
    test_client_deploy_state = tn_access_block.client
    assert isinstance(test_client_deploy_state, FakeInternalTNClient)
    deployed_definitions = test_client_deploy_state.deployed_stream_definitions_history
    deployed_ids_from_mock = {defn["stream_id"] for defn in deployed_definitions}
    assert deployed_ids_from_mock == expected_to_deploy_ids

    # Verify the final state of existing streams in the mock TN client
    final_existing_streams_state = initial_existing_streams_tn.union(expected_to_deploy_ids)

    test_client_final_state_check = tn_access_block.client
    assert isinstance(test_client_final_state_check, FakeInternalTNClient)
    assert test_client_final_state_check.existing_streams == final_existing_streams_state

    # Verify state block wasn't modified for streams already present
    # (The flow currently marks based on successful deployment call, not existence check)
    # Check that mark_multiple_as_deployed was called for the deployed streams
    # find the call for the deployed ids
    marked_call_found = False
    for ids, _timestamp in deployment_state_block.marked_streams:
        if set(ids) == expected_to_deploy_ids:
            marked_call_found = True
            break
    assert marked_call_found, "Expected deployed streams were not marked in the state block"


if __name__ == "__main__":
    pytest.main([__file__])
