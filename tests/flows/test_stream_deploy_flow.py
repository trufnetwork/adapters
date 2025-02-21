from typing import Optional

import pandas as pd
from pandera.typing import DataFrame
from pydantic import ConfigDict, SecretStr
import pytest
from pytest import FixtureRequest
import trufnetwork_sdk_c_bindings.exports as truf_sdk
import trufnetwork_sdk_py.client as tn_client

from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.flows.stream_deploy_flow import deploy_streams_flow


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

    def init_stream(self, stream_id: str, wait: bool = True) -> str:
        # Simulate a successful stream initialization
        return "dummy_tx_hash"

    def wait_for_tx(self, tx_hash: str) -> None:
        if tx_hash == "failed_tx_hash":
            raise Exception("Failed to deploy stream")
        pass


class TestTNAccessBlock(TNAccessBlock):
    """Test implementation of TNAccessBlock that uses our TestTNClient."""

    _test_client: TestTNClient
    model_config = ConfigDict(ignored_types=(object,))

    def __init__(self, existing_streams: set[str] | None = None):
        super().__init__(tn_provider="", tn_private_key=SecretStr(""))
        self._test_client = TestTNClient(existing_streams=existing_streams)

    @property
    def client(self) -> tn_client.TNClient:
        """Override client property to return our test client instead of creating a real one."""
        return self._test_client


@pytest.fixture
def tn_access_block() -> TestTNAccessBlock:
    """Fixture that provides a TestTNAccessBlock with no existing streams."""
    return TestTNAccessBlock(existing_streams=set())


@pytest.fixture
def primitive_descriptor(request: FixtureRequest) -> TestPrimitiveSourcesDescriptor:
    """Fixture that provides a TestPrimitiveSourcesDescriptor with configurable number of streams."""
    num_streams = getattr(request, "param", 3)  # Default to 3 streams if not parameterized
    return TestPrimitiveSourcesDescriptor(num_streams=num_streams)


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
def test_deploy_streams_flow_with_existing() -> None:
    """Test that only non-existing streams are deployed when some already exist."""
    # Create TNAccessBlock with some existing streams
    existing_streams = {"stream_0", "stream_2"}  # First and last streams exist
    tn_access_block = TestTNAccessBlock(existing_streams=existing_streams)

    # Create descriptor with 3 streams
    primitive_descriptor = TestPrimitiveSourcesDescriptor(num_streams=3)

    results = deploy_streams_flow(psd_block=primitive_descriptor, tna_block=tn_access_block)

    # Only stream_1 should be deployed, others should be skipped
    assert results["deployed_count"] == 1
    assert results["skipped_count"] == 2

    # Verify the actual streams that were deployed
    client = tn_access_block.client
    assert isinstance(client, TestTNClient)  # Type check for mypy
    assert client.deployed_streams == ["stream_1"]  # Changed from len check to exact match


if __name__ == "__main__":
    pytest.main([__file__])
