from time import sleep
from typing import Optional, cast

import pandas as pd
from pandera.typing import DataFrame
from pydantic import SecretStr
import pytest
from pytest import MonkeyPatch
import trufnetwork_sdk_c_bindings.exports as truf_sdk
import trufnetwork_sdk_py.client as tn_client

from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.flows.stream_deploy_flow import DeployStreamResults, deploy_streams_flow


class FakePrimitiveSourcesDescriptor(PrimitiveSourcesDescriptorBlock):
    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        # Create a simple DataFrame with three stream descriptors.
        data = {
            "stream_id": ["stream1", "stream2", "stream3"],
            "source_id": ["src1", "src2", "src3"],
            "source_type": ["type1", "type1", "type2"],
        }
        return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(data))


class FakeTNClient(tn_client.TNClient):
    def __init__(self, existing_streams: set[str] | None = None):
        # We do not call the real TNClient __init__ to avoid any side effects.
        if existing_streams is None:
            existing_streams = set()
        self.existing_streams = existing_streams
        self.deployed_streams: list[str] = []
        # Instead of self.client = self (which is recursive), we set it to a dummy object.
        self.client = object()

    def __getstate__(self):
        """
        Override serialization to remove the problematic 'client' attribute.
        This prevents infinite recursion during parameter encoding.
        """
        state = self.__dict__.copy()
        if "client" in state:
            del state["client"]
        return state

    def get_current_account(self):
        return "dummy_account"

    def stream_exists(self, stream_id: str, data_provider: Optional[str] = None) -> bool:
        return stream_id in self.existing_streams

    def deploy_stream(self, stream_id: str, stream_type: str = truf_sdk.StreamTypePrimitive, wait: bool = True):
        # Instead of calling the real SDK, we simulate deployment by calling our fake init_stream.
        self.init_stream(stream_id, wait=wait)
        return "dummy_tx_hash"

    def init_stream(self, stream_id: str, wait: bool = True):
        # Simulate a successful stream initialization.
        self.deployed_streams.append(stream_id)
        self.existing_streams.add(stream_id)
        return "dummy_tx_hash"


# --- Fake TNAccessBlock that returns our FakeTNClient ---
class FakeTNAccessBlock(TNAccessBlock):
    def __init__(self, fake_client: FakeTNClient):
        # Provide dummy values for required fields.
        super().__init__(
            tn_provider="2b5ad5c4795c026514f8317c7a215e218dccd6cf",
            tn_private_key=SecretStr("0000000000000000000000000000000000000000000000000000000000000001"),
        )
        self._fake_client = fake_client

    def get_client(self):
        return self._fake_client


@pytest.mark.usefixtures("prefect_test_fixture")
def test_deploy_streams_flow_all_new():
    """
    When none of the streams exist, all should be deployed.
    """
    fake_psd = FakePrimitiveSourcesDescriptor()
    fake_client = FakeTNClient(existing_streams=set())
    fake_tna = FakeTNAccessBlock(fake_client=fake_client)

    results = deploy_streams_flow(
        psd_block=fake_psd, tna_block=fake_tna
    )

    # All three streams should be deployed.
    assert set(fake_client.deployed_streams) == {"stream1", "stream2", "stream3"}

    # Each message should indicate that the stream was deployed (or possibly "already exists"
    # if, for example, a retry ran).
    for message in results:
        assert "Deployed and initialized stream" in message or "already exists" in message


# Dummy implementation for the primitive source descriptor block
class DummyPrimitiveSourcesDescriptorBlock(PrimitiveSourcesDescriptorBlock):
    num_streams: int = 10000

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        # Create a DataFrame with 10,000 stream descriptors
        data = {
            "stream_id": [f"stream_{i}" for i in range(self.num_streams)],
            "source_id": [f"src_{i}" for i in range(self.num_streams)],
            "source_type": ["type1" for _ in range(self.num_streams)],
        }
        return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(data))


# Dummy TN client to simulate account retrieval
class DummyTNClient(tn_client.TNClient):
    def __init__(self):
        pass

    def get_current_account(self):
        return "dummy_account"


# Dummy TNAccessBlock to simulate tn server behavior
class DummyTNAccessBlock(TNAccessBlock):
    def __init__(self):
        super().__init__(
            tn_provider="2b5ad5c4795c026514f8317c7a215e218dccd6cf",
            tn_private_key=SecretStr("0000000000000000000000000000000000000000000000000000000000000001"),
        )

    @property
    def client(self):
        return DummyTNClient()

    def get_client(self):
        return self.client

    def stream_exists(self, data_provider: str, stream_id: str) -> bool:
        # For testing, assume the stream never exists
        return False

    def deploy_stream(self, stream_id: str, stream_type: str = truf_sdk.StreamTypePrimitive, wait: bool = True):
        # wait 0.01 seconds
        # sleep(0.01)
        return f"deploy_tx_{stream_id}"

    def init_stream(self, stream_id: str, wait: bool = True):
        return f"init_tx_{stream_id}"

    def wait_for_tx(self, tx_hash: str):
        # wait 0.1 seconds
        # sleep(0.1)
        return


# Dummy implementations for tn module tasks to avoid real network calls

@pytest.fixture
def tn_server():
    # Return our dummy TNAccessBlock instance
    return DummyTNAccessBlock()


@pytest.fixture
def primitive_descriptor():
    dummy = DummyPrimitiveSourcesDescriptorBlock(num_streams=5)
    return dummy


@pytest.mark.usefixtures("prefect_test_fixture")
def test_deploy_streams_flow(
    tn_server: DummyTNAccessBlock, primitive_descriptor: DummyPrimitiveSourcesDescriptorBlock
):
    # Execute the deployment flow which deploys 5 streams
    result = deploy_streams_flow(
        psd_block=primitive_descriptor, tna_block=tn_server
    )

    # Assert that the result is a list with 5 messages
    assert result["deployed_count"] == 5
    assert result["skipped_count"] == 0


if __name__ == "__main__":
    pytest.main([__file__])
