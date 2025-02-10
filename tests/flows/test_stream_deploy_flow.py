import pandas as pd
import pytest
from pandera.typing import DataFrame
import trufnetwork_sdk_py.client as tn_client
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourcesDescriptorBlock,
    PrimitiveSourceDataModel,
)
from tsn_adapters.flows.stream_deploy_flow import deploy_streams_flow
from tsn_adapters.blocks.tn_access import TNAccessBlock
from pydantic import SecretStr


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
    def __init__(self, existing_streams=None):
        # We do not call the real TNClient __init__ to avoid any side effects.
        if existing_streams is None:
            existing_streams = set()
        self.existing_streams = set(existing_streams)
        self.deployed_streams = []
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

    def stream_exists(self, stream_id: str) -> bool:
        return stream_id in self.existing_streams

    def deploy_stream(self, stream_id: str, stream_type, wait: bool = True):
        # Instead of calling the real SDK, we simulate deployment by calling our fake init_stream.
        self.init_stream(stream_id, wait=wait)

    def init_stream(self, stream_id: str, wait: bool = True):
        # Simulate a successful stream initialization.
        self.deployed_streams.append(stream_id)
        self.existing_streams.add(stream_id)
        return "dummy_tx_hash"

# --- Fake TNAccessBlock that returns our FakeTNClient ---
class FakeTNAccessBlock(TNAccessBlock):
    def __init__(self, fake_client: FakeTNClient):
        # Provide dummy values for required fields.
        super().__init__(tn_provider="2b5ad5c4795c026514f8317c7a215e218dccd6cf", tn_private_key=SecretStr("0000000000000000000000000000000000000000000000000000000000000001"))
        self._fake_client = fake_client

    def get_client(self):
        return self._fake_client

def test_deploy_streams_flow_all_new():
    """
    When none of the streams exist, all should be deployed.
    """
    fake_psd = FakePrimitiveSourcesDescriptor()
    fake_client = FakeTNClient(existing_streams=set())
    fake_tna = FakeTNAccessBlock(fake_client=fake_client)

    results = deploy_streams_flow(psd_block=fake_psd, tn_client=fake_client, tna_block=fake_tna)

    # Because the flow returns Prefect task "future" objects,
    # extract the underlying result from each.
    resolved_messages = []
    for res in results:
        if hasattr(res, "result"):
            val = res.result
            if callable(val):
                val = val()
            resolved_messages.append(val)
        else:
            resolved_messages.append(res)

    # All three streams should be deployed.
    assert set(fake_client.deployed_streams) == {"stream1", "stream2", "stream3"}

    # Each message should indicate that the stream was deployed (or possibly "already exists"
    # if, for example, a retry ran).
    for message in resolved_messages:
        assert "Deployed and initialized stream" in message or "already exists" in message


if __name__ == "__main__":
    pytest.main([__file__])
