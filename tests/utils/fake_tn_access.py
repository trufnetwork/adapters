from typing import Any, Optional

from pydantic import SecretStr
import trufnetwork_sdk_py.client as tn_client

from tsn_adapters.blocks.tn_access import (
    StreamAlreadyExistsError,
    TNAccessBlock,
)


class FakeInternalTNClient(tn_client.TNClient):
    """Internal fake TNClient for FakeTNAccessBlock."""

    def __init__(
        self, 
        existing_streams: Optional[set[str]] = None, 
        initial_account: str = "fake_account_from_internal_client"
    ):
        # Do NOT call super().__init__ to avoid real client initialization
        self._current_account: str = initial_account
        self._existing_streams: set[str] = set(existing_streams) if existing_streams else set()
        self._deployed_stream_definitions: list[tn_client.StreamDefinitionInput] = []
        self._deploy_calls: list[str] = [] # Tracks stream_ids from deploy_stream and batch_deploy_streams
        self._error_on: dict[str, Any] = {}

    def get_current_account(self) -> str:
        if self._error_on.get("get_current_account"):
            raise self._error_on["get_current_account"]
        return self._current_account

    def batch_deploy_streams(self, definitions: list[tn_client.StreamDefinitionInput], wait: bool = True) -> str:
        if self._error_on.get("batch_deploy_streams"):
            raise self._error_on["batch_deploy_streams"]
        
        self._deployed_stream_definitions.extend(definitions)
        for defn in definitions:
            if defn["stream_id"] in self._existing_streams:
                # Behavior for deploying already existing stream can be debated for a fake.
                # For now, let's mimic a potential error or just re-add.
                # If the flow expects an error, this might need adjustment or error injection.
                pass # Or raise StreamAlreadyExistsError(defn["stream_id"])
            self._existing_streams.add(defn["stream_id"])
            if defn["stream_id"] not in self._deploy_calls:
                 self._deploy_calls.append(defn["stream_id"])
        return "fake_batch_tx_hash"

    def batch_filter_streams_by_existence(
        self,
        locators: list[tn_client.StreamLocatorInput],
        return_existing: bool,
    ) -> list[tn_client.StreamLocatorInput]:
        if self._error_on.get("batch_filter_streams_by_existence"):
            raise self._error_on["batch_filter_streams_by_existence"]
        
        results: list[tn_client.StreamLocatorInput] = []
        for locator in locators:
            exists = locator["stream_id"] in self._existing_streams
            if return_existing and exists:
                results.append(locator)
            elif not return_existing and not exists:
                results.append(locator)
        
        return results
    
    def deploy_stream(self, stream_id: str, stream_type: str = tn_client.STREAM_TYPE_PRIMITIVE, wait: bool = True) -> str:
        if self._error_on.get("deploy_stream"):
            raise self._error_on["deploy_stream"]
        if stream_id in self._existing_streams:
            raise StreamAlreadyExistsError(stream_id)
        self._existing_streams.add(stream_id)
        self._deploy_calls.append(stream_id)
        return f"deploy_tx_{stream_id}"
    
    def wait_for_tx(self, tx_hash: str) -> None:
        if self._error_on.get("wait_for_tx") : raise self._error_on["wait_for_tx"]
        pass # No-op for fake

    # --- Helper methods for testing this fake client ---
    def reset(self) -> None:
        self._existing_streams.clear()
        self._deployed_stream_definitions.clear()
        self._deploy_calls.clear()
        self._error_on.clear()

    def set_error(self, method_name: str, error: Exception) -> None:
        self._error_on[method_name] = error

    def clear_error(self, method_name: str) -> None:
        if method_name in self._error_on:
            del self._error_on[method_name]

    def set_existing_streams(self, streams: set[str]) -> None:
        """Directly set the existing streams for test setup."""
        self._existing_streams = set(streams)

    # --- Public properties for accessing internal state ---
    @property
    def existing_streams(self) -> set[str]:
        return self._existing_streams
    
    @property
    def deploy_calls_history(self) -> list[str]: # Renamed to avoid clash if FakeTNAccessBlock has deploy_calls property
        return self._deploy_calls
    
    @property
    def deployed_stream_definitions_history(self) -> list[tn_client.StreamDefinitionInput]: # Renamed
        return self._deployed_stream_definitions


class FakeTNAccessBlock(TNAccessBlock):
    """
    In-memory fake TNAccessBlock for testing using an internal _FakeInternalTNClient.

    - Tracks deployed and initialized streams in memory.
    - No real network or SDK calls.
    - Allows error injection for negative test cases.
    - Extensible for future test scenarios.
    - Provides public properties for test assertions: .deployed_streams, .deploy_calls
    - Provides set_deployed_streams() for test setup.
    """

    def __init__(
        self,
        existing_streams: Optional[set[str]] = None, # Streams to pre-populate in the fake client
        initial_account: str = "fake_block_account",
        # initialized_streams: Optional[set[str]] = None, # This was for the old direct fake, might be less relevant now
        error_on_block_method: Optional[dict[str, Any]] = None, # For errors on FakeTNAccessBlock methods themselves
    ):
        # Call TNAccessBlock's init with dummy values, as client will be overridden
        super().__init__(
            tn_provider="fake_provider",
            tn_private_key=SecretStr("fake_key"),
        )
        self._internal_fake_client = FakeInternalTNClient(existing_streams=existing_streams, initial_account=initial_account)
        self._block_error_on: dict[str, Any] = error_on_block_method or {}
        
        # Properties that were directly on FakeTNAccessBlock before might now proxy to _internal_fake_client
        # or be handled by it.
        # self._initialized_streams, self._destroyed_streams seem less relevant if client handles existence.
        self._init_calls: list[str] = [] # Keep if init_stream is faked at block level

    @property
    def client(self) -> tn_client.TNClient:
        """Returns the internal fake TNClient instance."""
        return self._internal_fake_client
    
    @property
    def current_account(self) -> str:
        # Delegate to the internal client's current_account or override if block has a different one
        return self._internal_fake_client.get_current_account()

    # --- Methods to control the internal fake client for testing ---
    def set_deployed_streams(self, streams: set[str]) -> None:
        """Set the deployed streams in the internal fake client."""
        self._internal_fake_client.set_existing_streams(streams)

    @property
    def deployed_streams(self) -> set[str]:
        """Set of currently deployed streams from the internal fake client."""
        return set(self._internal_fake_client.existing_streams)

    @property
    def deploy_calls(self) -> list[str]: # From internal client
        return list(self._internal_fake_client.deploy_calls_history)
    
    @property
    def deployed_stream_definitions(self) -> list[tn_client.StreamDefinitionInput]: # From internal client
        return list(self._internal_fake_client.deployed_stream_definitions_history)

    # --- Block-level fake methods (if different from direct client pass-through) ---
    # Example: stream_exists might have block-level logic or just delegate
    def stream_exists(self, data_provider: str, stream_id: str) -> bool:
        if self._block_error_on.get("stream_exists"):
            raise self._block_error_on["stream_exists"]
        # For this fake, assume data_provider matching current_account implicitly
        # This might need adjustment based on how data_provider is used with stream_exists
        if data_provider != self.current_account:
            # Or log a warning, or return False, depending on desired strictness for data_provider
            pass 
        return stream_id in self._internal_fake_client.existing_streams

    # deploy_stream on FakeTNAccessBlock might be kept if it adds block-level logic,
    # or removed if all deployments go via client.batch_deploy_streams or client.deploy_stream
    # For now, let's assume it delegates or that tests will call client.deploy_stream directly if needed.
    # We removed the direct batch_deploy_streams from FakeTNAccessBlock

    def init_stream(self, stream_id: str, wait: bool = True) -> str:
        # This method was on the old FakeTNAccessBlock. 
        # The real TNAccessBlock doesn't have init_stream. TNClient might if it's a custom extension.
        # If TNClient *does* have init_stream, _FakeInternalTNClient should implement it.
        # If not, this method might be specific to this fake's old design.
        # For now, let's assume it's a block-level fake logic if kept.
        if self._block_error_on.get("init_stream"):
            raise self._block_error_on["init_stream"]
        
        # Check existence via the internal client
        if stream_id not in self._internal_fake_client.existing_streams:
            raise RuntimeError(f"Stream {stream_id} does not exist for init via block.")
        # self._initialized_streams.add(stream_id) # This state needs to move to _FakeInternalTNClient if relevant
        self._init_calls.append(stream_id)
        return f"block_init_tx_{stream_id}"

    # filter_initialized_streams was also on the old FakeTNAccessBlock.
    # The real TNAccessBlock.filter_initialized_streams calls self.client.batch_filter_streams_by_initialization_status
    # So, _FakeInternalTNClient would need batch_filter_streams_by_initialization_status
    # For now, I'm keeping batch_filter_streams_by_existence in _FakeInternalTNClient as that's what the flow uses.
    # If filter_initialized_streams is still needed by other tests using FakeTNAccessBlock, 
    # it would need to call a corresponding method on _FakeInternalTNClient.

    def get_stream_type(self, data_provider: str, stream_id: str) -> str:
        if self._block_error_on.get("get_stream_type"):
            raise self._block_error_on["get_stream_type"]
        if stream_id not in self._internal_fake_client.existing_streams:
            raise RuntimeError(f"Stream {stream_id} not found for data_provider {data_provider}.")
        # For simplicity, always return primitive. Add more logic if needed.
        return tn_client.STREAM_TYPE_PRIMITIVE

    # Utility methods for tests (for block-level error injection)
    def set_block_error(self, method: str, error: Exception) -> None:
        self._block_error_on[method] = error

    def clear_block_error(self, method: str) -> None:
        if method in self._block_error_on:
            del self._block_error_on[method]

    def reset(self) -> None:
        self._internal_fake_client.reset()
        self._block_error_on.clear()
        self._init_calls.clear()
        # self._initialized_streams.clear() # If this state moves to client
        # self._destroyed_streams.clear() # If this state moves to client
