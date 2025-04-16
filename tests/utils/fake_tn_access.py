from typing import Any, Optional

from pandera.typing import DataFrame
from pydantic import SecretStr

from tsn_adapters.blocks.shared_types import StreamLocatorModel
from tsn_adapters.blocks.tn_access import (
    MetadataProcedureNotFoundError,
    StreamAlreadyExistsError,
    TNAccessBlock,
    convert_to_typed_df,
)


class FakeTNAccessBlock(TNAccessBlock):
    """
    In-memory fake TNAccessBlock for testing.

    - Tracks deployed and initialized streams in memory.
    - No real network or SDK calls.
    - Allows error injection for negative test cases.
    - Extensible for future test scenarios.
    - Provides public properties for test assertions: .deployed_streams, .deploy_calls
    - Provides set_deployed_streams() for test setup.
    """

    def __init__(
        self,
        existing_streams: Optional[set[str]] = None,
        initialized_streams: Optional[set[str]] = None,
        error_on: Optional[dict[str, Any]] = None,
    ):
        super().__init__(
            tn_provider="fake",
            tn_private_key=SecretStr("fake"),
            helper_contract_name="fake",
            helper_contract_deployer=None,
        )
        # Override client to prevent real SDK initialization
        self._client = None  # Fake block doesn't use a real client
        self._current_account = "fake_account"  # Provide a dummy account
        self._existing_streams: set[str] = set(existing_streams) if existing_streams else set()
        self._initialized_streams: set[str] = set(initialized_streams) if initialized_streams else set()
        self._error_on: dict[str, Any] = error_on or {}
        self._destroyed_streams: set[str] = set()
        self._deploy_calls: list[str] = []
        self._init_calls: list[str] = []

    # Keep the current_account override
    @property
    def current_account(self) -> str:
        return self._current_account

    def set_deployed_streams(self, streams: set[str]) -> None:
        """Set the deployed streams for test setup."""
        self._existing_streams = set(streams)

    @property
    def deployed_streams(self) -> set[str]:
        """Set of currently deployed streams (for test assertions)."""
        return set(self._existing_streams)

    @property
    def deploy_calls(self) -> list[str]:
        """List of stream_ids passed to deploy_stream (for test assertions)."""
        return list(self._deploy_calls)

    @property
    def initialized_streams(self) -> set[str]:
        """Set of currently initialized streams (for test assertions)."""
        return set(self._initialized_streams)

    def set_initialized_streams(self, streams: set[str]) -> None:
        """
        Set the initialized streams for this fake block directly (for test setup).
        Args:
            streams (set[str]): The set of stream IDs to mark as initialized.
        """
        self._initialized_streams = set(streams)

    def stream_exists(self, data_provider: str, stream_id: str) -> bool:
        if self._error_on.get("stream_exists"):
            raise self._error_on["stream_exists"]
        return stream_id in self._existing_streams and stream_id not in self._destroyed_streams

    def deploy_stream(self, stream_id: str, stream_type: str = "primitive", wait: bool = True) -> str:
        if self._error_on.get("deploy_stream"):
            raise self._error_on["deploy_stream"]
        if stream_id in self._existing_streams:
            raise StreamAlreadyExistsError(stream_id)
        self._existing_streams.add(stream_id)
        self._deploy_calls.append(stream_id)
        return f"deploy_tx_{stream_id}"

    def init_stream(self, stream_id: str, wait: bool = True) -> str:
        if self._error_on.get("init_stream"):
            raise self._error_on["init_stream"]
        if stream_id not in self._existing_streams:
            raise RuntimeError(f"Stream {stream_id} does not exist for init.")
        self._initialized_streams.add(stream_id)
        self._init_calls.append(stream_id)
        return f"init_tx_{stream_id}"

    def wait_for_tx(self, tx_hash: str) -> None:
        if self._error_on.get("wait_for_tx"):
            raise self._error_on["wait_for_tx"]
        # No-op for fake

    def destroy_stream(self, stream_id: str, wait: bool = True) -> str:
        self._destroyed_streams.add(stream_id)
        self._existing_streams.discard(stream_id)
        self._initialized_streams.discard(stream_id)
        return f"destroy_tx_{stream_id}"

    def get_stream_type(self, data_provider: str, stream_id: str) -> str:
        # First check existence
        if stream_id not in self._existing_streams:
            # Raise an error indicating the stream doesn't exist
            # Using RuntimeError for now, adjust if a more specific error exists
            raise RuntimeError(f"Stream {stream_id} not found for data_provider {data_provider}.")
        
        # Then check initialization status if it exists
        if stream_id in self._initialized_streams:
            return "primitive"
        
        # If it exists but isn't initialized
        raise RuntimeError(f"Stream {stream_id} exists but is not initialized.")

    def filter_initialized_streams(
        self, stream_ids: list[str], data_providers: list[str]
    ) -> DataFrame[StreamLocatorModel]:
        """Fake batch filter for initialized streams, with error injection.

        Mimics real behavior where the batch call fails if any stream doesn't exist.
        """
        if self._error_on.get("batch_init"):
            raise self._error_on["batch_init"]

        # Check for non-existent streams first - the real batch call would likely fail
        non_existent = [sid for sid in stream_ids if sid not in self._existing_streams]
        if non_existent:
            # Raise MetadataProcedureNotFoundError, as this is what the real system might raise
            raise MetadataProcedureNotFoundError(
                f"Batch initialization check failed: Streams do not exist: {non_existent}"
            )

        # Return only streams that are both deployed and initialized
        result = []
        for stream_id, data_provider in zip(stream_ids, data_providers):
            if stream_id in self._existing_streams and stream_id in self._initialized_streams:
                result.append({"data_provider": data_provider, "stream_id": stream_id})
        df = DataFrame(result, columns=["data_provider", "stream_id"])
        return convert_to_typed_df(df, StreamLocatorModel)

    # Utility methods for tests
    def set_error(self, method: str, error: Exception) -> None:
        self._error_on[method] = error

    def clear_error(self, method: str) -> None:
        if method in self._error_on:
            del self._error_on[method]

    def reset(self) -> None:
        self._existing_streams.clear()
        self._initialized_streams.clear()
        self._destroyed_streams.clear()
        self._deploy_calls.clear()
        self._init_calls.clear()
