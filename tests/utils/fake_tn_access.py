from typing import Any, Optional

import pandas as pd
from pandera.typing import DataFrame as PanderaDataFrame
from pydantic import SecretStr
import trufnetwork_sdk_py.client as tn_client
from trufnetwork_sdk_py.client import RecordBatch, StreamDefinitionInput, StreamLocatorInput

from tsn_adapters.blocks.tn_access import (
    StreamAlreadyExistsError,
    TNAccessBlock,
)
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel


class FakeInternalTNClient(tn_client.TNClient):
    """Internal fake TNClient for FakeTNAccessBlock."""

    def __init__(
        self, existing_streams: Optional[set[str]] = None, initial_account: str = "fake_account_from_internal_client"
    ):
        # Do NOT call super().__init__() to avoid real client initialization / signer error
        self._current_account: str = initial_account
        self._existing_streams: set[str] = set(existing_streams) if existing_streams else set()
        self._deployed_stream_definitions: list[StreamDefinitionInput] = []
        self._deploy_calls: list[str] = []
        self._error_on: dict[str, Any] = {}
        self._inserted_dataframes_history: list[PanderaDataFrame[TnDataRowModel]] = []  # Stores DataFrames

    def get_current_account(self) -> str:
        if self._error_on.get("get_current_account"):
            raise self._error_on["get_current_account"]
        return self._current_account

    def batch_deploy_streams(self, definitions: list[StreamDefinitionInput], wait: bool = True) -> str:
        if self._error_on.get("batch_deploy_streams"):
            raise self._error_on["batch_deploy_streams"]

        self._deployed_stream_definitions.extend(definitions)
        for defn in definitions:
            if defn["stream_id"] in self._existing_streams:
                pass
            self._existing_streams.add(defn["stream_id"])
            if defn["stream_id"] not in self._deploy_calls:
                self._deploy_calls.append(defn["stream_id"])
        return "fake_batch_tx_hash"

    def batch_filter_streams_by_existence(
        self,
        locators: list[StreamLocatorInput],
        return_existing: bool,
    ) -> list[StreamLocatorInput]:
        print(f"[FakeInternalTNClient.batch_filter_streams_by_existence] Called with return_existing={return_existing}")
        print(
            f"[FakeInternalTNClient.batch_filter_streams_by_existence] self._existing_streams: {self._existing_streams}"
        )
        print(f"[FakeInternalTNClient.batch_filter_streams_by_existence] locators: {locators}")

        if self._error_on.get("batch_filter_streams_by_existence"):
            raise self._error_on["batch_filter_streams_by_existence"]

        results: list[StreamLocatorInput] = []
        for locator in locators:
            exists = locator["stream_id"] in self._existing_streams
            if return_existing and exists:
                results.append(locator)
            elif not return_existing and not exists:
                results.append(locator)
        return results

    def batch_insert_records(self, batches: list[RecordBatch], wait: bool = False) -> list[str]:
        if self._error_on.get("batch_insert_records"):
            raise self._error_on["batch_insert_records"]

        all_reconstructed_records = []
        for batch in batches:
            for record_input in batch["inputs"]:
                all_reconstructed_records.append(
                    {
                        "stream_id": batch["stream_id"],
                        "date": record_input["date"],
                        "value": str(record_input["value"]),
                        "data_provider": self._current_account,
                    }
                )

        if all_reconstructed_records:
            df = pd.DataFrame.from_records(all_reconstructed_records)
            df = df[["data_provider", "stream_id", "date", "value"]]
            df["date"] = df["date"].astype(int)
            self._inserted_dataframes_history.append(PanderaDataFrame[TnDataRowModel](df))

        return (
            [f"fake_client_tx_hash_{idx+1}" for idx in range(len(self._inserted_dataframes_history))]
            if self._inserted_dataframes_history and batches
            else []
        )

    def deploy_stream(
        self, stream_id: str, stream_type: str = tn_client.STREAM_TYPE_PRIMITIVE, wait: bool = True
    ) -> str:
        if self._error_on.get("deploy_stream"):
            raise self._error_on["deploy_stream"]
        if stream_id in self._existing_streams:
            raise StreamAlreadyExistsError(stream_id)
        self._existing_streams.add(stream_id)
        self._deploy_calls.append(stream_id)
        return f"deploy_tx_{stream_id}"

    def wait_for_tx(self, tx_hash: str) -> None:
        if self._error_on.get("wait_for_tx"):
            raise self._error_on["wait_for_tx"]
        pass

    def reset(self) -> None:
        self._existing_streams.clear()
        self._deployed_stream_definitions.clear()
        self._deploy_calls.clear()
        self._error_on.clear()
        self._inserted_dataframes_history.clear()

    def set_error(self, method_name: str, error: Exception) -> None:
        self._error_on[method_name] = error

    def clear_error(self, method_name: str) -> None:
        if method_name in self._error_on:
            del self._error_on[method_name]

    def set_existing_streams(self, streams: set[str]) -> None:
        self._existing_streams = set(streams)

    @property
    def existing_streams(self) -> set[str]:
        return self._existing_streams

    @property
    def deploy_calls_history(self) -> list[str]:
        return self._deploy_calls

    @property
    def deployed_stream_definitions_history(self) -> list[StreamDefinitionInput]:
        return self._deployed_stream_definitions

    @property
    def inserted_dataframes_history(self) -> list[PanderaDataFrame[TnDataRowModel]]:
        return self._inserted_dataframes_history


class FakeTNAccessBlock(TNAccessBlock):
    """
    In-memory fake TNAccessBlock for testing using an internal _FakeInternalTNClient.
    Allows inspection of data passed to batch_insert_tn_records via the `inserted_records` property.
    """

    def __init__(
        self,
        existing_streams: Optional[set[str]] = None,
        initial_account: str = "fake_block_account",
        error_on_block_method: Optional[dict[str, Any]] = None,
    ):
        # Initialize parent with a valid-looking dummy hex private key to avoid signer errors
        # if TNAccessBlock.__init__ attempts to create a real client early.
        super().__init__(
            tn_provider="fake_provider",
            tn_private_key=SecretStr("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"),
        )
        # Override the client with our fake internal client
        self._internal_fake_client = FakeInternalTNClient(
            existing_streams=existing_streams, initial_account=initial_account
        )
        self._block_error_on: dict[str, Any] = error_on_block_method or {}
        # _inserted_records_df_history is now managed by FakeInternalTNClient
        self._init_calls: list[str] = []

    @property
    def client(self) -> tn_client.TNClient:  # Ensures this FakeTNAccessBlock uses the FakeInternalTNClient
        """Returns the internal fake TNClient instance."""
        return self._internal_fake_client

    @property
    def current_account(self) -> str:
        # Delegate to the internal client's current_account
        return self._internal_fake_client.get_current_account()

    def set_deployed_streams(self, streams: set[str]) -> None:
        """Set the deployed streams in the internal fake client."""
        self._internal_fake_client.set_existing_streams(streams)

    @property
    def deployed_streams(self) -> set[str]:
        """Set of currently deployed streams from the internal fake client."""
        return set(self._internal_fake_client.existing_streams)

    @property
    def deploy_calls(self) -> list[str]:
        return list(self._internal_fake_client.deploy_calls_history)

    @property
    def deployed_stream_definitions(self) -> list[StreamDefinitionInput]:
        return list(self._internal_fake_client.deployed_stream_definitions_history)

    @property
    def inserted_records(self) -> list[PanderaDataFrame[TnDataRowModel]]:
        """Provides access to DataFrames passed to TNAccessBlock.batch_insert_tn_records."""
        # Assumes FakeInternalTNClient has 'inserted_dataframes_history' property
        return self._internal_fake_client.inserted_dataframes_history

    # No need to override batch_insert_tn_records here.
    # The goal is to let TNAccessBlock.batch_insert_tn_records run its course,
    # which will call self.client.batch_insert_records (our faked one).

    def stream_exists(self, data_provider: str, stream_id: str) -> bool:
        if self._block_error_on.get("stream_exists"):
            raise self._block_error_on["stream_exists"]
        if data_provider != self.current_account:
            pass
        return stream_id in self._internal_fake_client.existing_streams

    def init_stream(self, stream_id: str, wait: bool = True) -> str:
        if self._block_error_on.get("init_stream"):
            raise self._block_error_on["init_stream"]
        if stream_id not in self._internal_fake_client.existing_streams:
            raise RuntimeError(f"Stream {stream_id} does not exist for init via block.")
        self._init_calls.append(stream_id)
        return f"block_init_tx_{stream_id}"

    def get_stream_type(self, data_provider: str, stream_id: str) -> str:
        if self._block_error_on.get("get_stream_type"):
            raise self._block_error_on["get_stream_type"]
        if stream_id not in self._internal_fake_client.existing_streams:
            raise RuntimeError(f"Stream {stream_id} not found for data_provider {data_provider}.")
        return tn_client.STREAM_TYPE_PRIMITIVE

    def set_block_error(self, method: str, error: Exception) -> None:
        self._block_error_on[method] = error

    def clear_block_error(self, method: str) -> None:
        if method in self._block_error_on:
            del self._block_error_on[method]

    def reset(self) -> None:
        if hasattr(self, "_internal_fake_client") and self._internal_fake_client:  # Ensure client exists
            self._internal_fake_client.reset()
        self._block_error_on.clear()
        self._init_calls.clear()
        # No _inserted_records_df_history to clear here, it's on the client
