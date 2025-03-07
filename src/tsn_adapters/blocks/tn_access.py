from datetime import datetime, timedelta, timezone
import decimal
from functools import wraps
from typing import (
    Any,
    Callable,
    Literal,
    Optional,
    TypedDict,
    TypeVar,
    Union,
    cast,
    overload,
)

import pandas as pd
from pandera.typing import DataFrame
from prefect import Task, get_run_logger, task
from prefect.blocks.core import Block
from prefect.client.schemas.objects import State, TaskRun
from prefect.concurrency.sync import concurrency, rate_limit
from prefect.states import Completed
from pydantic import ConfigDict, Field, SecretStr
import trufnetwork_sdk_c_bindings.exports as truf_sdk
import trufnetwork_sdk_py.client as tn_client

from tsn_adapters.blocks.shared_types import DivideAndConquerResult
from tsn_adapters.common.trufnetwork.models.tn_models import StreamLocatorModel, TnDataRowModel, TnRecord, TnRecordModel
from tsn_adapters.utils.date_type import ShortIso8601Date
from tsn_adapters.utils.logging import get_logger_safe
from tsn_adapters.utils.unix import check_unix_timestamp

# --- Type Variables ---
T = TypeVar("T")  # Generic type for DataFrame models


# --- Utility Functions ---
def create_empty_df(columns: list[str]) -> pd.DataFrame:
    """Create an empty DataFrame with specified columns."""
    return pd.DataFrame(columns=columns)


def create_typed_empty_df(model_type: type[T], columns: list[str]) -> DataFrame[T]:
    """Create an empty typed DataFrame with specified columns."""
    return DataFrame[model_type](create_empty_df(columns))


def append_to_df(df: pd.DataFrame, row_data: Union[dict[str, Any], "pd.Series[Any]"]) -> pd.DataFrame:
    """Append a row to a DataFrame and handle empty DataFrame case."""
    row_df = pd.DataFrame([row_data])
    return pd.concat([df, row_df]) if not df.empty else row_df


def convert_to_typed_df(df: pd.DataFrame, model_type: type[T]) -> DataFrame[T]:
    """Convert a pandas DataFrame to a typed DataFrame."""
    if df.empty:
        return cast(DataFrame[T], df)
    return DataFrame[model_type](df)


def process_stream_check_futures(futures: list[tuple[Any, Any]], logger: Any) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process futures from stream check operations and separate into initialized and uninitialized.

    Args:
        futures: List of (future, row) tuples where future.result() returns a boolean
        logger: Logger instance for reporting errors

    Returns:
        Tuple of (initialized_streams, uninitialized_streams) DataFrames
    """
    initialized_streams = create_empty_df(["data_provider", "stream_id"])
    uninitialized_streams = create_empty_df(["data_provider", "stream_id"])

    for future, row in futures:
        try:
            initialized = future.result()
            if initialized:
                initialized_streams = append_to_df(initialized_streams, row)
            else:
                uninitialized_streams = append_to_df(uninitialized_streams, row)
        except Exception as e:
            logger.warning(f"Error checking if stream {row['stream_id']} is initialized: {e!s}")
            uninitialized_streams = append_to_df(uninitialized_streams, row)

    return initialized_streams, uninitialized_streams


def extract_stream_locators(records: DataFrame[TnDataRowModel]) -> DataFrame[StreamLocatorModel]:
    """Extract unique stream locators from records DataFrame."""
    unique_stream_locators = records[["data_provider", "stream_id"]].drop_duplicates()
    return convert_to_typed_df(unique_stream_locators, StreamLocatorModel)


def create_empty_stream_locator_df() -> DataFrame[StreamLocatorModel]:
    """Create an empty DataFrame for stream locators."""
    return create_typed_empty_df(StreamLocatorModel, ["data_provider", "stream_id"])


def get_stream_locator_set(df: pd.DataFrame) -> set[tuple[str, str]]:
    """Convert a DataFrame with data_provider and stream_id to a set of tuples."""
    return {(row["data_provider"] or "", row["stream_id"]) for _, row in df.iterrows()}


def diff_stream_locator_sets(
    all_streams: set[tuple[str, str]], initialized_streams: set[tuple[str, str]]
) -> list[dict[str, str]]:
    """Get the difference between two sets of stream locators and convert to a list of dicts."""
    uninitialized_set = all_streams - initialized_streams
    return [{"data_provider": dp, "stream_id": sid} for dp, sid in uninitialized_set]


# --- Constants ---
UNUSED_INFINITY_RETRIES = 1000000  # Prefect doesn't have an infinite retry option

F = TypeVar("F", bound=Callable[..., Any])


def handle_tn_errors(func: F) -> F:
    """Decorator to handle TN network and DB timeout errors."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if TNNodeNetworkError.is_tn_node_network_error(e):
                raise TNNodeNetworkError.from_error(e)
            if TNDbTimeoutError.is_db_timeout_error(e):
                raise TNDbTimeoutError.from_error(e)
            if MetadataProcedureNotFoundError.is_metadata_procedure_not_found_error(e):
                raise MetadataProcedureNotFoundError.from_error(e)
            raise

    return cast(F, wrapper)


def tn_special_retry_condition(max_other_error_retries: int):
    """
    Custom retry condition for Prefect tasks.

    If the raised exception is a TNNodeNetworkError or TNDbTimeoutError, always retry (indefinitely).
    Otherwise, only retry if the current attempt (based on task_run's run_count) is less than max_retries.

    Args:
        max_retries: Maximum allowed retries for non-network errors.

    Returns:
        A callable that can be used as retry_condition_fn in Prefect tasks.
    """

    def _retry_condition(task: Task[Any, Any], task_run: TaskRun, state: State[Any]) -> bool:
        try:
            # This will re-raise the exception if the task failed.
            state.result()
        except (TNNodeNetworkError, TNDbTimeoutError):
            # Always retry on network and DB timeout errors.
            return True
        except (MetadataProcedureNotFoundError,):
            # This won't change with retries.
            return False
        except Exception:
            # For non-network errors, use the task_run's run_count to decide.
            if task_run.run_count <= max_other_error_retries:
                return True
            return False
        return False

    return _retry_condition


class SplitInsertResults(TypedDict):
    """Results of batch insertions."""

    success_tx_hashes: list[str]
    failed_records: DataFrame[TnDataRowModel]
    failed_reasons: list[str]


class FilterStreamsResults(TypedDict):
    """Results of stream filtering operations."""

    existent_streams: DataFrame[StreamLocatorModel]
    missing_streams: DataFrame[StreamLocatorModel]


class TNNodeNetworkError(Exception):
    """Error raised when a TN node network error occurs."""

    # RuntimeError: error deploying stream: http post failed: Post "http://staging.node-1.tsn.truflation.com:8484/rpc/v1": dial tcp 18.189.163.27:8484: connect: connection refused

    @classmethod
    def from_error(cls, error: Exception) -> "TNNodeNetworkError":
        if isinstance(error, TNNodeNetworkError):
            return error
        if isinstance(error, RuntimeError) and "http post failed" in str(error) and "tcp" in str(error):
            return cls(str(error))
        raise error

    @classmethod
    def is_tn_node_network_error(cls, error: Exception) -> bool:
        try:
            cls.from_error(error)
            return True
        except Exception:
            return False


class TNDbTimeoutError(Exception):
    """Error raised when a TN database timeout occurs."""

    @classmethod
    def from_error(cls, error: Exception) -> "TNDbTimeoutError":
        if isinstance(error, TNDbTimeoutError):
            return error
        # if "db timeout" in str(error).lower():
        #     return cls(str(error))
        if isinstance(error, RuntimeError) and "db timeout" in str(error):
            return cls(str(error))
        raise error

    @classmethod
    def is_db_timeout_error(cls, error: Exception) -> bool:
        try:
            cls.from_error(error)
            return True
        except Exception:
            return False


class MetadataProcedureNotFoundError(Exception):
    """Error raised when the metadata procedure is not found."""

    @classmethod
    def is_metadata_procedure_not_found_error(cls, error: Exception) -> bool:
        try:
            cls.from_error(error)
            return True
        except Exception:
            return False

    @classmethod
    def from_error(cls, error: Exception) -> "MetadataProcedureNotFoundError":
        if isinstance(error, MetadataProcedureNotFoundError):
            return error
        # More flexible matching for various quote styles and error formats
        if isinstance(error, RuntimeError) and "get_metadata" in str(error) and "not found in schema" in str(error):
            return cls(str(error))
        raise error


class TNAccessBlock(Block):
    """Prefect Block for managing TSN access credentials.

    This block securely stores and manages TSN access credentials for
    authenticating with TSN API in Prefect flows.
    """

    class Error(Exception):
        """Base error class for TNAccessBlock errors."""

        pass

    class StreamNotFoundError(Error):
        """Error raised when a TN stream is not found."""

        pass

    class InvalidRecordFormatError(Error):
        """Error raised when a record has invalid format."""

        pass

    class InvalidTimestampError(Error):
        """Error raised when a timestamp has invalid format."""

        pass

    tn_provider: str
    tn_private_key: SecretStr
    helper_contract_name: str = Field(default="", description="Name of the helper contract. Name != ID.")
    helper_contract_deployer: Optional[str] = Field(
        default=None,
        description="Address of the deployer of the helper contract. If not provided, will use the current wallet.",
    )
    model_config = ConfigDict(ignored_types=(Task,))

    @property
    def current_account(self):
        if not hasattr(self, "_current_account"):
            self._current_account = self.client.get_current_account()
        return self._current_account

    @property
    @handle_tn_errors
    def client(self) -> tn_client.TNClient:
        if not hasattr(self, "_client"):
            self._client = tn_client.TNClient(
                url=self.tn_provider,
                token=self.tn_private_key.get_secret_value(),
            )
        return self._client

    @property
    def helper_contract_stream_name(self):
        if not self.helper_contract_name:
            raise self.Error("Helper contract name is not set")
        return self.helper_contract_name

    @property
    def helper_contract_provider(self):
        return self.helper_contract_deployer or self.client.get_current_account()

    @property
    def logger(self):
        return get_logger_safe(__name__)

    def get_client(self):
        """
        deprecated: Use client property instead
        """
        return self.client

    @handle_tn_errors
    def stream_exists(self, data_provider: str, stream_id: str) -> bool:
        """Check if a stream exists"""
        with concurrency("tn-read", occupy=1):
            try:
                return self.client.stream_exists(stream_id=stream_id, data_provider=data_provider)
            except Exception as e:
                if "get_metadata" in str(e) and "not found in schema" in str(e):
                    return False
                raise e

    @handle_tn_errors
    def get_stream_type(self, data_provider: str, stream_id: str) -> str:
        """Check if a stream is initialized by getting its stream type.

        This method is used to verify both stream existence and initialization status.
        A successful response indicates the stream exists and is initialized.

        Args:
            data_provider: The data provider of the stream
            stream_id: The ID of the stream to check

        Returns:
            str: The stream type if initialized
        """
        rate_limit("tn-read-rate-limit", occupy=1)
        return self.client.get_type(stream_id=stream_id, data_provider=data_provider)

    @handle_tn_errors
    def is_allowed_to_write(self, data_provider: str, stream_id: str) -> bool:
        """Check if a stream is initialized by getting its stream type.

        Args:
            data_provider: The data provider of the stream
            stream_id: The ID of the stream to check

        Returns:
            bool: True if the stream is initialized and can be written to, False otherwise
        """
        raise NotImplementedError("is_allowed_to_write is not implemented")

    @handle_tn_errors
    def get_earliest_date(
        self, stream_id: str, data_provider: Optional[str] = None, is_unix: bool = False
    ) -> Optional[datetime]:
        """
        Get the earliest date available for a stream.

        Args:
            stream_id: ID of the stream to query
            data_provider: Optional data provider

        Returns:
            The earliest date if found, otherwise None

        Raises:
            StreamNotFoundError: If the stream does not exist
            InvalidRecordFormatError: If the record format is invalid
            InvalidTimestampError: If the timestamp format is invalid
            TNAccessBlock.Error: For other TN-related errors
        """
        try:
            first_record = self.get_first_record(stream_id=stream_id, data_provider=data_provider, is_unix=is_unix)
            if first_record is None:
                return None

            date_value = first_record.date
            if date_value is None:
                raise self.InvalidRecordFormatError(f"Invalid record format for {stream_id}: missing DateValue")

            if is_unix:
                try:
                    timestamp = int(date_value)
                    return datetime.fromtimestamp(timestamp, tz=timezone.utc)
                except (ValueError, TypeError) as e:
                    raise self.InvalidTimestampError(f"Invalid timestamp format for {stream_id}: {e}") from e
            else:
                return datetime.fromisoformat(date_value)

        except Exception as e:
            error_msg = str(e).lower()
            if "stream not found" in error_msg:
                raise self.StreamNotFoundError(f"Stream {stream_id} not found")

            raise self.Error(f"Failed to query TN for {stream_id}: {e}") from e

    @handle_tn_errors
    def read_all_records(self, stream_id: str, data_provider: Optional[str] = None) -> pd.DataFrame:
        """Read all records from TSN"""
        return self.read_records(stream_id, data_provider, date_from="1000-01-01")

    @handle_tn_errors
    def call_procedure(
        self, stream_id: str, data_provider: Optional[str] = None, procedure: str = "", args: Optional[list[Any]] = None
    ) -> list[dict[str, Any]]:
        with concurrency("tn-read", occupy=1):
            return self.client.call_procedure(stream_id, data_provider, procedure, args)

    @handle_tn_errors
    def get_first_record(
        self, stream_id: str, data_provider: Optional[str] = None, is_unix: bool = False
    ) -> Optional[TnRecord]:
        with concurrency("tn-read", occupy=1):
            if is_unix:
                result = self.client.get_first_record_unix(stream_id, data_provider)
            else:
                result = self.client.get_first_record(stream_id, data_provider)

        if result is None:
            return None

        return TnRecord(date=str(result["date"]), value=str(result["value"]))

    @overload
    def read_records(
        self,
        stream_id: str,
        data_provider: Optional[str] = None,
        date_from: Optional[ShortIso8601Date] = None,
        date_to: Optional[ShortIso8601Date] = None,
        *,  # Force is_unix to be keyword-only
        is_unix: Literal[False] = False,
    ) -> DataFrame[TnRecordModel]: ...

    @overload
    def read_records(
        self,
        stream_id: str,
        data_provider: Optional[str] = None,
        date_from: Optional[int] = None,
        date_to: Optional[int] = None,
        *,  # Force is_unix to be keyword-only
        is_unix: Literal[True],
    ) -> DataFrame[TnRecordModel]: ...

    @handle_tn_errors
    def read_records(
        self,
        stream_id: str,
        data_provider: Optional[str] = None,
        date_from: Union[ShortIso8601Date, int, None] = None,
        date_to: Union[ShortIso8601Date, int, None] = None,
        *,  # Force is_unix to be keyword-only
        is_unix: bool = False,
    ) -> DataFrame[TnRecordModel]:
        try:
            with concurrency("tn-read", occupy=1):
                if is_unix:
                    unix_from = int(cast(int, date_from)) if date_from is not None else None
                    unix_to = int(cast(int, date_to)) if date_to is not None else None
                    recs = self.client.get_records_unix(
                        stream_id,
                        data_provider,
                        unix_from,
                        unix_to,
                    )
                else:
                    iso_from = str(cast(ShortIso8601Date, date_from)) if date_from is not None else None
                    iso_to = str(cast(ShortIso8601Date, date_to)) if date_to is not None else None
                    recs = self.client.get_records(
                        stream_id,
                        data_provider,
                        iso_from,
                        iso_to,
                    )

            recs_list = [
                {
                    "date": rec["DateValue"],
                    "value": decimal.Decimal(rec["Value"]),
                    **{k: v for k, v in rec.items() if k not in ("DateValue", "Value")},
                }
                for rec in recs
            ]
            df = pd.DataFrame(recs_list, columns=["date", "value"])

        except Exception as e:
            self.logger.error(
                f"Error reading records from TN, stream_id: {stream_id}, "
                f"data_provider: {data_provider}, date_from: {date_from}, "
                f"date_to: {date_to}: {e}"
            )
            raise e

        return DataFrame[TnRecordModel](df)

    @handle_tn_errors
    def insert_tn_records(
        self,
        stream_id: str,
        records: DataFrame[TnRecordModel],
        data_provider: Optional[str] = None,
        include_current_date: bool = True,
    ) -> Optional[str]:
        logging = get_run_logger()

        if len(records) == 0:
            logging.info(f"No records to insert for stream {stream_id}")
            return None

        if include_current_date:
            current_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            args: list[list[str | float | int | list[str] | list[float]]] = [
                [record["date"], str(record["value"]), current_date] for record in records.to_dict(orient="records")
            ]
        else:
            args: list[list[str | float | int | list[str] | list[float]]] = [
                [record["date"], str(record["value"])] for record in records.to_dict(orient="records")
            ]

        for col in ["date", "value"]:
            if col not in records.columns:
                logging.error(f"Missing required column '{col}' in records DataFrame.")
                raise ValueError(f"Missing required column '{col}' in records DataFrame.")

        with concurrency("tn-write", occupy=1):
            logging.info(f"Inserting {len(records)} records into stream {stream_id}")
            txHash = self.client.execute_procedure(
                stream_id=stream_id,
                procedure="insert_record",
                args=args,
                wait=False,
                data_provider=data_provider or "",
            )
            logging.debug(f"Inserted {len(records)} records into stream {stream_id}")
            return txHash

    @handle_tn_errors
    def insert_unix_tn_records(
        self,
        stream_id: str,
        records: DataFrame[TnRecordModel],
        data_provider: Optional[str] = None,
    ) -> Optional[str]:
        logging = get_run_logger()

        if len(records) == 0:
            logging.info(f"No records to insert for stream {stream_id}")
            return None

        logging.info(f"Inserting {len(records)} records into stream {stream_id}")
        args = [[record["date"], str(record["value"])] for record in records.to_dict(orient="records")]

        for col in ["date", "value"]:
            if col not in records.columns:
                logging.error(f"Missing required column '{col}' in records DataFrame.")
                raise ValueError(f"Missing required column '{col}' in records DataFrame.")

        with concurrency("tn-write", occupy=1):
            txHash = self.client.execute_procedure(
                stream_id=stream_id,
                procedure="insert_record",
                args=args,
                wait=False,
                data_provider=data_provider or "",
            )
            logging.debug(f"Inserted {len(records)} records into stream {stream_id}")

        return txHash

    @handle_tn_errors
    def batch_insert_records_with_external_created_at(
        self,
        batches: list[dict[str, Any]],
        helper_contract_stream_id: str,
        helper_contract_provider: str,
        wait: bool = False,
    ) -> tn_client.BatchInsertResults:
        """
        created for compatibility with truflation's streams which take an additional
        created_at column

        TODO: move this to data-provider specific code
        """
        if len(batches) == 0:
            raise ValueError("No batches to insert")

        # format yyyy-mm-ddTHH:MM:SSZ
        created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        fallback_data_provider = self.client.get_current_account()

        data_providers: list[str] = []
        stream_ids: list[str] = []
        date_values: list[str] = []
        values: list[str] = []
        external_created_at: list[str] = []

        for batch in batches:
            for record in batch["inputs"]:
                # expected: $data_providers text[], $stream_ids text[], $date_values text[], $values decimal(36,18)[], $external_created_at text[]
                data_providers.append(batch.get("data_provider", fallback_data_provider))
                stream_ids.append(batch["stream_id"])
                date_values.append(record["date"])
                values.append(str(record["value"]))
                external_created_at.append(created_at)

        with concurrency("tn-write", occupy=1):
            tx_hash = self.client.execute_procedure(
                stream_id=helper_contract_stream_id,
                procedure="insert_records_truflation",
                args=[[data_providers, stream_ids, date_values, values, external_created_at]],
                wait=wait,
                data_provider=helper_contract_provider,
            )
        return {"tx_hash": tx_hash}

    @handle_tn_errors
    def batch_insert_tn_records(
        self,
        records: DataFrame[TnDataRowModel],
        is_unix: bool = False,
        has_external_created_at: bool = False,
    ) -> Optional[str]:
        """Batch insert records into multiple streams.

        Args:
            records: DataFrame containing records with stream_id column
            is_unix: If True, insert records with unix timestamps
            has_external_created_at: If True, insert records with an external created_at timestamp

        Returns:
            Transaction hash if successful, None otherwise
        """
        if len(records) == 0:
            return None

        # Convert DataFrame to format expected by client
        batches: list[dict[str, Any]] = []
        stream_locators = records[["data_provider", "stream_id"]].drop_duplicates()
        for _, row in stream_locators.iterrows():
            stream_records = records[
                (records["stream_id"] == row["stream_id"])
                & (records["data_provider"].fillna("") == (row["data_provider"] or ""))
            ]
            if is_unix:
                batch = {
                    "stream_id": row["stream_id"],
                    # TODO: support data provider on sdk insertion
                    # "data_provider": row["data_provider"],
                    "inputs": [
                        {"date": int(record["date"]), "value": float(record["value"])}
                        for record in stream_records.to_dict(orient="records")
                    ],
                }
                # check that all dates are unix timestamps
                if not all(check_unix_timestamp(record["date"]) for record in batch["inputs"]):
                    raise ValueError("All dates must be unix timestamps")
            else:
                batch = {
                    "stream_id": row["stream_id"],
                    # TODO: support data provider on sdk insertion
                    # "data_provider": row["data_provider"],
                    "inputs": [
                        {"date": str(record[1]["date"]), "value": float(record[1]["value"])}
                        for record in stream_records.iterrows()
                    ],
                }
            batches.append(batch)

        if not batches:
            return None

        with concurrency("tn-write", occupy=1):
            if has_external_created_at:
                results = self.batch_insert_records_with_external_created_at(
                    batches=batches,
                    helper_contract_stream_id=self.helper_contract_stream_name,
                    helper_contract_provider=self.helper_contract_provider,
                    wait=False,
                )
            elif is_unix:
                results = self.client.batch_insert_records_unix(
                    batches=batches,
                    helper_contract_stream_id=self.helper_contract_stream_name,
                    helper_contract_data_provider=self.helper_contract_provider,
                    wait=False,
                )
            else:
                results = self.client.batch_insert_records(
                    batches=batches,
                    helper_contract_stream_id=self.helper_contract_stream_name,
                    helper_contract_data_provider=self.helper_contract_provider,
                    wait=False,
                )
            return results["tx_hash"]

    @handle_tn_errors
    def wait_for_tx(self, tx_hash: str) -> None:
        with concurrency("tn-read", occupy=1):
            self.client.wait_for_tx(tx_hash)

    @handle_tn_errors
    def deploy_stream(self, stream_id: str, stream_type: str = truf_sdk.StreamTypePrimitive, wait: bool = True) -> str:
        with concurrency("tn-write", occupy=1):
            return self.client.deploy_stream(stream_id, stream_type, wait)

    @handle_tn_errors
    def init_stream(self, stream_id: str, wait: bool = True) -> str:
        with concurrency("tn-write", occupy=1):
            return self.client.init_stream(stream_id, wait)

    @handle_tn_errors
    def destroy_stream(self, stream_id: str, wait: bool = True) -> str:
        """Destroy a stream with the given stream ID.

        Args:
            stream_id: The ID of the stream to destroy
            wait: If True, wait for the transaction to be confirmed

        Returns:
            The transaction hash
        """
        with concurrency("tn-write", occupy=1):
            return self.client.destroy_stream(stream_id, wait)

    @staticmethod
    def split_records(
        records: DataFrame[TnDataRowModel],
        max_batch_size: int = 25000,
    ) -> list[DataFrame[TnDataRowModel]]:
        return [
            DataFrame[TnDataRowModel](records.iloc[i : i + max_batch_size])
            for i in range(0, len(records), max_batch_size)
        ]

    @handle_tn_errors
    def filter_initialized_streams(
        self, stream_ids: list[str], data_providers: list[str]
    ) -> DataFrame[StreamLocatorModel]:
        """
        Filter a list of streams to only those that are initialized.

        Args:
            stream_ids: List of stream IDs to check
            data_providers: Optional list of data providers corresponding to each stream ID
                If None, the current account will be used for all streams

        Returns:
            List of dictionaries containing initialized streams with 'stream_id' and 'data_provider' keys
        """
        if any(data_provider == "" for data_provider in data_providers):
            raise ValueError("Data provider cannot be empty")

        with concurrency("tn-read", occupy=1):
            records = self.client.filter_initialized_streams(
                stream_ids=stream_ids,
                data_providers=data_providers,
                helper_contract_stream_id=self.helper_contract_stream_name,
                helper_contract_data_provider=self.helper_contract_provider,
            )
        if len(records) == 0:
            return convert_to_typed_df(create_empty_df(["data_provider", "stream_id"]), StreamLocatorModel)
        return convert_to_typed_df(DataFrame[StreamLocatorModel](records), StreamLocatorModel)


# --- Top Level Task Functions ---
@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_read_all_records(block: TNAccessBlock, stream_id: str, data_provider: Optional[str] = None) -> pd.DataFrame:
    return block.read_all_records(stream_id, data_provider)


@overload
def task_read_records(
    block: TNAccessBlock,
    stream_id: str,
    data_provider: Optional[str] = None,
    date_from: Optional[ShortIso8601Date] = None,
    date_to: Optional[ShortIso8601Date] = None,
    *,  # Force is_unix to be keyword-only
    is_unix: Literal[False] = False,
) -> DataFrame[TnRecordModel]: ...


@overload
def task_read_records(
    block: TNAccessBlock,
    stream_id: str,
    data_provider: Optional[str] = None,
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
    *,  # Force is_unix to be keyword-only
    is_unix: Literal[True],
) -> DataFrame[TnRecordModel]: ...

@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_read_records(
        block: TNAccessBlock,
        stream_id: str,
        data_provider: Optional[str] = None,
        date_from: Union[ShortIso8601Date, int, None] = None,
        date_to: Union[ShortIso8601Date, int, None] = None,
        *,  # Force is_unix to be keyword-only
        is_unix: bool = False,
) -> DataFrame[TnRecordModel]:
    """Read records from TSN with support for both ISO dates and Unix timestamps.

    Args:
        block: The TNAccessBlock instance
        stream_id: The stream ID to read from
        data_provider: Optional data provider
        date_from: Start date (ISO string or Unix timestamp)
        date_to: End date (ISO string or Unix timestamp)
        is_unix: If True, treat dates as Unix timestamps

    Returns:
        DataFrame containing the records
    """
    if is_unix:
        return block.read_records(
            stream_id=stream_id,
            data_provider=data_provider,
            date_from=cast(Optional[int], date_from),
            date_to=cast(Optional[int], date_to),
            is_unix=True,
        )
    else:
        return block.read_records(
            stream_id=stream_id,
            data_provider=data_provider,
            date_from=cast(Optional[ShortIso8601Date], date_from),
            date_to=cast(Optional[ShortIso8601Date], date_to),
            is_unix=False,
        )

@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_insert_tn_records(
    block: TNAccessBlock,
    stream_id: str,
    records: DataFrame[TnRecordModel],
    data_provider: Optional[str] = None,
) -> Optional[str]:
    return block.insert_tn_records(stream_id, records, data_provider)


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_insert_unix_tn_records(
    block: TNAccessBlock,
    stream_id: str,
    records: DataFrame[TnRecordModel],
    data_provider: Optional[str] = None,
) -> Optional[str]:
    return block.insert_unix_tn_records(stream_id, records, data_provider)


@task(
    retries=UNUSED_INFINITY_RETRIES,
    retry_delay_seconds=10,
    retry_condition_fn=tn_special_retry_condition(5),
    cache_key_fn=lambda _, args: hash_record_stream_id(args["records"]),
)
def task_filter_initialized_streams(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    max_depth: int = 5,
    max_filter_size: int = 100,
) -> DivideAndConquerResult:
    """
    Filter initialized streams from a list of records.

    Args:
        block: The TN access block to use
        records: The records to filter
        max_depth: The maximum recursion depth for divide and conquer
        max_filter_size: The maximum size for batch filtering

    Returns:
        A DataFrame of records with initialized streams only
    """
    # We'll do the import here to avoid circular imports
    from .stream_filtering import task_filter_streams_divide_conquer

    logger = get_run_logger()
    logger.info(f"Filtering {len(records)} records for initialized streams")

    if records.empty:
        logger.info("No records to filter")
        return {
            "initialized_streams": convert_to_typed_df(
                create_empty_df(["data_provider", "stream_id"]), StreamLocatorModel
            ),
            "uninitialized_streams": convert_to_typed_df(
                create_empty_df(["data_provider", "stream_id"]), StreamLocatorModel
            ),
            "depth": 0,
            "fallback_used": False,
        }

    stream_locators_typed = extract_stream_locators(records)

    # Use the divide and conquer algorithm
    result = task_filter_streams_divide_conquer(
        block=block, stream_locators=stream_locators_typed, max_depth=max_depth, max_filter_size=max_filter_size
    )

    logger.info(
        f"Stream filtering complete: {len(result['initialized_streams'])} initialized, "
        f"{len(result['uninitialized_streams'])} uninitialized, "
        f"max depth {result['depth']}, fallback used: {result['fallback_used']}"
    )

    # Return in the expected format
    return result


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_split_and_insert_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    max_batch_size: int = 25000,
    wait: bool = True,
    is_unix: bool = False,
    fail_on_batch_error: bool = False,
    filter_deployed_streams: bool = True,
    max_filter_size: int = 5000,
    filter_cache_duration: timedelta = timedelta(days=1),
    max_filter_depth: int = 10,
    # this is used only by truflation's data adapter streams
    has_external_created_at: bool = False,
) -> SplitInsertResults:
    """
    Split records into batches and insert them into TN.

    This function filters out streams that are not initialized using a divide-and-conquer approach,
    then splits the remaining records into batches and inserts them.

    Args:
        block: The TNAccessBlock instance
        records: The records to insert
        max_batch_size: Maximum number of records per batch
        wait: Whether to wait for the transaction to be mined
        is_unix: Whether the date column is a unix timestamp
        fail_on_batch_error: Whether to fail if a batch fails
        filter_deployed_streams: Whether to filter out streams that are not deployed
        max_filter_size: Maximum number of streams to process in a single batch during filtering
        filter_cache_duration: How long to cache the filter results
        max_filter_depth: Maximum recursion depth for divide-and-conquer approach
        has_external_created_at: Whether the records have external_created_at

    Returns:
        SplitInsertResults if successful, None if no records to insert
    """
    logger = get_logger_safe(__name__)
    failed_reasons: list[str] = []

    # fill empty data provider with current account
    records["data_provider"] = records["data_provider"].fillna(block.current_account)

    if filter_deployed_streams:
        # Use the divide-and-conquer approach with caching
        filter_with_cache = task_filter_initialized_streams.with_options(cache_expiration=filter_cache_duration)
        filter_results = filter_with_cache(
            block=block, records=records, max_depth=max_filter_depth, max_filter_size=max_filter_size
        )

        if len(filter_results["uninitialized_streams"]) > 0:
            logger.warning(f"Total missing streams: {len(filter_results['uninitialized_streams'])}")

        same_stream_id_mask = records["stream_id"].isin(filter_results["initialized_streams"]["stream_id"])
        same_data_provider_mask = records["data_provider"].isin(filter_results["initialized_streams"]["data_provider"])

        filtered_records = records[same_stream_id_mask & same_data_provider_mask]
        records = DataFrame[TnDataRowModel](filtered_records)

    split_records = block.split_records(records, max_batch_size)
    if len(split_records) == 0:
        logger.warning("No records to insert")
        failed_records_typed = DataFrame[TnDataRowModel](columns=["data_provider", "stream_id", "date", "value"])
        return SplitInsertResults(
            success_tx_hashes=[], failed_records=failed_records_typed, failed_reasons=failed_reasons
        )

    tx_hashes: list[str] = []
    failed_records: list[DataFrame[TnDataRowModel]] = []
    for batch in split_records:
        tx_hash_state = task_batch_insert_tn_records(
            block=block,
            records=batch,
            is_unix=is_unix,
            wait=wait,
            return_state=True,
            has_external_created_at=has_external_created_at,
        )
        try:
            tx_hash = tx_hash_state.result(raise_on_failure=True)
            if tx_hash:
                tx_hashes.append(tx_hash)
        except Exception as e:
            failed_records.append(batch)
            failed_reasons.append(str(e))
    if len(failed_records) > 0:
        failed_records_typed = DataFrame[TnDataRowModel](pd.concat(failed_records))
    else:
        failed_records_typed = DataFrame[TnDataRowModel](columns=["data_provider", "stream_id", "date", "value"])

    return SplitInsertResults(
        success_tx_hashes=tx_hashes, failed_records=failed_records_typed, failed_reasons=failed_reasons
    )


def hash_record_stream_id(records: DataFrame[TnDataRowModel]) -> str:
    """Hash records to check for duplicates"""
    unique_stream_locators = records[["data_provider", "stream_id"]].drop_duplicates()
    unique_stream_locators.sort_values(by=["data_provider", "stream_id"], inplace=True)
    intval = pd.util.hash_pandas_object(unique_stream_locators).sum()
    return str(intval)


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def _task_only_batch_insert_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    is_unix: bool = False,
    has_external_created_at: bool = False,
) -> Optional[str]:
    """Insert records into TSN without waiting for transaction confirmation"""
    return block.batch_insert_tn_records(
        records=records, is_unix=is_unix, has_external_created_at=has_external_created_at
    )


# we don't use retries here because their individual tasks already have retries
@task
def task_batch_insert_tn_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    is_unix: bool = False,
    wait: bool = False,
    has_external_created_at: bool = False,
) -> Optional[str]:
    """Batch insert records into multiple streams

    Args:
        block: The TNAccessBlock instance
        records: DataFrame containing records with stream_id column
        is_unix: If True, insert unix timestamp records
        wait: Whether to wait for transactions to complete
        has_external_created_at: If True, insert with external created_at timestamps

    Returns:
        Transaction hash if successful, None otherwise
    """
    logging = get_run_logger()

    logging.info(f"Batch inserting {len(records)} records across {len(records['stream_id'].unique())} streams")
    # we use task so it may retry on network or nonce errors
    tx_or_none = _task_only_batch_insert_records(
        block=block, records=records, is_unix=is_unix, has_external_created_at=has_external_created_at
    )

    if wait and tx_or_none is not None:
        # we need to use task so it may retry on network errors
        task_wait_for_tx(block=block, tx_hash=tx_or_none)

    return tx_or_none


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=2, retry_condition_fn=tn_special_retry_condition(3))
def task_wait_for_tx(block: TNAccessBlock, tx_hash: str) -> None:
    return block.wait_for_tx(tx_hash)


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_insert_and_wait_for_tx(
    block: TNAccessBlock,
    stream_id: str,
    records: DataFrame[TnRecordModel],
    data_provider: Optional[str] = None,
) -> State[Any] | str:
    """Insert records into TSN and wait for transaction confirmation"""
    logging = get_run_logger()

    logging.info(f"Inserting {len(records)} records into stream {stream_id}")
    insertion = task_insert_tn_records(block=block, stream_id=stream_id, records=records, data_provider=data_provider)

    if insertion is None:
        return Completed(message="No records to insert")

    try:
        task_wait_for_tx(block=block, tx_hash=insertion)
    except Exception as e:
        if "duplicate key value violates unique constraint" in str(e):
            logging.warning(f"Continuing after duplicate key value violation: {e}")
        else:
            raise e
    return insertion


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_insert_unix_and_wait_for_tx(
    block: TNAccessBlock,
    stream_id: str,
    records: DataFrame[TnRecordModel],
    data_provider: Optional[str] = None,
):
    """Insert unix records into TSN and wait for transaction confirmation"""
    logging = get_run_logger()

    logging.info(f"Inserting {len(records)} records into stream {stream_id}")
    insertion = task_insert_unix_tn_records(
        block=block, stream_id=stream_id, records=records, data_provider=data_provider
    )

    if insertion is None:
        return Completed(message="No records to insert")

    try:
        task_wait_for_tx(block=block, tx_hash=insertion)
    except Exception as e:
        if "duplicate key value violates unique constraint" in str(e):
            logging.warning(f"Continuing after duplicate key value violation: {e}")
        else:
            raise e
    return insertion


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_destroy_stream(block: TNAccessBlock, stream_id: str, wait: bool = True) -> str:
    """Task to destroy a stream with the given stream ID.

    Args:
        block: The TNAccessBlock instance
        stream_id: The ID of the stream to destroy
        wait: If True, wait for the transaction to be confirmed

    Returns:
        The transaction hash
    """
    return block.destroy_stream(stream_id, wait)


@task(retries=2, retry_delay_seconds=5, retry_condition_fn=tn_special_retry_condition(1))
def task_filter_batch_initialized_streams(
    block: TNAccessBlock, stream_locators: DataFrame[StreamLocatorModel]
) -> DataFrame[StreamLocatorModel]:
    """
    Filter a batch of streams to find which ones are initialized.

    This task wraps the TNAccessBlock.filter_initialized_streams method and handles retries.

    Args:
        block: The TNAccessBlock instance
        stream_locators: DataFrame with data_provider and stream_id columns

    Returns:
        DataFrame of stream locators with initialized streams
    """
    # Prepare data
    stream_ids: list[str] = stream_locators["stream_id"].tolist()
    data_providers: list[str] = stream_locators["data_provider"].tolist()

    # Call the method with retry handling
    return block.filter_initialized_streams(stream_ids=stream_ids, data_providers=data_providers)


if __name__ == "__main__":
    TNAccessBlock.register_type_and_schema()
