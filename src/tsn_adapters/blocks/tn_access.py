from datetime import datetime, timedelta, timezone
import decimal
from functools import wraps

# Import hashing and json libs, Prefect context
import hashlib
import json
from math import ceil
from typing import (
    Any,
    Callable,
    Optional,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

import pandas as pd
from pandera.typing import DataFrame
from prefect import Task, get_run_logger, task
from prefect.blocks.core import Block
from prefect.client.schemas.objects import State, TaskRun
from prefect.concurrency.sync import concurrency, rate_limit
from prefect.context import TaskRunContext
from prefect.states import Completed
from pydantic import ConfigDict, Field, SecretStr
import trufnetwork_sdk_c_bindings.exports as truf_sdk
import trufnetwork_sdk_py.client as tn_client
from typing_extensions import ParamSpec

from tsn_adapters.common.trufnetwork.models.tn_models import StreamLocatorModel, TnDataRowModel, TnRecord, TnRecordModel
from tsn_adapters.common.trufnetwork.tn import task_batch_filter_streams_by_existence
from tsn_adapters.utils.logging import get_logger_safe
from tsn_adapters.utils.time_utils import date_string_to_unix
from tsn_adapters.utils.tn_record import create_record_batches
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
            if StreamAlreadyExistsError.is_stream_already_exists_error(e):
                raise StreamAlreadyExistsError.from_error(e)
            if StreamAlreadyInitializedError.is_stream_already_initialized_error(e):
                raise StreamAlreadyInitializedError.from_error(e)
            raise

    return cast(F, wrapper)


P = ParamSpec("P")
R = TypeVar("R")


def tn_special_retry_condition(max_other_error_retries: int) -> Any:
    """
    Custom retry condition for Prefect tasks.

    If the raised exception is a TNNodeNetworkError or TNDbTimeoutError, always retry (indefinitely).
    Otherwise, only retry if the current attempt (based on task_run's run_count) is less than max_retries.

    Args:
        max_retries: Maximum allowed retries for non-network errors.

    Returns:
        A callable that can be used as retry_condition_fn in Prefect tasks.
    """

    def _retry_condition(task: Task[P, R], task_run: TaskRun, state: State[R]) -> bool:
        try:
            # This will re-raise the exception if the task failed.
            state.result()
        except (TNNodeNetworkError, TNDbTimeoutError):
            # Always retry on network and DB timeout errors.
            return True
        except (MetadataProcedureNotFoundError, StreamAlreadyExistsError, StreamAlreadyInitializedError):
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


class StreamAlreadyExistsError(Exception):
    """Custom exception raised when attempting to deploy a stream that already exists."""

    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        super().__init__(f"Stream '{stream_id}' already exists.")

    @classmethod
    def from_error(cls, error: Exception) -> "StreamAlreadyExistsError":
        """
        Convert a generic exception to StreamAlreadyExistsError if the error message matches.
        """
        if isinstance(error, StreamAlreadyExistsError):
            return error
        msg = str(error).lower()
        import re

        # Match "transaction failed: dataset exists: <stream_id>"
        match = re.search(r"dataset exists: ([a-z0-9]+)", msg)
        if match:
            stream_id = match.group(1)
            return cls(stream_id)
        if isinstance(error, RuntimeError) and ("dataset exists" in msg or "already exists" in msg):
            # Try to extract stream_id if possible, else use a placeholder
            import re

            match = re.search(r"stream '([^']+)' already exists", str(error))
            stream_id = match.group(1) if match else "unknown"
            return cls(stream_id)
        raise error

    @classmethod
    def is_stream_already_exists_error(cls, error: Exception) -> bool:
        """
        Check if the given exception is a StreamAlreadyExistsError or matches its error pattern.
        """
        try:
            cls.from_error(error)
            return True
        except Exception:
            return False


class StreamAlreadyInitializedError(Exception):
    """Custom exception raised when attempting to initialize a stream that is already initialized."""

    def __init__(self, stream_id: str):
        self.stream_id = stream_id
        super().__init__(f"Stream '{stream_id}' is already initialized.")

    @classmethod
    def from_error(cls, error: Exception) -> "StreamAlreadyInitializedError":
        """
        Convert a generic exception to StreamAlreadyInitializedError if the error message matches.
        """
        if isinstance(error, StreamAlreadyInitializedError):
            return error
        msg = str(error).lower()
        if isinstance(error, RuntimeError) and ("already initialized" in msg):
            import re

            match = re.search(r"stream '([^']+)' is already initialized", str(error))
            stream_id = match.group(1) if match else "unknown"
            return cls(stream_id)
        raise error

    @classmethod
    def is_stream_already_initialized_error(cls, error: Exception) -> bool:
        """
        Check if the given exception is a StreamAlreadyInitializedError or matches its error pattern.
        """
        try:
            cls.from_error(error)
            return True
        except Exception:
            return False


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
    def get_earliest_date(self, stream_id: str, data_provider: Optional[str] = None) -> Optional[datetime]:
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
            first_record = self.get_first_record(stream_id=stream_id, data_provider=data_provider)
            if first_record is None:
                return None

            date_value = first_record.date

            try:
                timestamp = int(date_value)
                return datetime.fromtimestamp(timestamp, tz=timezone.utc)
            except (ValueError, TypeError) as e:
                raise self.InvalidTimestampError(f"Invalid timestamp format for {stream_id}: {e}") from e

        except Exception as e:
            error_msg = str(e).lower()
            if "stream not found" in error_msg:
                raise self.StreamNotFoundError(f"Stream {stream_id} not found")

            raise self.Error(f"Failed to query TN for {stream_id}: {e}") from e

    @handle_tn_errors
    def read_all_records(self, stream_id: str, data_provider: Optional[str] = None) -> pd.DataFrame:
        """Read all records from TSN"""
        return self.read_records(stream_id, data_provider, date_from=date_string_to_unix("1000-01-01"))

    @handle_tn_errors
    def get_first_record(self, stream_id: str, data_provider: Optional[str] = None) -> Optional[TnRecord]:
        with concurrency("tn-read", occupy=1):
            result = self.client.get_first_record(stream_id, data_provider)

        if result is None:
            return None

        return TnRecord(date=int(result["date"]), value=float(result["value"]))

    @handle_tn_errors
    def read_records(
        self,
        stream_id: str,
        data_provider: Optional[str] = None,
        date_from: Union[int, None] = None,
        date_to: Union[int, None] = None,
    ) -> DataFrame[TnRecordModel]:
        try:
            with concurrency("tn-read", occupy=1):
                recs = self.client.get_records(
                    stream_id,
                    data_provider,
                    date_from,
                    date_to,
                )

            recs_list = [
                {
                    "date": rec["EventTime"],
                    "value": decimal.Decimal(rec["Value"]),
                    **{k: v for k, v in rec.items() if k not in ("EventTime", "Value")},
                }
                for rec in recs
            ]
            df = pd.DataFrame(recs_list, columns=["date", "value"])

        except Exception as e:
            msg = str(e).lower()
            # If no records exist, return an empty typed DataFrame instead of erroring
            self.logger.info(f"msg is {msg}")
            if "record not found" in msg:
                self.logger.warning(
                    f"No records found for stream '{stream_id}' "
                    f"(provider={data_provider}, from={date_from}, to={date_to}); "
                    "returning empty DataFrame."
                )

                # utility from above: create an empty DataFrame with the right schema
                return create_typed_empty_df(TnRecordModel, ["date", "value"])

            # All other errors are real failures
            self.logger.error(
                f"Error reading records from TN, stream_id: {stream_id}, "
                f"data_provider: {data_provider}, date_from: {date_from}, "
                f"date_to: {date_to}: {e}"
            )
            raise e

        return DataFrame[TnRecordModel](df)

    @handle_tn_errors
    def insert_tn_records(
        self, stream_id: str, records: DataFrame[TnRecordModel], records_per_batch: int = 300
    ) -> Optional[list[str]]:
        logging = get_run_logger()

        if len(records) == 0:
            logging.info(f"No records to insert for stream {stream_id}")
            return None

        logging.info(f"Inserting {len(records)} records into stream {stream_id}")
        records_dict = records.to_dict(orient="records")

        num_batches = ceil(len(records_dict) / records_per_batch)
        batches = create_record_batches(stream_id, records_dict, num_batches, records_per_batch)

        for col in ["date", "value"]:
            if col not in records.columns:
                logging.error(f"Missing required column '{col}' in records DataFrame.")
                raise ValueError(f"Missing required column '{col}' in records DataFrame.")

        with concurrency("tn-write", occupy=1):
            tx_hashes = self.client.batch_insert_records(batches)
            logging.debug(f"Inserted {len(records)} records into stream {stream_id}")

        return tx_hashes

    @handle_tn_errors
    def batch_insert_tn_records(
        self,
        records: DataFrame[TnDataRowModel],
    ) -> Optional[list[str]]:
        """Batch insert records into multiple streams.

        Args:
            records: DataFrame containing records with stream_id column

        Returns:
            Transaction hash if successful, None otherwise
        """
        if len(records) == 0:
            return None

        # Convert DataFrame to format expected by client
        batches: list[tn_client.RecordBatch] = []
        stream_locators = records[["data_provider", "stream_id"]].drop_duplicates()
        for _, row in stream_locators.iterrows():
            stream_records = records[
                (records["stream_id"] == row["stream_id"])
                & (records["data_provider"].fillna("") == (row["data_provider"] or ""))
            ]

            inputs = [
                tn_client.Record(date=int(record["date"]), value=float(record["value"]))
                for record in stream_records.to_dict(orient="records")
            ]

            # check that all dates are unix timestamps
            if not all(check_unix_timestamp(int(record["date"])) for record in inputs):
                raise ValueError("All dates must be unix timestamps")

            batches.append(tn_client.RecordBatch(stream_id=row["stream_id"], inputs=inputs))

        if not batches:
            return None

        with concurrency("tn-write", occupy=1):
            tx_hashes = self.client.batch_insert_records(
                batches=batches,
                wait=False,
            )

            return tx_hashes

    @handle_tn_errors
    def wait_for_tx(self, tx_hash: str) -> None:
        with concurrency("tn-read", occupy=1):
            self.client.wait_for_tx(tx_hash)

    @handle_tn_errors
    def deploy_stream(self, stream_id: str, stream_type: str = truf_sdk.StreamTypePrimitive, wait: bool = True) -> str:
        with concurrency("tn-write", occupy=1):
            return self.client.deploy_stream(stream_id, stream_type, wait)

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
    def batch_deploy_streams(self, definitions: list[tn_client.StreamDefinitionInput], wait: bool = True) -> str:
        """
        Deploy multiple streams using the batch SDK function.

        Args:
            definitions: A list of stream definitions, where each definition is a
                         `tn_client.StreamDefinitionInput` typed dict.
            wait: If True, wait for the transaction to be confirmed.

        Returns:
            The transaction hash of the batch deployment.
        """
        self.logger.debug(f"Batch deploying {len(definitions)} streams (wait: {wait}).")
        # Assuming a concurrency limit name, replace 'tn_write_operations' if a different one is used
        # or if no specific limit is needed here beyond what the SDK/network handles.
        # For consistency with batch_insert_tn_records, let's assume a general write limit.
        with concurrency(
            "tn-write", timeout_seconds=300
        ):  # timeout needs to be set based on typical network conditions for batch deployment
            tx_hash = self.client.batch_deploy_streams(definitions=definitions, wait=wait)
        self.logger.info(
            f"Batch deployment transaction submitted for {len(definitions)} streams: {tx_hash} (wait: {wait})"
        )
        return tx_hash

    @handle_tn_errors
    def batch_filter_streams_by_existence(
        self, locators: list[tn_client.StreamLocatorInput], return_existing: bool
    ) -> list[tn_client.StreamLocatorInput]:
        """
        Filter a list of streams based on their existence using the batch SDK function.

        Args:
            locators: A list of stream locators, where each locator is a
                      `tn_client.StreamLocatorInput` typed dict.
            return_existing: If True, returns streams that exist.
                             If False, returns streams that do not exist.

        Returns:
            A list of `tn_client.StreamLocatorInput` for streams that match the filter criteria.
        """
        self.logger.debug(f"Batch filtering {len(locators)} streams by existence (return_existing: {return_existing}).")
        # Assuming read operations might have a different or no specific concurrency limit
        # If TN operations are generally limited, apply a relevant concurrency scope.
        # For now, let's assume this is a lighter operation not needing the same write lock.
        # If issues arise, a 'tn-read' concurrency scope could be added.
        filtered_locators = self.client.batch_filter_streams_by_existence(
            locators=locators, return_existing=return_existing
        )
        self.logger.info(f"Batch filter by existence complete. Found {len(filtered_locators)} matching streams.")
        return filtered_locators


# --- Top Level Task Functions ---
@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_read_all_records(block: TNAccessBlock, stream_id: str, data_provider: Optional[str] = None) -> pd.DataFrame:
    return block.read_all_records(stream_id, data_provider)


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_read_records(
    block: TNAccessBlock,
    stream_id: str,
    data_provider: Optional[str] = None,
    date_from: Union[int, None] = None,
    date_to: Union[int, None] = None,
) -> DataFrame[TnRecordModel]:
    """Read records from TSN with support for both ISO dates and Unix timestamps.

    Args:
        block: The TNAccessBlock instance
        stream_id: The stream ID to read from
        data_provider: Optional data provider
        date_from: Start date (Unix timestamp)
        date_to: End date (Unix timestamp)

    Returns:
        DataFrame containing the records
    """
    return block.read_records(
        stream_id=stream_id,
        data_provider=data_provider,
        date_from=date_from,
        date_to=date_to,
    )


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_insert_tn_records(
    block: TNAccessBlock,
    stream_id: str,
    records: DataFrame[TnRecordModel],
) -> Optional[list[str]]:
    return block.insert_tn_records(stream_id, records)


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_split_and_insert_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    max_batch_size: int = 25000,
    wait: bool = True,
    filter_deployed_streams: bool = True,
    filter_cache_duration: timedelta = timedelta(days=1),
    max_streams_per_existence_check: int = 1000,
) -> SplitInsertResults:
    """
    Splits records into batches and inserts them into TN, optionally filtering by stream existence first.

    Orchestrates filtering and batch insertion logic.

    Args:
        block: The TNAccessBlock instance.
        records: The records to insert.
        max_batch_size: Maximum number of records per insert batch.
        wait: Whether to wait for the insertion transaction(s) to be mined.
        filter_deployed_streams: Whether to filter out streams that do not exist on TN.
        filter_cache_duration: How long to cache the stream existence filter results.
        max_streams_per_existence_check: Max streams to check in one existence API call.

    Returns:
        SplitInsertResults containing transaction hashes and any failed records/reasons.
    """
    logger = get_logger_safe(__name__)
    processed_records = records.copy()

    # 1. Fill default data provider
    processed_records["data_provider"] = processed_records["data_provider"].fillna(block.current_account)

    # 2. Optionally Filter Records
    if filter_deployed_streams and not processed_records.empty:
        try:
            processed_records = _filter_records_by_stream_existence(
                block=block,
                records=processed_records,
                max_streams_per_existence_check=max_streams_per_existence_check,
                filter_cache_duration=filter_cache_duration,
            )
        except Exception as e:
            # Error during filtering is critical, re-raise to fail the task
            logger.error(f"Halting task due to error during stream existence filtering: {e!s}", exc_info=True)
            raise

    # 3. Perform Batch Insertions
    if processed_records.empty:
        logger.warning("No records remaining to insert after filtering.")
        # Return empty success result
        empty_df = DataFrame[TnDataRowModel](columns=["data_provider", "stream_id", "date", "value"])
        return SplitInsertResults(success_tx_hashes=[], failed_records=empty_df, failed_reasons=[])

    # Call the helper function for insertion
    # Pass through necessary params if they were kept (e.g., wait)
    insertion_results = _perform_batch_insertions(
        block=block,
        records_to_insert=processed_records,
        max_batch_size=max_batch_size,
        wait=wait,
    )

    return insertion_results


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
) -> Optional[list[str]]:
    """Insert records into TSN without waiting for transaction confirmation"""
    return block.batch_insert_tn_records(records=records)


# we don't use retries here because their individual tasks already have retries
@task
def task_batch_insert_tn_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    wait: bool = False,
) -> Optional[list[str]]:
    """Batch insert records into multiple streams

    Args:
        block: The TNAccessBlock instance
        records: DataFrame containing records with stream_id column
        wait: Whether to wait for transactions to complete
        has_external_created_at: If True, insert with external created_at timestamps

    Returns:
        Transaction hash if successful, None otherwise
    """
    logging = get_run_logger()

    logging.info(f"Batch inserting {len(records)} records across {len(records['stream_id'].unique())} streams")

    # we use task so it may retry on network or nonce errors
    tx_hashes = _task_only_batch_insert_records(block=block, records=records)

    if wait and tx_hashes is not None:
        # we need to use task so it may retry on network errors
        for tx_hash in tx_hashes:
            task_wait_for_tx(block=block, tx_hash=tx_hash)

    return tx_hashes


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=2, retry_condition_fn=tn_special_retry_condition(3))
def task_wait_for_tx(block: TNAccessBlock, tx_hash: str) -> None:
    return block.wait_for_tx(tx_hash)


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_insert_and_wait_for_tx(
    block: TNAccessBlock,
    stream_id: str,
    records: DataFrame[TnRecordModel],
    data_provider: Optional[str] = None,
):
    """Insert records into TSN and wait for transaction confirmation"""
    logging = get_run_logger()

    logging.info(f"Inserting {len(records)} records into stream {stream_id}")
    insertion = task_insert_tn_records(block=block, stream_id=stream_id, records=records, data_provider=data_provider)

    if insertion.result() is None:
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


# --- Custom Cache Key Function ---


def filter_existence_cache_key(
    context: TaskRunContext,  # Prefect provides context
    parameters: dict[str, Any],  # Prefect provides parameters
) -> str:
    """
    Generates a cache key for task_batch_filter_streams_by_existence
    based on a stable representation of locators and the return_existing flag.
    Raises KeyError if required parameters are missing.
    """
    try:
        locators = parameters["locators"]  # Direct access, will raise KeyError if missing
        return_existing = parameters["return_existing"]  # Direct access
    except KeyError as e:
        # Fail fast with an informative error
        raise KeyError(
            f"Cache key generation failed: Missing expected parameter '{e.args[0]}' for task referenced by "
            f"TaskRunContext '{context.task_run.name if context and context.task_run else 'Unknown'}'"
        ) from e

    # Proceed with sorting and hashing only if parameters are present
    try:
        # Ensure keys exist and handle potential None values robustly
        stable_locators = sorted(
            [(str(loc.get("stream_id", "")), str(loc.get("data_provider", ""))) for loc in locators]
        )
    except Exception as e:
        # Log error if locators aren't as expected, fall back to less specific key
        # get_run_logger() might not be available outside task/flow context, use basic print/log
        print(f"WARNING: Could not generate stable locator list for caching: {e}. Using basic key.")
        stable_locators = []  # Avoid hashing potentially unstable input on error

    # Combine with return_existing flag
    key_data = {
        "locators_repr": stable_locators,  # Use a different key name from param
        "return_existing": return_existing,
    }

    # Use json dumps with sort_keys for a stable string representation, then hash
    # Using sha256 for robustness against potential hash collisions with large inputs
    hasher = hashlib.sha256()
    hasher.update(json.dumps(key_data, sort_keys=True).encode("utf-8"))
    return hasher.hexdigest()


# --- Helper Function for Filtering ---


def _filter_records_by_stream_existence(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    max_streams_per_existence_check: int,
    filter_cache_duration: timedelta,
) -> DataFrame[TnDataRowModel]:
    """Filters records based on stream existence on TN.

    Args:
        block: TNAccessBlock instance.
        records: Input DataFrame with potential records.
        max_streams_per_existence_check: Max streams per filter API call.
        filter_cache_duration: Cache duration for the existence check.

    Returns:
        Filtered DataFrame containing only records for existing streams.

    Raises:
        Exception: If the underlying existence check task fails.
    """
    logger = get_logger_safe(__name__)
    logger.info(
        f"Filtering {len(records)} records by stream existence (batch size: {max_streams_per_existence_check})..."
    )
    unique_locators_df = extract_stream_locators(records)

    locators_to_check: list[tn_client.StreamLocatorInput] = [
        tn_client.StreamLocatorInput(stream_id=str(row["stream_id"]), data_provider=str(row["data_provider"]))
        for _, row in unique_locators_df.iterrows()
    ]

    if not locators_to_check:
        logger.info("No unique stream locators found to filter.")
        return records  # Return original if no locators to check

    total_non_existent_set = set()
    num_filter_batches = ceil(len(locators_to_check) / max_streams_per_existence_check)
    logger.info(f"Checking existence for {len(locators_to_check)} unique locators in {num_filter_batches} batches (asking for non-existent).")

    for i in range(num_filter_batches):
        start_idx = i * max_streams_per_existence_check
        end_idx = start_idx + max_streams_per_existence_check
        current_filter_batch = locators_to_check[start_idx:end_idx]
        batch_num_log = i + 1

        logger.debug(
            f"Checking existence filter batch {batch_num_log}/{num_filter_batches} ({len(current_filter_batch)} locators, asking for non-existent)..."
        )

        try:
            # Apply custom cache key function
            existence_check_task = task_batch_filter_streams_by_existence.with_options(
                cache_key_fn=filter_existence_cache_key, cache_expiration=filter_cache_duration
            )
            # Request non-existent streams
            non_existent_locators_in_batch = existence_check_task.submit(
                block=block, locators=current_filter_batch, return_existing=False
            ).result()

            batch_non_existent_set = {
                (str(loc["data_provider"]), str(loc["stream_id"])) for loc in non_existent_locators_in_batch
            }
            total_non_existent_set.update(batch_non_existent_set)
            logger.debug(
                f"Existence filter batch {batch_num_log}: Found {len(batch_non_existent_set)} non-existent streams."
            )

        except Exception as e:
            logger.error(
                f"Error during stream existence filtering for batch {batch_num_log}: {e!s}. Halting filter process.",
                exc_info=True,
            )
            raise  # Re-raise to fail the calling task

    # Filter the original records using the aggregated set of non-existent streams
    original_count = len(records)
    if total_non_existent_set:
        # Create a MultiIndex of non-existent streams to check against
        non_existent_tuples = list(total_non_existent_set)
        non_existent_index = pd.MultiIndex.from_tuples(non_existent_tuples, names=["data_provider", "stream_id"])
        
        # Create a MultiIndex from the records DataFrame for efficient filtering
        records_index = pd.MultiIndex.from_frame(records[["data_provider", "stream_id"]])
        
        # Keep records that are NOT in the non_existent_index
        filtered_records = records[~records_index.isin(non_existent_index)]
    else:
        # If no streams were reported as non-existent, all are considered existent
        filtered_records = records.copy()


    filtered_out_count = original_count - len(filtered_records)
    logger.info(
        f"Finished existence check. Filtered out {filtered_out_count} records belonging to non-existent streams."
    )
    return DataFrame[TnDataRowModel](filtered_records)  # Ensure Pandera type is returned


# --- Helper Function for Batch Insertion ---


def _perform_batch_insertions(
    block: TNAccessBlock,
    records_to_insert: DataFrame[TnDataRowModel],
    max_batch_size: int,
    wait: bool,
) -> SplitInsertResults:
    """Splits records and performs batch insertions, collecting results.

    Args:
        block: TNAccessBlock instance.
        records_to_insert: DataFrame of records ready for insertion.
        max_batch_size: Max records per insertion API call.
        wait: Whether to wait for insertion transactions.

    Returns:
        SplitInsertResults dictionary.
    """
    logger = get_logger_safe(__name__)
    failed_reasons: list[str] = []
    success_tx_hashes: list[str] = []
    failed_records_list: list[DataFrame[TnDataRowModel]] = []

    split_records_batches = block.split_records(records_to_insert, max_batch_size)

    if not split_records_batches:
        logger.warning("No record batches to insert.")
        # Fall through to return empty results
    else:
        logger.info(
            f"Submitting {len(records_to_insert)} records for insertion in {len(split_records_batches)} batches."
        )
        for i, batch in enumerate(split_records_batches):
            batch_num_log = i + 1
            logger.debug(
                f"Submitting insertion batch {batch_num_log}/{len(split_records_batches)} ({len(batch)} records)..."
            )
            try:
                # Assuming task_batch_insert_tn_records handles the actual API call
                # It might need `is_unix` and `has_external_created_at` if those params are still relevant
                # Passing them through from the main task signature if needed.
                tx_hashes_or_none = task_batch_insert_tn_records(
                    block=block,
                    records=batch,
                    wait=wait,
                    # Add is_unix and has_external_created_at here if they were kept in task_split_and_insert_records signature
                    # is_unix=is_unix,
                    # has_external_created_at=has_external_created_at
                )
                if tx_hashes_or_none:
                    success_tx_hashes.extend(tx_hashes_or_none)
                logger.debug(f"Insertion batch {batch_num_log} submitted. TXs: {tx_hashes_or_none}")
            except Exception as e:
                logger.error(f"Insertion batch {batch_num_log} failed: {e!s}", exc_info=True)
                failed_records_list.append(batch)
                failed_reasons.append(str(e))
                # Decide if fail_on_batch_error logic is needed here or handled by Prefect retries on the task

    # Combine failed records
    if failed_records_list:
        failed_records_df = DataFrame[TnDataRowModel](pd.concat(failed_records_list))
    else:
        failed_records_df = DataFrame[TnDataRowModel](columns=["data_provider", "stream_id", "date", "value"])

    return SplitInsertResults(
        success_tx_hashes=success_tx_hashes,
        failed_records=failed_records_df,
        failed_reasons=failed_reasons,
    )


if __name__ == "__main__":
    TNAccessBlock.register_type_and_schema()
