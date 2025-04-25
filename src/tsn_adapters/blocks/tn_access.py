import decimal
from math import ceil
import pandas as pd
import trufnetwork_sdk_c_bindings.exports as truf_sdk
import trufnetwork_sdk_py.client as tn_client

from datetime import datetime, timezone
from functools import wraps
from typing import (
    Any,
    Callable,
    List,
    Optional,
    TypedDict,
    TypeVar,
    Union,
    cast,
)
from typing_extensions import ParamSpec
from pandera.typing import DataFrame
from prefect import Task, get_run_logger, task
from prefect.blocks.core import Block
from prefect.client.schemas.objects import State, TaskRun
from prefect.concurrency.sync import concurrency, rate_limit
from prefect.states import Completed
from pydantic import ConfigDict, Field, SecretStr

from tsn_adapters.common.trufnetwork.models.tn_models import StreamLocatorModel, TnDataRowModel, TnRecord, TnRecordModel
from tsn_adapters.utils.logging import get_logger_safe
from tsn_adapters.utils.time_utils import date_string_to_unix
from tsn_adapters.utils.unix import check_unix_timestamp
from tsn_adapters.utils.tn_record import create_record_batches

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
        if (
            isinstance(error, RuntimeError) and ("dataset exists" in msg or "already exists" in msg)
        ):
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
        if (
            isinstance(error, RuntimeError) and ("already initialized" in msg)
        ):
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
    def get_earliest_date(
        self, stream_id: str, data_provider: Optional[str] = None
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
    def get_first_record(
        self, stream_id: str, data_provider: Optional[str] = None
    ) -> Optional[TnRecord]:
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
            return DataFrame[TnRecordModel](df)

        except Exception as e:
            msg = str(e).lower()
            # If no records exist, return an empty typed DataFrame instead of erroring
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

    @handle_tn_errors
    def insert_tn_records(
        self,
        stream_id: str,
        records: DataFrame[TnRecordModel],
        records_per_batch: int = 300
    ) -> Optional[List[str]]:
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
    ) -> Optional[List[str]]:
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
    
            batches.append(tn_client.RecordBatch(
                stream_id=row["stream_id"],
                inputs=inputs
            ))

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
) -> Optional[List[str]]:
    return block.insert_tn_records(stream_id, records)

@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_split_and_insert_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    max_batch_size: int = 25000,
    wait: bool = True,
) -> SplitInsertResults:
    """
    Split records into batches and insert them into TN.

    Args:
        block: The TNAccessBlock instance
        records: The records to insert
        max_batch_size: Maximum number of records per batch
        wait: Whether to wait for the transaction to be mined

    Returns:
        SplitInsertResults if successful, None if no records to insert
    """
    logger = get_logger_safe(__name__)
    failed_reasons: list[str] = []

    # fill empty data provider with current account
    records["data_provider"] = records["data_provider"].fillna(block.current_account)
    split_records = block.split_records(records, max_batch_size)
    if len(split_records) == 0:
        logger.warning("No records to insert")
        failed_records_typed = DataFrame[TnDataRowModel](columns=["data_provider", "stream_id", "date", "value"])
        return SplitInsertResults(
            success_tx_hashes=[], failed_records=failed_records_typed, failed_reasons=failed_reasons
        )

    tx_hashes: list[str] | None = []
    failed_records: list[DataFrame[TnDataRowModel]] = []
    for batch in split_records:
        try:
            tx_hashes = task_batch_insert_tn_records(
                block=block,
                records=batch,
                wait=wait,
            )
        except Exception as e:
            failed_records.append(batch)
            failed_reasons.append(str(e))

    if len(failed_records) > 0:
        failed_records_typed = DataFrame[TnDataRowModel](pd.concat(failed_records))
    else:
        failed_records_typed = DataFrame[TnDataRowModel](columns=["data_provider", "stream_id", "date", "value"])

    return SplitInsertResults(
        success_tx_hashes=(tx_hashes or []), failed_records=failed_records_typed, failed_reasons=failed_reasons
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
) -> Optional[List[str]]:
    """Insert records into TSN without waiting for transaction confirmation"""
    return block.batch_insert_tn_records(records=records)


# we don't use retries here because their individual tasks already have retries
@task
def task_batch_insert_tn_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    wait: bool = False,
) -> Optional[List[str]]:
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

    logging.info("Inserting records bbbbb")
    logging.info(f"Batch inserting {len(records)} records across {len(records['stream_id'].unique())} streams")

    logging.info(f"Columns: {list(records.columns)}")
    # logging.info(f"Records to insert: {records}")

    # for idx, row in records.iterrows():
    #     vals = ", ".join(f"{col}={row[col]!r}" for col in records.columns)
        # logging.info(f"Row {idx + 1}: {vals}")
        # and if you want a full table print:
        # logging.info("\n" + records.to_string(index=True))

    logging.info("Batch insert complete cccc")

    # we use task so it may retry on network or nonce errors
    tx_hashes = _task_only_batch_insert_records(
        block=block, records=records
    )

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
    insertion = task_insert_tn_records(
        block=block, stream_id=stream_id, records=records, data_provider=data_provider
    )

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


if __name__ == "__main__":
    TNAccessBlock.register_type_and_schema()
