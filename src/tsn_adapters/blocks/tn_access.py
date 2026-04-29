from datetime import datetime, timezone
import decimal
from functools import wraps
from math import ceil
import time
from typing import (
    Any,
    Callable,
    Optional,
    TypedDict,
    TypeVar,
    Union,
    cast,
)
import warnings

import pandas as pd
from pandera.typing import DataFrame
from prefect import Task, task
from prefect.blocks.core import Block
from prefect.client.schemas.objects import State, TaskRun
from prefect.concurrency.sync import concurrency, rate_limit
from pydantic import ConfigDict, SecretStr
import trufnetwork_sdk_c_bindings.exports as truf_sdk
from trufnetwork_sdk_py import BulkInserter, BulkInsertError
import trufnetwork_sdk_py.client as tn_client
from typing_extensions import ParamSpec

from tsn_adapters.common.trufnetwork.models.tn_models import StreamLocatorModel, TnDataRowModel, TnRecord, TnRecordModel
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
        except (MetadataProcedureNotFoundError, StreamAlreadyExistsError):
            # This won't change with retries.
            return False
        except Exception:
            # For non-network errors, use the task_run's run_count to decide.
            if task_run.run_count <= max_other_error_retries:
                return True
            return False
        return False

    return _retry_condition


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


class TNAccessBlock(Block):
    """Prefect Block for managing TSN access credentials.

    This block securely stores and manages TSN access credentials for
    authenticating with TSN API in Prefect flows.
    """

    @staticmethod
    def _value_is_nonzero_str(val: str) -> bool:
        """Return True if the decimal string val quantized to 18 decimals is non-zero."""
        try:
            d = decimal.Decimal(val)
            q = d.quantize(decimal.Decimal("1e-18"), rounding=decimal.ROUND_DOWN)
            return not q.is_zero()
        except decimal.InvalidOperation:
            raise ValueError(f"Could not parse '{val}' in 'value' column as Decimal.")

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
    def logger(self):
        return get_logger_safe(__name__)

    def get_client(self):
        """
        deprecated: Use client property instead
        """
        return self.client

    @handle_tn_errors
    def get_stream_type(self, data_provider: str, stream_id: str) -> str:
        """Check stream type.

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
        self,
        stream_id: str,
        data_provider: Optional[str] = None,
        after_date: Optional[datetime] = None,
    ) -> Optional[datetime]:
        """
        Get the earliest date available for a stream.

        Args:
            stream_id: ID of the stream to query
            data_provider: Optional data provider
            after_date: Optional date to start the search after, inclusively

        Returns:
            The earliest date if found, otherwise None

        Raises:
            StreamNotFoundError: If the stream does not exist
            InvalidRecordFormatError: If the record format is invalid
            InvalidTimestampError: If the timestamp format is invalid
            TNAccessBlock.Error: For other TN-related errors
        """
        try:
            first_record = self.get_first_record(
                stream_id=stream_id, data_provider=data_provider, after_date=after_date
            )
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
    def read_all_records(self, stream_id: str, data_provider: Optional[str] = None) -> DataFrame[TnRecordModel]:
        """Read all records from TSN"""
        return self.read_records(stream_id, data_provider, date_from=date_string_to_unix("1000-01-01"))

    @handle_tn_errors
    def get_first_record(
        self,
        stream_id: str,
        data_provider: Optional[str] = None,
        after_date: Optional[datetime] = None,
    ) -> Optional[TnRecord]:
        with concurrency("tn-read", occupy=1):
            # Convert after_date to Unix timestamp for the client
            after_date_unix = int(after_date.timestamp()) if after_date else None
            result = self.client.get_first_record(stream_id, data_provider, after_date_unix)

        if result is None:
            return None

        return TnRecord(date=int(result["EventTime"]), value=float(result["Value"]))

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
    ) -> Optional[str]:
        warnings.warn(
            "insert_tn_records is deprecated. Use batch_insert_tn_records for better performance and batch processing.",
            DeprecationWarning,
            stacklevel=2
        )
    
        # Filter out records where value is zero
        if 'value' in records.columns and not records.empty:
            original_count = len(records)
            mask = records['value'].apply(self._value_is_nonzero_str)
            records = records[mask]
            filtered_count = len(records)
            if original_count > filtered_count:
                self.logger.info(f"Filtered out {original_count - filtered_count} records with zero values for stream {stream_id}.")

        if len(records) == 0:
            self.logger.info(f"No records to insert for stream {stream_id}.")
            return None

        self.logger.info(f"Inserting {len(records)} records into stream {stream_id}")
        records_dict = records.to_dict(orient="records")

        num_batches = ceil(len(records_dict) / records_per_batch)
        batches = create_record_batches(stream_id, records_dict, num_batches, records_per_batch)

        for col in ["date", "value"]:
            if col not in records.columns:
                self.logger.error(f"Missing required column '{col}' in records DataFrame for stream {stream_id}.")
                raise ValueError(f"Missing required column '{col}' in records DataFrame.")

        with concurrency("tn-write", occupy=1):
            tx_hash = self.client.batch_insert_records(batches)
            self.logger.debug(f"Inserted {len(records)} records into stream {stream_id}")

        return tx_hash

    def _records_to_batches(
        self,
        records: DataFrame[TnDataRowModel],
    ) -> list[tn_client.RecordBatch]:
        """Filter zero-values and group a TnDataRow DataFrame into per-stream RecordBatches."""
        if "value" in records.columns and not records.empty:
            original_count = len(records)
            mask = records["value"].apply(self._value_is_nonzero_str)
            records = records[mask]
            filtered_count = len(records)
            if original_count > filtered_count:
                self.logger.info(
                    f"Filtered out {original_count - filtered_count} records with zero values from the batch."
                )

        if len(records) == 0:
            self.logger.info("No records to insert from the batch.")
            return []

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

            if not all(check_unix_timestamp(int(record["date"])) for record in inputs):
                raise ValueError("All dates must be unix timestamps")

            batches.append(tn_client.RecordBatch(stream_id=row["stream_id"], inputs=inputs))

        return batches

    # Node-level per-tx insert cap (honored by bulk_insert_tn_records which
    # chunks automatically; batch_insert_tn_records rejects over-cap inputs).
    _INSERT_RECORDS_CAP = 10

    @handle_tn_errors
    def batch_insert_tn_records(
        self,
        records: DataFrame[TnDataRowModel],
    ) -> Optional[str]:
        """Batch insert records into multiple streams in a single transaction.

        Enforces the node's per-tx insert cap (10 rows total, across streams).
        For larger datasets, use ``bulk_insert_tn_records`` which chunks and
        pipelines automatically.

        Args:
            records: DataFrame containing records with stream_id column

        Returns:
            Transaction hash if successful, None otherwise

        Raises:
            ValueError: if total rows exceed the protocol cap.
        """
        batches = self._records_to_batches(records)
        if not batches:
            return None

        total_rows = sum(len(b["inputs"]) for b in batches)
        if total_rows > self._INSERT_RECORDS_CAP:
            raise ValueError(
                f"batch_insert_tn_records received {total_rows} rows but the node's "
                f"per-tx cap is {self._INSERT_RECORDS_CAP}. "
                f"Use bulk_insert_tn_records for larger datasets."
            )

        with concurrency("tn-write", occupy=1):
            return self.client.batch_insert_records(
                batches=batches,
                wait=False,
            )

    @handle_tn_errors
    def bulk_insert_tn_records(
        self,
        records: DataFrame[TnDataRowModel],
        batch_size: int = 10,
    ) -> list[str]:
        """Pipelined high-throughput insert via sdk-py BulkInserter, with
        in-process resume on transient backend failures.

        Chunks records into ``batch_size`` per insert_records tx (must be
        <= the node's protocol cap, currently 10) and broadcasts with a
        cached nonce so admission (~50ms) becomes the rate limit instead
        of inclusion (~1-2s/block).

        On chunk failure after BulkInserter exhausts its internal retries,
        this method consults ``BulkInsertError.failed_chunk_index``, slices
        off the records that were already broadcast, pauses briefly, and
        re-attempts the remainder. This avoids re-broadcasting tens of
        thousands of records when a transient catch-up event happens deep
        into a large batch (the 2026-04-25 incident pattern). The Prefect
        task wrapper still retries the whole task on a final failure, but
        each Prefect retry only redoes the work that actually didn't get
        committed.

        Args:
            records: DataFrame with stream_id, data_provider, date, value.
                Row order is the canonical order for resume — a record's
                position in this DataFrame determines its chunk index.
            batch_size: Records per insert_records tx (default 10 = cap).

        Returns:
            List of tx hashes in submission order across all resume attempts.

        Raises:
            ValueError: if batch_size is outside [1, _INSERT_RECORDS_CAP].
            BulkInsertError: if the backend keeps failing after MAX_RESUMES
                in-process resume attempts. The error carries the cumulative
                tx_hashes from every successful broadcast so far.
        """
        if not isinstance(batch_size, int) or not 1 <= batch_size <= self._INSERT_RECORDS_CAP:
            raise ValueError(
                f"batch_size must be an int in [1, {self._INSERT_RECORDS_CAP}]; got {batch_size!r}"
            )

        if records.empty:
            return []

        # Mirror the filter+coerce logic from _records_to_batches (the previous
        # entry path) so the wire payload is identical to the old code's:
        #
        # 1. Drop zero values — these are no-ops on the node side and waste a
        #    tx slot. The check goes through Decimal so "0", "0.0", "0e-18",
        #    etc. all collapse to the same zero.
        # 2. Validate dates are unix timestamps. Catching this here turns an
        #    obscure Go-side reject into a clear ValueError at the boundary.
        # 3. Coerce `value` to float — the gopy NewInsertRecordInputForProvider
        #    binding accepts `float64`. Passing a Decimal or str gets rejected
        #    by gopy with TypeError("must be real number, not str") (see the
        #    2026-04-26 incident on cherubic-orca).
        if "value" in records.columns:
            original_count = len(records)
            mask = records["value"].apply(self._value_is_nonzero_str)
            records = records[mask]
            filtered_count = len(records)
            if original_count > filtered_count:
                self.logger.info(
                    f"Filtered out {original_count - filtered_count} records with zero values from the batch."
                )
        if records.empty:
            self.logger.info("No records to insert from the batch.")
            return []

        # Pack one record per RecordBatch so BulkInserter's flatten produces
        # exactly our DataFrame row order — `flat[failed_chunk_index*batch_size:]`
        # then reliably gives the leftover work. The InsertRecords action
        # accepts mixed-stream chunks (each input carries its own stream_id),
        # so the per-tx cost is identical to stream-grouped batches.
        flat_batches: list[tn_client.RecordBatch] = []
        for row in records.itertuples(index=False):
            date_int = int(row.date)
            if not check_unix_timestamp(date_int):
                raise ValueError(
                    f"date {row.date} for stream {row.stream_id} is not a unix timestamp"
                )
            flat_batches.append({
                "stream_id": str(row.stream_id),
                "inputs": [{"date": date_int, "value": float(row.value)}],
            })

        # Resume budget. 5 in-process attempts x 30s pause covers ~2.5 min of
        # backend recovery time before falling back to the Prefect-task retry,
        # which then has its own 5 retries. Total cushion ≈ 12 min.
        MAX_RESUMES = 5
        RESUME_PAUSE_SECONDS = 30

        cumulative_tx_hashes: list[str] = []
        remaining = flat_batches

        # The tn-write slot is held only across the actual insert attempt;
        # backoff sleep + resume bookkeeping run lock-free so other writers
        # aren't starved while we wait.
        for attempt in range(MAX_RESUMES + 1):
            if not remaining:
                return cumulative_tx_hashes

            inserter = BulkInserter(self.client, batch_size=batch_size)
            try:
                with concurrency("tn-write", occupy=1):
                    tx_hashes = inserter.insert_all(remaining)
                cumulative_tx_hashes.extend(tx_hashes)
                return cumulative_tx_hashes
            except BulkInsertError as exc:
                cumulative_tx_hashes.extend(exc.tx_hashes)

                # A drain failure means every chunk WAS broadcast — only
                # WaitTx polling failed. Re-running here makes no sense
                # (we'd just re-broadcast already-admitted txs). Surface
                # to the Prefect retry / caller with the full hash set.
                if exc.drain_failure:
                    raise BulkInsertError(
                        str(exc),
                        tx_hashes=cumulative_tx_hashes,
                        drain_failure=True,
                        failed_chunk_index=exc.failed_chunk_index,
                    ) from exc

                done = exc.failed_chunk_index * batch_size
                new_remaining = remaining[done:]
                self.logger.warning(
                    "BulkInserter resume #%d: %d records committed this pass, "
                    "%d remaining (failed at chunk %d: %s)",
                    attempt + 1, done, len(new_remaining), exc.failed_chunk_index,
                    str(exc).splitlines()[0],
                )

                # Need real progress AND budget left to keep going.
                if (
                    attempt < MAX_RESUMES
                    and new_remaining
                    and len(new_remaining) < len(remaining)
                ):
                    remaining = new_remaining
                    time.sleep(RESUME_PAUSE_SECONDS)
                    continue

                # Out of attempts (or no progress). Re-raise carrying the
                # cumulative hashes, not just this attempt's, so the outer
                # Prefect retry can dedupe / report correctly.
                raise BulkInsertError(
                    str(exc),
                    tx_hashes=cumulative_tx_hashes,
                    drain_failure=False,
                    failed_chunk_index=exc.failed_chunk_index,
                ) from exc

        # Defensive: loop only exits via return or raise, but mypy/pyright
        # need a terminal statement.
        return cumulative_tx_hashes

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


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=2, retry_condition_fn=tn_special_retry_condition(3))
def task_wait_for_tx(block: TNAccessBlock, tx_hash: str) -> None:
    return block.wait_for_tx(tx_hash)


if __name__ == "__main__":
    TNAccessBlock.register_type_and_schema()
