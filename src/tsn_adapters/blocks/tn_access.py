from datetime import datetime, timezone
import decimal
from typing import Any, Literal, Optional, TypedDict, Union, cast, overload

import pandas as pd
from pandera.typing import DataFrame
from prefect import Task, get_run_logger, task
from prefect.blocks.core import Block
from prefect.concurrency.sync import concurrency
from prefect.states import Completed
from pydantic import ConfigDict, Field, SecretStr
import trufnetwork_sdk_py.client as tn_client
from trufnetwork_sdk_py.utils import generate_stream_id

from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel, TnRecord, TnRecordModel
from tsn_adapters.utils.date_type import ShortIso8601Date
from tsn_adapters.utils.logging import get_logger_safe


class SplitInsertResults(TypedDict):
    """Results of batch insertions."""

    success_tx_hashes: list[str]
    failed_records: DataFrame[TnDataRowModel]


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
    def client(self):
        if not hasattr(self, "_client"):
            self._client = tn_client.TNClient(
                url=self.tn_provider,
                token=self.tn_private_key.get_secret_value(),
            )
        return self._client

    @property
    def helper_contract_stream_id(self):
        if not self.helper_contract_name:
            raise self.Error("Helper contract name is not set")
        return generate_stream_id(self.helper_contract_name)

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
            if date_value is None:
                raise self.InvalidRecordFormatError(f"Invalid record format for {stream_id}: missing DateValue")

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

    def read_all_records(self, stream_id: str, data_provider: Optional[str] = None) -> pd.DataFrame:
        """Read all records from TSN"""
        return self.read_records(stream_id, data_provider, date_from="1000-01-01")

    def call_procedure(
        self, stream_id: str, data_provider: Optional[str] = None, procedure: str = "", args: Optional[list[Any]] = None
    ) -> list[dict[str, Any]]:
        return self.get_client().call_procedure(stream_id, data_provider, procedure, args)

    def get_first_record(
        self, stream_id: str, data_provider: Optional[str] = None, is_unix: bool = False
    ) -> Optional[TnRecord]:
        if is_unix:
            result = self.get_client().get_first_record_unix(stream_id, data_provider)
        else:
            result = self.get_client().get_first_record(stream_id, data_provider)

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
                    # Cast to int for unix timestamps
                    unix_from = int(cast(int, date_from)) if date_from is not None else None
                    unix_to = int(cast(int, date_to)) if date_to is not None else None
                    recs = self.get_client().get_records_unix(
                        stream_id,
                        data_provider,
                        unix_from,
                        unix_to,
                    )
                else:
                    # Cast to str for ISO format
                    iso_from = str(cast(ShortIso8601Date, date_from)) if date_from is not None else None
                    iso_to = str(cast(ShortIso8601Date, date_to)) if date_to is not None else None
                    recs = self.get_client().get_records(
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
            args: list[list[str | float | int]] = [
                [record["date"], str(record["value"]), current_date] for record in records.to_dict(orient="records")
            ]
        else:
            args: list[list[str | float | int]] = [
                [record["date"], str(record["value"])] for record in records.to_dict(orient="records")
            ]

        for col in ["date", "value"]:
            if col not in records.columns:
                logging.error(f"Missing required column '{col}' in records DataFrame.")
                raise ValueError(f"Missing required column '{col}' in records DataFrame.")

        with concurrency("tn-write", occupy=1):
            logging.info(f"Inserting {len(records)} records into stream {stream_id}")
            txHash = self.get_client().execute_procedure(
                stream_id=stream_id,
                procedure="insert_record",
                args=args,
                wait=False,
                data_provider=data_provider or "",
            )
            logging.debug(f"Inserted {len(records)} records into stream {stream_id}")
            return txHash

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
            txHash = self.get_client().execute_procedure(
                stream_id=stream_id,
                procedure="insert_record",
                args=args,
                wait=False,
                data_provider=data_provider or "",
            )
            logging.debug(f"Inserted {len(records)} records into stream {stream_id}")

        return txHash

    def batch_insert_unix_tn_records(
        self,
        records: DataFrame[TnDataRowModel],
        data_provider: Optional[str] = None,
    ) -> Optional[str]:
        """Batch insert records with unix timestamps into multiple streams.

        Args:
            records: DataFrame containing records with stream_id column
            data_provider: Optional data provider name

        Returns:
            Transaction hash if successful, None otherwise
        """
        if len(records) == 0:
            return None

        # Convert DataFrame to format expected by client
        batches = []
        for stream_id in records["stream_id"].unique():
            stream_records = records[records["stream_id"] == stream_id]
            batch = {
                "stream_id": stream_id,
                "inputs": [
                    {"date": int(row["date"]), "value": float(row["value"])} for _, row in stream_records.iterrows()
                ],
            }
            batches.append(batch)

        if not batches:
            return None

        with concurrency("tn-write", occupy=1):
            try:
                results = self.get_client().batch_insert_records_unix(
                    batches=batches,
                    helper_contract_stream_id=self.helper_contract_stream_id,
                    helper_contract_data_provider=self.helper_contract_provider,
                    wait=False,
                )
                return results["tx_hash"]
            except Exception as e:
                self.logger.error(f"Error in batch insert: {e}")
                return None

    def wait_for_tx(self, tx_hash: str) -> None:
        with concurrency("tn-read", occupy=1):
            self.get_client().wait_for_tx(tx_hash)

    def destroy_stream(self, stream_id: str, wait: bool = True) -> str:
        """Destroy a stream with the given stream ID.

        Args:
            stream_id: The ID of the stream to destroy
            wait: If True, wait for the transaction to be confirmed

        Returns:
            The transaction hash
        """
        with concurrency("tn-write", occupy=1):
            return self.get_client().destroy_stream(stream_id, wait)

    def split_and_insert_records_unix(
        self,
        records: DataFrame[TnDataRowModel],
        data_provider: Optional[str] = None,
        max_batch_size: int = 50000,
        wait: bool = True,
    ) -> Optional[SplitInsertResults]:
        """Split records into batches and insert them into TSN.

        Args:
            records: DataFrame containing records with stream_id column
            data_provider: Optional data provider name
            max_batch_size: Maximum number of records per batch
            wait: If True, wait for the transactions to be confirmed
        Returns:
            Results containing successful tx hashes and failed records
        """
        if len(records) == 0:
            return None

        # Split records into batches
        success_tx_hashes = []
        all_failed_records = []

        batch_hashes: dict[str, DataFrame[TnDataRowModel]] = {}

        # Split records into batches
        for i in range(0, len(records), max_batch_size):
            batch = DataFrame[TnDataRowModel](records.iloc[i : i + max_batch_size])
            try:
                tx_hash = self.batch_insert_unix_tn_records(batch, data_provider)
                if tx_hash:
                    batch_hashes[tx_hash] = batch
                    if not wait:
                        success_tx_hashes.append(tx_hash)
                else:
                    all_failed_records.append(batch)
            except Exception as e:
                self.logger.error(f"Error inserting batch: {e}")
                all_failed_records.append(batch)

        if wait:
            for tx_hash in success_tx_hashes:
                try:
                    self.wait_for_tx(tx_hash)
                    success_tx_hashes.append(tx_hash)
                except Exception as e:
                    self.logger.error(f"Error waiting for tx: {e}")
                    all_failed_records.append(batch_hashes[tx_hash])

        # Combine all failed records
        failed_records_df = DataFrame[TnDataRowModel](
            pd.concat(all_failed_records)
            if all_failed_records
            else pd.DataFrame(columns=["stream_id", "date", "value"])
        )

        return SplitInsertResults(success_tx_hashes=success_tx_hashes, failed_records=failed_records_df)


# --- Top Level Task Functions ---
@task()
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


@task()
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
        return cast(
            DataFrame[TnRecordModel],
            block.read_records(
                stream_id=stream_id,
                data_provider=data_provider,
                date_from=cast(Optional[int], date_from),
                date_to=cast(Optional[int], date_to),
                is_unix=True,
            ),
        )
    else:
        return cast(
            DataFrame[TnRecordModel],
            block.read_records(
                stream_id=stream_id,
                data_provider=data_provider,
                date_from=cast(Optional[ShortIso8601Date], date_from),
                date_to=cast(Optional[ShortIso8601Date], date_to),
                is_unix=False,
            ),
        )


@task()
def task_insert_tn_records(
    block: TNAccessBlock,
    stream_id: str,
    records: DataFrame[TnRecordModel],
    data_provider: Optional[str] = None,
) -> Optional[str]:
    return block.insert_tn_records(stream_id, records, data_provider)


@task()
def task_insert_unix_tn_records(
    block: TNAccessBlock,
    stream_id: str,
    records: DataFrame[TnRecordModel],
    data_provider: Optional[str] = None,
) -> Optional[str]:
    return block.insert_unix_tn_records(stream_id, records, data_provider)


@task(retries=3, retry_delay_seconds=2)
def task_wait_for_tx(block: TNAccessBlock, tx_hash: str) -> None:
    return block.wait_for_tx(tx_hash)


@task(retries=5, retry_delay_seconds=10)
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


@task(retries=5, retry_delay_seconds=10)
def task_insert_unix_and_wait_for_tx(
    block: TNAccessBlock,
    stream_id: str,
    records: DataFrame[TnRecordModel],
    data_provider: Optional[str] = None,
):
    """Insert records into TSN and wait for transaction confirmation"""
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


@task()
def task_batch_insert_unix_tn_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    data_provider: Optional[str] = None,
) -> Optional[str]:
    """Batch insert records with unix timestamps into multiple streams.

    Args:
        block: The TNAccessBlock instance
        records: DataFrame containing records with stream_id column
        data_provider: Optional data provider name

    Returns:
        Transaction hash if successful, None otherwise
    """
    return block.batch_insert_unix_tn_records(records, data_provider)


@task()
def task_split_and_insert_records_unix(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    data_provider: Optional[str] = None,
    max_batch_size: int = 50000,
    wait: bool = True,
) -> Optional[SplitInsertResults]:
    return block.split_and_insert_records_unix(records, data_provider, max_batch_size, wait)


@task(retries=5, retry_delay_seconds=10)
def task_batch_insert_unix_and_wait_for_tx(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    data_provider: Optional[str] = None,
):
    """Batch insert unix timestamp records into multiple streams and wait for all transactions.

    Args:
        block: The TNAccessBlock instance
        records: DataFrame containing records with stream_id column
        data_provider: Optional data provider name

    Returns:
        List of transaction hashes for completed inserts
    """
    logging = get_run_logger()

    logging.info(f"Batch inserting {len(records)} unix records across {len(records['stream_id'].unique())} streams")
    insertions = task_batch_insert_unix_tn_records(block=block, records=records, data_provider=data_provider)

    if not insertions:
        return Completed(message="No records to insert")

    for tx_hash in insertions:
        if tx_hash:  # Skip None values
            try:
                task_wait_for_tx(block=block, tx_hash=tx_hash)
            except Exception as e:
                if "duplicate key value violates unique constraint" in str(e):
                    logging.warning(f"Continuing after duplicate key value violation: {e}")
                else:
                    raise e

    return insertions


@task()
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
