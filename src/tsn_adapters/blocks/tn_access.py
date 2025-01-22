from datetime import datetime, timezone
import decimal
from typing import Literal, Optional, Union, cast, overload

import pandas as pd
from pandera.typing import DataFrame
from prefect import Task, get_run_logger, task
from prefect.blocks.core import Block
from prefect.concurrency.sync import concurrency
from prefect.states import Completed
from pydantic import ConfigDict, SecretStr
import trufnetwork_sdk_py.client as tn_client

from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel, TnRecordModel
from tsn_adapters.utils.date_type import ShortIso8601Date


class TNAccessBlock(Block):
    """Prefect Block for managing TSN access credentials.

    This block securely stores and manages TSN access credentials for
    authenticating with TSN API in Prefect flows.
    """

    tn_provider: str
    tn_private_key: SecretStr
    model_config = ConfigDict(ignored_types=(Task,))

    def get_client(self):
        return tn_client.TNClient(
            url=self.tn_provider,
            token=self.tn_private_key.get_secret_value(),
        )

    def read_all_records(self, stream_id: str, data_provider: Optional[str] = None) -> pd.DataFrame:
        """Read all records from TSN"""
        return self.read_records(stream_id, data_provider, date_from="1000-01-01")

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
        logger = get_run_logger()
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
            logger.error(
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
    ) -> Optional[str]:
        logging = get_run_logger()

        if len(records) == 0:
            logging.info(f"No records to insert for stream {stream_id}")
            return None

        current_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        args = [[record["date"], str(record["value"]), current_date] for record in records.to_dict(orient="records")]

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
        lock_write: bool = True,
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

        def write_data():
            txHash = self.get_client().execute_procedure(
                stream_id=stream_id,
                procedure="insert_record",
                args=args,
                wait=False,
                data_provider=data_provider or "",
            )
            logging.debug(f"Inserted {len(records)} records into stream {stream_id}")
            return txHash

        if lock_write:
            with concurrency("tn-write", occupy=1):
                txHash = write_data()
        else:
            txHash = write_data()

        return txHash

    def batch_insert_unix_tn_records(
        self,
        records: DataFrame[TnDataRowModel],
        data_provider: Optional[str] = None,
    ) -> Optional[list[str]]:
        """Batch insert records with unix timestamps into multiple streams.

        Args:
            records: DataFrame containing records with stream_id column
            data_provider: Optional data provider name

        Returns:
            List of transaction hashes for each stream insert
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

        with concurrency("tn-write", occupy=1):
            tx_hashes = self.get_client().batch_insert_records_unix(
                batches=batches,
                wait=False,
            )
        return tx_hashes

    def wait_for_tx(self, tx_hash: str) -> None:
        with concurrency("tn-read", occupy=1):
            self.get_client().wait_for_tx(tx_hash)


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
) -> Optional[list[str]]:
    """Batch insert records with unix timestamps into multiple streams.

    Args:
        block: The TNAccessBlock instance
        records: DataFrame containing records with stream_id column
        data_provider: Optional data provider name

    Returns:
        List of transaction hashes for each stream insert
    """
    return block.batch_insert_unix_tn_records(records, data_provider)


@task(retries=5, retry_delay_seconds=10)
def task_batch_insert_and_wait_for_tx(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    data_provider: Optional[str] = None,
):
    """Batch insert records into multiple streams and wait for all transactions.

    Args:
        block: The TNAccessBlock instance
        records: DataFrame containing records with stream_id column
        data_provider: Optional data provider name

    Returns:
        List of transaction hashes for completed inserts
    """
    logging = get_run_logger()

    logging.info(f"Batch inserting {len(records)} records across {len(records['stream_id'].unique())} streams")
    insertions = task_batch_insert_tn_records(block=block, records=records, data_provider=data_provider)

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


if __name__ == "__main__":
    TNAccessBlock.register_type_and_schema()
