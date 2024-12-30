from prefect.blocks.core import Block
from pydantic import ConfigDict, SecretStr
import decimal
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame
from prefect import get_run_logger, task, Task
from prefect.concurrency.sync import concurrency
from prefect.states import Completed

from tsn_adapters.tasks.trufnetwork.models.tn_models import TnRecordModel
import trufnetwork_sdk_py.client as tn_client

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

    def read_all_records(self, stream_id: str) -> pd.DataFrame:
        """Read all records from TSN"""
        return self.read_records(stream_id, date_from="1000-01-01")

    @pa.check_types
    def read_records(
            self,
            stream_id: str,
            data_provider: Optional[str] = None,
            date_from: Optional[ShortIso8601Date] = None,
            date_to: Optional[ShortIso8601Date] = None,
    ) -> DataFrame[TnRecordModel]:
        logger = get_run_logger()
        try:
            with concurrency("tn-read", occupy=1):
                recs = self.get_client().get_records(
                    stream_id,
                    data_provider,
                    date_from if date_from else None,
                    date_to if date_to else None,
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
            self, stream_id: str, records: DataFrame[TnRecordModel], data_provider: Optional[str] = None
    ) -> Optional[str]:
        logging = get_run_logger()

        if len(records) == 0:
            logging.info(f"No records to insert for stream {stream_id}")
            return None

        current_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        logging.info(f"Inserting {len(records)} records into stream {stream_id}")
        args = [
            [record["date"], str(record["value"]), current_date]
            for record in records.to_dict(orient="records")
        ]

        for col in ["date", "value"]:
            if col not in records.columns:
                logging.error(f"Missing required column '{col}' in records DataFrame.")
                raise ValueError(
                    f"Missing required column '{col}' in records DataFrame."
                )

        with concurrency("tn-write", occupy=1):
            txHash = self.get_client().execute_procedure(
                stream_id=stream_id, procedure="insert_record", args=args, wait=False, data_provider=data_provider
            )

        return txHash

    def wait_for_tx(self, tx_hash: str) -> None:
        with concurrency("tn-read", occupy=1):
            self.get_client().wait_for_tx(tx_hash)


# --- Top Level Task Functions ---
def read_all_records(block: TNAccessBlock, stream_id: str) -> pd.DataFrame:
    return block.read_all_records(stream_id)

def read_records(
        block: TNAccessBlock,
        stream_id: str,
        data_provider: Optional[str] = None,
        date_from: Optional[ShortIso8601Date] = None,
        date_to: Optional[ShortIso8601Date] = None,
) -> DataFrame[TnRecordModel]:
    return block.read_records(stream_id, data_provider, date_from, date_to)

def insert_tn_records(block: TNAccessBlock, stream_id: str, records: DataFrame[TnRecordModel], data_provider: Optional[str] = None) -> Optional[str]:
    return block.insert_tn_records(stream_id, records, data_provider)

def wait_for_tx(block: TNAccessBlock, tx_hash: str) -> None:
    return block.wait_for_tx(tx_hash)

@task(retries=5, retry_delay_seconds=10)
def insert_and_wait_for_tx(
        block: TNAccessBlock, stream_id: str, records: DataFrame[TnRecordModel],
        data_provider: Optional[str] = None,
):
    """Insert records into TSN and wait for transaction confirmation"""
    logging = get_run_logger()

    logging.info(f"waitInserting {len(records)} records into stream {stream_id}")
    insertion = insert_tn_records(block, stream_id=stream_id, records=records, data_provider=data_provider)

    if insertion is None:
        return Completed(message="No records to insert")

    try:
        wait_for_tx(block, tx_hash=insertion)
    except Exception as e:
        if "duplicate key value violates unique constraint" in str(e):
            logging.warning(f"Continuing after duplicate key value violation: {e}")
        else:
            raise e
    return insertion


if __name__ == "__main__":
    TNAccessBlock.register_type_and_schema()