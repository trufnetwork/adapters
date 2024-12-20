from prefect.blocks.core import Block
from pydantic import ConfigDict, SecretStr
import decimal
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame
from prefect import get_run_logger
from prefect.concurrency.sync import concurrency

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
    model_config = ConfigDict()


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
            self, stream_id: str, records: DataFrame[TnRecordModel]
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
                stream_id=stream_id, procedure="insert_record", args=args, wait=False
            )

        return txHash


if __name__ == "__main__":
    TNAccessBlock.register_type_and_schema()
