import os
import pandas as pd
import trufnetwork_sdk_c_bindings.exports as truf_sdk
import trufnetwork_sdk_py.client as tn_client
from trufnetwork_sdk_py.utils import generate_stream_id

from math import ceil
from prefect import flow, task
from dotenv import load_dotenv
from typing import Optional, List
from tsn_adapters.blocks.tn_access import UNUSED_INFINITY_RETRIES, TNAccessBlock, tn_special_retry_condition
from tsn_adapters.utils.time_utils import date_string_to_unix

load_dotenv()

@task(
    retries=UNUSED_INFINITY_RETRIES,
    retry_delay_seconds=10,
    retry_condition_fn=tn_special_retry_condition(3),
    tags=["tn", "tn-write"],
)
def task_insert_tsn_records(
    stream_id: str,
    records: pd.DataFrame,
    client: tn_client.TNClient,
    wait: bool = False,
):
    return insert_tsn_records(stream_id, records, client, wait)


def insert_tsn_records(
    stream_id: str,
    records: pd.DataFrame,
    client: tn_client.TNClient,
    wait: bool = True,
    records_per_batch: int = 100
):
    if len(records) == 0:
        print(f"No records to insert for stream {stream_id}")
        return

    print(f"Inserting {len(records)} records into stream {stream_id}")

    records_dict = records.to_dict(orient="records")
    num_batches = ceil(len(records_dict) / records_per_batch)

    batches: List[tn_client.RecordBatch] = []
    for batch_idx in range(num_batches):
        start_idx = batch_idx * records_per_batch
        end_idx = start_idx + records_per_batch
        batch_data = records_dict[start_idx:end_idx]
        
        batch_records = []
        for record in batch_data:
            batch_records.append(
                tn_client.Record(
                    date=record["date"],
                    value=float(record["value"])
                )
            )
        
        batches.append(
            tn_client.RecordBatch(
                stream_id=stream_id,
                inputs=batch_records
            )
    )
        
    client.batch_insert_records(batches, wait)

"""
This task fetches all the records from the TSN for a given stream_id and data_provider

- stream_id: the stream_id to fetch the records from
- data_provider: the data provider to fetch the records from. Optional, if not provided, will use the one from the client
- tsn_provider: the TSN provider to fetch the records from
"""  # noqa: E501


@task(tags=["tn", "tn-read"])
def task_get_all_tsn_records(
    stream_id: str, client: tn_client.TNClient, data_provider: Optional[str] = None
) -> pd.DataFrame:
    return get_all_tsn_records(stream_id, client, data_provider)


def get_all_tsn_records(
    stream_id: str, client: tn_client.TNClient, data_provider: Optional[str] = None
) -> pd.DataFrame:
    recs = client.get_records(stream_id=stream_id, data_provider=data_provider, date_from=date_string_to_unix("1000-01-01"))
    print(recs)
    recs_list = [
        {
            "date": rec["EventTime"],
            "value": float(rec["Value"]),
            **{k: v for k, v in rec.items() if k not in ("DateValue", "Value")},
        }
        for rec in recs
    ]
    df = pd.DataFrame(recs_list)
    return df


@task(
    retries=UNUSED_INFINITY_RETRIES,
    retry_delay_seconds=10,
    retry_condition_fn=tn_special_retry_condition(3),
    tags=["tn", "tn-write"],
)
def task_deploy_primitive(block: TNAccessBlock, stream_id: str, wait: bool = True) -> str:
    return block.deploy_stream(
        stream_id=stream_id,
        stream_type=truf_sdk.StreamTypePrimitive,
        wait=wait,
    )

if __name__ == "__main__":
    @flow(log_prints=True)
    def test_get_all_tsn_records():
        client = tn_client.TNClient(url=os.environ["TSN_PROVIDER"], token=os.environ["TSN_PRIVATE_KEY"])
        stream_id = generate_stream_id("test_stream_tsn")

        # remove stream leftover from previous test, if not cleaned yet
        try:
            client.destroy_stream(stream_id)
        except:
            print("stream doesn't exist, initializing a new one..")

        client.deploy_stream(stream_id)
        insert_tsn_records(stream_id, pd.DataFrame({
            "date": [date_string_to_unix("2023-01-01"), date_string_to_unix("2023-01-02")],
            "value": [10.5, 20.3]
        }), client)
        recs = get_all_tsn_records(stream_id, client)
        print(recs)
        
        # cleanup
        client.destroy_stream(stream_id)

    test_get_all_tsn_records()
