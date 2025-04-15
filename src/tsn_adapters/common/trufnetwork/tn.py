import os
import pandas as pd
import trufnetwork_sdk_c_bindings.exports as truf_sdk
import trufnetwork_sdk_py.client as tn_client

from trufnetwork_sdk_py.utils import generate_stream_id
from math import ceil
from prefect import flow, task
from dotenv import load_dotenv
from typing import Any, Dict, Hashable, Optional, List
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
    records_per_batch: int = 300
):
    if len(records) == 0:
        print(f"No records to insert for stream {stream_id}")
        return

    print(f"Inserting {len(records)} records into stream {stream_id}")

    records_dict = records.to_dict(orient="records")
    num_batches = ceil(len(records_dict) / records_per_batch)
    batches = create_record_batches(stream_id, records_dict, num_batches, records_per_batch)
        
    client.batch_insert_records(batches, wait)

@task(
    retries=UNUSED_INFINITY_RETRIES,
    retry_delay_seconds=10,
    retry_condition_fn=tn_special_retry_condition(3),
    tags=["tn", "tn-write"],
)
def task_insert_multiple_tsn_records(
    data: Dict[str, pd.DataFrame],
    client: tn_client.TNClient,
    wait: bool = False,
):
    return insert_multiple_tsn_records(data, client, wait)

def insert_multiple_tsn_records(
    data: Dict[str, pd.DataFrame],
    client: tn_client.TNClient,
    wait: bool = True,
    records_per_batch: int = 300
):
    for stream_id, records in data.items():
        print(f"Processing stream {stream_id}...")

        records_dict = records.to_dict(orient="records")
        total_records = len(records_dict)
        num_batches = ceil(len(records_dict) / records_per_batch)

        print(f"Stream {stream_id} has {total_records} records to process in {num_batches} batches")

        # Process all batches for this stream before moving to next stream
        for batch_num in range(num_batches):
            start_idx = batch_num * records_per_batch
            end_idx = start_idx + records_per_batch
            batch_records = records_dict[start_idx:end_idx]
            
            print(f"Processing batch {batch_num + 1}/{num_batches} for stream {stream_id} "
                  f"(records {start_idx + 1}-{min(end_idx, total_records)})")
            
            # Create and insert the batch
            batches = create_record_batches(stream_id, batch_records, batch_num + 1, records_per_batch)
            client.batch_insert_records(batches, wait)
        
        print(f"Finished processing all batches for stream {stream_id}\n")

def create_record_batches(stream_id: str, records_dict: list[Dict[Hashable, Any]], num_batches: int, records_per_batch: int):
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

    return batches
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

        # deploy test stream
        client.deploy_stream(stream_id)

        # insert records into test stream
        insert_tsn_records(stream_id, pd.DataFrame({
            "date": [date_string_to_unix("2023-01-01"), date_string_to_unix("2023-01-02")],
            "value": [10.5, 20.3]
        }), client)
        insert_multiple_tsn_records({
            stream_id: pd.DataFrame({
                "date": [date_string_to_unix("2023-01-03")],
                "value": [12]
            })
        }, client)

        # get records
        recs = get_all_tsn_records(stream_id, client)
        print(recs)
        
        # cleanup
        client.destroy_stream(stream_id)

    test_get_all_tsn_records()
