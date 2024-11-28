import os
import pandas as pd
import trufnetwork_sdk_py.client as tn_client
import trufnetwork_sdk_c_bindings.exports as tsn_bindings
from prefect import flow, task
from typing import Optional


@task(tags=["tsn", "tsn-write"])
def task_insert_tsn_records(stream_id: str, records: pd.DataFrame, client: tn_client.TNClient, wait: bool = True):
    return insert_tsn_records(stream_id, records, client, wait)

def insert_tsn_records(stream_id: str, records: pd.DataFrame, client: tn_client.TNClient, wait: bool = True):
    # check if the records are empty
    if len(records) == 0:
        print(f"No records to insert for stream {stream_id}")
        return

    print(f"Inserting {len(records)} records into stream {stream_id}")
    # generate tuples, [("2024-01-01", "100", inserted_date), ...]
    args = [(record["date"], str(record["value"])) for record in records.to_dict(orient="records")]

    # args is a list of tuples with the keys: date str, value float
    client.execute_procedure(
        stream_id=stream_id,
        procedure="insert_record",
        args=args,
        wait=wait
    )

"""
This task fetches all the records from the TSN for a given stream_id and data_provider

- stream_id: the stream_id to fetch the records from
- data_provider: the data provider to fetch the records from. Optional, if not provided, will use the one from the client
- tsn_provider: the TSN provider to fetch the records from
"""
@task(tags=["tsn", "tsn-read"])
def task_get_all_tsn_records(stream_id: str, client: tn_client.TNClient, data_provider: Optional[str] = None) -> pd.DataFrame:
    return get_all_tsn_records(stream_id, client, data_provider)

def get_all_tsn_records(stream_id: str, client: tn_client.TNClient, data_provider: Optional[str] = None) -> pd.DataFrame:
    recs = client.get_records(
        stream_id=stream_id,
        data_provider=data_provider,
        date_from="1000-01-01"
    )

    recs_list = [
        {
            "date": rec['DateValue'],
            "value": float(rec['Value']),
            **{k: v for k, v in rec.items() if k not in ("DateValue", "Value")}
        }
        for rec in recs
    ]
    df = pd.DataFrame(recs_list)
    return df

@task(tags=["tsn", "tsn-write"])
def task_deploy_primitive(stream_id: str, client: tn_client.TNClient):
    return deploy_primitive(stream_id, client)

def deploy_primitive(stream_id: str, client: tn_client.TNClient):
    client.deploy_stream(stream_id, stream_type=tsn_bindings.StreamTypePrimitive, wait=True)


if __name__ == "__main__":
    @flow(log_prints=True)
    def test_get_all_tsn_records():
        client = tn_client.TNClient(url=os.environ["TSN_PROVIDER"], token=os.environ["TSN_PRIVATE_KEY"])
        recs = get_all_tsn_records("st2393fded6ff3bde0e77209bc41f964", client)
        print(recs)

    test_get_all_tsn_records()