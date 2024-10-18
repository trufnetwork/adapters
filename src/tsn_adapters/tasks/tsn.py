import os
from typing import Optional
import tsn_sdk.client as tsn_client
import tsn_sdk.utils as tsn_utils
import tsn_sdk_c_bindings.exports as tsn_bindings
from prefect import flow, task
import pandas as pd
from datetime import datetime

@task(tags=["tsn", "tsn-write"])
def insert_tsn_records(stream_id: str, records: pd.DataFrame, client: tsn_client.TSNClient):
    # check if the records are empty
    if len(records) == 0:
        print(f"No records to insert for stream {stream_id}")
        return

    # format YYYY-MM-DDT00:00:00Z
    inserted_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    print(f"Inserting {len(records)} records into stream {stream_id}")
    # generate tuples, [("2024-01-01", "100", inserted_date), ...]
    args = [(record["date"], record["value"], inserted_date) for record in records.to_dict(orient="records")]
    
    # recrods is a list of dicts with the keys: date str, value float
    client.execute_procedure(
        stream_id=stream_id,
        procedure="insert_record",
        args=args,
        wait=True
    )

"""
This task fetches all the records from the TSN for a given stream_id and data_provider

- stream_id: the stream_id to fetch the records from
- data_provider: the data provider to fetch the records from. Optional, if not provided, will use the one from the client
- tsn_provider: the TSN provider to fetch the records from
"""
@task(tags=["tsn", "tsn-read"])
def get_all_tsn_records(stream_id: str, client: tsn_client.TSNClient, data_provider: Optional[str] = None) -> pd.DataFrame:
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
def deploy_primitive(stream_id: str, client: tsn_client.TSNClient):
    client.deploy_stream(stream_id, stream_type=tsn_bindings.StreamTypePrimitive, wait=True)


if __name__ == "__main__":
    @flow(log_prints=True)
    def test_get_all_tsn_records():
        client = tsn_client.TSNClient(tsn_provider=os.environ["TSN_PROVIDER"], token=os.environ["TSN_PRIVATE_KEY"])
        recs = get_all_tsn_records("st2393fded6ff3bde0e77209bc41f964", client)
        print(recs)

    test_get_all_tsn_records()