import os
from typing import Optional

import pandas as pd
from prefect import flow, task
import trufnetwork_sdk_c_bindings.exports as truf_sdk
import trufnetwork_sdk_py.client as tn_client

from tsn_adapters.blocks.tn_access import (
    UNUSED_INFINITY_RETRIES,
    StreamAlreadyExistsError,
    TNAccessBlock,
    tn_special_retry_condition,
)
from tsn_adapters.utils.logging import get_logger_safe


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
    data_provider: Optional[str] = None,
):
    return insert_tsn_records(stream_id, records, client, wait, data_provider)


def insert_tsn_records(
    stream_id: str,
    records: pd.DataFrame,
    client: tn_client.TNClient,
    wait: bool = True,
    data_provider: Optional[str] = None,
):
    # check if the records are empty
    if len(records) == 0:
        print(f"No records to insert for stream {stream_id}")
        return

    print(f"Inserting {len(records)} records into stream {stream_id}")
    # generate tuples, [("2024-01-01", "100", inserted_date), ...]
    args = [[record["date"], str(record["value"])] for record in records.to_dict(orient="records")]

    # args is a list of tuples with the keys: date str, value float
    client.execute_procedure(
        stream_id=stream_id,
        procedure="insert_record",
        args=args,
        wait=wait,
        data_provider=data_provider or "",
    )


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
    recs = client.get_records(stream_id=stream_id, data_provider=data_provider, date_from="1000-01-01")

    recs_list = [
        {
            "date": rec["DateValue"],
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
def task_deploy_primitive(block: TNAccessBlock, stream_id: str, wait: bool = True, is_unix: bool = False) -> str:
    """Deploy a primitive stream.

    Returns:
        Transaction hash on successful deployment.

    Raises:
        StreamAlreadyExistsError: If the stream already exists.
        Exception: For other deployment errors after retries.
    """
    logger = get_logger_safe()
    stream_type = truf_sdk.StreamTypePrimitive if not is_unix else truf_sdk.StreamTypePrimitiveUnix
    try:
        logger.debug(f"Attempting to deploy stream {stream_id} (type: {stream_type}, wait: {wait})")
        tx_hash = block.deploy_stream(
            stream_id=stream_id,
            wait=wait,
            stream_type=stream_type,
        )
        logger.debug(f"Created TX for stream deployment {stream_id} (tx: {tx_hash}, wait: {wait})")
        return tx_hash
    except StreamAlreadyExistsError as e:
        logger.info(f"Stream {stream_id} already exists. Skipping deployment.")
        raise e
    except Exception as e:
        # For any other error, re-raise it to be handled by Prefect's retry mechanism
        logger.error(f"Error deploying stream {stream_id}: {e!s}", exc_info=True)
        raise e


@task(
    retries=UNUSED_INFINITY_RETRIES,
    retry_delay_seconds=10,
    retry_condition_fn=tn_special_retry_condition(3),
    tags=["tn", "tn-write"],
)
def task_init_stream(block: TNAccessBlock, stream_id: str, wait: bool = True) -> str:
    return block.init_stream(stream_id=stream_id, wait=wait)


if __name__ == "__main__":

    @flow(log_prints=True)
    def test_get_all_tsn_records():
        client = tn_client.TNClient(url=os.environ["TSN_PROVIDER"], token=os.environ["TSN_PRIVATE_KEY"])
        recs = get_all_tsn_records("st2393fded6ff3bde0e77209bc41f964", client)
        print(recs)

    test_get_all_tsn_records()
