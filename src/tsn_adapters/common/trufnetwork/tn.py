import hashlib
import json
from math import ceil
import os
from typing import Any, Optional, TypedDict

import pandas as pd
from prefect import flow, task
from prefect.context import TaskRunContext
import trufnetwork_sdk_c_bindings.exports as truf_sdk
import trufnetwork_sdk_py.client as tn_client
from trufnetwork_sdk_py.utils import generate_stream_id

from tsn_adapters.blocks.tn_access import (
    UNUSED_INFINITY_RETRIES,
    StreamAlreadyExistsError,
    TNAccessBlock,
    tn_special_retry_condition,
)
from tsn_adapters.utils.logging import get_logger_safe
from tsn_adapters.utils.time_utils import date_string_to_unix
from tsn_adapters.utils.tn_record import create_record_batches


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
    stream_id: str, records: pd.DataFrame, client: tn_client.TNClient, wait: bool = True, records_per_batch: int = 300
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
    data: dict[str, pd.DataFrame],
    client: tn_client.TNClient,
    wait: bool = False,
):
    return insert_multiple_tsn_records(data, client, wait)


def insert_multiple_tsn_records(
    data: dict[str, pd.DataFrame], client: tn_client.TNClient, wait: bool = True, records_per_batch: int = 300
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

            print(
                f"Processing batch {batch_num + 1}/{num_batches} for stream {stream_id} "
                f"(records {start_idx + 1}-{min(end_idx, total_records)})"
            )

            # Create and insert the batch
            batches = create_record_batches(stream_id, batch_records, batch_num + 1, records_per_batch)
            client.batch_insert_records(batches, wait)

        print(f"Finished processing all batches for stream {stream_id}\n")


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
    try:
        recs = client.get_records(
            stream_id=stream_id, data_provider=data_provider, date_from=date_string_to_unix("1000-01-01")
        )
    except:
        recs = []

    recs_list = [
        {
            "date": rec["EventTime"],
            "value": float(rec["Value"]),
            **{k: v for k, v in rec.items() if k not in ("EventTime", "Value")},
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
    """
     Deploy a primitive stream.
    Returns:
        Transaction hash on successful deployment.
    Raises:
        StreamAlreadyExistsError: If the stream already exists.
        Exception: For other deployment errors after retries.
    """
    logger = get_logger_safe()

    try:
        logger.debug(f"Attempting to deploy stream {stream_id} (wait: {wait})")
        tx_hash = block.deploy_stream(
            stream_id=stream_id,
            stream_type=truf_sdk.StreamTypePrimitive,
            wait=wait,
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
    name="Batch Deploy Streams",
    retries=UNUSED_INFINITY_RETRIES,
    retry_delay_seconds=10,
    retry_condition_fn=tn_special_retry_condition(3),
    tags=["tn", "tn-write"],
)
def task_batch_deploy_streams(
    block: TNAccessBlock, definitions: list[tn_client.StreamDefinitionInput], wait: bool = True
) -> str:
    """
    Task to deploy multiple streams using the TNAccessBlock.

    Args:
        block: The TNAccessBlock instance.
        definitions: A list of stream definitions for batch deployment.
        wait: If True, wait for the transaction to be confirmed.

    Returns:
        The transaction hash of the batch deployment.
    """
    logger = get_logger_safe()
    logger.info(f"Task: Batch deploying {len(definitions)} streams (wait: {wait}).")
    try:
        tx_hash = block.batch_deploy_streams(definitions=definitions, wait=wait)
        logger.info(f"Task: Batch deployment TX hash: {tx_hash}")
        return tx_hash
    except Exception as e:
        logger.error(f"Task: Error during batch deployment of {len(definitions)} streams: {e!s}", exc_info=True)
        raise


@task(
    name="Batch Filter Streams by Existence",
    retries=UNUSED_INFINITY_RETRIES,  # Read operations might not need as aggressive retries, adjust if necessary
    retry_delay_seconds=5,
    retry_condition_fn=tn_special_retry_condition(3),
    tags=["tn", "tn-read"],
)
def task_batch_filter_streams_by_existence(
    block: TNAccessBlock, locators: list[tn_client.StreamLocatorInput], return_existing: bool
) -> list[tn_client.StreamLocatorInput]:
    """
    Task to filter a list of streams based on their existence using TNAccessBlock.

    Args:
        block: The TNAccessBlock instance.
        locators: A list of stream locators.
        return_existing: If True, returns streams that exist. Otherwise, returns non-existent ones.

    Returns:
        A list of stream locators matching the filter criteria.
    """
    logger = get_logger_safe()
    logger.info(f"Task: Batch filtering {len(locators)} streams by existence (return_existing: {return_existing}).")
    try:
        filtered_locators = block.batch_filter_streams_by_existence(locators=locators, return_existing=return_existing)
        logger.info(f"Task: Found {len(filtered_locators)} matching streams after filtering.")
        return filtered_locators
    except Exception as e:
        logger.error(f"Task: Error during batch filtering of {len(locators)} streams: {e!s}", exc_info=True)
        raise


class FilterAndDeployResult(TypedDict):
    deployed_stream_ids: list[str]
    skipped_stream_ids_already_exist: list[str]
    deployment_tx_hash: Optional[str]


@task(
    name="Filter and Deploy Streams",
    retries=UNUSED_INFINITY_RETRIES,  # Relies on sub-task retries primarily
    retry_delay_seconds=5,
    retry_condition_fn=tn_special_retry_condition(2),  # Fewer retries for the orchestrator
    tags=["tn", "tn-write", "tn-read"],
)
def task_filter_and_deploy_streams(
    block: TNAccessBlock,
    potential_definitions: list[tn_client.StreamDefinitionInput],
    wait_for_deployment_tx: bool = False,  # If True, task_batch_deploy_streams waits
) -> FilterAndDeployResult:
    """
    Filters a list of potential stream definitions for existence on TN,
    then deploys those that do not exist. Orchestrates other batch tasks.
    All definitions are assumed to be for primitive streams.

    Args:
        block: The TNAccessBlock instance.
        potential_definitions: A list of stream definitions that might need deployment.
        wait_for_deployment_tx: Passed to task_batch_deploy_streams's 'wait' param.
                                  If False, a tx_hash is returned for later waiting.

    Returns:
        A FilterAndDeployResult dictionary.
    """
    logger = get_logger_safe()
    logger.info(f"Task: Starting filter and deploy for {len(potential_definitions)} potential streams.")

    deployed_sids: list[str] = []
    skipped_sids_exist: list[str] = []
    tx_hash: Optional[str] = None

    if not potential_definitions:
        logger.info("Task: No potential definitions provided. Nothing to do.")
        return FilterAndDeployResult(
            deployed_stream_ids=deployed_sids,
            skipped_stream_ids_already_exist=skipped_sids_exist,
            deployment_tx_hash=tx_hash,
        )

    # 1. Create locators for existence check
    locators_for_check: list[tn_client.StreamLocatorInput] = []
    data_provider = block.current_account
    potential_ids_map = {defn["stream_id"]: defn for defn in potential_definitions}

    for defn in potential_definitions:
        locators_for_check.append(
            tn_client.StreamLocatorInput(stream_id=defn["stream_id"], data_provider=data_provider)
        )

    # 2. Filter by existence (call task directly)
    try:
        logger.debug(f"Task: Checking existence for {len(locators_for_check)} streams.")
        # Use return_existing=True to find streams that ARE on TN
        existent_locators = task_batch_filter_streams_by_existence(
            block=block, locators=locators_for_check, return_existing=True
        )

        existent_sids = {str(loc["stream_id"]) for loc in existent_locators}
        logger.debug(f"Task: Found {len(existent_sids)} streams already on TN.")

    except Exception as e:
        logger.error(
            f"Task: Error during batch stream existence check: {e!s}. Cannot proceed with this batch.", exc_info=True
        )
        # This is critical; if we can't check existence, we shouldn't deploy.
        raise  # Re-raise to let Prefect handle retry for this orchestrating task

    # 3. Determine streams to deploy and skipped streams
    definitions_to_deploy: list[tn_client.StreamDefinitionInput] = []
    for sid, defn in potential_ids_map.items():
        if sid in existent_sids:
            skipped_sids_exist.append(sid)
        else:
            definitions_to_deploy.append(defn)

    logger.info(
        f"Task: {len(definitions_to_deploy)} streams to deploy, {len(skipped_sids_exist)} streams already exist."
    )

    # 4. Deploy non-existent streams if any
    if definitions_to_deploy:
        try:
            logger.debug(
                f"Task: Deploying {len(definitions_to_deploy)} streams (wait_for_tx: {wait_for_deployment_tx})."
            )
            # Call task directly
            tx_hash = task_batch_deploy_streams(
                block=block, definitions=definitions_to_deploy, wait=wait_for_deployment_tx
            )
            deployed_sids = [str(d["stream_id"]) for d in definitions_to_deploy]
            logger.info(f"Task: Submitted deployment for {len(deployed_sids)} streams. TX Hash: {tx_hash}")
        except Exception as e:
            logger.error(f"Task: Error during batch stream deployment: {e!s}.", exc_info=True)
            # This is critical for the streams that were attempted.
            raise  # Re-raise
    else:
        logger.info("Task: No new streams to deploy.")

    return FilterAndDeployResult(
        deployed_stream_ids=deployed_sids,
        skipped_stream_ids_already_exist=skipped_sids_exist,
        deployment_tx_hash=tx_hash,
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
        insert_tsn_records(
            stream_id,
            pd.DataFrame(
                {"date": [date_string_to_unix("2023-01-01"), date_string_to_unix("2023-01-02")], "value": [10.5, 20.3]}
            ),
            client,
        )
        insert_multiple_tsn_records(
            {stream_id: pd.DataFrame({"date": [date_string_to_unix("2023-01-03")], "value": [12]})}, client
        )

        # get records
        recs = get_all_tsn_records(stream_id, client)
        print(recs)

        # cleanup
        client.destroy_stream(stream_id)

    test_get_all_tsn_records()