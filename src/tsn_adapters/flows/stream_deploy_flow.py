"""
Stream Deployment Flow

This module contains a Prefect flow to deploy primitive streams from a generic
primitive source descriptor. For each stream in the descriptor, the flow verifies
if the stream exists (using the TN SDK's stream_exists function). If the stream
does not exist, it creates a deployment transaction (with wait=False) under a tn-write
concurrency limiter and then waits for confirmation using tn-read concurrency via TNAccessBlock.
"""

from typing import List
import pandas as pd
from pandera.typing import DataFrame
from prefect import flow, get_run_logger, task, unmapped

# Import the primitive source descriptor interface and data model
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourcesDescriptorBlock,
    PrimitiveSourceDataModel,
)

# Import the TN client types and the task to deploy a primitive stream.
import trufnetwork_sdk_py.client as tn_client
from tsn_adapters.common.trufnetwork.tn import (
    task_deploy_primitive,
    task_init_stream
)

# Import TNAccessBlock and task_wait_for_tx so that we can use its waiting functionality.
from tsn_adapters.blocks.tn_access import TNAccessBlock, task_wait_for_tx


@task(tags=["tn", "tn-write"], retries=3, retry_delay_seconds=5)
def check_and_deploy_stream(
    stream_id: str, tn_client: tn_client.TNClient, tna_block: TNAccessBlock
) -> str:
    """
    Checks if a stream exists using the SDK's stream_exists function.
    If the stream does not exist, deploys it using task_deploy_primitive.
    Returns a message indicating the action taken.
    """
    logger = get_run_logger()
    if tn_client.stream_exists(stream_id):
        message = f"Stream {stream_id} already exists. Skipping deployment."
        logger.info(message)
        return message
    else:
        # Create the deployment transaction without waiting.
        logger.info(f"Deploying stream {stream_id}.")
        tx_deploy = task_deploy_primitive(stream_id=stream_id, client=tn_client, wait=False)
        # Wait for deployment confirmation using the TNAccessBlock (tn-read concurrency).
        task_wait_for_tx(block=tna_block, tx_hash=tx_deploy)
        logger.debug(f"Deployed stream {stream_id}.")

        # Create the initialization transaction without waiting.
        tx_init = task_init_stream(stream_id=stream_id, client=tn_client, wait=False)
        logger.debug(f"Initialize stream {stream_id}.")
        # Wait for initialization confirmation.
        task_wait_for_tx(block=tna_block, tx_hash=tx_init)
        logger.debug(f"Initialized stream {stream_id}.")

        return f"Deployed and initialized stream {stream_id}."

@flow(name="Stream Deployment Flow")
def deploy_streams_flow(
    psd_block: PrimitiveSourcesDescriptorBlock,
    tn_client: tn_client.TNClient,
    tna_block: TNAccessBlock,
) -> List[str]:
    """
    Deploys primitive streams defined in the provided primitive source descriptor.

    For each stream descriptor:
      - Verifies if the stream exists.
      - If not, creates deployment and initialization transactions (without waiting)
        and then waits for confirmation using tn-read concurrency.

    Args:
        psd_block: A PrimitiveSourcesDescriptorBlock providing the stream descriptors.
        tn_client: A TNClient instance for deployment interactions.
        tna_block: A TNAccessBlock instance used to wait for transaction confirmation.

    Returns:
        A list of messages indicating whether each stream was deployed or skipped.
    """
    logger = get_run_logger()
    logger.info("Starting stream deployment flow.")

    # Retrieve the descriptor DataFrame.
    descriptor_df: DataFrame[PrimitiveSourceDataModel] = psd_block.get_descriptor()
    if descriptor_df.empty:
        logger.info("No stream descriptors found. Exiting flow.")
        return []

    # Extract stream_ids from the descriptor.
    stream_ids = descriptor_df["stream_id"].tolist()
    logger.info(f"Found {len(stream_ids)} stream descriptors.")

    # Map over the stream IDs using the check/deploy task.
    deployment_messages = check_and_deploy_stream.map(
        stream_id=stream_ids, tn_client=unmapped(tn_client), tna_block=unmapped(tna_block)
    )

    logger.info(f"Deployment results: {deployment_messages}")
    return deployment_messages