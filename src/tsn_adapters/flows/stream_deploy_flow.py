"""
Stream Deployment Flow

This module contains a Prefect flow to deploy primitive streams from a generic
primitive source descriptor. For each stream in the descriptor, the flow verifies
if the stream exists (using the TN SDK's stream_exists function). If the stream
does not exist, it creates a deployment transaction (with wait=False) under a tn-write
concurrency limiter and then waits for confirmation using tn-read concurrency via TNAccessBlock.
"""

from typing import Literal

from pandera.typing import DataFrame
from prefect import flow, get_run_logger, task

# Import the TN client types and the task to deploy a primitive stream.
from typing_extensions import TypedDict

# Import the primitive source descriptor interface and data model
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)

# Import TNAccessBlock and task_wait_for_tx so that we can use its waiting functionality.
from tsn_adapters.blocks.tn_access import TNAccessBlock, task_wait_for_tx
from tsn_adapters.common.trufnetwork.tn import task_deploy_primitive, task_init_stream


class DeployStreamResult(TypedDict):
    stream_id: str
    status: Literal["deployed", "skipped"]


@task(tags=["tn", "tn-write"], retries=3, retry_delay_seconds=5)
def check_and_deploy_stream(stream_id: str, tna_block: TNAccessBlock, is_unix: bool = False) -> DeployStreamResult:
    """
    Checks if a stream exists using the SDK's stream_exists function.
    If the stream does not exist, deploys it using task_deploy_primitive.
    Returns a message indicating the action taken.
    """
    logger = get_run_logger()
    tn_client = tna_block.client
    account = tn_client.get_current_account()
    if tna_block.stream_exists(account, stream_id):
        message = f"Stream {stream_id} already exists. Skipping deployment."
        logger.debug(message)
        return DeployStreamResult(stream_id=stream_id, status="skipped")
    else:
        # Create the deployment transaction without waiting.
        logger.debug(f"Deploying stream {stream_id}.")
        tx_deploy = task_deploy_primitive(block=tna_block, stream_id=stream_id, wait=False, is_unix=is_unix)
        # Wait for deployment confirmation using the TNAccessBlock (tn-read concurrency).
        task_wait_for_tx(block=tna_block, tx_hash=tx_deploy)
        logger.debug(f"Deployed stream {stream_id}.")

        # Create the initialization transaction without waiting.
        tx_init = task_init_stream(block=tna_block, stream_id=stream_id, wait=False)
        logger.debug(f"Initialize stream {stream_id}.")
        # Wait for initialization confirmation.
        task_wait_for_tx(block=tna_block, tx_hash=tx_init)
        logger.debug(f"Initialized stream {stream_id}.")

        return DeployStreamResult(stream_id=stream_id, status="deployed")


class DeployStreamResults(TypedDict):
    deployed_count: int
    skipped_count: int


@flow(name="Stream Deployment Flow")
def deploy_streams_flow(
    psd_block: PrimitiveSourcesDescriptorBlock,
    tna_block: TNAccessBlock,
    is_unix: bool = False,
    batch_size: int = 500,
    start_from_batch: int = 0,
) -> DeployStreamResults:
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
        batch_size: The number of streams to deploy in each batch. Defaults to 500.
        start_from_batch: The batch number to start from. Defaults to 0.

    Returns:
        A list of messages indicating whether each stream was deployed or skipped.
    """
    logger = get_run_logger()
    logger.info("Starting stream deployment flow.")

    # Retrieve the descriptor DataFrame.
    descriptor_df: DataFrame[PrimitiveSourceDataModel] = psd_block.get_descriptor()
    if descriptor_df.empty:
        logger.info("No stream descriptors found. Exiting flow.")
        return DeployStreamResults(deployed_count=0, skipped_count=0)

    # Extract stream_ids from the descriptor.
    stream_ids = descriptor_df["stream_id"].tolist()
    logger.info(f"Found {len(stream_ids)} stream descriptors.")

    # we will deploy in batches of 500 to avoid infinite threads creation
    batches = [stream_ids[i : i + batch_size] for i in range(0, len(stream_ids), batch_size)]

    aggregated_results: list[DeployStreamResult] = []
    for batch in batches[start_from_batch:]:
        # for each batch, we will completely deploy -> iniialize and wait for each confirmation
        futures = []
        # Map over the stream IDs using the check/deploy task.
        for stream_id in batch:
            futures.append(check_and_deploy_stream.submit(stream_id=stream_id, tna_block=tna_block, is_unix=is_unix))

        # Wait for all futures to complete
        results: list[DeployStreamResult] = []
        for future in futures:
            results.append(future.result())
        aggregated_results.extend(results)

    # aggregate results to print
    deployed = [result for result in aggregated_results if result["status"] == "deployed"]
    skipped = [result for result in aggregated_results if result["status"] == "skipped"]
    summary_results = DeployStreamResults(deployed_count=len(deployed), skipped_count=len(skipped))
    logger.info(summary_results)
    return summary_results
