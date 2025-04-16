"""
Stream Deployment Flow

This module contains a Prefect flow to deploy primitive streams from a generic
primitive source descriptor. For each stream in the descriptor, the flow:

1. Filters out already deployed streams (using DeploymentStateBlock if provided)
2. Checks if remaining streams exist using the TN SDK
3. Deploys non-existent streams with proper transaction handling
4. Tracks deployment results and updates deployment state

The flow uses concurrency control via Prefect tasks and appropriate wait mechanisms
for transaction confirmations.
"""

from datetime import datetime, timezone
from typing import Literal, Optional
from pandera.typing import DataFrame
from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture

# Import the TN client types and the task to deploy a primitive stream.
from typing_extensions import TypedDict

from tsn_adapters.blocks.deployment_state import DeploymentStateBlock

# Import the primitive source descriptor interface and data model
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)

# Import TNAccessBlock and task_wait_for_tx so that we can use its waiting functionality.
from tsn_adapters.blocks.tn_access import TNAccessBlock, task_wait_for_tx
from tsn_adapters.common.trufnetwork.tn import task_deploy_primitive

# Configuration constants
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY_SECONDS = 5
DEFAULT_BATCH_SIZE = 500


class DeployStreamResult(TypedDict):
    """Result of deploying or checking a single stream."""

    stream_id: str
    status: Literal["deployed", "skipped"]
    # Could add in future:
    # tx_deploy_hash: Optional[str]
    # tx_init_hash: Optional[str]
    # error: Optional[str]


class DeployStreamResults(TypedDict):
    """Aggregated results of a stream deployment operation."""

    deployed_count: int
    skipped_count: int


@task(
    name="Check and Deploy Stream",
    tags=["tn", "tn-write"],
    retries=DEFAULT_RETRY_ATTEMPTS,
    retry_delay_seconds=DEFAULT_RETRY_DELAY_SECONDS,
)
def check_deploy_stream(stream_id: str, tna_block: TNAccessBlock) -> DeployStreamResult:
    """
    Check deploy stream.

    Args:
        stream_id: ID of the stream to check and potentially deploy
        tna_block: TNAccessBlock instance for TN interactions
        is_unix: Whether to use Unix timestamps (default: False)

    Returns:
        A DeployStreamResult indicating whether the stream was deployed or skipped
    """
    logger = get_run_logger()

    # Deploy the stream if it doesn't exist
    logger.debug(f"Deploying stream {stream_id}.")

    # Create deployment transaction and wait for confirmation
    try:
        tx_deploy = task_deploy_primitive(block=tna_block, stream_id=stream_id)
        task_wait_for_tx(block=tna_block, tx_hash=tx_deploy)
        logger.debug(f"Deployed stream {stream_id} (tx: {tx_deploy}).")
    except Exception as e:
        logger.debug(f"Failed to deploy stream {stream_id}: {e}")

    return DeployStreamResult(stream_id=stream_id, status="deployed")


@task(name="Mark Batch as Deployed", retries=DEFAULT_RETRY_ATTEMPTS, retry_delay_seconds=DEFAULT_RETRY_DELAY_SECONDS)
def mark_batch_deployed_task(
    stream_ids: list[str], deployment_state: DeploymentStateBlock, timestamp: datetime
) -> None:
    """
    Mark a batch of streams as deployed in the deployment state.

    Args:
        stream_ids: List of stream IDs that were successfully deployed
        deployment_state: Block for tracking deployment state
        timestamp: UTC timestamp to use for all streams in this batch
    """
    logger = get_run_logger()

    # Skip if no streams to mark
    if not stream_ids:
        logger.debug("No streams to mark as deployed.")
        return

    # Ensure timestamp is UTC
    if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None:
        logger.warning("Received timestamp without timezone for marking deployment. Assuming UTC.")
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    elif timestamp.tzinfo != timezone.utc:
        timestamp = timestamp.astimezone(timezone.utc)

    # Mark streams as deployed with the provided timestamp
    try:
        deployment_state.mark_multiple_as_deployed(stream_ids, timestamp)
        logger.debug(f"Marked {len(stream_ids)} streams as deployed at {timestamp.isoformat()}.")
    except Exception as e:
        logger.warning(f"Failed to mark streams as deployed: {e!s}. Continuing with flow execution.", exc_info=True)


def _create_stream_batches(stream_ids: list[str], batch_size: int, start_from_batch: int) -> list[list[str]]:
    """
    Split a list of stream IDs into batches of specified size.

    Args:
        stream_ids: List of stream IDs to batch
        batch_size: Maximum number of streams per batch
        start_from_batch: Index of the first batch to process

    Returns:
        List of batches, where each batch is a list of stream IDs
    """
    # Create batches of specified size
    all_batches = [stream_ids[i : i + batch_size] for i in range(0, len(stream_ids), batch_size)]

    # Return only batches starting from the specified index
    return all_batches[start_from_batch:]


def _process_deployment_results(
    deployment_results: list[DeployStreamResult],
    deployment_state: Optional[DeploymentStateBlock] = None,
    batch_timestamp: Optional[datetime] = None,
) -> None:
    """
    Process deployment results and update deployment state if provided.

    Args:
        deployment_results: List of individual stream deployment results
        deployment_state: Optional block for tracking deployment state
        batch_timestamp: Timestamp to use for this batch (required if deployment_state is provided)
    """
    logger = get_run_logger()

    # Skip if no deployment state provided
    if deployment_state is None:
        return

    # We need a timestamp to mark streams as deployed
    if batch_timestamp is None:
        batch_timestamp = datetime.now(timezone.utc)

    # Collect successfully deployed stream IDs
    deployed_stream_ids = [result["stream_id"] for result in deployment_results if result["status"] == "deployed"]

    # Skip if no streams were deployed
    if not deployed_stream_ids:
        return

    # Mark batch as deployed in deployment state
    try:
        mark_batch_deployed_task.submit(
            stream_ids=deployed_stream_ids, deployment_state=deployment_state, timestamp=batch_timestamp
        )
    except Exception as e:
        logger.warning(f"Failed to submit marking task: {e!s}. Continuing with next batch.")


@flow(name="Stream Deployment Flow")
def deploy_streams_flow(
    psd_block: PrimitiveSourcesDescriptorBlock,
    tna_block: TNAccessBlock,
    batch_size: int = DEFAULT_BATCH_SIZE,
    start_from_batch: int = 0,
    deployment_state: Optional[DeploymentStateBlock] = None,
) -> DeployStreamResults:
    """
    Deploy primitive streams from a descriptor, with optional deployment state tracking.

    This flow handles the entire stream deployment process:
    1. Retrieves stream descriptors from the provided block
    2. Deploys streams in batches with concurrency control
    3. Updates deployment state for successfully deployed streams
    4. Returns summary statistics of deployed and skipped streams

    Args:
        psd_block: Block providing the stream descriptors
        tna_block: Block for TN interactions
        is_unix: Whether to use Unix timestamps (default: False)
        batch_size: Number of streams to deploy in each batch (default: 500)
        start_from_batch: Batch number to start from (default: 0)
        deployment_state: Optional block for tracking deployment state

    Returns:
        Statistics on deployed and skipped streams
    """
    logger = get_run_logger()
    logger.info(
        f"Starting stream deployment flow. Batch size: {batch_size}, Start batch: {start_from_batch}."
    )

    # SECTION 1: Retrieve and validate descriptor DataFrame
    try:
        descriptor_df: DataFrame[PrimitiveSourceDataModel] = psd_block.get_descriptor()
    except Exception as e:
        logger.error(f"Failed to retrieve stream descriptors: {e!s}", exc_info=True)
        raise  # Cannot proceed without descriptors

    if descriptor_df.empty:
        logger.info("No stream descriptors found. Exiting flow.")
        return DeployStreamResults(deployed_count=0, skipped_count=0)

    # Track original stream count for calculating filtered count
    original_stream_count = len(descriptor_df)
    logger.info(f"Retrieved {original_stream_count} stream descriptors.")
    filtered_by_state_count = 0

    # SECTION 2: Prepare for batch processing
    # Extract stream IDs from filtered descriptor and ensure proper typing
    stream_ids: list[str] = [str(sid) for sid in descriptor_df["stream_id"]]

    logger.info(f"Found {len(stream_ids)} stream descriptors to process.")

    # Create batches for processing
    batches = _create_stream_batches(stream_ids, batch_size, start_from_batch)
    total_batches = len(batches)

    # Check if start_from_batch is out of range
    if start_from_batch >= total_batches and total_batches > 0:
        logger.warning(
            f"Start batch {start_from_batch} is out of range (total batches: {total_batches}). "
            f"No batches will be processed."
        )
        return DeployStreamResults(deployed_count=0, skipped_count=filtered_by_state_count)
    elif start_from_batch > 0:
        logger.info(f"Starting processing from batch {start_from_batch+1}/{total_batches}.")

    # SECTION 3: Process batches
    all_deployment_results: list[DeployStreamResult] = []

    for batch_index, batch in enumerate(batches, start=start_from_batch):
        # Log batch progress
        logger.info(f"Processing batch {batch_index + 1}/{total_batches} with {len(batch)} streams.")

        # Process each stream in the batch, in parallel
        batch_futures: dict[str, PrefectFuture[DeployStreamResult]] = {}
        for stream_id in batch:
            # Submit the task with the new name
            future = check_deploy_stream.submit(stream_id=stream_id, tna_block=tna_block)
            batch_futures[stream_id] = future

        # Collect batch results
        batch_results: list[DeployStreamResult] = []
        for stream_id, future in batch_futures.items():
            try:
                result = future.result()
                batch_results.append(result)
            except Exception as e:
                logger.error(f"Task failed for stream {stream_id} even after retries: {e!s}", exc_info=True)
                # Continue with other streams even if one fails

        # Add to overall results
        all_deployment_results.extend(batch_results)

        # Update deployment state if provided
        if deployment_state is not None:
            batch_timestamp = datetime.now(timezone.utc)
            _process_deployment_results(batch_results, deployment_state, batch_timestamp)

        logger.info(f"Finished processing batch {batch_index + 1}/{total_batches}.")

    # SECTION 4: Aggregate and summarize results
    deployed_results = [result for result in all_deployment_results if result["status"] == "deployed"]
    skipped_during_deploy = [result for result in all_deployment_results if result["status"] == "skipped"]

    # Calculate summary statistics
    deployed_count = len(deployed_results)
    skipped_during_processing = len(skipped_during_deploy)
    total_skipped_count = skipped_during_processing + filtered_by_state_count

    # Prepare final results
    summary_results = DeployStreamResults(deployed_count=deployed_count, skipped_count=total_skipped_count)

    # Log summary
    logger.info(
        f"Deployment summary: {summary_results['deployed_count']} deployed, "
        f"{summary_results['skipped_count']} skipped "
        f"({filtered_by_state_count} filtered by deployment state, "
        f"{skipped_during_processing} skipped during processing)."
    )

    return summary_results
