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
import math
from typing import Optional

from pandera.typing import DataFrame
from prefect import flow, get_run_logger, task
import trufnetwork_sdk_py.client as tn_client
from typing_extensions import TypedDict

from tsn_adapters.blocks.deployment_state import DeploymentStateBlock

# Import the primitive source descriptor interface and data model
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)

# Import TNAccessBlock and task_wait_for_tx so that we can use its waiting functionality.
from tsn_adapters.blocks.tn_access import TNAccessBlock, task_wait_for_tx

# Import new batch tasks
from tsn_adapters.common.trufnetwork.tn import (
    task_filter_and_deploy_streams,
)

# Configuration constants
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY_SECONDS = 5
DEFAULT_MAX_STREAMS_PER_BATCH_API_CALL = 1000


class DeployStreamResults(TypedDict):
    """Aggregated results of a stream deployment operation."""

    deployed_count: int
    skipped_count: int


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


def _create_deployment_super_batches(
    definitions: list[tn_client.StreamDefinitionInput], max_streams_per_call: int
) -> list[list[tn_client.StreamDefinitionInput]]:
    """
    Split a list of stream definitions into super-batches for API calls.

    Args:
        definitions: List of stream definitions to batch
        max_streams_per_call: Maximum number of streams per batch API call

    Returns:
        List of batches, where each batch is a list of stream definitions
    """
    if not definitions:
        return []
    num_super_batches = math.ceil(len(definitions) / max_streams_per_call)
    super_batches = []
    for i in range(num_super_batches):
        start_idx = i * max_streams_per_call
        end_idx = start_idx + max_streams_per_call
        super_batches.append(definitions[start_idx:end_idx])
    return super_batches


@flow(name="Stream Deployment Flow")
def deploy_streams_flow(
    psd_block: PrimitiveSourcesDescriptorBlock,
    tna_block: TNAccessBlock,
    max_streams_per_batch_api_call: int = DEFAULT_MAX_STREAMS_PER_BATCH_API_CALL,
    deployment_state: Optional[DeploymentStateBlock] = None,
) -> DeployStreamResults:
    """
    Deploy primitive streams from a descriptor using batch SDK functions.

    This flow handles the stream deployment process:
    1. Retrieves stream descriptors.
    2. Optionally filters streams already marked deployed by DeploymentStateBlock.
    3. Filters streams by actual existence on TN using a batch call.
    4. Deploys non-existent streams in chunked "super-batches" using batch SDK calls.
    5. Updates deployment state for successfully deployed streams.
    6. Returns summary statistics.

    Args:
        psd_block: Block providing the stream descriptors.
        tna_block: Block for TN interactions.
        max_streams_per_batch_api_call: Max streams to include in a single batch API deployment call.
        deployment_state: Optional block for tracking and filtering by deployment state.

    Returns:
        Statistics on deployed and skipped streams.
    """
    logger = get_run_logger()
    logger.info(f"Starting stream deployment flow. Max streams per batch API call: {max_streams_per_batch_api_call}.")

    deployed_count = 0
    skipped_count = 0

    # SECTION 1: Retrieve and validate descriptor DataFrame
    try:
        descriptor_df: DataFrame[PrimitiveSourceDataModel] = psd_block.get_descriptor()
    except Exception as e:
        logger.error(f"Failed to retrieve stream descriptors: {e!s}", exc_info=True)
        raise

    if descriptor_df.empty:
        logger.info("No stream descriptors found. Exiting flow.")
        return DeployStreamResults(deployed_count=0, skipped_count=0)

    original_stream_count = len(descriptor_df)
    logger.info(f"Retrieved {original_stream_count} stream descriptors.")

    current_streams_to_process_df = descriptor_df.copy()

    # SECTION 2: Initial Filtering (Optional, using DeploymentStateBlock)
    if deployment_state:
        all_stream_ids_from_descriptor = [str(sid) for sid in current_streams_to_process_df["stream_id"]]
        if all_stream_ids_from_descriptor:
            logger.info(f"Checking {len(all_stream_ids_from_descriptor)} streams against DeploymentStateBlock.")
            already_deployed_sids = deployment_state.check_multiple_streams(all_stream_ids_from_descriptor)

            streams_not_in_state_block = current_streams_to_process_df[
                ~current_streams_to_process_df["stream_id"].astype(str).isin(already_deployed_sids)
            ]
            num_filtered_by_state = len(current_streams_to_process_df) - len(streams_not_in_state_block)
            skipped_count += num_filtered_by_state
            logger.info(f"Filtered out {num_filtered_by_state} streams already marked in DeploymentStateBlock.")
            current_streams_to_process_df = streams_not_in_state_block

            if current_streams_to_process_df.empty:
                logger.info("All streams from descriptor were already marked in DeploymentStateBlock. Exiting.")
                return DeployStreamResults(deployed_count=0, skipped_count=skipped_count)
        else:
            logger.info(
                "No streams from descriptor to check against DeploymentStateBlock (empty after initial load or previous filters)."
            )

    # SECTION 3: Prepare for TN Existence Check
    if current_streams_to_process_df.empty:
        logger.info("No streams left to process after DeploymentStateBlock filtering. Exiting.")
        return DeployStreamResults(deployed_count=deployed_count, skipped_count=skipped_count)

    # SECTION 5: Prepare Definitions for Batch Deployment
    initial_deployment_definitions: list[tn_client.StreamDefinitionInput] = []
    for _, row in current_streams_to_process_df.iterrows():
        initial_deployment_definitions.append(
            tn_client.StreamDefinitionInput(
                stream_id=str(row["stream_id"]), stream_type=tn_client.STREAM_TYPE_PRIMITIVE
            )
        )

    if not initial_deployment_definitions:
        logger.info("No streams definitions to process after initial filtering. Exiting.")
        return DeployStreamResults(deployed_count=deployed_count, skipped_count=skipped_count)

    # SECTION 6: Chunking for Batch API Calls (Super-Batches)
    super_batches = _create_deployment_super_batches(initial_deployment_definitions, max_streams_per_batch_api_call)
    total_super_batches = len(super_batches)
    logger.info(
        f"Prepared {len(initial_deployment_definitions)} potential streams for deployment in {total_super_batches} super-batches based on descriptor and state block."
    )

    # SECTION 7: Iterate and Deploy Super-Batches (using the new orchestrating task)
    for i, current_super_batch_definitions in enumerate(super_batches):
        batch_num_for_logging = i + 1
        logger.info(
            f"Processing super-batch {batch_num_for_logging}/{total_super_batches} "
            f"with {len(current_super_batch_definitions)} potential streams using task_filter_and_deploy_streams."
        )

        if not current_super_batch_definitions:
            logger.info(f"Super-batch {batch_num_for_logging}: No definitions to process. Skipping.")
            continue

        try:
            # Call the new orchestrating task. It handles existence check and deployment attempt.
            # We set wait_for_deployment_tx=False because we want to get the tx_hash back
            # and then explicitly wait for it in the flow, allowing the flow to manage this.
            # The user's previous direct calls to tasks also implied this pattern.
            filter_deploy_result = task_filter_and_deploy_streams(
                block=tna_block, 
                potential_definitions=current_super_batch_definitions, 
                wait_for_deployment_tx=False
            )

            # Process results from the new task
            num_actually_deployed = len(filter_deploy_result["deployed_stream_ids"])
            num_skipped_exist = len(filter_deploy_result["skipped_stream_ids_already_exist"])
            batch_tx_hash = filter_deploy_result["deployment_tx_hash"]

            if num_skipped_exist > 0:
                skipped_count += num_skipped_exist
                logger.info(f"Super-batch {batch_num_for_logging}: Skipped {num_skipped_exist} streams that already exist on TN.")

            if batch_tx_hash: # A deployment was attempted
                logger.info(
                    f"Super-batch {batch_num_for_logging}: Deployment attempt resulted in TX: {batch_tx_hash}. Waiting for confirmation."
                )
                # Explicitly wait for the transaction in the flow
                task_wait_for_tx(block=tna_block, tx_hash=batch_tx_hash)
                logger.info(f"Super-batch {batch_num_for_logging}: Transaction {batch_tx_hash} confirmed.")
                
                # If TX confirmed and streams were intended for deployment
                if num_actually_deployed > 0:
                    deployed_count += num_actually_deployed
                    successfully_deployed_ids_in_batch = filter_deploy_result["deployed_stream_ids"]
                    
                    if deployment_state and successfully_deployed_ids_in_batch:
                        batch_timestamp = datetime.now(timezone.utc)
                        mark_batch_deployed_task(
                            stream_ids=successfully_deployed_ids_in_batch,
                            deployment_state=deployment_state,
                            timestamp=batch_timestamp,
                        )
                        logger.info(
                            f"Super-batch {batch_num_for_logging}: Submitted task to mark {num_actually_deployed} streams as deployed in DeploymentStateBlock."
                        )
                elif num_actually_deployed == 0 and filter_deploy_result["deployed_stream_ids"] == []:
                     # This case means a TX was made, but it deployed zero streams from the ones filtered for deployment by the task.
                     # This could happen if all streams passed to task_batch_deploy_streams inside the new task already existed
                     # despite the prior check, or if the batch deploy call itself deployed nothing but gave a TX.
                     logger.info(f"Super-batch {batch_num_for_logging}: Deployment TX {batch_tx_hash} confirmed, but no new streams were reported as deployed by the task for this TX.")

            elif num_actually_deployed > 0 and not batch_tx_hash:
                # This case should ideally not happen if deployment means a tx_hash is always returned.
                # It implies streams were marked for deployment by the task but no tx_hash was provided.
                logger.warning(
                    f"Super-batch {batch_num_for_logging}: {num_actually_deployed} streams reported for deployment by orchestrator task, but no TX hash was returned. State not updated."
                )
            
            # If no deployment was attempted (e.g., all streams in batch already existed, or definitions_to_deploy was empty within the task)
            # and no tx_hash, then num_actually_deployed would be 0. This is fine.
            if num_actually_deployed == 0 and not batch_tx_hash:
                logger.info(f"Super-batch {batch_num_for_logging}: No new streams were deployed (either all existed or none to deploy after filtering within task). No TX hash.")

        except Exception as e:
            # This exception is from the orchestrating task task_filter_and_deploy_streams itself failing after its retries.
            logger.error(
                f"Super-batch {batch_num_for_logging}/{total_super_batches}: Orchestrating task 'task_filter_and_deploy_streams' failed: {e!s}. "
                f"Continuing to next super-batch if any.",
                exc_info=True,
            )
            # Streams in this failed super-batch (where the orchestrator task failed) are not counted.

    # SECTION 8: Return Results
    logger.info(f"Stream deployment flow finished. Deployed: {deployed_count}, Skipped: {skipped_count} streams.")
    return DeployStreamResults(deployed_count=deployed_count, skipped_count=skipped_count)
