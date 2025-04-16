"""
Stream Deployment Flow

This module defines a Prefect flow for robust, idempotent deployment of primitive streams
using a batch-oriented, state-aware approach. It supports both detailed state checking
(for efficiency) and a simple deploy+init mode (for simplicity), and integrates with
an optional deployment state block to avoid redundant work.
"""

from datetime import datetime, timezone
from typing import Any, Literal, Optional

from pandera.typing import DataFrame
from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture

# Import the TN client types and the task to deploy a primitive stream.
from typing_extensions import TypedDict

from tsn_adapters.blocks.deployment_state import DeploymentStateBlock

# Import the primitive source descriptor interface and data model
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourcesDescriptorBlock,
)

# Import state checking task and helpers
from tsn_adapters.blocks.stream_filtering import (
    task_get_stream_states_divide_conquer,
)

# Import TNAccessBlock and task_wait_for_tx so that we can use its waiting functionality.
# Import retry helpers
from tsn_adapters.blocks.tn_access import (
    DataFrame,
    StreamAlreadyExistsError,
    StreamAlreadyInitializedError,  # Re-import locally if needed
    StreamLocatorModel,  # Re-import locally if needed
    TNAccessBlock,
    create_empty_stream_locator_df,
    task_wait_for_tx,
)
from tsn_adapters.common.trufnetwork.models.tn_models import StreamLocatorModel
from tsn_adapters.common.trufnetwork.tn import task_deploy_primitive, task_init_stream
from tsn_adapters.utils import cast_future

# Configuration constants
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY_SECONDS = 5
DEFAULT_BATCH_SIZE = 500


# --- Internal Helper Functions for Task Submission ---


@task(
    name="Check Deployment State",
    retries=DEFAULT_RETRY_ATTEMPTS,
    retry_delay_seconds=DEFAULT_RETRY_DELAY_SECONDS,
    tags=["deployment-state"],
)
def task_check_deployment_state(deployment_state: DeploymentStateBlock, stream_ids: list[str]) -> dict[str, bool]:
    """
    Checks which streams are already marked as deployed in the deployment state block.

    Args:
        deployment_state: Block tracking deployment status.
        stream_ids: List of stream IDs to check.

    Returns:
        Dict mapping stream IDs to booleans (True if deployed).

    Why:
        Used to efficiently skip streams that are already deployed, minimizing unnecessary network calls.
    """
    logger = get_run_logger()
    if not stream_ids:
        logger.debug("No stream IDs provided to check deployment state.")
        return {}
    try:
        logger.debug(f"Checking deployment state for {len(stream_ids)} streams.")
        result = deployment_state.check_multiple_streams(stream_ids)
        logger.debug("Deployment state check complete.")
        return result
    except Exception as e:
        logger.error(f"Failed to check deployment state: {e!s}", exc_info=True)
        raise e


@task(
    retries=3,
    retry_delay_seconds=5,
    name="check_exist_deploy_init",
    tags=["deploy", "init"],
)
def task_check_exist_deploy_init(
    stream_id: str,
    tna_block: TNAccessBlock,
    is_unix: bool = False,
    force_init_only: bool = False,
) -> Literal["deployed", "initialized"]:
    """
    Deploys and/or initializes a stream, handling idempotency and state.

    Args:
        stream_id: Stream identifier.
        tna_block: TNAccessBlock for network operations.
        is_unix: Whether the stream uses Unix timestamps.
        force_init_only: If True, skips deploy and only attempts initialization.

    Returns:
        "deployed" if deploy+init succeeded,
        "initialized" if only init was needed (already existed or force_init_only).

    Raises:
        Exception: Any unexpected error will propagate and fail the task (and flow),
        rather than returning a string literal for failure.

    Why:
        Ensures streams are always initialized, even if already deployed, and allows
        callers to skip deploy if prior state checks guarantee it.
    """
    logger = get_run_logger()
    try:
        if force_init_only:
            logger.info(f"Stream {stream_id}: force_init_only=True, skipping deploy and attempting initialization.")
            init_future = task_init_stream.submit(stream_id=stream_id, block=tna_block, return_state=False, wait=False)
            init_wait_future = task_wait_for_tx.submit(tx_hash=cast_future(init_future), block=tna_block)
            _ = init_wait_future.result(raise_on_failure=True)
            return "initialized"
        try:
            deploy_future = task_deploy_primitive.submit(
                stream_id=stream_id, block=tna_block, is_unix=is_unix, wait=False
            )
            deploy_wait_future = task_wait_for_tx.submit(tx_hash=cast_future(deploy_future), block=tna_block)
            init_future = task_init_stream.submit(
                stream_id=stream_id,
                block=tna_block,
                return_state=False,
                wait_for=cast_future(deploy_wait_future),
                wait=False,
            )
            task_wait_for_tx(tx_hash=cast_future(init_future), block=tna_block)
            return "deployed"
        except StreamAlreadyExistsError:
            logger.info(f"Stream {stream_id} already exists, attempting initialization.")
            init_future = task_init_stream.submit(stream_id=stream_id, block=tna_block, return_state=False, wait=False)
            task_wait_for_tx(tx_hash=cast_future(init_future), block=tna_block)
            return "initialized"
    except StreamAlreadyInitializedError:
        logger.info(f"Stream {stream_id} already initialized.")
        return "initialized"
    except Exception as e:
        logger.error(f"Failed to deploy/init stream {stream_id}: {e}")
        raise e


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


@flow(name="Stream Deployment Flow")
def deploy_streams_flow(
    psd_block: PrimitiveSourcesDescriptorBlock,
    tna_block: TNAccessBlock,
    check_stream_state: bool = True,
    is_unix: bool = False,
    max_filter_size: int = 1000,
    batch_size: int = DEFAULT_BATCH_SIZE,
    start_from_batch: int = 0,
    deployment_state: Optional[DeploymentStateBlock] = None,
) -> DeployStreamResults:
    """
    Deploys primitive streams in batches, using state-aware logic for idempotency and efficiency.

    Args:
        psd_block: Provides stream descriptors.
        tna_block: Truflation Network access block.
        check_stream_state: If True, checks each stream's detailed state against TN to minimize unnecessary deploy/init.
        is_unix: Whether streams use Unix timestamps.
        batch_size: Number of streams per batch.
        start_from_batch: Batch index to start from.
        deployment_state: Optional block to track and skip already deployed streams.

    Returns:
        DeployStreamResults: Counts of deployed and skipped streams.

    Why:
        This flow ensures robust, efficient, and idempotent deployment of streams, supporting both
        high-throughput and fine-grained state management scenarios.
    """
    logger = get_run_logger()
    start_time = datetime.now(timezone.utc)
    logger.info(f"Starting stream deployment flow at {start_time.isoformat()}...")
    logger.info(
        f"Parameters: check_stream_state={check_stream_state}, is_unix={is_unix}, "
        f"batch_size={batch_size}, start_from_batch={start_from_batch}, "
        f"deployment_state provided: {deployment_state is not None}"
    )

    # Retrieve all stream descriptors to process.
    try:
        descriptor_df = psd_block.get_descriptor()
        if descriptor_df.empty:
            logger.info("No primitive sources found in the descriptor block. Exiting.")
            return DeployStreamResults(deployed_count=0, skipped_count=0)
        logger.info(f"Retrieved {len(descriptor_df)} stream descriptors.")
    except Exception as e:
        logger.error(f"Failed to retrieve primitive sources: {e!s}", exc_info=True)
        # Cannot proceed without descriptors
        raise e

    total_streams = len(descriptor_df)
    total_deployed = 0
    total_skipped_state = 0
    total_skipped_already_initialized = 0

    num_batches = (len(descriptor_df) + batch_size - 1) // batch_size
    logger.info(f"Processing {total_streams} streams in {num_batches} batches of size {batch_size}.")

    # 3. Iterate through batches
    for i in range(start_from_batch, num_batches):
        batch_start_time = datetime.now(timezone.utc)
        logger.info(f"Processing batch {i+1}/{num_batches}...")

        # Get the current batch DataFrame
        start_index = i * batch_size
        end_index = start_index + batch_size
        current_batch_df = descriptor_df.iloc[start_index:end_index].copy()
        batch_stream_ids = current_batch_df["stream_id"].tolist()
        logger.debug(
            f"Batch {i+1} contains {len(current_batch_df)} streams: {batch_stream_ids[:5]}{'...' if len(batch_stream_ids) > 5 else ''}"
        )

        # Initialize batch-specific variables
        filtered_batch_df = current_batch_df  # Start with the full batch
        skipped_in_batch_state = 0
        successfully_processed_in_batch_ids: list[str] = []  # To track success for marking state later

        # Batch-Level State Block Filtering:
        # Skip streams already marked as deployed in the deployment state block to avoid redundant work.
        if deployment_state is not None:
            logger.debug(f"Checking deployment state for batch {i+1}...")
            deployed_status_dict: dict[str, bool] = {}
            try:
                deployed_status_dict = task_check_deployment_state(
                    deployment_state=deployment_state, stream_ids=batch_stream_ids
                )
                logger.debug(f"State check successful for batch {i+1}.")

                # Filter out streams already marked as deployed
                already_deployed_ids = {stream_id for stream_id, deployed in deployed_status_dict.items() if deployed}

                if already_deployed_ids:
                    initial_batch_count = len(filtered_batch_df)
                    filtered_batch_df = filtered_batch_df[~filtered_batch_df["stream_id"].isin(already_deployed_ids)]
                    skipped_in_batch_state = initial_batch_count - len(filtered_batch_df)
                    total_skipped_state += skipped_in_batch_state
                    logger.info(
                        f"Batch {i+1}: Filtered out {skipped_in_batch_state} streams based on deployment state. "
                        f"{len(filtered_batch_df)} streams remain for processing in this batch."
                    )
                else:
                    logger.debug(f"Batch {i+1}: No streams were marked as deployed in the state block.")

            except Exception as e:
                logger.warning(
                    f"Failed to check deployment state for batch {i+1} after retries: {e!s}. "
                    f"Proceeding without state filtering for this batch.",
                    exc_info=True,
                )
                # Keep filtered_batch_df as the original current_batch_df

        # Check if any streams remain in the batch after filtering
        if filtered_batch_df.empty:
            logger.info(
                f"Batch {i+1}: All streams were filtered out by the deployment state block. Skipping to next batch."
            )
            continue

        # --- Conditional Stream Processing based on check_stream_state ---
        final_wait_futures_map: dict[str, PrefectFuture[Any]] = {}

        if check_stream_state:
            logger.info(f"Batch {i+1}: Processing with check_stream_state=True (Detailed check)")
            try:
                # Build StreamLocatorModel DataFrame for the batch.
                if not filtered_batch_df.empty:
                    batch_locators_df = DataFrame[StreamLocatorModel](
                        {
                            "stream_id": filtered_batch_df["stream_id"],
                            "data_provider": tna_block.current_account,
                        }
                    )
                else:
                    batch_locators_df = create_empty_stream_locator_df()

                if not batch_locators_df.empty:
                    logger.debug(
                        f"Batch {i+1}: Calling task_get_stream_states_divide_conquer for {len(batch_locators_df)} streams."
                    )
                    # Partition streams by detailed state to minimize unnecessary deploy/init.
                    stream_states = task_get_stream_states_divide_conquer(
                        block=tna_block,
                        stream_locators=batch_locators_df,
                        max_filter_size=max_filter_size,
                    )
                    logger.debug(
                        f"Batch {i+1}: State check result: "
                        f"NonDeployed={len(stream_states['non_deployed'])}, "
                        f"DeployedButNotInit={len(stream_states['deployed_but_not_initialized'])}, "
                        f"DeployedAndInit={len(stream_states['deployed_and_initialized'])}"
                    )

                    # Count streams already deployed and initialized, so we can report them as skipped.
                    deployed_and_init_df = stream_states["deployed_and_initialized"]
                    skipped_in_batch_already_initialized = len(deployed_and_init_df)
                    total_skipped_already_initialized += skipped_in_batch_already_initialized

                    # Submit deploy+init for non-deployed streams.
                    non_deployed_df = stream_states["non_deployed"]
                    if not non_deployed_df.empty:
                        logger.info(
                            f"Batch {i+1}: Submitting deploy+init for {len(non_deployed_df)} non-deployed streams."
                        )
                        for stream_id in non_deployed_df["stream_id"].tolist():
                            final_wait_future = task_check_exist_deploy_init.submit(
                                stream_id=stream_id,
                                tna_block=tna_block,
                                is_unix=is_unix,
                                force_init_only=False,
                            )
                            final_wait_futures_map[stream_id] = final_wait_future

                    # Submit init only for deployed but not initialized streams.
                    dep_not_init_df = stream_states["deployed_but_not_initialized"]
                    if not dep_not_init_df.empty:
                        logger.info(
                            f"Batch {i+1}: Submitting init for {len(dep_not_init_df)} deployed but not initialized streams."
                        )
                        for stream_id in dep_not_init_df["stream_id"].tolist():
                            final_wait_future = task_check_exist_deploy_init.submit(
                                stream_id=stream_id,
                                tna_block=tna_block,
                                is_unix=is_unix,
                                force_init_only=True,
                            )
                            final_wait_futures_map[stream_id] = final_wait_future

                else:
                    logger.info(f"Batch {i+1}: No streams remaining in batch after state filtering for detailed check.")

            except Exception as state_check_err:
                logger.error(
                    f"Batch {i+1}: Failed during detailed stream state check (task_get_stream_states_divide_conquer): {state_check_err!s}. Skipping processing for this batch.",
                    exc_info=True,
                )

        else:
            logger.info(f"Batch {i+1}: Processing with check_stream_state=False (Direct deploy/init)")
            logger.info(f"Batch {i+1}: Submitting direct deploy/init tasks for {len(filtered_batch_df)} streams.")
            for _, row in filtered_batch_df.iterrows():
                stream_id = row["stream_id"]
                final_wait_future = task_check_exist_deploy_init.submit(
                    stream_id=stream_id,
                    tna_block=tna_block,
                    is_unix=is_unix,
                    force_init_only=False,
                )
                final_wait_futures_map[stream_id] = final_wait_future

        # Unified Waiting Logic:
        # Wait for all submitted deploy/init tasks to complete, and track which streams succeeded.
        if final_wait_futures_map:
            logger.info(f"Batch {i+1}: Waiting for results of {len(final_wait_futures_map)} submitted tasks...")
            stream_final_status: dict[str, bool] = {}

            for stream_id, future in final_wait_futures_map.items():
                try:
                    result = future.result(raise_on_failure=True)
                    if result in ("deployed", "initialized"):
                        stream_final_status[stream_id] = True
                        logger.debug(f"Batch {i+1}: Stream {stream_id} processed successfully ({result}).")
                except Exception as wait_err:
                    logger.error(f"Batch {i+1}: Failed processing stream {stream_id}: {wait_err!s}", exc_info=True)
                    # Fail fast: re-raise the exception to fail the flow
                    raise

            successfully_processed_in_batch_ids = [sid for sid, success in stream_final_status.items() if success]
            failed_count_batch = len(final_wait_futures_map) - len(successfully_processed_in_batch_ids)
            logger.info(
                f"Batch {i+1}: Processing complete. Successfully processed: {len(successfully_processed_in_batch_ids)}, Failed: {failed_count_batch}"
            )
        else:
            logger.info(f"Batch {i+1}: No tasks submitted or all submission failed.")

        # Mark Batch Deployed:
        # Update the deployment state block for all streams successfully processed in this batch.
        if deployment_state and successfully_processed_in_batch_ids:
            logger.debug(
                f"Batch {i+1}: Marking {len(successfully_processed_in_batch_ids)} streams as deployed in state block."
            )
            current_time = datetime.now(timezone.utc)
            mark_batch_deployed_task.submit(
                stream_ids=successfully_processed_in_batch_ids,
                deployment_state=deployment_state,
                timestamp=current_time,
            )
        elif deployment_state:
            logger.debug(f"Batch {i+1}: No streams successfully processed in this batch, skipping state marking.")

        # Aggregate results for the batch:
        # Update running totals for deployed and skipped streams.
        deployed_in_batch = len(successfully_processed_in_batch_ids)
        total_deployed += deployed_in_batch
        logger.info(
            f"Batch {i+1} aggregated results - Deployed/Initialized this batch: {deployed_in_batch}, Skipped by State: {skipped_in_batch_state}"
        )

        batch_end_time = datetime.now(timezone.utc)
        batch_duration = batch_end_time - batch_start_time
        logger.info(f"Finished processing batch {i+1}/{num_batches}. Duration: {batch_duration}")

    # Final Result Aggregation:
    # The final skipped count includes those skipped by state block and those skipped by TSN checks (already deployed+initialized).
    final_skipped_count = total_skipped_state + total_skipped_already_initialized

    end_time = datetime.now(timezone.utc)
    duration = end_time - start_time
    logger.info(f"Stream deployment flow finished at {end_time.isoformat()}. Duration: {duration}.")
    logger.info(
        f"Flow Summary: Deployed/Initialized={total_deployed}, "
        f"Skipped(StateBlock)={total_skipped_state}, "
        f"Skipped(AlreadyInitialized)={total_skipped_already_initialized}, "
        f"Total Skipped={final_skipped_count} (Total Initially={total_streams})"
    )

    return DeployStreamResults(deployed_count=total_deployed, skipped_count=final_skipped_count)
