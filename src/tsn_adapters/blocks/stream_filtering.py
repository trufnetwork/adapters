"""
Stream filtering utilities for Truflation Stream Network.

This module provides functions for filtering streams based on their initialization status
using various strategies, including a divide-and-conquer approach.
"""

import traceback
from typing import Any, Optional, cast, TypedDict

import pandas as pd
from prefect import get_run_logger, task

from tsn_adapters.blocks.shared_types import DivideAndConquerResult
from tsn_adapters.blocks.tn_access import (
    DataFrame,
    MetadataProcedureNotFoundError,
    StreamLocatorModel,
    TNAccessBlock,
    append_to_df,
    convert_to_typed_df,
    create_empty_df,
    create_empty_stream_locator_df,
    diff_stream_locator_sets,
    get_stream_locator_set,
    task_filter_batch_initialized_streams,
    UNUSED_INFINITY_RETRIES,
    tn_special_retry_condition,
)


class StreamStatesResult(TypedDict):
    """Result of the stream state checking operation."""

    non_deployed: DataFrame[StreamLocatorModel]
    deployed_but_not_initialized: DataFrame[StreamLocatorModel]
    deployed_and_initialized: DataFrame[StreamLocatorModel]
    depth: int
    fallback_used: bool


def fallback_method_filter_streams(
    block: TNAccessBlock,
    stream_locators: DataFrame[StreamLocatorModel],
    current_depth: int,
    logger: Any,
) -> DivideAndConquerResult:
    """
    Filter streams using a fallback method, checking one by one.

    This is used when batch method fails or for smaller batches.

    Args:
        block: TNAccessBlock instance
        stream_locators: DataFrame of stream locators
        current_depth: Current recursion depth
        logger: Logger instance

    Returns:
        DivideAndConquerResult with initialized and uninitialized streams
    """
    logger.debug(f"Using fallback method for {len(stream_locators)} streams at depth {current_depth}")

    # Initialize empty DataFrames for results
    all_exist: DataFrame[StreamLocatorModel] = create_empty_stream_locator_df()
    all_uninitialized: DataFrame[StreamLocatorModel] = create_empty_stream_locator_df()
    all_initialized: DataFrame[StreamLocatorModel] = create_empty_stream_locator_df()
    # Check existence for each stream
    for _, row in stream_locators.iterrows():
        exists = block.stream_exists(data_provider=row["data_provider"], stream_id=row["stream_id"])
        if exists:
            all_exist = cast(DataFrame[StreamLocatorModel], append_to_df(all_exist, row))
        else:
            all_uninitialized = cast(DataFrame[StreamLocatorModel], append_to_df(all_uninitialized, row))


    # check the using batch since now we know which streams exist
    if not all_exist.empty:
        batch_result = task_filter_batch_initialized_streams(block=block, stream_locators=all_exist)

        if not batch_result.empty:
            # add to initialized
            all_initialized = cast(DataFrame[StreamLocatorModel], pd.concat([all_initialized, batch_result]))
    
    # append the ones that exist but are not initialized to the uninitialized
    exist_but_uninitialized = all_exist[~all_exist["stream_id"].isin(all_initialized["stream_id"])]
    all_uninitialized = cast(DataFrame[StreamLocatorModel], pd.concat([all_uninitialized, exist_but_uninitialized]))

    # Convert results to typed DataFrames
    initialized_streams = convert_to_typed_df(all_initialized, StreamLocatorModel)
    uninitialized_streams = convert_to_typed_df(all_uninitialized, StreamLocatorModel)

    return DivideAndConquerResult(
        initialized_streams=initialized_streams,
        uninitialized_streams=uninitialized_streams,
        depth=current_depth,
        fallback_used=True,
    )


def batch_method_filter_streams(
    block: TNAccessBlock,
    stream_locators: DataFrame[StreamLocatorModel],
    current_depth: int,
    logger: Any,
) -> Optional[DivideAndConquerResult]:
    """
    Filter streams using the batch method.

    Args:
        block: TNAccessBlock instance
        stream_locators: DataFrame of stream locators to check
        current_depth: Current recursion depth
        logger: Logger instance

    Returns:
        DivideAndConquerResult with initialized and uninitialized streams, or None if batch method fails
    """
    try:
        # Try to use the batch method
        logger.debug(f"Attempting batch method for {len(stream_locators)} streams")
        batch_result = task_filter_batch_initialized_streams(block=block, stream_locators=stream_locators)

        # Process the batch result
        initialized_df = pd.DataFrame(batch_result)
        logger.debug(f"Batch method result: {len(initialized_df)} initialized streams")

        # If we got results, convert to the expected format
        if not initialized_df.empty:
            # Create dataframe of uninitialized streams by finding the difference
            all_streams = get_stream_locator_set(stream_locators)
            initialized_set = get_stream_locator_set(initialized_df)
            uninitialized_data = diff_stream_locator_sets(all_streams, initialized_set)

            uninitialized_df = (
                pd.DataFrame(uninitialized_data)
                if uninitialized_data
                else create_empty_df(["data_provider", "stream_id"])
            )

            # Convert to the expected types
            initialized_streams = convert_to_typed_df(initialized_df, StreamLocatorModel)
            uninitialized_streams = convert_to_typed_df(uninitialized_df, StreamLocatorModel)

            logger.info(
                f"Batch method success: {len(initialized_streams)} initialized, "
                f"{len(uninitialized_streams)} uninitialized streams"
            )

            return DivideAndConquerResult(
                initialized_streams=initialized_streams,
                uninitialized_streams=uninitialized_streams,
                depth=current_depth,
                fallback_used=False,
            )
        return None
    except MetadataProcedureNotFoundError as e:
        # this is expected if the stream does not exist, we let the fallback handle this bundle
        logger.debug(f"MetadataProcedureNotFoundError encountered: {e!s}, proceeding with divide and conquer")
        return None
    except Exception as e:
        # For other errors, propagate the exception up
        logger.error(f"Batch method failed with an unexpected error: {e!s}")
        # Capture detailed exception info for debugging
        logger.debug(f"Unexpected error details: {traceback.format_exc()}")
        raise e


def combine_divide_conquer_results(
    left_result: DivideAndConquerResult,
    right_result: DivideAndConquerResult,
    logger: Any,
) -> DivideAndConquerResult:
    """
    Combine the results of two divide-and-conquer operations.

    Args:
        left_result: Result from the left half
        right_result: Result from the right half
        logger: Logger instance

    Returns:
        Combined DivideAndConquerResult
    """
    # Combine the results
    initialized_streams_list = [left_result["initialized_streams"], right_result["initialized_streams"]]

    uninitialized_streams_list = [left_result["uninitialized_streams"], right_result["uninitialized_streams"]]

    # Filter out empty DataFrames before concatenation to avoid pandas warnings
    initialized_streams_list = [df for df in initialized_streams_list if not df.empty]
    uninitialized_streams_list = [df for df in uninitialized_streams_list if not df.empty]

    if initialized_streams_list:
        initialized_streams = pd.concat(initialized_streams_list)
    else:
        initialized_streams = create_empty_df(["data_provider", "stream_id"])

    if uninitialized_streams_list:
        uninitialized_streams = pd.concat(uninitialized_streams_list)
    else:
        uninitialized_streams = create_empty_df(["data_provider", "stream_id"])

    # Convert to typed DataFrames
    initialized_streams_typed = convert_to_typed_df(initialized_streams, StreamLocatorModel)
    uninitialized_streams_typed = convert_to_typed_df(uninitialized_streams, StreamLocatorModel)

    # Determine if fallback was used in either branch
    fallback_used = left_result["fallback_used"] or right_result["fallback_used"]

    logger.info(
        f"Divide-and-conquer result: {len(initialized_streams_typed)} initialized, "
        f"{len(uninitialized_streams_typed)} uninitialized streams, fallback used: {fallback_used}"
    )

    return DivideAndConquerResult(
        initialized_streams=initialized_streams_typed,
        uninitialized_streams=uninitialized_streams_typed,
        depth=max(left_result["depth"], right_result["depth"]),
        fallback_used=fallback_used,
    )


@task
def task_filter_streams_divide_conquer(
    block: TNAccessBlock,
    stream_locators: DataFrame[StreamLocatorModel],
    max_depth: int = 10,
    current_depth: int = 0,
    force_fallback: bool = False,
    max_filter_size: int = 5000,
) -> DivideAndConquerResult:
    """
    Filter streams using a divide-and-conquer approach.

    This function recursively divides the stream locators into smaller batches
    until it can efficiently determine which streams are initialized.

    Args:
        block: TNAccessBlock instance
        stream_locators: DataFrame of stream locators to check
        max_depth: Maximum recursion depth
        current_depth: Current recursion depth
        force_fallback: Force using the fallback method
        max_filter_size: Maximum number of streams to process in a single batch

    Returns:
        DivideAndConquerResult with initialized and uninitialized streams
    """
    logger = get_run_logger()
    stream_ids: list[str] = stream_locators["stream_id"].tolist() if not stream_locators.empty else []
    logger.info(
        f"Filtering {len(stream_locators)} streams at depth {current_depth}: {stream_ids[:5]}{'...' if len(stream_ids) > 5 else ''}"
    )

    # Base case: empty input
    if len(stream_locators) == 0:
        empty_df = create_empty_stream_locator_df()
        logger.info("No streams to filter, returning empty results")
        return DivideAndConquerResult(
            initialized_streams=empty_df, uninitialized_streams=empty_df, depth=current_depth, fallback_used=False
        )

    # Base case: max depth reached or only one stream left
    if current_depth >= max_depth or len(stream_locators) == 1:
        return fallback_method_filter_streams(block, stream_locators, current_depth, logger)

    # If the number of streams exceeds max_filter_size, skip the batch method and go straight to divide-and-conquer
    if not force_fallback and len(stream_locators) <= max_filter_size:
        # Try the batch method first, if it succeeds, return the result
        batch_result = batch_method_filter_streams(block, stream_locators, current_depth, logger)
        if batch_result:
            return batch_result

    # If batch method failed with get_metadata error or was skipped, use divide and conquer
    if len(stream_locators) > 1:
        # Log if we're skipping the batch method due to size
        if len(stream_locators) > max_filter_size:
            logger.info(
                f"Batch size ({len(stream_locators)}) exceeds max_filter_size ({max_filter_size}), splitting into smaller batches"
            )

        # Split the streams into two halves
        mid = len(stream_locators) // 2
        left_half = stream_locators.iloc[:mid]
        right_half = stream_locators.iloc[mid:]

        left_ids: list[str] = left_half["stream_id"].tolist() if not left_half.empty else []
        right_ids: list[str] = right_half["stream_id"].tolist() if not right_half.empty else []
        logger.debug(
            f"Split into left ({len(left_half)}): {left_ids[:3]}{'...' if len(left_ids) > 3 else ''}, "
            f"right ({len(right_half)}): {right_ids[:3]}{'...' if len(right_ids) > 3 else ''}"
        )

        # Process each half recursively
        try:
            # Convert DataFrames to proper typed DataFrames for recursive calls
            left_half_typed = left_half
            right_half_typed = right_half

            left = task_filter_streams_divide_conquer.submit(
                block=block,
                stream_locators=left_half_typed,
                max_depth=max_depth,
                current_depth=current_depth + 1,
                max_filter_size=max_filter_size,
            )

            right = task_filter_streams_divide_conquer.submit(
                block=block,
                stream_locators=right_half_typed,
                max_depth=max_depth,
                current_depth=current_depth + 1,
                max_filter_size=max_filter_size,
            )

            left_result = left.result()
            right_result = right.result()

            return combine_divide_conquer_results(left_result, right_result, logger)

        except Exception as e:
            logger.error(f"Error in divide-and-conquer recursive call: {e!s}")
            # Capture detailed exception info for debugging
            logger.debug(f"Recursive call error details: {traceback.format_exc()}")
            raise e
    else:
        # If we have just one stream and the batch method failed, use the fallback
        logger.info(
            f"Single stream and batch method failed, using fallback for stream: {stream_locators.iloc[0]['stream_id']}"
        )
        return task_filter_streams_divide_conquer(
            block=block,
            stream_locators=stream_locators,
            max_depth=max_depth,
            current_depth=current_depth,
            force_fallback=True,
            max_filter_size=max_filter_size,
        )


@task(
    name="Get Stream States (Divide & Conquer)",
    retries=UNUSED_INFINITY_RETRIES,
    retry_delay_seconds=10,
    retry_condition_fn=tn_special_retry_condition(3),
)
def task_get_stream_states_divide_conquer(
    block: TNAccessBlock,
    stream_locators: DataFrame[StreamLocatorModel],
    max_depth: int = 10,
    current_depth: int = 0,
    force_fallback: bool = False,
    max_filter_size: int = 5000,
) -> StreamStatesResult:
    """
    Determine the precise state of streams using a divide-and-conquer approach.

    States:
    - non_deployed: The stream ID does not exist on the network for the provider.
    - deployed_but_not_initialized: The stream exists but has not been initialized.
    - deployed_and_initialized: The stream exists and has been initialized.

    Args:
        block: TNAccessBlock instance for network interactions.
        stream_locators: DataFrame containing stream IDs and providers to check.
        max_depth: Maximum recursion depth to prevent infinite loops.
        current_depth: Current recursion depth.
        force_fallback: If True, forces the use of the fallback method (checking one by one).
        max_filter_size: Threshold above which the batch method is skipped initially.

    Returns:
        A StreamStatesResult TypedDict containing partitioned DataFrames for each state.
    """
    logger = get_run_logger()
    logger.info(
        f"Getting stream states for {len(stream_locators)} streams at depth {current_depth}. "
        f"Max depth: {max_depth}, Force fallback: {force_fallback}, Max filter size: {max_filter_size}"
    )

    # Base case: empty input
    if stream_locators.empty:
        logger.info("Input stream_locators is empty, returning empty result.")
        empty_df = create_empty_stream_locator_df()
        return StreamStatesResult(
            non_deployed=empty_df,
            deployed_but_not_initialized=empty_df,
            deployed_and_initialized=empty_df,
            depth=current_depth,
            fallback_used=False,
        )

    # --- Batch Attempt (Step 4) ---
    if not force_fallback and len(stream_locators) <= max_filter_size:
        try:
            logger.debug(
                f"Attempting batch method (task_filter_batch_initialized_streams) for {len(stream_locators)} streams."
            )
            # This task returns only the *initialized* streams from the input
            initialized_df = task_filter_batch_initialized_streams(block=block, stream_locators=stream_locators)

            logger.debug(f"Batch method returned {len(initialized_df)} initialized streams.")

            # Since the batch call succeeding implies existence, all streams in the input
            # are either deployed_and_initialized or deployed_but_not_initialized.
            # Non-deployed streams would cause MetadataProcedureNotFoundError.

            # Identify initialized streams
            deployed_and_initialized_df = convert_to_typed_df(initialized_df, StreamLocatorModel)

            # Identify uninitialized (but existing) streams
            all_input_set = get_stream_locator_set(stream_locators)
            initialized_set = get_stream_locator_set(deployed_and_initialized_df)
            uninitialized_data = diff_stream_locator_sets(all_input_set, initialized_set)

            deployed_but_not_initialized_df = convert_to_typed_df(
                pd.DataFrame(uninitialized_data) if uninitialized_data else create_empty_df(["data_provider", "stream_id"]),
                StreamLocatorModel,
            )

            logger.info(
                f"Batch method success: DeployedAndInitialized={len(deployed_and_initialized_df)}, "
                f"DeployedButNotInitialized={len(deployed_but_not_initialized_df)}"
            )

            return StreamStatesResult(
                non_deployed=create_empty_stream_locator_df(), # Batch success implies all exist
                deployed_but_not_initialized=deployed_but_not_initialized_df,
                deployed_and_initialized=deployed_and_initialized_df,
                depth=current_depth,
                fallback_used=False,
            )

        except MetadataProcedureNotFoundError:
            # This is expected if *any* stream in the batch does not exist or the helper contract is wrong.
            # Fall through to divide/fallback logic which handles non-existence.
            logger.debug(
                "Batch method failed with MetadataProcedureNotFoundError. Falling back to divide/individual checks."
            )
            # Proceed to divide/fallback logic below
        except Exception as e:
            # For other unexpected errors during the batch attempt, log and re-raise
            # This might indicate a deeper issue with TN or the SDK
            logger.error(f"Unexpected error during batch state check: {e!s}", exc_info=True)
            raise e

    # --- Fallback Logic (Step 3) or Divide/Recurse (Step 5) ---

    # Determine if fallback should be used
    use_fallback = current_depth >= max_depth or force_fallback or len(stream_locators) == 1

    if use_fallback:
        logger.debug(f"Using fallback method for {len(stream_locators)} streams at depth {current_depth}")

        # Initialize lists/DataFrames
        non_deployed_list: list[dict[str, str]] = []
        provisionally_existing_df = create_empty_stream_locator_df()
        deployed_and_initialized_df = create_empty_stream_locator_df()
        deployed_but_not_initialized_df = create_empty_stream_locator_df()

        # 1. Check existence for each stream
        for _, row in stream_locators.iterrows():
            try:
                exists = block.stream_exists(data_provider=row["data_provider"], stream_id=row["stream_id"])
                if exists:
                    provisionally_existing_df = cast(
                        DataFrame[StreamLocatorModel], append_to_df(provisionally_existing_df, row)
                    )
                else:
                    non_deployed_list.append(row.to_dict())
            except Exception as e:
                # If existence check fails, tentatively mark as non-deployed but log error
                logger.error(
                    f"Error checking existence for stream {row['stream_id']} from {row['data_provider']}: {e!s}. Marking as non-deployed.",
                    exc_info=True,
                )
                non_deployed_list.append(row.to_dict())

        # 2. Check initialization for provisionally existing streams
        if not provisionally_existing_df.empty:
            try:
                # Use the batch task to efficiently check initialization
                logger.debug(
                    f"Calling task_filter_batch_initialized_streams for {len(provisionally_existing_df)} provisionally existing streams."
                )
                initialized_in_batch_df = task_filter_batch_initialized_streams(
                    block=block, stream_locators=provisionally_existing_df
                )
                deployed_and_initialized_df = convert_to_typed_df(initialized_in_batch_df, StreamLocatorModel)

                # Find those that existed but were not returned by the initialization check
                if not deployed_and_initialized_df.empty:
                    existing_set = get_stream_locator_set(provisionally_existing_df)
                    initialized_set = get_stream_locator_set(deployed_and_initialized_df)
                    uninitialized_data = diff_stream_locator_sets(existing_set, initialized_set)
                    deployed_but_not_initialized_df = convert_to_typed_df(
                        pd.DataFrame(uninitialized_data),
                        StreamLocatorModel,
                    )
                else:
                    # None were initialized
                    deployed_but_not_initialized_df = provisionally_existing_df

            except MetadataProcedureNotFoundError:
                # If batch init check itself fails (e.g., helper contract issue?), assume none are initialized
                logger.warning(
                    "task_filter_batch_initialized_streams failed with MetadataProcedureNotFoundError during fallback. "
                    "Assuming all provisionally existing streams are deployed_but_not_initialized."
                )
                deployed_but_not_initialized_df = provisionally_existing_df
                deployed_and_initialized_df = create_empty_stream_locator_df() # Ensure it's empty
            except Exception as e:
                # Handle other potential errors during batch initialization check
                logger.error(
                    f"Error during task_filter_batch_initialized_streams in fallback: {e!s}. "
                    "Assuming all provisionally existing streams are deployed_but_not_initialized.",
                    exc_info=True,
                )
                deployed_but_not_initialized_df = provisionally_existing_df
                deployed_and_initialized_df = create_empty_stream_locator_df() # Ensure it's empty

        # 3. Combine results
        non_deployed_df = convert_to_typed_df(
            pd.DataFrame(non_deployed_list) if non_deployed_list else create_empty_df(["data_provider", "stream_id"]),
            StreamLocatorModel,
        )

        logger.info(
            f"Fallback result: NonDeployed={len(non_deployed_df)}, "
            f"DeployedButNotInitialized={len(deployed_but_not_initialized_df)}, "
            f"DeployedAndInitialized={len(deployed_and_initialized_df)}"
        )

        return StreamStatesResult(
            non_deployed=non_deployed_df,
            deployed_but_not_initialized=deployed_but_not_initialized_df,
            deployed_and_initialized=deployed_and_initialized_df,
            depth=current_depth,
            fallback_used=True,
        )

    # --- Divide and Recurse (Step 5) ---
    else: # Neither batch success nor fallback condition met
        logger.debug(f"Dividing {len(stream_locators)} streams for recursive state check at depth {current_depth}.")
        mid = len(stream_locators) // 2
        left_half = stream_locators.iloc[:mid]
        right_half = stream_locators.iloc[mid:]

        # Submit recursive calls as subtasks (or directly if not using Prefect tasks yet)
        # NOTE: Using .submit() requires the function to be a @task, which is Step 6.
        # For now, we'll call directly for testing, assuming it's not yet a task.
        # TODO: Convert to .submit() in Step 6
        # UPDATE: Now that it's a task, we should use .submit()
        logger.debug(f"Submitting left half ({len(left_half)}) for depth {current_depth + 1}")
        left_future = task_get_stream_states_divide_conquer.submit(
            block=block,
            stream_locators=left_half,
            max_depth=max_depth,
            current_depth=current_depth + 1,
            force_fallback=force_fallback,
            max_filter_size=max_filter_size,
        )

        logger.debug(f"Submitting right half ({len(right_half)}) for depth {current_depth + 1}")
        right_future = task_get_stream_states_divide_conquer.submit(
            block=block,
            stream_locators=right_half,
            max_depth=max_depth,
            current_depth=current_depth + 1,
            force_fallback=force_fallback,
            max_filter_size=max_filter_size,
        )

        # Wait for results
        left_result = left_future.result()
        right_result = right_future.result()

        # Combine results from both halves
        logger.debug("Combining results from recursive calls.")
        # Need a helper function to combine StreamStatesResult
        combined_result = _combine_stream_states_results(left_result, right_result)
        return combined_result


def _combine_stream_states_results(
    left: StreamStatesResult, right: StreamStatesResult
) -> StreamStatesResult:
    """Helper function to combine two StreamStatesResult objects."""
    combined_non_deployed = pd.concat([left["non_deployed"], right["non_deployed"]])
    combined_dep_not_init = pd.concat([left["deployed_but_not_initialized"], right["deployed_but_not_initialized"]])
    combined_dep_and_init = pd.concat([left["deployed_and_initialized"], right["deployed_and_initialized"]])

    # Ensure correct types after concat
    non_deployed_typed = convert_to_typed_df(combined_non_deployed, StreamLocatorModel)
    dep_not_init_typed = convert_to_typed_df(combined_dep_not_init, StreamLocatorModel)
    dep_and_init_typed = convert_to_typed_df(combined_dep_and_init, StreamLocatorModel)

    return StreamStatesResult(
        non_deployed=non_deployed_typed,
        deployed_but_not_initialized=dep_not_init_typed,
        deployed_and_initialized=dep_and_init_typed,
        depth=max(left["depth"], right["depth"]), # Depth is max of branches
        fallback_used=left["fallback_used"] or right["fallback_used"], # Fallback if used in either branch
    )


# --- Existing task below ---
