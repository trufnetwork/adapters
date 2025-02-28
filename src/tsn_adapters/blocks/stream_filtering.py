"""
Stream filtering utilities for Truflation Stream Network.

This module provides functions for filtering streams based on their initialization status
using various strategies, including a divide-and-conquer approach.
"""

import traceback
from typing import Any, Optional, cast

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
)


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
            left_half_typed = cast(DataFrame[StreamLocatorModel], left_half)
            right_half_typed = cast(DataFrame[StreamLocatorModel], right_half)

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
