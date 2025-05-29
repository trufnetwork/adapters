
from math import ceil
from typing import (
    Optional,
    TypedDict,
)

import pandas as pd
from pandera.typing import DataFrame
from prefect import get_run_logger, task
from prefect.states import Completed
import trufnetwork_sdk_py.client as tn_client

from tsn_adapters.blocks.tn_access import (
    UNUSED_INFINITY_RETRIES,
    TNAccessBlock,
    extract_stream_locators,
    task_wait_for_tx,
    tn_special_retry_condition,
)
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel, TnRecordModel
from tsn_adapters.common.trufnetwork.tn import task_batch_filter_streams_by_existence
from tsn_adapters.utils.logging import get_logger_safe


class SplitInsertResults(TypedDict):
    """Results of batch insertions."""

    success_tx_hashes: list[str]
    failed_records: DataFrame[TnDataRowModel]
    failed_reasons: list[str]


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_split_and_insert_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    max_batch_size: int = 25000,
    wait: bool = True,
    filter_deployed_streams: bool = True,
    max_streams_per_existence_check: int = 1000,
) -> SplitInsertResults:
    """
    Splits records into batches and inserts them into TN, optionally filtering by stream existence first.

    Orchestrates filtering and batch insertion logic.

    Args:
        block: The TNAccessBlock instance.
        records: The records to insert.
        max_batch_size: Maximum number of records per insert batch.
        wait: Whether to wait for the insertion transaction(s) to be mined.
        filter_deployed_streams: Whether to filter out streams that do not exist on TN.
        filter_cache_duration: How long to cache the stream existence filter results.
        max_streams_per_existence_check: Max streams to check in one existence API call.

    Returns:
        SplitInsertResults containing transaction hashes and any failed records/reasons.
    """
    logger = get_logger_safe(__name__)
    processed_records = records.copy()

    # 1. Fill default data provider
    processed_records["data_provider"] = processed_records["data_provider"].fillna(block.current_account)

    # 2. Optionally Filter Records
    if filter_deployed_streams and not processed_records.empty:
        try:
            processed_records = _filter_records_by_stream_existence(
                block=block,
                records=processed_records,
                max_streams_per_existence_check=max_streams_per_existence_check,
            )
        except Exception as e:
            # Error during filtering is critical, re-raise to fail the task
            logger.error(f"Halting task due to error during stream existence filtering: {e!s}", exc_info=True)
            raise

    # 3. Perform Batch Insertions
    if processed_records.empty:
        logger.warning("No records remaining to insert after filtering.")
        # Return empty success result
        empty_df = DataFrame[TnDataRowModel](columns=["data_provider", "stream_id", "date", "value"])
        return SplitInsertResults(success_tx_hashes=[], failed_records=empty_df, failed_reasons=[])

    # Call the helper function for insertion
    # Pass through necessary params if they were kept (e.g., wait)
    insertion_results = _perform_batch_insertions(
        block=block,
        records_to_insert=processed_records,
        max_batch_size=max_batch_size,
        wait=wait,
    )

    return insertion_results


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def _task_only_batch_insert_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
) -> Optional[str]:
    """Insert records into TSN without waiting for transaction confirmation"""
    return block.batch_insert_tn_records(records=records)


# we don't use retries here because their individual tasks already have retries
@task
def task_batch_insert_tn_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    wait: bool = False,
) -> Optional[str]:
    """Batch insert records into multiple streams

    Args:
        block: The TNAccessBlock instance
        records: DataFrame containing records with stream_id column
        wait: Whether to wait for transactions to complete
        has_external_created_at: If True, insert with external created_at timestamps

    Returns:
        Transaction hash if successful, None otherwise
    """
    logging = get_run_logger()

    logging.info(f"Batch inserting {len(records)} records across {len(records['stream_id'].unique())} streams")

    # we use task so it may retry on network or nonce errors
    tx_hash = _task_only_batch_insert_records(block=block, records=records)

    if wait and tx_hash is not None:
        # we need to use task so it may retry on network errors
        task_wait_for_tx(block=block, tx_hash=tx_hash)

    return tx_hash

@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_insert_tn_records(
    block: TNAccessBlock,
    stream_id: str,
    records: DataFrame[TnRecordModel],
) -> Optional[str]:
    return block.insert_tn_records(stream_id, records)


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_insert_and_wait_for_tx(
    block: TNAccessBlock,
    stream_id: str,
    records: DataFrame[TnRecordModel],
    data_provider: Optional[str] = None,
):
    """Insert records into TSN and wait for transaction confirmation"""
    logging = get_run_logger()

    logging.info(f"Inserting {len(records)} records into stream {stream_id}")
    insertion = task_insert_tn_records(block=block, stream_id=stream_id, records=records, data_provider=data_provider)

    if insertion.result() is None:
        return Completed(message="No records to insert")

    try:
        task_wait_for_tx(block=block, tx_hash=insertion)
    except Exception as e:
        if "duplicate key value violates unique constraint" in str(e):
            logging.warning(f"Continuing after duplicate key value violation: {e}")
        else:
            raise e

    return insertion


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_destroy_stream(block: TNAccessBlock, stream_id: str, wait: bool = True) -> str:
    """Task to destroy a stream with the given stream ID.

    Args:
        block: The TNAccessBlock instance
        stream_id: The ID of the stream to destroy
        wait: If True, wait for the transaction to be confirmed

    Returns:
        The transaction hash
    """
    return block.destroy_stream(stream_id, wait)


# --- Helper Function for Filtering ---


def _filter_records_by_stream_existence(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    max_streams_per_existence_check: int,
) -> DataFrame[TnDataRowModel]:
    """Filters records based on stream existence on TN.

    Args:
        block: TNAccessBlock instance.
        records: Input DataFrame with potential records.
        max_streams_per_existence_check: Max streams per filter API call.

    Returns:
        Filtered DataFrame containing only records for existing streams.

    Raises:
        Exception: If the underlying existence check task fails.
    """
    logger = get_logger_safe(__name__)
    logger.info(
        f"Filtering {len(records)} records by stream existence (batch size: {max_streams_per_existence_check})..."
    )
    unique_locators_df = extract_stream_locators(records)

    locators_to_check: list[tn_client.StreamLocatorInput] = [
        tn_client.StreamLocatorInput(stream_id=str(row["stream_id"]), data_provider=str(row["data_provider"]))
        for _, row in unique_locators_df.iterrows()
    ]

    if not locators_to_check:
        logger.info("No unique stream locators found to filter.")
        return records  # Return original if no locators to check

    total_non_existent_set = set()
    num_filter_batches = ceil(len(locators_to_check) / max_streams_per_existence_check)
    logger.info(
        f"Checking existence for {len(locators_to_check)} unique locators in {num_filter_batches} batches (asking for non-existent)."
    )

    for i in range(num_filter_batches):
        start_idx = i * max_streams_per_existence_check
        end_idx = start_idx + max_streams_per_existence_check
        current_filter_batch = locators_to_check[start_idx:end_idx]
        batch_num_log = i + 1

        logger.debug(
            f"Checking existence filter batch {batch_num_log}/{num_filter_batches} ({len(current_filter_batch)} locators, asking for non-existent)..."
        )

        try:
            # Request non-existent streams
            non_existent_locators_in_batch = task_batch_filter_streams_by_existence.submit(
                block=block, locators=current_filter_batch, return_existing=False
            ).result()

            batch_non_existent_set = {
                (str(loc["data_provider"]), str(loc["stream_id"])) for loc in non_existent_locators_in_batch
            }
            total_non_existent_set.update(batch_non_existent_set)
            logger.debug(
                f"Existence filter batch {batch_num_log}: Found {len(batch_non_existent_set)} non-existent streams."
            )

        except Exception as e:
            logger.error(
                f"Error during stream existence filtering for batch {batch_num_log}: {e!s}. Halting filter process.",
                exc_info=True,
            )
            raise  # Re-raise to fail the calling task

    # Filter the original records using the aggregated set of non-existent streams
    original_count = len(records)
    if total_non_existent_set:
        # Create a MultiIndex of non-existent streams to check against
        non_existent_tuples = list(total_non_existent_set)
        non_existent_index = pd.MultiIndex.from_tuples(non_existent_tuples, names=["data_provider", "stream_id"])

        # Create a MultiIndex from the records DataFrame for efficient filtering
        records_index = pd.MultiIndex.from_frame(records[["data_provider", "stream_id"]])

        # Keep records that are NOT in the non_existent_index
        filtered_records = records[~records_index.isin(non_existent_index)]
    else:
        # If no streams were reported as non-existent, all are considered existent
        filtered_records = records.copy()

    filtered_out_count = original_count - len(filtered_records)
    logger.info(
        f"Finished existence check. Filtered out {filtered_out_count} records belonging to non-existent streams."
    )
    return DataFrame[TnDataRowModel](filtered_records)  # Ensure Pandera type is returned


# --- Helper Function for Batch Insertion ---


def _perform_batch_insertions(
    block: TNAccessBlock,
    records_to_insert: DataFrame[TnDataRowModel],
    max_batch_size: int,
    wait: bool,
) -> SplitInsertResults:
    """Splits records and performs batch insertions, collecting results.

    Args:
        block: TNAccessBlock instance.
        records_to_insert: DataFrame of records ready for insertion.
        max_batch_size: Max records per insertion API call.
        wait: Whether to wait for insertion transactions.

    Returns:
        SplitInsertResults dictionary.
    """
    logger = get_logger_safe(__name__)
    failed_reasons: list[str] = []
    success_tx_hashes: list[str] = []
    failed_records_list: list[DataFrame[TnDataRowModel]] = []

    split_records_batches = block.split_records(records_to_insert, max_batch_size)

    if not split_records_batches:
        logger.warning("No record batches to insert.")
        # Fall through to return empty results
    else:
        logger.info(
            f"Submitting {len(records_to_insert)} records for insertion in {len(split_records_batches)} batches."
        )
        for i, batch in enumerate(split_records_batches):
            batch_num_log = i + 1
            logger.debug(
                f"Submitting insertion batch {batch_num_log}/{len(split_records_batches)} ({len(batch)} records)..."
            )
            try:
                tx_hash_or_none = task_batch_insert_tn_records(
                    block=block,
                    records=batch,
                    wait=wait,
                )
                if tx_hash_or_none:
                    success_tx_hashes.append(tx_hash_or_none)
                logger.debug(f"Insertion batch {batch_num_log} submitted. TX: {tx_hash_or_none}")
            except Exception as e:
                logger.error(f"Insertion batch {batch_num_log} failed: {e!s}", exc_info=True)
                failed_records_list.append(batch)
                failed_reasons.append(str(e))

    # Combine failed records
    if failed_records_list:
        failed_records_df = DataFrame[TnDataRowModel](pd.concat(failed_records_list))
    else:
        failed_records_df = DataFrame[TnDataRowModel](columns=["data_provider", "stream_id", "date", "value"])

    return SplitInsertResults(
        success_tx_hashes=success_tx_hashes,
        failed_records=failed_records_df,
        failed_reasons=failed_reasons,
    )
