from decimal import Decimal, InvalidOperation
from math import ceil
from typing import Optional, TypedDict

import pandas as pd
import trufnetwork_sdk_py.client as tn_client
from pandera.typing import DataFrame
from prefect import get_run_logger, task
from prefect.states import Completed

from tsn_adapters.blocks.tn_access import (UNUSED_INFINITY_RETRIES,
                                           TNAccessBlock,
                                           extract_stream_locators,
                                           task_wait_for_tx,
                                           tn_special_retry_condition)
from tsn_adapters.common.trufnetwork.models.tn_models import (TnDataRowModel,
                                                              TnRecordModel)
from tsn_adapters.common.trufnetwork.tn import \
    task_batch_filter_streams_by_existence
from tsn_adapters.utils.logging import get_logger_safe

# Every stream in the batch costs one read, and TNAccessBlock.read_records holds a
# global `tn-read` concurrency slot, so those reads are serialised. That is fine for
# the hundreds of streams a single-source adapter touches and hopeless for the
# 18k-stream FMP run or the 155k-stream Argentina run. Refuse past this rather than
# quietly turning a pipeline into a queue of blocking reads; those callers need a
# batched last-value read on the node, which does not exist yet.
MAX_STREAMS_FOR_UNCHANGED_CHECK = 500

# get_record_primitive caps its result at 10000 rows
# (node/internal/migrations/005-primitive-query.sql). A span read that returns this
# many may be truncated, and a truncated read cannot be reasoned about safely.
GET_RECORD_ROW_LIMIT = 10000


class SplitInsertResults(TypedDict):
    """Results of batch insertions."""

    success_tx_hashes: list[str]
    failed_records: DataFrame[TnDataRowModel]
    failed_reasons: list[str]


@task(retries=UNUSED_INFINITY_RETRIES, retry_delay_seconds=10, retry_condition_fn=tn_special_retry_condition(5))
def task_split_and_insert_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    max_batch_size: int = 10,
    wait: bool = True,
    filter_deployed_streams: bool = True,
    max_streams_per_existence_check: int = 1000,
    skip_unchanged: bool = False,
    max_streams_for_unchanged_check: int = MAX_STREAMS_FOR_UNCHANGED_CHECK,
) -> SplitInsertResults:
    """
    Inserts records into TN via BulkInserter (cached-nonce pipelining),
    optionally filtering by stream existence first.

    Args:
        block: The TNAccessBlock instance.
        records: The records to insert.
        max_batch_size: Records per insert_records tx (passed to BulkInserter).
        wait: Legacy. Effectively always-true.
        filter_deployed_streams: Whether to filter out streams that do not exist on TN.
        max_streams_per_existence_check: Max streams to check in one existence API call.
        skip_unchanged: Drop a record whose value equals the one the stream already
            resolves to just before it. TN carries the last observation forward, so
            such a record answers no query differently than its absence would, while
            still costing a transaction, a write fee, and storage on every node.
            Defaults to False, so no pipeline changes behaviour until it opts in.
        max_streams_for_unchanged_check: Refuse the unchanged check above this many
            distinct streams. See `_filter_unchanged_records` for why the ceiling
            exists.
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

    # 3. Optionally drop records that restate the value already standing
    if skip_unchanged and not processed_records.empty:
        processed_records = _filter_unchanged_records(
            block=block,
            records=processed_records,
            max_streams_for_unchanged_check=max_streams_for_unchanged_check,
        )

    # 4. Perform Batch Insertions
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


def _filter_unchanged_records(
    block: TNAccessBlock,
    records: DataFrame[TnDataRowModel],
    max_streams_for_unchanged_check: int,
) -> DataFrame[TnDataRowModel]:
    """Drop records that restate the value the stream already resolves to.

    TN carries the last observation forward: a query for a time with no record
    returns the newest record at or before it. A record whose value equals the one
    already standing therefore answers every query exactly as its absence would,
    while still costing a transaction, a write fee, and permanent storage on every
    node in the network.

    The comparison is against what TN actually holds, not against the previous row
    of a source file. `filter_unchanged_products` in the Argentina pipeline does the
    latter and says in its own docstring what that costs: a product missing from the
    previous day's file gets republished even though its value has not moved. Reading
    the chain has no such hole and needs no assumption about the source's
    completeness.

    One read per stream covers the batch's whole span, so records already on chain
    between two rows of the batch take part in the comparison. That matters for
    backfills, which write history out of order: the record before a candidate may
    be one this batch is not writing.

    Raises:
        ValueError: If the batch spans more streams than the check can afford.
    """
    logger = get_logger_safe(__name__)

    frame = records.copy()
    frame["_row_order"] = range(len(frame))
    unique_streams = frame[["data_provider", "stream_id"]].drop_duplicates()

    if len(unique_streams) > max_streams_for_unchanged_check:
        raise ValueError(
            f"skip_unchanged was asked to check {len(unique_streams)} streams, above the "
            f"{max_streams_for_unchanged_check} ceiling. Each stream costs one serialised read, so "
            "this would stall the flow rather than speed it up. A batched last-value read on the "
            "node is what this size needs; until that exists, leave skip_unchanged off here."
        )

    logger.info(f"Checking {len(frame)} records across {len(unique_streams)} streams for unchanged values...")

    drop_positions: set[int] = set()
    for _, locator in unique_streams.iterrows():
        data_provider = str(locator["data_provider"])
        stream_id = str(locator["stream_id"])
        group = frame[(frame["data_provider"] == data_provider) & (frame["stream_id"] == stream_id)]
        drop_positions.update(
            _unchanged_row_positions(
                block=block,
                data_provider=data_provider,
                stream_id=stream_id,
                group=group,
            )
        )

    kept = frame[~frame["_row_order"].isin(drop_positions)].drop(columns=["_row_order"])
    logger.info(
        f"Finished unchanged check. Dropped {len(drop_positions)} of {len(records)} records that "
        f"restated a value already standing."
    )
    return DataFrame[TnDataRowModel](kept)


def _unchanged_row_positions(
    block: TNAccessBlock,
    data_provider: str,
    stream_id: str,
    group: pd.DataFrame,
) -> set[int]:
    """Return the `_row_order` positions in one stream's rows that say nothing new.

    Walks the stream's timeline in event-time order, merging what TN already holds
    with what this batch would write, and carries the standing value forward. A
    candidate is dropped when it equals the value standing immediately before it.
    """
    logger = get_logger_safe(__name__)

    candidates: list[tuple[int, Decimal, int]] = []
    for _, row in group.iterrows():
        try:
            candidates.append((int(row["date"]), Decimal(str(row["value"])), int(row["_row_order"])))
        except (InvalidOperation, TypeError, ValueError):
            # An unparseable value is not our problem to diagnose here; let it through
            # and fail where it would have failed without this filter.
            logger.warning(f"Could not read value {row['value']!r} for {stream_id} as a number; keeping the record.")
            return set()

    candidates.sort(key=lambda c: c[0])
    first_event_time = candidates[0][0]
    last_event_time = candidates[-1][0]

    # date_from sits one second before the earliest candidate so the read carries the
    # anchor, the newest record at or before that point. get_record treats `from` as
    # exclusive on its interval and inclusive on its anchor, so this returns the
    # standing value plus everything already on chain inside the batch's span.
    existing = block.read_records(
        stream_id=stream_id,
        data_provider=data_provider,
        date_from=first_event_time - 1,
        date_to=last_event_time,
    )

    # get_record_primitive ends `ORDER BY event_time ASC LIMIT 10000`. A read that
    # comes back at the cap may have been truncated, which would hide records that
    # sit between candidates and make a genuine change look like a repeat. Skipping
    # nothing is the safe answer; a redundant write costs a fee, a wrongly dropped
    # one loses data.
    if len(existing) >= GET_RECORD_ROW_LIMIT:
        logger.warning(
            f"Stream {stream_id} returned {len(existing)} records for the batch span, at or above the "
            f"{GET_RECORD_ROW_LIMIT}-row read limit. The read may be truncated, so no record is dropped."
        )
        return set()

    on_chain: dict[int, Decimal] = {}
    for _, row in existing.iterrows():
        try:
            on_chain[int(row["date"])] = Decimal(str(row["value"]))
        except (InvalidOperation, TypeError, ValueError):
            logger.warning(f"Stream {stream_id} returned an unreadable value at {row['date']!r}; keeping its records.")
            return set()

    # Merge both sides into one timeline. At a shared event_time the on-chain record
    # is what stands, so it is compared against first; a candidate that matches it is
    # a restatement of the same point and adds nothing either.
    timeline = sorted(
        [(t, v, None) for t, v in on_chain.items()] + [(t, v, pos) for t, v, pos in candidates],
        key=lambda item: (item[0], item[2] is not None),
    )

    dropped: set[int] = set()
    standing: Optional[Decimal] = None
    for _event_time, value, position in timeline:
        if position is not None and standing is not None and value == standing:
            dropped.add(position)
            continue
        standing = value

    return dropped


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
    """Insert records via sdk-py BulkInserter (cached-nonce pipelining).

    Args:
        block: TNAccessBlock instance.
        records_to_insert: DataFrame of records ready for insertion.
        max_batch_size: Passed through to bulk_insert_tn_records as batch_size.
        wait: Legacy. Effectively always-true.

    Returns:
        SplitInsertResults. On failure, raises (does not return partial results).
    """
    del wait

    logger = get_logger_safe(__name__)

    if records_to_insert.empty:
        logger.warning("No records to insert.")
        empty_df = DataFrame[TnDataRowModel](columns=["data_provider", "stream_id", "date", "value"])
        return SplitInsertResults(success_tx_hashes=[], failed_records=empty_df, failed_reasons=[])

    logger.info(f"Submitting {len(records_to_insert)} records via BulkInserter (batch_size={max_batch_size}).")
    success_tx_hashes = block.bulk_insert_tn_records(records_to_insert, batch_size=max_batch_size)
    logger.info(f"BulkInserter submitted {len(success_tx_hashes)} txs.")

    empty_df = DataFrame[TnDataRowModel](columns=["data_provider", "stream_id", "date", "value"])
    return SplitInsertResults(
        success_tx_hashes=success_tx_hashes,
        failed_records=empty_df,
        failed_reasons=[],
    )
