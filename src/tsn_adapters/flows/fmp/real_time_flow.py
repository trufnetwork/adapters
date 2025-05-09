"""
Real-Time Market Data Sync Flow

Flow Structure:
    1. Descriptor Extraction: Get list of symbols to process
    2. Batch Creation: Split symbols into manageable chunks
    3. Quote Fetching: Parallel processing of batches
    4. Result Combination: Merge successful results
    5. Data Processing: Transform to TN format
    6. Data Insertion: Store in TN network

Initial Scale:
    - Expected 60K+ tickers in descriptor
    - One quote per ticker
    - Two transactions per flow run
"""

from typing import Any, Optional, TypedDict, TypeGuard, cast

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow, task, unmapped

from tsn_adapters.blocks.fmp import BatchQuoteShort, FMPBlock
from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel, PrimitiveSourcesDescriptorBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.common.trufnetwork.tasks.insert import task_split_and_insert_records
from tsn_adapters.utils.logging import get_logger_safe

from ...utils.deroutine import force_sync


class QuoteData(TypedDict):
    """
    Type-safe structure for quote data returned from FMP API.

    Design Note:
        Using TypedDict instead of a class to maintain compatibility with
        the raw API response structure while adding type safety.
    """

    results: list[dict[str, float | str]]  # Each quote has symbol, price, volume


class FlowResult(TypedDict):
    """
    Type-safe structure for flow execution results.

    Design Note:
        This structure provides a consistent interface for error reporting
        and success metrics, making it easier to handle partial failures
        and track processing statistics.

    Fields:
        success: True if any data was processed successfully
        processed_quotes: Number of quotes successfully processed
        filtered_quotes: Number of quotes filtered out (e.g., null prices)
        failed_batches: Number of batches that failed processing
        errors: List of detailed error messages for debugging
    """

    success: bool
    processed_quotes: int
    filtered_quotes: int
    failed_batches: int
    errors: list[str]


def create_error_result(
    error_msg: str,
    total_symbols: int,
    failed_batches: int,
    processed_quotes: int = 0,
    existing_errors: list[str] | None = None,
) -> FlowResult:
    """
    Create a standardized error result for the flow.

    Design Note:
        This helper function ensures consistent error reporting across all
        failure points in the flow. It automatically calculates filtered
        quotes based on total symbols and processed quotes.

    Args:
        error_msg: The error message to add
        total_symbols: Total number of symbols being processed
        failed_batches: Number of batches that failed
        processed_quotes: Number of quotes processed (default 0)
        existing_errors: List of existing error messages to include

    Returns:
        FlowResult with error details and processing statistics
    """
    errors = existing_errors or []
    errors.append(error_msg)
    return FlowResult(
        success=False,
        processed_quotes=processed_quotes,
        filtered_quotes=total_symbols - processed_quotes,
        failed_batches=failed_batches,
        errors=errors,
    )


def is_valid_quotes_df(df: Any) -> TypeGuard[DataFrame[BatchQuoteShort]]:
    """
    Type guard to verify if a DataFrame contains valid quote data.

    Design Note:
        This function serves multiple purposes:
        1. Type safety: Ensures DataFrame has correct structure
        2. Data validation: Verifies required columns exist
        3. Runtime checking: Guards against malformed data

    Args:
        df: Any value to check

    Returns:
        True if df is a valid DataFrame[BatchQuoteShort]
    """
    if not isinstance(df, pd.DataFrame):
        return False
    required_columns = {"symbol", "price", "volume"}
    return all(col in df.columns for col in required_columns)


@task
def get_symbols_from_descriptor(psd_block: PrimitiveSourcesDescriptorBlock) -> DataFrame[PrimitiveSourceDataModel]:
    """
    Extracts symbols and their metadata from the primitive source descriptor.

    Design Note:
        This task is separated from the main flow to:
        1. Allow for retries specifically for descriptor fetching
        2. Cache the descriptor results for reuse
        3. Provide clear error boundaries

    Args:
        psd_block: Block for accessing primitive source descriptors

    Returns:
        DataFrame containing source descriptors with symbol metadata

    Raises:
        Exception: If descriptor fetch fails (handled by flow)
    """
    logger = get_logger_safe()
    descriptor_df = psd_block.get_descriptor()
    logger.info(
        "Extracted source descriptors",
        extra={
            "total_symbols": len(descriptor_df),
            "descriptor_columns": list(descriptor_df.columns),
        },
    )
    return descriptor_df


@task
def batch_symbols(descriptor_df: DataFrame[PrimitiveSourceDataModel], batch_size: int) -> list[list[str]]:
    """
    Creates batches of symbols for efficient API processing.

    Design Note:
        Batching is crucial for:
        1. Staying within API rate limits
        2. Optimizing memory usage
        3. Enabling parallel processing
        4. Providing granular error handling

    Args:
        descriptor_df: DataFrame containing source descriptors
        batch_size: Number of symbols per batch

    Returns:
        List of batches, where each batch is a list of symbol strings

    Raises:
        Exception: If batching fails (handled by flow)
    """
    logger = get_logger_safe()
    symbols: list[str] = cast(list[str], descriptor_df["source_id"].tolist())
    batches: list[list[str]] = [symbols[i : i + batch_size] for i in range(0, len(symbols), batch_size)]
    logger.info(
        "Created symbol batches",
        extra={
            "total_symbols": len(symbols),
            "total_batches": len(batches),
            "batch_size": batch_size,
        },
    )
    return batches


def convert_quotes_to_tn_data(
    quotes_df: DataFrame[BatchQuoteShort],
    id_mapping: dict[str, str],
    timestamp: Optional[int] = None,
) -> DataFrame[TnDataRowModel]:
    """
    Convert BatchQuoteShort DataFrame to TnDataRowModel format.

    Args:
        quotes_df: DataFrame containing quotes in BatchQuoteShort format
        id_mapping: Mapping from source_id to stream_id
        timestamp: Optional timestamp to use, defaults to current time

    Returns:
        DataFrame in TnDataRowModel format
    """
    if len(quotes_df) == 0:
        return DataFrame[TnDataRowModel](pd.DataFrame(columns=["stream_id", "date", "value", "data_provider"]))

    # Create a copy to avoid modifying the input
    result_df = quotes_df.copy()

    # Map symbol to stream_id
    result_df["stream_id"] = result_df["symbol"].astype(str).map(id_mapping)

    # Drop rows where we couldn't map the symbol to a stream_id and create a new DataFrame
    valid_rows = ~result_df["stream_id"].isna()
    result_df = result_df[valid_rows]

    # Filter out null prices
    valid_prices = ~result_df["price"].isna()
    result_df = result_df[valid_prices]

    # Drop any duplicates on stream_id
    result_df = result_df.drop_duplicates(subset=["stream_id"])

    # Set the timestamp
    current_timestamp = timestamp or int(pd.Timestamp.now().timestamp())
    result_df["date"] = ensure_unix_timestamp(current_timestamp)  # Keep as numeric for batch_insert_unix_tn_records

    # Use price as the value, converting to string as required by TnRecordModel
    result_df["value"] = result_df["price"].astype(str)

    # Add data provider
    result_df["data_provider"] = None

    # Select only the required columns
    result_df = result_df[["stream_id", "date", "value", "data_provider"]]

    return DataFrame[TnDataRowModel](result_df)


def ensure_unix_timestamp(time: int) -> int:
    """
    Ensure the timestamp is a valid unix timestamp (seconds since epoch).

    This function validates and converts timestamps to ensure they are in
    seconds since epoch format. It handles cases where the input might be
    in milliseconds or microseconds.

    Args:
        time: Integer timestamp that might be in seconds, milliseconds,
              microseconds, or nanoseconds since epoch

    Returns:
        Integer timestamp in seconds since epoch

    Raises:
        ValueError: If the timestamp is invalid or outside the reasonable range
    """
    if time < 0:
        raise ValueError("Timestamp must be a positive integer")

    # Define valid range for seconds since epoch
    min_valid_timestamp = 0  # 1970-01-01
    max_valid_timestamp = 4102444800  # 2100-01-01

    # Convert to seconds if in a larger unit
    converted_time = time
    if time > max_valid_timestamp:
        # Handle different possible time units
        if time > 10**18:  # nanoseconds
            converted_time = time // 10**9
        elif time > 10**15:  # microseconds
            converted_time = time // 10**6
        elif time > 10**12:  # milliseconds
            converted_time = time // 10**3
        else:  # assume seconds but with some future date
            converted_time = time // 10**9  # aggressive conversion to be safe

    # Validate the range after conversion
    if converted_time < min_valid_timestamp or converted_time > max_valid_timestamp:
        raise ValueError(f"Timestamp outside valid range (1970-2100): {converted_time}")

    return converted_time


@task(retries=3, retry_delay_seconds=10)
def fetch_quotes_for_batch(
    fmp_block: FMPBlock,
    symbols_batch: list[str],
    batch_number: int | None = None,
    total_batches: int | None = None,
) -> DataFrame[BatchQuoteShort]:
    """
    Fetches real-time quotes for a batch of symbols from FMP API.

    Design Note:
        This task is configured with retries because:
        1. API calls are prone to transient failures
        2. Rate limits may require backoff
        3. Network issues should be handled gracefully

    The task is also designed to:
        - Process batches independently for fault isolation
        - Provide clear progress tracking
        - Filter invalid data early in the pipeline

    Args:
        fmp_block: Block for FMP API interactions
        symbols_batch: List of symbols to fetch quotes for
        batch_number: Current batch number for logging (optional)
        total_batches: Total number of batches for logging (optional)

    Returns:
        DataFrame containing quote data for the batch

    Raises:
        RuntimeError: If there's an unrecoverable error fetching quotes from FMP
    """
    logger = get_logger_safe()
    batch_info = f"Batch {batch_number}/{total_batches}" if batch_number and total_batches else "Batch"

    try:
        logger.info(
            f"Fetching quotes for {batch_info}",
            extra={
                "batch_size": len(symbols_batch),
                "batch_number": batch_number,
                "total_batches": total_batches,
            },
        )
        return fmp_block.get_batch_quote(symbols_batch)
    except Exception as e:
        error_msg = f"Failed to fetch quotes for {batch_info}: {e!s}"
        logger.error(error_msg, exc_info=True)
        raise RuntimeError(error_msg) from e


@task
def combine_batch_results(
    *batches: Optional[DataFrame[BatchQuoteShort]],
) -> DataFrame[BatchQuoteShort]:
    """
    Combines multiple batches of quote data into a single DataFrame.

    Args:
        *batches: Variable number of quote data batches

    Returns:
        Combined DataFrame of all valid batches

    Raises:
        RuntimeError: If no valid batches are available or if any batch is an error
    """
    logger = get_logger_safe()
    valid_batches: list[DataFrame[BatchQuoteShort]] = []
    failed_batches = 0
    error_messages: list[str] = []

    for i, batch in enumerate(batches, 1):
        if batch is None:
            error_messages.append(f"Batch {i} is None")
            failed_batches += 1
            continue

        if isinstance(batch, Exception):
            error_messages.append(f"Batch {i}: {batch}")
            failed_batches += 1
            continue

        if not is_valid_quotes_df(batch):
            error_messages.append(f"Batch {i} has invalid structure")
            failed_batches += 1
            continue

        valid_batches.append(batch)

    if error_messages:
        error_msg = f"Failed fetching quote batches: {', '.join(error_messages)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    if not valid_batches:
        error_msg = "No valid batches to process"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    logger.info(
        "Combining batch results",
        extra={
            "valid_batches": len(valid_batches),
            "failed_batches": failed_batches,
            "success_rate": f"{(len(valid_batches) / (len(valid_batches) + failed_batches)) * 100:.2f}%",
        },
    )

    result = pd.concat(valid_batches, ignore_index=True)
    return DataFrame[BatchQuoteShort](result)


@task
def process_data(
    quotes_df: DataFrame[BatchQuoteShort] | None | Exception,
    descriptor_df: DataFrame[PrimitiveSourceDataModel],
) -> DataFrame[TnDataRowModel]:
    """
    Processes and transforms quote data into the TN data model format.

    Design Note:
        This task is responsible for:
        1. Data transformation: Converting between data models
        2. Data enrichment: Adding metadata from descriptors
        3. Data validation: Ensuring output meets TN requirements

    The separation of this task allows for:
        - Clear data flow boundaries
        - Independent scaling of processing
        - Focused error handling
        - Easy testing of transformations

    Args:
        quotes_df: DataFrame containing quote data, or None/Exception if previous steps failed
        descriptor_df: DataFrame containing source descriptors

    Returns:
        DataFrame in TN data model format

    Raises:
        RuntimeError: If quotes_df is None, an Exception, or not a valid DataFrame[BatchQuoteShort]
    """
    logger = get_logger_safe()

    # Handle None case
    if quotes_df is None:
        error_msg = "Cannot process data: quotes DataFrame is None"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    # Handle Exception case
    if isinstance(quotes_df, Exception):
        error_msg = f"Cannot process data: quotes DataFrame is {quotes_df}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    logger.info(
        "Processing real-time market data",
        extra={
            "quotes_count": len(quotes_df),
            "descriptor_count": len(descriptor_df),
        },
    )

    if not is_valid_quotes_df(quotes_df):
        error_msg = "Invalid quotes DataFrame structure"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    # Create mapping from source_id to stream_id
    source_ids = descriptor_df["source_id"].astype(str)
    stream_ids = descriptor_df["stream_id"].astype(str)
    id_mapping: dict[str, str] = dict(zip(source_ids, stream_ids))

    # Convert quotes to TnDataRowModel format
    result_df = convert_quotes_to_tn_data(quotes_df, id_mapping)

    logger.info(
        "Processed quotes into TnDataRowModel format",
        extra={
            "input_quotes": len(quotes_df),
            "output_records": len(result_df),
            "filtered_records": len(quotes_df) - len(result_df),
        },
    )

    return result_df


@flow(name="Real-Time Market Data Sync Flow")
def real_time_flow(
    fmp_block: FMPBlock,
    psd_block: PrimitiveSourcesDescriptorBlock,
    tn_block: TNAccessBlock,
    tickers_per_request: int = 20000,
    fetch_task: Any = None,
    max_filter_size: int = 5000,
    insert_batch_size: int = 25000,
) -> FlowResult:
    """
    Main flow to fetch and update real-time market data.
    This flow uses FMPBlock for API interactions, primitive source descriptor for symbols,
    and TNAccessBlock for inserting the data.

    Error Handling:
    - Individual batch failures are logged but don't stop processing
    - Flow continues processing remaining batches when errors occur
    - All successful data is collected and processed
    - Detailed error reporting is provided in the return value
    - Partial results are returned with success/failure counts

    Args:
        fmp_block: Block for FMP API interactions
        psd_block: Block for primitive source descriptors
        tn_block: Block for TN interactions
        tickers_per_request: Size of symbol batches for API calls
        fetch_task: Optional task override for testing (e.g., with retries disabled)

    Returns:
        FlowResult containing success status, processing statistics, and error details
    """
    logger = get_logger_safe()
    logger.info("Starting real-time market data sync flow", extra={"tickers_per_request": tickers_per_request})
    error_details: list[str] = []

    # Extract source descriptors
    try:
        descriptor_df = get_symbols_from_descriptor(psd_block=psd_block)
        total_symbols = len(descriptor_df)
        logger.info(
            "Extracted source descriptors",
            extra={
                "total_symbols": total_symbols,
                "descriptor_columns": list(descriptor_df.columns),
            },
        )
    except Exception as e:
        error_msg = f"Critical failure: Unable to get source descriptors - {e!s}"
        logger.error(error_msg, exc_info=True)
        return create_error_result(error_msg, total_symbols=0, failed_batches=0)

    # Batch symbols
    try:
        batches = batch_symbols(descriptor_df=descriptor_df, batch_size=tickers_per_request)
        total_batches = len(batches)
        logger.info(
            "Created symbol batches",
            extra={
                "total_symbols": total_symbols,
                "total_batches": total_batches,
                "batch_size": tickers_per_request,
            },
        )
    except Exception as e:
        error_msg = f"Critical failure: Unable to batch symbols - {e!s}"
        logger.error(error_msg, exc_info=True)
        return create_error_result(error_msg, total_symbols=total_symbols, failed_batches=0)

    # Use the provided fetch task or default to fetch_quotes_for_batch
    fetch = fetch_task if fetch_task is not None else fetch_quotes_for_batch

    # Initialize error tracking
    failed_batches = 0

    try:
        # Fetch quotes for each batch using task mapping with keyword arguments
        quotes_batches = fetch.map(
            fmp_block=unmapped(fmp_block),
            symbols_batch=batches,
            batch_number=list(range(1, total_batches + 1)),
            total_batches=unmapped(total_batches),
        )

        # Combine all batch results
        try:
            combined_data = combine_batch_results(*quotes_batches)
            failed_batches = sum(1 for batch in quotes_batches if batch is None or isinstance(batch, Exception))
            logger.info(
                "Combined batch results",
                extra={
                    "total_batches": total_batches,
                    "failed_batches": failed_batches,
                    "success_rate": f"{((total_batches - failed_batches) / total_batches) * 100:.2f}%",
                },
            )
        except RuntimeError as e:
            error_msg = f"Failed to combine batch results: {e!s}"
            logger.error(error_msg, exc_info=True)
            return create_error_result(error_msg, total_symbols=total_symbols, failed_batches=total_batches)

        # Process the combined data into TnDataRowModel format
        try:
            processed_data = process_data(quotes_df=combined_data, descriptor_df=descriptor_df, return_state=True)
            # Get the actual processed data and ensure it's the right type
            processed_df = force_sync(processed_data.result)()

            if isinstance(processed_df, Exception):
                raise processed_df

        except Exception as e:
            error_msg = f"Failed to process combined data: {e!s}"
            logger.error(error_msg, exc_info=True)
            return create_error_result(error_msg, total_symbols=total_symbols, failed_batches=failed_batches)

        # Calculate filtered quotes
        total_filtered = total_symbols - len(processed_df)
        if total_filtered > 0:
            logger.warning(
                "Quotes filtered during processing",
                extra={
                    "total_filtered": total_filtered,
                    "total_symbols": total_symbols,
                },
            )

        # Insert processed data into TN
        try:
            task_split_and_insert_records(
                block=tn_block,
                records=processed_data,
                max_batch_size=insert_batch_size,
            )
            logger.info(
                "Completed real-time market data sync flow",
                extra={
                    "processed_quotes": len(processed_df),
                    "filtered_quotes": total_filtered,
                    "failed_batches": failed_batches,
                    "success_rate": f"{((total_batches - failed_batches) / total_batches) * 100:.2f}%",
                },
            )
            return FlowResult(
                success=True,
                processed_quotes=len(processed_df),
                filtered_quotes=total_filtered,
                failed_batches=failed_batches,
                errors=error_details,
            )
        except Exception as e:
            error_msg = f"Failed to insert records: {e!s}"
            logger.error(error_msg, exc_info=True)
            return create_error_result(
                error_msg,
                total_symbols=total_symbols,
                failed_batches=failed_batches,
                processed_quotes=len(processed_df),
                existing_errors=error_details,
            )

    except Exception as e:
        # Handle any unexpected errors
        error_msg = f"Unexpected error: {e!s}"
        logger.error(error_msg, exc_info=True)
        return create_error_result(error_msg, total_symbols=total_symbols, failed_batches=total_batches)
