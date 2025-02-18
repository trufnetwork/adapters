"""
Real-Time Market Data Sync Flow

This module contains a Prefect flow to sync real-time market data from Financial Modeling Prep (FMP) API.

Flow tasks:
    - fetch_realtime_data: Fetch the last market data snapshot using fmpsdk.
    - process_data: Process and transform fetched data (placeholder).
    - update_primitives: Update the primitive sources with the new data (placeholder).

We expect initially:
- 60K+ tickers in the descriptor
- only 1 quote per ticker
- 2 transactions per flow run
"""

from typing import Any, Optional, TypedDict

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow, get_run_logger, task, unmapped

from tsn_adapters.blocks.fmp import BatchQuoteShort, FMPBlock
from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel, PrimitiveSourcesDescriptorBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock, task_split_and_insert_records
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel


class QuoteData(TypedDict):
    """Type-safe structure for quote data returned from FMP API"""

    results: list[dict[str, float | str]]  # Each quote has symbol, price, volume


@task
def get_symbols_from_descriptor(psd_block: PrimitiveSourcesDescriptorBlock) -> DataFrame[PrimitiveSourceDataModel]:
    """
    Extracts symbols and their metadata from the primitive source descriptor.
    Returns the full descriptor DataFrame for further processing.
    """
    logger = get_run_logger()
    descriptor_df = psd_block.get_descriptor()
    logger.info(f"Extracted {len(descriptor_df)} source descriptors")
    return descriptor_df


@task
def batch_symbols(descriptor_df: DataFrame[PrimitiveSourceDataModel], batch_size: int) -> list[list[str]]:
    """
    Splits the list of symbols from the descriptor into batches of specified batch_size.
    Uses source_id as the symbol identifier.
    """
    logger = get_run_logger()
    symbols = descriptor_df["source_id"].tolist()
    batches = [symbols[i : i + batch_size] for i in range(0, len(symbols), batch_size)]
    logger.info(f"Batched {len(symbols)} symbols into {len(batches)} batches.")
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
        return DataFrame[TnDataRowModel](pd.DataFrame(columns=["stream_id", "date", "value"]))

    # Create a copy to avoid modifying the input
    result_df = quotes_df.copy()

    # Map symbol to stream_id
    result_df["stream_id"] = result_df["symbol"].map(id_mapping)

    # Drop rows where we couldn't map the symbol to a stream_id
    result_df = result_df.dropna(subset=["stream_id"])

    # Set the timestamp
    current_timestamp = timestamp or int(pd.Timestamp.now().timestamp())
    result_df["date"] = ensure_unix_timestamp(current_timestamp)  # Keep as numeric for batch_insert_unix_tn_records

    # Use price as the value, converting to string as required by TnRecordModel
    result_df["value"] = result_df["price"].astype(str)

    # Select only the required columns
    result_df = result_df[["stream_id", "date", "value"]]

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
        raise ValueError(
            f"Timestamp outside valid range (1970-2100): {converted_time}"
        )
    
    return converted_time


@task(retries=3, retry_delay_seconds=10)
def fetch_quotes_for_batch(fmp_block: FMPBlock, symbols_batch: list[str]) -> DataFrame[BatchQuoteShort]:
    """
    Fetches quotes for a batch of symbols using the provided FMPBlock.
    Returns quotes as a DataFrame[BatchQuoteShort].
    """
    logger = get_run_logger()
    try:
        df = fmp_block.get_batch_quote(symbols_batch)
        logger.info(f"Fetched quotes for symbols: {symbols_batch}")
        return df
    except RuntimeError as e:
        logger.error(f"Error fetching quotes for batch {symbols_batch}: {e}")
        raise  # Re-raise the RuntimeError directly
    except Exception as e:
        logger.error(f"Error fetching quotes for batch {symbols_batch}: {e}")
        raise RuntimeError(f"API Error: {e}") from e


@task
def combine_batch_results(quotes_batches: list[DataFrame[BatchQuoteShort]]) -> DataFrame[BatchQuoteShort]:
    """
    Combines multiple quote batches into a single DataFrame.

    Args:
        quotes_batches: List of DataFrames containing quotes in BatchQuoteShort format

    Returns:
        Combined DataFrame in BatchQuoteShort format

    Raises:
        ValueError: If any batch in quotes_batches is None, indicating a failed API call
        RuntimeError: If propagating an API error from the FMP block
    """
    errors = []
    for i, batch in enumerate(quotes_batches):
        if isinstance(batch, Exception):
            errors.append(f"Batch {i}: {batch}")
        elif batch is None:
            errors.append(f"Batch {i} is None")
    if errors:
        error_msg = "Failed fetching quote batches: " + "; ".join(errors)
        raise RuntimeError(error_msg)

    # At this point all batches are valid DataFrames
    return DataFrame[BatchQuoteShort](pd.concat(quotes_batches, ignore_index=True))


@task
def process_data(
    quotes_df: Optional[DataFrame[BatchQuoteShort]], descriptor_df: pd.DataFrame
) -> DataFrame[TnDataRowModel]:
    """
    Process real-time market data.

    Args:
        quotes_df: DataFrame containing quotes in BatchQuoteShort format
        descriptor_df: DataFrame containing source descriptors

    Returns:
        DataFrame in TnDataRowModel format

    Raises:
        ValueError: If quotes_df is None, indicating a failed API call
        RuntimeError: If propagating an API error from the FMP block
    """
    logger = get_run_logger()
    logger.info("Processing real-time market data")

    # If quotes_df is None or an Exception, raise an error with details
    if quotes_df is None or isinstance(quotes_df, Exception):
        error_msg = f"Cannot process data: quotes DataFrame is {quotes_df}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    # Create mapping from source_id to stream_id
    id_mapping = dict(zip(descriptor_df["source_id"], descriptor_df["stream_id"]))

    # Convert quotes to TnDataRowModel format
    result_df = convert_quotes_to_tn_data(quotes_df, id_mapping)

    logger.info(f"Processed {len(result_df)} quotes into TnDataRowModel format")
    return result_df


@flow(name="Real-Time Market Data Sync Flow")
def real_time_flow(
    fmp_block: FMPBlock,
    psd_block: PrimitiveSourcesDescriptorBlock,
    tn_block: TNAccessBlock,
    tickers_per_request: int = 20000,
    fetch_task: Optional[Any] = None,
):
    """
    Main flow to fetch and update real-time market data.
    This flow uses FMPBlock for API interactions, primitive source descriptor for symbols,
    and TNAccessBlock for inserting the data.

    Error Handling:
    All API and data processing errors are caught and re-raised as RuntimeError("API Error").
    The flow fails fast on any error to prevent partial data updates.

    Args:
        fmp_block: Block for FMP API interactions
        psd_block: Block for primitive source descriptors
        tn_block: Block for TN interactions
        batch_size: Size of symbol batches for API calls
        fetch_task: Optional task override for testing (e.g., with retries disabled)

    Raises:
        RuntimeError: If there is an API error from the FMP block
    """
    logger = get_run_logger()
    logger.info("Starting real-time market data sync flow")

    # Extract source descriptors
    descriptor_df = get_symbols_from_descriptor(psd_block=psd_block)

    # Batch symbols
    batches = batch_symbols(descriptor_df=descriptor_df, batch_size=tickers_per_request)

    # Use the provided fetch task or default to fetch_quotes_for_batch
    fetch = fetch_task if fetch_task is not None else fetch_quotes_for_batch

    try:
        # Fetch quotes for each batch using task mapping with keyword arguments
        # we let prefect handle the concurrency aspect
        quotes_batches = fetch.map(fmp_block=unmapped(fmp_block), symbols_batch=batches)  # type: ignore

        # Combine all batch results
        combined_data = combine_batch_results(quotes_batches=quotes_batches)

        # Process the combined data into TnDataRowModel format
        processed_data = process_data(quotes_df=combined_data, descriptor_df=descriptor_df)

        # Insert processed data into TN
        results = task_split_and_insert_records(
            block=tn_block,
            records=processed_data,
            wait=False,
            is_unix=True,
        )
        logger.info(f"Completed real-time market data sync flow: {results}")
    except RuntimeError as e:
        # Re-raise the RuntimeError to maintain the original error type
        raise RuntimeError("API Error") from e
    except ValueError as e:
        # Convert ValueError to RuntimeError to maintain consistent error handling
        raise RuntimeError("API Error") from e
