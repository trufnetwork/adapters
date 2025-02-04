"""
Real-Time Market Data Sync Flow

This module contains a Prefect flow to sync real-time market data from Financial Modeling Prep (FMP) API.

Flow tasks:
    - fetch_realtime_data: Fetch the last market data snapshot using fmpsdk.
    - process_data: Process and transform fetched data (placeholder).
    - update_primitives: Update the primitive sources with the new data (placeholder).

The flow uses lightweight error handling and logging.
"""

from typing import Any, Optional, TypedDict

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow, get_run_logger, task, unmapped

from tsn_adapters.blocks.fmp import BatchQuoteShort, FMPBlock
from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel, PrimitiveSourcesDescriptorBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock, task_batch_insert_unix_and_wait_for_tx
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
    result_df["date"] = current_timestamp

    # Use price as the value, converting to string as required by TnRecordModel
    result_df["value"] = result_df["price"].astype(str)

    # Select only the required columns
    result_df = result_df[["stream_id", "date", "value"]]

    return DataFrame[TnDataRowModel](result_df)


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
    # Check for failed batches
    if any(batch is None for batch in quotes_batches):
        # If any batch is None due to a RuntimeError, propagate it
        for batch in quotes_batches:
            if isinstance(batch, RuntimeError):
                raise batch
        # Otherwise, raise a ValueError with a descriptive message
        raise ValueError("One or more quote batches failed to fetch. Cannot proceed with partial data.")

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

    # If quotes_df is None (API error), raise an error
    if quotes_df is None:
        # If quotes_df is None due to a RuntimeError, propagate it
        if isinstance(quotes_df, RuntimeError):
            raise quotes_df
        # Otherwise, raise a ValueError with a descriptive message
        raise ValueError("Cannot process data: quotes DataFrame is None due to failed API call")

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
    batch_size: int = 20000,
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
    batches = batch_symbols(descriptor_df=descriptor_df, batch_size=batch_size)

    # Use the provided fetch task or default to fetch_quotes_for_batch
    fetch = fetch_task if fetch_task is not None else fetch_quotes_for_batch

    try:
        # Fetch quotes for each batch using task mapping
        quotes_batches = fetch.map(unmapped(fmp_block), batches)

        # Combine all batch results
        combined_data = combine_batch_results(quotes_batches=quotes_batches)

        # Process the combined data into TnDataRowModel format
        processed_data = process_data(quotes_df=combined_data, descriptor_df=descriptor_df)

        # Insert processed data into TN
        task_batch_insert_unix_and_wait_for_tx(block=tn_block, records=processed_data)

        logger.info("Completed real-time market data sync flow")
    except RuntimeError as e:
        # Re-raise the RuntimeError to maintain the original error type
        raise RuntimeError("API Error") from e
    except ValueError as e:
        # Convert ValueError to RuntimeError to maintain consistent error handling
        raise RuntimeError("API Error") from e