"""
Historical Market Data Sync Flow

This module contains a Prefect flow to sync historical market data from Financial Modeling Prep (FMP) API.
The flow fetches daily interval data for the past year for each ticker in the descriptor.

It's expected initially:
- 60K+ tickers in the descriptor
- 30 years of data for each ticker
- If the data is daily, this would mean 657M records
- 657M records = 13140 batches -> 438 batches per run
"""

import datetime
from typing import Any, NamedTuple, Optional, TypeGuard, TypeVar, Union, cast

import pandas as pd
from pandera.typing import DataFrame, Series
from prefect import flow, task
from prefect.futures import PrefectFuture
from prefect.tasks import task_input_hash

from tsn_adapters.blocks.fmp import EODData, FMPBlock
from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel, PrimitiveSourcesDescriptorBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock, task_batch_insert_tn_records
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.utils import deroutine
from tsn_adapters.utils.logging import get_logger_safe

# Constants for flow control
DEFAULT_MIN_FETCH_DATE = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=365 * 30)

# Type variables for RxPy
T = TypeVar("T")
R = TypeVar("R")

# Type aliases for blocks and iterrows
BlockType = Union[FMPBlock, PrimitiveSourcesDescriptorBlock, TNAccessBlock]


# Error classes for better error handling
class HistoricalFlowError(Exception):
    """Base error class for historical flow errors."""

    pass


class StreamNotFoundError(HistoricalFlowError):
    """Error raised when a TN stream is not found."""

    pass


class FMPDataFetchError(HistoricalFlowError):
    """Error raised when FMP data fetch fails."""

    pass


class TNQueryError(HistoricalFlowError):
    """Error raised when TN query fails."""

    pass


class TickerSuccess(NamedTuple):
    """Container for successful ticker data."""

    symbol: str
    stream_id: str
    data: DataFrame[EODData]


class TickerError(NamedTuple):
    """Container for ticker error information."""

    symbol: str
    stream_id: str
    error: str


TickerResult = Union[TickerSuccess, TickerError]


def is_ticker_success(result: TickerResult) -> TypeGuard[TickerSuccess]:
    """Type guard to check if a TickerResult is successful."""
    return isinstance(result, TickerSuccess)


def is_ticker_error(result: TickerResult) -> TypeGuard[TickerError]:
    """Type guard to check if a TickerResult is an error."""
    return isinstance(result, TickerError)


@task
def get_active_tickers(psd_block: PrimitiveSourcesDescriptorBlock) -> DataFrame[PrimitiveSourceDataModel]:
    """
    Retrieve the list of active tickers from the primitive sources descriptor.

    Args:
        psd_block: Block for accessing primitive source descriptors

    Returns:
        DataFrame containing active tickers and their stream mappings

    Raises:
        HistoricalFlowError: If there's an error retrieving the tickers
    """
    logger = get_logger_safe(__name__)
    try:
        descriptor_df = psd_block.get_descriptor()
        logger.info(f"Retrieved {len(descriptor_df)} active tickers")
        return descriptor_df
    except Exception as e:
        logger.error(f"Error retrieving active tickers: {e}")
        raise HistoricalFlowError(f"Failed to get active tickers: {e}") from e


@task
def get_earliest_data_date(tn_block: TNAccessBlock, stream_id: str) -> Optional[datetime.datetime]:
    """
    Query TN for the earliest available data for the given stream.

    Args:
        tn_block: Block for TN interactions
        stream_id: ID of the stream to query

    Returns:
        The earliest date if found, otherwise None

    Raises:
        TNQueryError: If there's an error querying TN that is not related to stream not found
        TNAccessBlock.StreamNotFoundError: If the stream does not exist
    """
    logger = get_logger_safe(__name__)
    try:
        return tn_block.get_earliest_date(stream_id=stream_id)
    except TNAccessBlock.StreamNotFoundError:
        logger.warning(f"Stream {stream_id} not found")
        raise  # Re-raise the StreamNotFoundError
    except (TNAccessBlock.InvalidRecordFormatError, TNAccessBlock.InvalidTimestampError, TNAccessBlock.Error) as e:
        logger.error(f"Error getting earliest date for {stream_id}: {e}")
        raise TNQueryError(str(e)) from e


def ensure_unix_timestamp(dt: Series[Any]) -> Series[int]:
    """Convert datetime series to Unix timestamp (seconds since epoch).

    This function handles various datetime formats and ensures the output
    is always in seconds since epoch (Unix timestamp). It explicitly handles
    the conversion from nanoseconds and validates the output range.

    Args:
        dt: A pandas Series containing datetime data in various formats

    Returns:
        A pandas Series containing Unix timestamps (seconds since epoch)

    Raises:
        ValueError: If the resulting timestamps are outside the valid range
                   or if the conversion results in unexpected units
    """
    # Convert to datetime if not already
    if not pd.api.types.is_datetime64_any_dtype(dt):
        dt = pd.to_datetime(dt, utc=True)

    # Get nanoseconds since epoch
    ns_timestamps = dt.astype("int64")

    # Convert to seconds (integer division by 1e9 for nanoseconds)
    second_timestamps = ns_timestamps // 10**9

    # Validate the range (basic sanity check)
    # Unix timestamps should be between 1970 and 2100 approximately
    min_valid_timestamp = 0  # 1970-01-01
    max_valid_timestamp = 4102444800  # 2100-01-01

    if second_timestamps.lt(min_valid_timestamp).any() or second_timestamps.gt(max_valid_timestamp).any():
        raise ValueError(
            f"Converted timestamps outside valid range: "
            f"min={second_timestamps.min()}, max={second_timestamps.max()}"
        )

    return second_timestamps


@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def fetch_historical_data(
    fmp_block: FMPBlock,
    symbol: str,
    start_date: str,
    end_date: str,
) -> DataFrame[EODData]:
    """
    Fetch historical EOD data for the given symbol and date range.

    Args:
        fmp_block: Block for FMP API interactions
        symbol: Stock symbol to fetch data for
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        DataFrame containing historical EOD data

    Raises:
        FMPDataFetchError: If there's an error fetching data from FMP
    """
    logger = get_logger_safe(__name__)
    try:
        df = fmp_block.get_historical_eod_data(symbol, start_date=start_date, end_date=end_date)
        logger.info(f"Fetched {len(df)} historical records for {symbol}")
        return df
    except Exception as e:
        logger.error(f"Error fetching historical data for {symbol}: {e}")
        raise FMPDataFetchError(f"Failed to fetch data for {symbol}: {e}") from e


def convert_eod_to_tn_df(
    eod_df: DataFrame[EODData],
    stream_id: str,
) -> DataFrame[TnDataRowModel]:
    """
    Convert EODData DataFrame to TnDataRowModel format.
    Uses the 'price' as the value.

    The dates in EODData are assumed to be in UTC at midnight (00:00:00).
    This is standard for EOD data - the closing price for a day is timestamped
    at midnight UTC of that day.

    Args:
        eod_df: DataFrame containing EOD data
        stream_id: ID of the stream to associate with the data

    Returns:
        DataFrame in TN format with UTC timestamps

    Raises:
        ValueError: If the conversion fails or validation fails
    """
    if len(eod_df) == 0:
        # Return an empty DataFrame of data rows
        return cast(DataFrame[TnDataRowModel], pd.DataFrame())

    try:
        # Create the DataFrame with explicit types
        result_df = pd.DataFrame(
            {
                "stream_id": pd.Series([stream_id] * len(eod_df), dtype=str),
                # Convert ISO dates to UTC midnight timestamps
                "date": ensure_unix_timestamp(
                    pd.to_datetime(eod_df["date"])
                    .dt.tz_localize("UTC")  # Explicitly set UTC timezone
                    .dt.normalize()  # Ensure midnight in UTC
                ),
                "value": eod_df["price"].astype(str),
            }
        )
        # Convert to DataFrame[TnDataRowModel] and validate
        return DataFrame[TnDataRowModel](result_df)

    except Exception as e:
        raise ValueError(f"Failed to convert EOD data to TN format: {e}") from e


@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def process_ticker(
    row: Series[Any],
    fmp_block: FMPBlock,
    tn_block: TNAccessBlock,
    min_fetch_date: datetime.datetime,
    max_fetch_period: datetime.timedelta,
) -> TickerResult:
    """
    Process a single ticker row.

    Args:
        row: Row from the descriptor DataFrame containing ticker information
        fmp_block: Block for FMP API interactions
        tn_block: Block for TN interactions
        min_fetch_date: Minimum date to fetch data from
        max_fetch_period: Maximum period to fetch data for

    Returns:
        TickerResult containing either the fetched data or error information
    """
    logger = get_logger_safe(__name__)
    symbol = str(row["source_id"])
    stream_id = str(row["stream_id"])

    try:
        # Get earliest data date first - this implicitly checks if stream exists
        earliest_date = get_earliest_data_date(tn_block=tn_block, stream_id=stream_id)
        if earliest_date is None:
            earliest_date = datetime.datetime.now(datetime.timezone.utc)

        # Calculate date range
        end_date = earliest_date.strftime("%Y-%m-%d")
        min_fetch_date = min_fetch_date.replace(tzinfo=None)
        start_date = max((earliest_date - max_fetch_period), min_fetch_date).strftime("%Y-%m-%d")

        # Fetch historical data
        eod_data = fetch_historical_data(
            fmp_block=fmp_block,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )

        if len(eod_data) == 0:
            logger.warning(f"No data found for {symbol}")
            return TickerError(symbol, stream_id, error="no_data")

        logger.info(f"Successfully fetched data for {symbol} from {start_date} to {end_date}")
        return TickerSuccess(symbol, stream_id, data=eod_data)

    except Exception as e:
        logger.error(f"Error processing {symbol}: {e}")
        return TickerError(symbol, stream_id, error=str(e))


def run_ticker_pipeline(
    descriptor_df: DataFrame[PrimitiveSourceDataModel],
    fmp_block: FMPBlock,
    tn_block: TNAccessBlock,
    min_fetch_date: datetime.datetime,
    logger: Any,
    max_fetch_period: datetime.timedelta,
    batch_size: int,
    ticker_chunk_size: int,
) -> None:
    """
    Create and execute a pipeline for processing tickers with backpressure control.

    Backpressure Control:
      - Throttle FMP API calls using Prefect's rate_limit.
      - Accumulate TN data rows and synchronously, until BATCH_SIZE is reached,
        insert them before processing the next ticker.
      - This is to avoid overwhelming the ingestor.

    Args:
        descriptor_df: DataFrame containing ticker descriptors.
        fmp_block: Block for FMP API interactions.
        tn_block: Block for TN interactions.
        min_fetch_date: Minimum date to fetch data from.
        logger: Logger instance.
        max_fetch_period: Maximum period to fetch data for.
        batch_size: Number of records to accumulate before inserting.
        ticker_chunk_size: Number of tickers to process in each chunk.
    Raises:
        Exceptions during processing are logged and raised.
    """

    try:
        # Process tickers in chunks
        total_tickers = len(descriptor_df)
        for chunk_start in range(0, total_tickers, ticker_chunk_size):
            chunk_end = min(chunk_start + ticker_chunk_size, total_tickers)
            logger.info(f"Processing ticker chunk {chunk_start}-{chunk_end} of {total_tickers}")

            # Get the current chunk of tickers
            chunked_tickers = descriptor_df.iloc[chunk_start:chunk_end]

            result_futures: list[PrefectFuture[TickerResult]] = []
            # Process tickers with backpressure control
            records_to_insert = pd.DataFrame()

            # Submit all ticker processing tasks in the chunk
            for _, row_data in chunked_tickers.iterrows():
                result_futures.append(
                    process_ticker.submit(
                        row=row_data,
                        fmp_block=fmp_block,
                        tn_block=tn_block,
                        min_fetch_date=min_fetch_date,
                        max_fetch_period=max_fetch_period,
                    )
                )

            # Process results as they complete
            for future in result_futures:
                result = future.result()
                if is_ticker_success(result):
                    tn_df = convert_eod_to_tn_df(result.data, result.stream_id)
                    records_to_insert = pd.concat([records_to_insert, tn_df])
                elif is_ticker_error(result):
                    logger.warning(f"Skipping ticker {result.symbol} due to error: {result.error}")
                else:
                    logger.error(f"Unexpected result type for {row_data['source_id']}: {type(result)}")

                    if len(records_to_insert) >= batch_size:
                        validated_df = DataFrame[TnDataRowModel](records_to_insert)
                        records_to_insert = pd.DataFrame()
                        task_batch_insert_tn_records(
                            block=tn_block,
                            records=validated_df,
                        )

                    logger.info("Completed ticker processing pipeline")

            # Process remaining records for this chunk
            if len(records_to_insert) > 0:
                validated_df = DataFrame[TnDataRowModel](records_to_insert)
                task_batch_insert_tn_records(
                    block=tn_block,
                    records=validated_df,
                )

            logger.info(f"Completed processing ticker chunk {chunk_start}-{chunk_end}")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


@flow(name="Historical Market Data Sync Flow")
async def historical_flow(
    fmp_block: FMPBlock,
    psd_block: PrimitiveSourcesDescriptorBlock,
    tn_block: TNAccessBlock,
    min_fetch_date: datetime.datetime = DEFAULT_MIN_FETCH_DATE,
    batch_size: int = 10000,
    start_from_n_ticker: int = 0,
    max_fetch_period: datetime.timedelta = datetime.timedelta(days=365),
    ticker_chunk_size: int = 100,
) -> None:
    """
    Main flow to fetch and update historical market data.

    Args:
        fmp_block: Block for FMP API interactions
        psd_block: Block for primitive source descriptors
        tn_block: Block for TN interactions
        min_fetch_date: Minimum date to fetch data from
        batch_size: Number of records to accumulate before inserting
        start_from_n_ticker: Index of the first ticker to process
        max_fetch_period: Maximum period to fetch data for
        ticker_chunk_size: Number of tickers to process in each chunk

    Raises:
        HistoricalFlowError: If there's a critical error in the flow
    """
    logger = get_logger_safe(__name__)
    logger.info("Starting historical market data sync flow")

    try:
        # Get active tickers and their stream mappings
        descriptor_df = get_active_tickers(psd_block=psd_block)
        if len(descriptor_df) == 0:
            logger.warning("No active tickers found")
            return

        # Skip the first n tickers
        if start_from_n_ticker > 0:
            descriptor_df = descriptor_df.iloc[start_from_n_ticker:]

        # Create and run the reactive pipeline
        run_ticker_pipeline(
            descriptor_df=descriptor_df,
            fmp_block=fmp_block,
            tn_block=tn_block,
            min_fetch_date=min_fetch_date,
            logger=logger,
            batch_size=batch_size,
            max_fetch_period=max_fetch_period,
            ticker_chunk_size=ticker_chunk_size,
        )

        logger.info("Completed historical market data sync flow")

    except Exception as e:
        logger.error(f"Critical error in historical flow: {e}")
        raise HistoricalFlowError(f"Flow failed: {e}") from e


if __name__ == "__main__":
    # When running directly, we need to provide the blocks
    import asyncio

    async def main():
        # Load blocks using deroutine since they are not actually awaitable
        fmp_block = deroutine(FMPBlock.load("default"))
        psd_block = deroutine(PrimitiveSourcesDescriptorBlock.load("default"))
        tn_block = deroutine(TNAccessBlock.load("default"))
        await historical_flow(fmp_block=fmp_block, psd_block=psd_block, tn_block=tn_block)

    asyncio.run(main())
