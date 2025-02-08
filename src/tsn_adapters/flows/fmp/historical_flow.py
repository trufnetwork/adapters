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
from pandera.typing import DataFrame
from prefect import flow, task
from prefect.concurrency.sync import rate_limit
from prefect.tasks import task_input_hash

from tsn_adapters.blocks.fmp import FMPBlock, IntradayData
from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel, PrimitiveSourcesDescriptorBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.utils import deroutine
from tsn_adapters.utils.logging import get_logger_safe

# Constants for flow control
MAX_CONCURRENT_INSERTS = 5
MAX_CONCURRENT_FETCHES = 3
BATCH_SIZE = 50000  # Number of records to batch for TN insertion
DEFAULT_MIN_FETCH_DATE = datetime.datetime.now() - datetime.timedelta(days=365 * 30)

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
    data: DataFrame[IntradayData]


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


def ensure_unix_timestamp(dt: pd.Series) -> pd.Series:
    """Convert datetime series to Unix timestamp (seconds since epoch)."""
    return dt.astype(int) // 10**9


@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash)
def fetch_historical_data(
    fmp_block: FMPBlock,
    symbol: str,
    start_date: str,
    end_date: str,
) -> DataFrame[IntradayData]:
    """
    Fetch historical intraday data for the given symbol and date range.

    Args:
        fmp_block: Block for FMP API interactions
        symbol: Stock symbol to fetch data for
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        DataFrame containing historical intraday data

    Raises:
        FMPDataFetchError: If there's an error fetching data from FMP
    """
    logger = get_logger_safe(__name__)
    try:
        df = fmp_block.get_intraday_data(symbol, start_date=start_date, end_date=end_date)
        logger.info(f"Fetched {len(df)} historical records for {symbol}")
        return df
    except Exception as e:
        logger.error(f"Error fetching historical data for {symbol}: {e}")
        raise FMPDataFetchError(f"Failed to fetch data for {symbol}: {e}") from e


def convert_intraday_to_tn_df(
    intraday_df: DataFrame[IntradayData],
    stream_id: str,
) -> DataFrame[TnDataRowModel]:
    """
    Convert IntradayData DataFrame to TnDataRowModel format.
    Uses the 'close' price as the value.

    Args:
        intraday_df: DataFrame containing intraday data
        stream_id: ID of the stream to associate with the data

    Returns:
        DataFrame in TN format

    Raises:
        ValueError: If the conversion fails or validation fails
    """
    if len(intraday_df) == 0:
        # Return an empty DataFrame of data rows
        return cast(DataFrame[TnDataRowModel], pd.DataFrame())

    try:
        # Create the DataFrame with explicit types
        result_df = pd.DataFrame(
            {
                "stream_id": pd.Series([stream_id] * len(intraday_df), dtype=str),
                "date": ensure_unix_timestamp(pd.to_datetime(intraday_df["date"])),  # Convert to Unix timestamp
                "value": intraday_df["close"].astype(str),
            }
        )
        # Convert to DataFrame[TnDataRowModel] and validate
        return DataFrame[TnDataRowModel](result_df)

    except Exception as e:
        raise ValueError(f"Failed to convert intraday data to TN format: {e}") from e


def run_ticker_pipeline(
    descriptor_df: DataFrame[PrimitiveSourceDataModel],
    fmp_block: FMPBlock,
    tn_block: TNAccessBlock,
    min_fetch_date: datetime.datetime,
    logger: Any,
) -> None:
    """
    Create and execute a pipeline for processing tickers with backpressure control.

    Backpressure Control:
      - Throttle FMP API calls using Prefect's rate_limit.
      - Accumulate TN data rows and synchronously, until BATCH_SIZE is reached, insert them before processing the next ticker.
      - This is to avoid overwhelming the TN system.

    Args:
        descriptor_df: DataFrame containing ticker descriptors.
        fmp_block: Block for FMP API interactions.
        tn_block: Block for TN interactions.
        min_fetch_date: Minimum date to fetch data from.
        logger: Logger instance.

    Raises:
        Exceptions during processing are logged and raised.
    """
    def process_ticker(row: pd.Series) -> TickerResult:
        """Process a single ticker row."""
        symbol = row["source_id"]
        stream_id = row["stream_id"]

        try:
            # Get earliest data date first - this implicitly checks if stream exists
            earliest_date = get_earliest_data_date(tn_block=tn_block, stream_id=stream_id)
            if earliest_date is None:
                logger.warning(f"Stream not found for {symbol}")
                return TickerError(symbol, stream_id, error="stream_not_found")

            # Calculate date range
            end_date = earliest_date.strftime("%Y-%m-%d")
            start_date = max((earliest_date - datetime.timedelta(days=365)), min_fetch_date).strftime("%Y-%m-%d")

            # Fetch historical data
            intraday_data = fetch_historical_data(
                fmp_block=fmp_block,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            )

            if len(intraday_data) == 0:
                logger.warning(f"No data found for {symbol}")
                return TickerError(symbol, stream_id, error="no_data")

            logger.info(f"Successfully fetched data for {symbol}")
            return TickerSuccess(symbol, stream_id, data=intraday_data)

        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            return TickerError(symbol, stream_id, error=str(e))

    # Process tickers with backpressure control
    records_to_insert = pd.DataFrame()
    try:
        # First, fetch all data
        for _, row_data in descriptor_df.iterrows():
            result = process_ticker(row_data)
            if is_ticker_success(result):
                tn_df = convert_intraday_to_tn_df(result.data, result.stream_id)
                records_to_insert = pd.concat([records_to_insert, tn_df])

            if len(records_to_insert) > BATCH_SIZE:
                validated_df = DataFrame[TnDataRowModel](records_to_insert.iloc[0:BATCH_SIZE])
                records_to_insert = records_to_insert.iloc[BATCH_SIZE:]
                tn_block.split_and_insert_records_unix(records=validated_df, wait=False)

            logger.info("Completed ticker processing pipeline")
    
        # Process remaining records
        if len(records_to_insert) > 0:
            validated_df = DataFrame[TnDataRowModel](records_to_insert)
            tn_block.split_and_insert_records_unix(records=validated_df, wait=False)

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


@flow(name="Historical Market Data Sync Flow")
async def historical_flow(
    fmp_block: FMPBlock,
    psd_block: PrimitiveSourcesDescriptorBlock,
    tn_block: TNAccessBlock,
    min_fetch_date: datetime.datetime = DEFAULT_MIN_FETCH_DATE,
) -> None:
    """
    Main flow to fetch and update historical market data.

    Args:
        fmp_block: Block for FMP API interactions
        psd_block: Block for primitive source descriptors
        tn_block: Block for TN interactions
        min_fetch_date: Minimum date to fetch data from

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

        # Create and run the reactive pipeline
        run_ticker_pipeline(
            descriptor_df=descriptor_df,
            fmp_block=fmp_block,
            tn_block=tn_block,
            min_fetch_date=min_fetch_date,
            logger=logger,
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
