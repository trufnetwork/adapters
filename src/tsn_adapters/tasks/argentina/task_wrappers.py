"""
Task wrappers for Argentina data pipeline.
"""

from datetime import datetime, timedelta

import pandas as pd
from pandera.typing import DataFrame
from prefect import get_run_logger, task
import prefect.cache_policies as policies
from prefect.concurrency.sync import concurrency

from tsn_adapters.common.interfaces.provider import IProviderGetter, IStreamSourceMapFetcher
from tsn_adapters.common.interfaces.reconciliation import IReconciliationStrategy
from tsn_adapters.common.interfaces.target import ITargetClient
from tsn_adapters.common.interfaces.transformer import IDataTransformer
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.tasks.argentina.models.category_map import SepaProductCategoryMapModel
from tsn_adapters.tasks.argentina.provider.factory import create_sepa_processed_provider
from tsn_adapters.tasks.argentina.reconciliation.strategies import create_reconciliation_strategy
from tsn_adapters.tasks.argentina.stream_details import PrimitiveSourcesTypeStr, create_stream_details_fetcher
from tsn_adapters.tasks.argentina.transformers.sepa import create_sepa_transformer
from tsn_adapters.tasks.argentina.types import (
    AggregatedPricesDF,
    DateStr,
    SepaDF,
    StreamId,
    StreamIdMap,
    StreamSourceMapDF,
)

from .models.stream_source import StreamSourceMetadataModel


# Stream Details Tasks
@task(retries=3)
def task_create_stream_fetcher(source_type: PrimitiveSourcesTypeStr, block_name: str) -> IStreamSourceMapFetcher:
    """Create and return a stream details fetcher."""
    logger = get_run_logger()
    logger.info(f"Creating stream fetcher for source type: {source_type}")
    try:
        fetcher = create_stream_details_fetcher(source_type=source_type, block_name=block_name)
        return fetcher
    except Exception as e:
        logger.error(f"Failed to create stream fetcher: {e}")
        raise


@task(retries=2)
def task_get_streams(fetcher: IStreamSourceMapFetcher) -> StreamSourceMapDF:
    """Get stream metadata using the fetcher."""
    logger = get_run_logger()
    logger.info("Fetching stream metadata")
    try:
        streams_df = fetcher.get_streams()
        logger.info(f"Found {len(streams_df)} streams")
        return StreamSourceMetadataModel.validate(streams_df)
    except Exception as e:
        logger.error(f"Failed to fetch streams: {e}")
        raise


# Provider Tasks
@task(retries=3)
def task_create_sepa_provider() -> IProviderGetter[DateStr, DataFrame[AggregatedPricesDF]]:
    """
    Create a SEPA provider instance.

    Args:
        provider_type: Type of provider to create ('website' or 's3')
        s3_block_name: Name of the S3 block to use (required for 's3' provider)
        s3_prefix: The prefix for S3 keys (only used for 's3' provider)
        delay_seconds: Delay between requests for website scraping (only used for 'website' provider)
        show_progress_bar: Whether to show progress bars during downloads

    Returns:
        IProviderGetter: The provider instance
    """
    logger = get_run_logger()
    logger.info("Creating SEPA provider")
    return create_sepa_processed_provider()


@task(
    name="get_data_for_date",
    task_run_name="get data for {date}",
    retries=3,
)
def task_get_data_for_date(provider: IProviderGetter[DateStr, AggregatedPricesDF], date: DateStr) -> AggregatedPricesDF:
    """
    Get SEPA data for a specific date.

    Args:
        provider: The provider to use
        date: The date to fetch data for

    Returns:
        DataFrame: The SEPA data
    """
    logger = get_run_logger()
    logger.info(f"Fetching data for date: {date}")
    try:
        df = provider.get_data_for(date)
        if df.empty:
            logger.warning(f"No data found for date: {date}")
        else:
            logger.info(f"Got {len(df)} rows for date: {date}")
        return df
    except Exception as e:
        logger.error(f"Failed to get data for date {date}: {e}")
        raise


# Target Tasks
@task(retries=2)
def task_get_latest_records(client: ITargetClient[StreamId], stream_id: StreamId, data_provider: str) -> pd.DataFrame:
    """
    Get latest records from the target system.

    Args:
        client: The target client to use
        stream_id: The stream ID to fetch data for
        data_provider: The data provider identifier

    Returns:
        pd.DataFrame: The latest records from the target system
    """
    logger = get_run_logger()
    logger.info(f"Getting latest records for stream: {stream_id}")
    try:
        df = client.get_latest(stream_id=stream_id, data_provider=data_provider)
        logger.info(f"Got {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Failed to get latest records: {e}")
        raise


@task
def task_insert_data(client: ITargetClient[StreamId], data: DataFrame[TnDataRowModel]) -> None:
    """
    Insert data into the target system.

    Args:
        client: The target client to use
        data: The data to insert
    """
    logger = get_run_logger()
    logger.info(f"Inserting {len(data)} rows")
    try:
        client.batch_insert_data(data=data)
        logger.info("Data inserted successfully")
    except Exception as e:
        logger.error(f"Failed to insert data: {e}")
        raise


# Reconciliation Tasks
@task
def task_create_reconciliation_strategy() -> IReconciliationStrategy[DateStr, StreamId]:
    """Create and return a reconciliation strategy."""
    logger = get_run_logger()
    logger.info("Creating reconciliation strategy")
    try:
        return create_reconciliation_strategy()
    except Exception as e:
        logger.error(f"Failed to create reconciliation strategy: {e}")
        raise


@task
def task_determine_needed_keys(
    strategy: IReconciliationStrategy[DateStr, StreamId],
    streams_df: StreamSourceMapDF,
    provider_getter: IProviderGetter[DateStr, SepaDF],
    target_client: ITargetClient[StreamId],
    data_provider: str,
) -> dict[StreamId, list[DateStr]]:
    """
    Determine which keys need to be fetched.

    - from the target, get the latest records for each stream
    - from the provider, get the dates for which data is available
    - determine which dates are needed by comparing the target and provider data

    we don't cache, because the intention is to detect changes on the provider or target
    """
    logger = get_run_logger()
    logger.info("Determining needed keys")
    try:
        needed_keys = strategy.determine_needed_keys(
            streams_df=streams_df,
            provider_getter=provider_getter,
            target_client=target_client,
            data_provider=data_provider,
        )
        for stream_id, keys in needed_keys.items():
            logger.info(f"Stream {stream_id} needs {len(keys)} keys")
        return needed_keys
    except Exception as e:
        logger.error(f"Failed to determine needed keys: {e}")
        raise


# Transformer Tasks
@task
def task_create_transformer(stream_id_map: StreamIdMap) -> IDataTransformer[AggregatedPricesDF]:
    """Create and return a data transformer."""
    logger = get_run_logger()
    logger.info("Creating data transformer")
    try:
        return create_sepa_transformer(stream_id_map=stream_id_map)
    except Exception as e:
        logger.error(f"Failed to create transformer: {e}")
        raise


@task
def task_transform_data(transformer: IDataTransformer[AggregatedPricesDF], data: AggregatedPricesDF) -> DataFrame[TnDataRowModel]:
    """Transform data from source format to target format."""
    logger = get_run_logger()
    logger.info("Transforming data")
    try:
        df = transformer.transform(data)
        logger.info(f"Transformed {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to transform data: {e}")
        raise


@task(
    cache_policy=policies.INPUTS + policies.TASK_SOURCE,
    cache_expiration=timedelta(days=7),
    cache_key_fn=lambda ctx, args: f"get_and_transform_data_{args['date']}",
)
def task_get_and_transform_data(
    provider: IProviderGetter[DateStr, AggregatedPricesDF], transformer: IDataTransformer[AggregatedPricesDF], date: DateStr
) -> DataFrame[TnDataRowModel]:
    """Get data for a date and transform it."""
    # guess we're using 1GB of memory
    with concurrency("memory-usage", 1000):
        data = task_get_data_for_date(provider=provider, date=date)
        return task_transform_data(transformer=transformer, data=data)


@task(retries=3, cache_expiration=timedelta(hours=1), cache_policy=policies.INPUTS + policies.TASK_SOURCE)
def task_load_category_map(url: str) -> pd.DataFrame:
    """Load the product category mapping from a URL."""
    logger = get_run_logger()
    logger.info(f"Loading category map from: {url}")
    try:
        df = SepaProductCategoryMapModel.from_url(url, sep="|", compression="zip")
        logger.info(f"Loaded {len(df)} category mappings")
        return df
    except Exception as e:
        logger.error(f"Failed to load category map: {e}")
        raise


@task
def task_dates_already_processed(needed_dates: list[DateStr]) -> bool:
    """
    If the needed dates are
    """
    # stringify the dates
    cache_key = "|".join(needed_dates)

    # get the current date with cache
    cache_expiration = timedelta(days=1)
    get_date_with_cache = task_get_now_date.with_options(cache_expiration=cache_expiration)

    real_date = datetime.now()
    maybe_cached_date = get_date_with_cache(cache_key=cache_key)

    # in theory is instant, but we'll give it a grace period to know its not cached
    FRESHNESS_PERIOD = timedelta(seconds=10)

    # check if the date is fresh or was cached
    its_cached = abs(real_date - maybe_cached_date) > FRESHNESS_PERIOD
    if its_cached:
        return True

    return False


# cache here is a placeholder. the real one should be set by the caller
@task(cache_policy=policies.INPUTS + policies.TASK_SOURCE)
def task_get_now_date(cache_key: str) -> datetime:
    """Get the current date.

    key is used to cache the date, so we can check if the date has changed
    """
    return datetime.now()
