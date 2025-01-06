"""
Task wrappers for Argentina SEPA data ingestion pipeline components.
"""

from typing import cast

import pandas as pd
from prefect import get_run_logger, task
from prefect.artifacts import create_markdown_artifact

from tsn_adapters.tasks.argentina.interfaces.base import (
    IDataTransformer,
    IProviderGetter,
    IReconciliationStrategy,
    ITargetGetter,
)
from tsn_adapters.tasks.argentina.models.category_map import SepaProductCategoryMapModel
from tsn_adapters.tasks.argentina.provider import SepaByDateGetter, create_sepa_provider
from tsn_adapters.tasks.argentina.reconciliation import create_reconciliation_strategy
from tsn_adapters.tasks.argentina.scrapers.sepa_scraper import SepaPreciosScraper
from tsn_adapters.tasks.argentina.stream_details import create_stream_details_fetcher
from tsn_adapters.tasks.argentina.target import create_trufnetwork_components
from tsn_adapters.tasks.argentina.transformers import SepaDataTransformer, create_sepa_transformer
from tsn_adapters.tasks.argentina.types import (
    DateStr,
    NeededKeysMap,
    SepaDF,
    StreamId,
    StreamIdMap,
    StreamMetadataDF,
)


# Stream Details Tasks
@task(retries=3)
def task_create_stream_fetcher(source_type: str, block_name: str):
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
def task_get_streams(fetcher) -> StreamMetadataDF:
    """Get stream metadata using the fetcher."""
    logger = get_run_logger()
    logger.info("Fetching stream metadata")

    try:
        streams_df = fetcher.get_streams()
    except Exception as e:
        logger.error(f"Failed to fetch streams: {e}")
        raise

    if streams_df.empty:
        logger.warning("No streams found in the fetcher response")
        return streams_df

    # Create summary artifact
    summary = ["# Streams Metadata Summary\n\n"]

    # Basic statistics
    total_streams = len(streams_df)
    logger.info(f"Successfully fetched {total_streams} streams")

    summary.extend(
        [
            "## Overview\n",
            f"- Total streams: {total_streams}\n\n",
        ]
    )

    # Stream details
    summary.append("## Streams\n")
    for _, stream in streams_df.iterrows():
        summary.extend(
            [
                f"- Stream Id: {stream['stream_id']}, Source Id: {stream['source_id']}\n",
            ]
        )

    create_markdown_artifact(
        key="streams-metadata-summary", markdown="".join(summary), description="Summary of available SEPA data streams"
    )

    return streams_df


# Provider Tasks
@task(retries=3)
def task_create_sepa_provider():
    """Create and return a SEPA provider."""
    logger = get_run_logger()
    logger.info("Creating SEPA provider")

    try:
        scraper = SepaPreciosScraper()
        provider = create_sepa_provider(scraper=scraper)
        provider.load_items()
    except Exception as e:
        logger.error(f"Failed to create SEPA provider: {e}")
        raise

    # Create summary of loaded historical items
    items = provider._historical_items
    if not items:
        logger.warning("No historical items found in provider")
        return provider

    summary = ["# Historical Items Summary\n\n"]

    # Basic statistics
    total_items = len(items)
    unique_dates = len(set(items.keys()))
    logger.info(f"Loaded {total_items:,} items across {unique_dates:,} unique dates")

    summary.extend(
        [
            "## Overview\n",
            f"- Total items: {total_items:,}\n",
            f"- Unique dates: {unique_dates:,}\n\n",
        ]
    )

    # Date range information
    if items:
        dates = sorted(items.keys())
        date_span = (pd.to_datetime(dates[-1]) - pd.to_datetime(dates[0])).days
        logger.info(f"Date range: {dates[0]} to {dates[-1]} ({date_span} days)")

        summary.extend(
            [
                "## Date Range\n",
                f"- First date: {dates[0]}\n",
                f"- Last date: {dates[-1]}\n",
                f"- Total span: {date_span} days\n\n",
            ]
        )

        # Sample of recent dates
        summary.extend(["## Most Recent Dates\n", *[f"- {date}\n" for date in dates[-5:]], "\n"])

    create_markdown_artifact(
        key="historical-items-summary",
        markdown="".join(summary),
        description="Summary of historical items loaded by the SEPA provider",
    )

    return provider


@task(retries=2)
def task_load_sepa_items(provider: SepaByDateGetter):
    """Load items into the provider."""
    logger = get_run_logger()
    logger.info("Loading SEPA items")
    try:
        provider.load_items()
        logger.info("Successfully loaded SEPA items")
    except Exception as e:
        logger.error(f"Failed to load SEPA items: {e}")
        raise


@task(name="get_data_for_date", task_run_name="get data for {date}", retries=3)
def task_get_data_for_date(provider: IProviderGetter, date: DateStr) -> SepaDF:
    """Get data for a specific date using the provider."""
    logger = get_run_logger()
    logger.info(f"Fetching data for date: {date}")
    try:
        data = provider.get_data_for(date)
        if data.empty:
            logger.warning(f"No data found for date: {date}")
        else:
            logger.info(f"Successfully fetched {len(data)} records for date: {date}")
        return data
    except Exception as e:
        logger.error(f"Failed to get data for date {date}: {e}")
        raise


# Target Tasks
@task(retries=3)
def task_create_trufnetwork_components(block_name: str):
    """Create and return TrufNetwork getter and setter components."""
    logger = get_run_logger()
    logger.info(f"Creating TrufNetwork components with block: {block_name}")
    try:
        components = create_trufnetwork_components(block_name=block_name)
        logger.info("Successfully created TrufNetwork components")
        return components
    except Exception as e:
        logger.error(f"Failed to create TrufNetwork components: {e}")
        raise


@task(retries=2)
def task_get_latest_records(getter: ITargetGetter, stream_id: StreamId, data_provider: str) -> pd.DataFrame:
    """Get latest records from the target system."""
    logger = get_run_logger()
    logger.info(f"Fetching latest records for stream {stream_id}")
    try:
        data = getter.get_latest(stream_id, data_provider)
        if data.empty:
            logger.warning(f"No latest records found for stream {stream_id}")
        else:
            logger.info(f"Successfully fetched {len(data)} latest records for stream {stream_id}")
        return data
    except Exception as e:
        logger.error(f"Failed to get latest records for stream {stream_id}: {e}")
        raise


@task
def task_insert_data(setter, stream_id: StreamId, data: pd.DataFrame, data_provider: str) -> None:
    """Insert data into the target system."""
    logger = get_run_logger()
    if data.empty:
        logger.warning(f"Skipping insertion for stream {stream_id}: No data to insert")
        return

    logger.info(f"Inserting {len(data)} records into stream {stream_id}")
    try:
        setter.insert_data(stream_id, data, data_provider)
        logger.info(f"Successfully inserted data into stream {stream_id}")
    except Exception as e:
        logger.error(f"Failed to insert data into stream {stream_id}: {e}")
        raise


# Reconciliation Tasks
@task
def task_create_reconciliation_strategy():
    """Create and return a reconciliation strategy."""
    logger = get_run_logger()
    logger.info("Creating reconciliation strategy")
    try:
        strategy = create_reconciliation_strategy()
        logger.info("Successfully created reconciliation strategy")
        return strategy
    except Exception as e:
        logger.error(f"Failed to create reconciliation strategy: {e}")
        raise


@task
def task_determine_needed_keys(
    strategy: IReconciliationStrategy, streams_df: StreamMetadataDF, target_getter: ITargetGetter, data_provider: str
) -> NeededKeysMap:
    """Determine which keys need to be fetched."""
    logger = get_run_logger()
    logger.info("Determining needed keys")
    try:
        needed_keys = strategy.determine_needed_keys(streams_df, target_getter, data_provider)
        if not needed_keys:
            logger.warning("No keys need to be fetched")
        else:
            logger.info(f"Found {len(needed_keys)} streams that need updating")
        return needed_keys
    except Exception as e:
        logger.error(f"Failed to determine needed keys: {e}")
        raise


# Transformer Tasks
@task
def task_create_transformer(product_category_map_df: pd.DataFrame, stream_id_map: StreamIdMap):
    """Create and return a SEPA transformer."""
    logger = get_run_logger()
    if product_category_map_df.empty:
        logger.error("Cannot create transformer: Product category map is empty")
        raise ValueError("Product category map is empty")

    logger.info("Creating SEPA transformer")
    try:
        transformer = create_sepa_transformer(
            product_category_map_df=product_category_map_df, stream_id_map=stream_id_map
        )
        logger.info("Successfully created SEPA transformer")
        return transformer
    except Exception as e:
        logger.error(f"Failed to create transformer: {e}")
        raise


@task
def task_transform_data(transformer: IDataTransformer, data: SepaDF) -> pd.DataFrame:
    """Transform data using the transformer."""
    logger = get_run_logger()
    if data.empty:
        logger.warning("No data to transform")
        return data

    logger.info(f"Transforming {len(data)} records")

    # Check for uncategorized products before transformation
    sepa_transformer = cast(SepaDataTransformer, transformer)
    try:
        uncategorized = sepa_transformer.get_uncategorized(data)
    except Exception as e:
        logger.error(f"Failed to check for uncategorized products: {e}")
        raise

    if not uncategorized.empty:
        logger.warning(f"Found {len(uncategorized)} uncategorized products")
        # Create summary for uncategorized products
        summary = ["# Uncategorized Products Report\n\n"]
        summary.extend(
            [
                "## Overview\n",
                f"- Total uncategorized products: {len(uncategorized)}\n",
                f"- Date: {uncategorized['date'].iloc[0]}\n\n",
                "## Sample Products\n",
            ]
        )

        # Show up to 10 sample products
        for _, row in uncategorized.head(10).iterrows():
            summary.extend(
                [
                    f"### Product: {row['productos_descripcion']}\n",
                    f"- ID: {row['id_producto']}\n",
                    f"- Price: {row['productos_precio_lista']}\n",
                    "\n",
                ]
            )

        create_markdown_artifact(
            key=f"uncategorized-products-{uncategorized['date'].iloc[0]}",
            markdown="".join(summary),
            description=f"Uncategorized products for date {uncategorized['date'].iloc[0]}",
        )

    # Proceed with the transformation
    try:
        transformed_data = transformer.transform(data)
        logger.info(f"Successfully transformed data into {len(transformed_data)} records")
        return transformed_data
    except Exception as e:
        logger.error(f"Failed to transform data: {e}")
        raise


# Category Map Tasks
@task(retries=3)
def task_load_category_map(url: str) -> pd.DataFrame:
    """Load the product category mapping."""
    logger = get_run_logger()
    logger.info(f"Loading category map from {url}")
    try:
        df = SepaProductCategoryMapModel.task_from_url(
            url=url,
            compression="zip",
            sep="|",
        )
        if df.empty:
            logger.error("Loaded category map is empty")
            raise ValueError("Category map is empty")
        logger.info(f"Successfully loaded category map with {len(df)} mappings")
        return df
    except Exception as e:
        logger.error(f"Failed to load category map: {e}")
        raise
