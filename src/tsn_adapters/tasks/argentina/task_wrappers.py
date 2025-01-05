"""
Task wrappers for Argentina SEPA data ingestion pipeline components.
"""

from typing import cast

import pandas as pd
from prefect import task
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
@task
def task_create_stream_fetcher(source_type: str, block_name: str):
    """Create and return a stream details fetcher."""
    fetcher = create_stream_details_fetcher(source_type=source_type, block_name=block_name)
    return fetcher


@task
def task_get_streams(fetcher) -> StreamMetadataDF:
    """Get stream metadata using the fetcher."""
    streams_df = fetcher.get_streams()

    # Create summary artifact
    summary = ["# Streams Metadata Summary\n\n"]

    # Basic statistics
    total_streams = len(streams_df)
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
@task
def task_create_sepa_provider():
    """Create and return a SEPA provider."""
    scraper = SepaPreciosScraper()
    provider = create_sepa_provider(scraper=scraper)
    provider.load_items()

    # Create summary of loaded historical items
    items = provider._historical_items

    summary = ["# Historical Items Summary\n\n"]

    # Basic statistics
    total_items = len(items)
    unique_dates = len(set(items.keys()))

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
        summary.extend(
            [
                "## Date Range\n",
                f"- First date: {dates[0]}\n",
                f"- Last date: {dates[-1]}\n",
                f"- Total span: {(pd.to_datetime(dates[-1]) - pd.to_datetime(dates[0])).days} days\n\n",
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


@task
def task_load_sepa_items(provider: SepaByDateGetter):
    """Load items into the provider."""
    provider.load_items()


@task(name="get_data_for_date", task_run_name="get data for {date}")
def task_get_data_for_date(provider: IProviderGetter, date: DateStr) -> SepaDF:
    """Get data for a specific date using the provider."""
    return provider.get_data_for(date)


# Target Tasks
@task
def task_create_trufnetwork_components(block_name: str):
    """Create and return TrufNetwork getter and setter components."""
    return create_trufnetwork_components(block_name=block_name)


@task
def task_get_latest_records(getter: ITargetGetter, stream_id: StreamId, data_provider: str) -> pd.DataFrame:
    """Get latest records from the target system."""
    return getter.get_latest(stream_id, data_provider)


@task
def task_insert_data(setter, stream_id: StreamId, data: pd.DataFrame, data_provider: str) -> None:
    """Insert data into the target system."""
    setter.insert_data(stream_id, data, data_provider)


# Reconciliation Tasks
@task
def task_create_reconciliation_strategy():
    """Create and return a reconciliation strategy."""
    return create_reconciliation_strategy()


@task
def task_determine_needed_keys(
    strategy: IReconciliationStrategy, streams_df: StreamMetadataDF, target_getter: ITargetGetter, data_provider: str
) -> NeededKeysMap:
    """Determine which keys need to be fetched."""
    return strategy.determine_needed_keys(streams_df, target_getter, data_provider)


# Transformer Tasks
@task
def task_create_transformer(product_category_map_df: pd.DataFrame, stream_id_map: StreamIdMap):
    """Create and return a SEPA transformer."""
    return create_sepa_transformer(product_category_map_df=product_category_map_df, stream_id_map=stream_id_map)


@task
def task_transform_data(transformer: IDataTransformer, data: SepaDF) -> pd.DataFrame:
    """Transform data using the transformer."""
    # Check for uncategorized products before transformation
    sepa_transformer = cast(SepaDataTransformer, transformer)
    uncategorized = sepa_transformer.get_uncategorized(data)

    if not uncategorized.empty:
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
    return transformer.transform(data)


# Category Map Tasks
@task
def task_load_category_map(url: str) -> pd.DataFrame:
    """Load the product category mapping."""
    return SepaProductCategoryMapModel.task_from_url(
        url=url,
        compression="zip",
        sep="|",
    )
