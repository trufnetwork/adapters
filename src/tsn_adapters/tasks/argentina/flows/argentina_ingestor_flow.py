"""
Argentina SEPA data ingestion flow using task-based architecture.
"""

from typing import cast

import pandas as pd
from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact

from tsn_adapters.tasks.argentina.target import create_trufnetwork_components
from tsn_adapters.tasks.argentina.task_wrappers import (
    task_create_reconciliation_strategy,
    task_create_sepa_provider,
    task_create_stream_fetcher,
    task_create_transformer,
    task_determine_needed_keys,
    task_get_data_for_date,
    task_get_streams,
    task_insert_data,
    task_load_category_map,
    task_transform_data,
)
from tsn_adapters.tasks.argentina.transformers import SepaDataTransformer
from tsn_adapters.tasks.argentina.types import DateStr, SepaDF, SourceId, StreamId, StreamIdMap
from tsn_adapters.utils.cast_future import cast_future


@flow(name="Argentina SEPA Ingestor")
def argentina_ingestor_flow(
    source_descriptor_type: str,
    source_descriptor_block_name: str,
    trufnetwork_access_block_name: str,
    product_category_map_url: str,
    data_provider: str,
):
    """
    Ingest Argentina SEPA data using task-based architecture.

    Args:
        source_descriptor_type: Type of source descriptor ("url" or "github")
        source_descriptor_block_name: Name of the source descriptor block
        trufnetwork_access_block_name: Name of the TrufNetwork access block
        product_category_map_url: URL to the product category mapping
        data_provider: The data provider identifier
    """
    logger = get_run_logger()

    # Step 1: Create components
    logger.info("Creating components...")

    # Create stream details fetcher and get streams
    fetcher = task_create_stream_fetcher(
        source_type=source_descriptor_type,
        block_name=source_descriptor_block_name,
    )
    streams_df = task_get_streams(fetcher=fetcher)

    # Create SEPA provider
    provider = task_create_sepa_provider()

    # Create TrufNetwork components
    target_getter, target_setter = create_trufnetwork_components(
        block_name=trufnetwork_access_block_name
    )

    # Create reconciliation strategy
    recon_strategy = task_create_reconciliation_strategy()

    # Load product category mapping
    product_category_map_df = task_load_category_map(url=product_category_map_url)

    # Create stream ID map
    stream_id_map: StreamIdMap = {
        cast(SourceId, source_id): cast(StreamId, stream_id)
        for source_id, stream_id in zip(streams_df["source_id"], streams_df["stream_id"])
    }

    # Create transformer
    transformer = task_create_transformer(
        product_category_map_df=product_category_map_df,
        stream_id_map=stream_id_map
    )

    # Step 2: Determine what data to fetch
    logger.info("Determining what data to fetch...")
    needed_keys = task_determine_needed_keys(
        strategy=recon_strategy,
        streams_df=streams_df,
        target_getter=target_getter,
        data_provider=data_provider
    )

    # Step 3: Process each date
    uncategorized_products: dict[DateStr, SepaDF] = {}
    dates_processed: list[DateStr] = []

    # Merge all needed dates across streams
    all_needed_dates = set()
    for dates in needed_keys.values():
        all_needed_dates.update(dates)

    logger.info(f"Processing {len(all_needed_dates)} unique dates...")

    
    tn_records_tasks = []
    records_to_insert = pd.DataFrame()
    # Process each date in parallel
    for date in list(all_needed_dates):
        # Fetch data
        data = task_get_data_for_date.submit(provider, date)

        # check if data is empty 
        if data.result().empty:
            logger.warning(f"No data found for date {date}")
            continue

        # Get uncategorized products and store them
        sepa_transformer = cast(SepaDataTransformer, transformer)
        uncategorized = sepa_transformer.get_uncategorized(data.result())
        if not uncategorized.empty:
            uncategorized_products[date] = uncategorized
        # Transform data
        tn_records = task_transform_data.submit(transformer, cast_future(data))
        tn_records_tasks.append(tn_records)
        dates_processed.append(date)

    # Wait for all tasks to complete
    for tn_records in tn_records_tasks:
        records_to_insert = pd.concat([records_to_insert, tn_records.result()])

    # Now process each stream in parallel
    insert_tasks = []
    for stream_id, needed_dates in needed_keys.items():
        stream_records = records_to_insert[records_to_insert["stream_id"] == stream_id]
        # filter stream_records to only include records for the needed dates
        stream_records = stream_records[stream_records["date"].isin(needed_dates)]

        # skip if no records for this stream
        if stream_records.empty:
            logger.warning(f"No records found for stream {stream_id}")
            continue

        # Insert into target
        insert_tasks.append(task_insert_data.submit(
            setter=target_setter,
            stream_id=stream_id,
            data=stream_records,
            data_provider=data_provider
        ))

    # Wait for all insert tasks to complete
    for insert_task in insert_tasks:
        insert_task.result()

    # Step 4: Create summary
    logger.info("Creating processing summary...")
    create_processing_summary(streams_df, set(dates_processed), uncategorized_products)

    logger.info("Flow completed successfully!")


def create_processing_summary(
    source_metadata_df: pd.DataFrame,
    dates_processed: set[DateStr],
    uncategorized_products: dict[DateStr, SepaDF],
) -> None:
    """Create a markdown summary of the processing results."""
    summary = [
        "# SEPA Data Processing Summary\n",
        f"## Streams Processed: {len(source_metadata_df)}\n",
        "### Stream Details:\n",
    ]

    for _, row in source_metadata_df.iterrows():
        summary.append(f"- Stream: {row['stream_id']} (Source: {row['source_id']})\n")

    summary.extend([f"\n## Dates Processed: {len(dates_processed)}\n", "### Date Details:\n"])

    for date in sorted(dates_processed):
        uncategorized_count = len(uncategorized_products.get(date, pd.DataFrame()))
        summary.append(f"- {date}: {uncategorized_count} uncategorized products\n")

    create_markdown_artifact(
        key="processing-summary",
        markdown="\n".join(summary),
        description="Summary of SEPA data processing",
    )


if __name__ == "__main__":
    # test run
    argentina_ingestor_flow(
        source_descriptor_type="github",
        source_descriptor_block_name="argentina-sepa-source-descriptor",
        trufnetwork_access_block_name="default",
        product_category_map_url="https://drive.usercontent.google.com/u/2/uc?id=1nfcAjCF-BYU5-rrWJW9eFqCw2AjNpc1x&export=download",
        # 000...001
        data_provider="7e5f4552091a69125d5dfcb7b8c2659029395bdf",
    ) 