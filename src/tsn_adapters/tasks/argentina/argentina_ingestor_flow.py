from collections.abc import Iterator
from datetime import timedelta
import os
import tempfile
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, cast, Union
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run, task_run
from prefect.concurrency.sync import concurrency
from enum import Enum
from prefect.artifacts import create_markdown_artifact, create_table_artifact

from pandera.typing import DataFrame as PaDataFrame

from tsn_adapters.blocks.primitive_source_descriptor import (
    GithubPrimitiveSourcesDescriptor,
    UrlPrimitiveSourcesDescriptor,
    get_descriptor_from_url,
    get_descriptor_from_github,
)
from tsn_adapters.blocks.tn_access import (
    TNAccessBlock,
    read_records,
    insert_and_wait_for_tx,
)
import pandas as pd

from tsn_adapters.tasks.argentina.aggregate.category_price_aggregator import (
    aggregate_prices_by_category,
)

from tsn_adapters.tasks.argentina.models.aggregated_prices import (
    sepa_aggregated_prices_to_tn_records,
)

from tsn_adapters.tasks.argentina.models.category_map import (
    SepaProductCategoryMapModel,
    get_uncategorized_products,
)

from tsn_adapters.tasks.argentina.models.sepa_models import (
    SepaAvgPriceProductModel,
    SepaProductosDataModel,
)

from tsn_adapters.tasks.argentina.sepa_resource_processor import SepaDirectoryProcessor
from tsn_adapters.tasks.argentina.sepa_scraper import (
    SepaHistoricalDataItem,
    SepaPreciosScraper,
    task_scrape_historical_items,
)
from tsn_adapters.tasks.trufnetwork.models.tn_models import TnDataRowModel

PrimitiveSourcesTypeStr = Literal["url", "github"]


class PrimitiveSourcesType(Enum):
    URL = "url"
    GITHUB = "github"

    def load_block(
        self, name: str
    ) -> Union[UrlPrimitiveSourcesDescriptor, GithubPrimitiveSourcesDescriptor]:
        if self == PrimitiveSourcesType.URL:
            return UrlPrimitiveSourcesDescriptor.load(name)
        elif self == PrimitiveSourcesType.GITHUB:
            return GithubPrimitiveSourcesDescriptor.load(name)
        else:
            raise ValueError(f"Invalid primitive sources type: {self}")

    @staticmethod
    def from_string(value: PrimitiveSourcesTypeStr) -> "PrimitiveSourcesType":
        if value == "url":
            return PrimitiveSourcesType.URL
        elif value == "github":
            return PrimitiveSourcesType.GITHUB
        else:
            raise ValueError(f"Invalid primitive sources type: {value}")


@task(name="Create Processing Summary")
def create_processing_summary(
    source_metadata_df: pd.DataFrame,
    dates_to_process: Set[str],
    uncategorized_products: Dict[str, PaDataFrame[SepaProductosDataModel]],
) -> None:
    """Create a markdown summary of the processing results."""
    summary = [
        "# SEPA Data Processing Summary\n",
        f"## Streams Processed: {len(source_metadata_df)}\n",
        "### Stream Details:\n",
    ]

    for _, row in source_metadata_df.iterrows():
        summary.append(f"- Stream: {row['stream_id']} (Source: {row['source_id']})\n")

    summary.extend(
        [f"\n## Dates Processed: {len(dates_to_process)}\n", "### Date Details:\n"]
    )

    for date in sorted(dates_to_process):
        uncategorized_count = len(uncategorized_products.get(date, pd.DataFrame()))
        summary.append(f"- {date}: {uncategorized_count} uncategorized products\n")

    create_markdown_artifact(
        key="processing-summary",
        markdown="\n".join(summary),
        description="Summary of SEPA data processing",
    )


@task(name="Fetch Source Metadata", retries=3)
def fetch_source_metadata(
    source_descriptor_type: PrimitiveSourcesTypeStr, source_descriptor_block_name: str
) -> pd.DataFrame:
    """Fetch metadata about available data streams."""
    source_descriptor_block = PrimitiveSourcesType.from_string(
        source_descriptor_type
    ).load_block(source_descriptor_block_name)

    source_metadata_df = (
        get_descriptor_from_url(
            cast(UrlPrimitiveSourcesDescriptor, source_descriptor_block)
        )
        if isinstance(source_descriptor_block, UrlPrimitiveSourcesDescriptor)
        else get_descriptor_from_github(
            cast(GithubPrimitiveSourcesDescriptor, source_descriptor_block)
        )
    )

    create_table_artifact(
        key="source-metadata",
        table=source_metadata_df.to_dict(orient="list"),
        description="Available data streams",
    )

    return source_metadata_df


@task(name="Get Last Processed Dates", retries=2)
def get_last_processed_dates(
    source_metadata_df: pd.DataFrame,
    trufnetwork_access_block: TNAccessBlock,
    data_provider: str,
) -> pd.DataFrame:
    """Get the last processed date for each stream."""
    last_processed_dates = []

    for _, row in source_metadata_df.iterrows():
        stream_id = row["stream_id"]
        source_id = row["source_id"]
        last_record_for_stream = read_records(
            block=trufnetwork_access_block,
            stream_id=stream_id,
            data_provider=data_provider,
        )

        last_processed_date = (
            last_record_for_stream["date"].iloc[0]
            if not last_record_for_stream.empty
            else "0000-00-00"
        )

        last_processed_dates.append(
            {
                "stream_id": stream_id,
                "source_id": source_id,
                "last_date": last_processed_date,
            }
        )

    last_processed_dates_df = pd.DataFrame(last_processed_dates)

    create_table_artifact(
        key="last-processed-dates",
        table=last_processed_dates_df.to_dict(orient="list"),
        description="Last processed dates by stream",
    )

    return last_processed_dates_df


def generate_single_date_name():
    parameters = task_run.parameters
    historical_data_item: SepaHistoricalDataItem = parameters["historical_data_item"]

    return f"process-single-date-{historical_data_item.date}"


@task(name="Process Single Date", task_run_name=generate_single_date_name)
def process_single_date(
    historical_data_item: SepaHistoricalDataItem,
    product_category_map_df: PaDataFrame[SepaProductCategoryMapModel],
) -> Tuple[PaDataFrame[TnDataRowModel], PaDataFrame[SepaProductosDataModel]]:
    """Process data for a single date."""
    logger = get_run_logger()

    # Use concurrency to limit large downloads
    with concurrency(names=["heavy-file-from-web"], create_if_missing=True):
        logger.info(f"Processing {historical_data_item.date}")
        sepa_data_for_date = task_get_sepa_data(historical_data_item)

    # Calculate average prices
    avg_price_by_product_df = SepaAvgPriceProductModel.from_sepa_product_data(
        sepa_data_for_date
    )

    # Aggregate by category
    aggregated_prices_for_date = aggregate_prices_by_category(
        product_category_map_df, avg_price_by_product_df
    )

    # Convert to TN records
    tn_records_for_date = sepa_aggregated_prices_to_tn_records(
        aggregated_prices_for_date, lambda category_id: category_id
    )

    # Get uncategorized products
    uncategorized_products = get_uncategorized_products(
        sepa_data_for_date, product_category_map_df
    )

    # Build a short summary
    total_products = len(sepa_data_for_date)
    uncategorized_count = len(uncategorized_products)
    categories_count = aggregated_prices_for_date["category_id"].nunique()
    date_str = historical_data_item.date

    # Create a Markdown artifact with the stats
    summary = [
        f"# Processing Summary for date {date_str}",
        "",
        f"- **Total products:** {total_products}",
        f"- **Unique categories:** {categories_count}",
        f"- **Uncategorized products:** {uncategorized_count}",
    ]

    create_markdown_artifact(
        key=f"process-summary-{date_str}",  # unique key per date
        markdown="\n".join(summary),
        description=f"Summary of data processing for {date_str}",
    )

    # Also log it (optional)
    logger.info(
        f"Processed {total_products} products on {date_str}, "
        f"{uncategorized_count} uncategorized, in {categories_count} categories."
    )

    return tn_records_for_date, uncategorized_products


@task(name="Process Historical Data")
def process_historical_data(
    dates_to_process: Set[str],
    historical_data_items_by_date: Dict[str, SepaHistoricalDataItem],
    product_category_map_df: PaDataFrame[SepaProductCategoryMapModel],
) -> Tuple[PaDataFrame[TnDataRowModel], Dict[str, PaDataFrame[SepaProductosDataModel]]]:
    """Process historical data for all dates in parallel."""
    # just first to debug
    aggregated_records = (
        pd.DataFrame()
    )  # Will be converted to PaDataFrame[TnDataRowModel] at return
    uncategorized_by_date: Dict[str, PaDataFrame[SepaProductosDataModel]] = {}

    # just first to debug
    sorted_dates_to_process = sorted(dates_to_process)[:1]

    # Process dates with limited concurrency
    futures = []
    for date in sorted_dates_to_process:
        historical_data_item = historical_data_items_by_date[date]
        future = process_single_date.submit(
            historical_data_item, product_category_map_df
        )
        futures.append((date, future))

    # Collect results
    for date, future in futures:
        tn_records, uncategorized = future.result()
        aggregated_records = pd.concat([aggregated_records, tn_records])
        uncategorized_by_date[date] = uncategorized

    return PaDataFrame[TnDataRowModel](aggregated_records), uncategorized_by_date


@task(name="Get Available Dates")
def get_available_dates(
    sepa_scraper: SepaPreciosScraper,
) -> Dict[str, SepaHistoricalDataItem]:
    """Get all available dates from the SEPA scraper."""
    all_historical_data_items = task_scrape_historical_items(sepa_scraper)
    return {data_item.date: data_item for data_item in all_historical_data_items}


@task(name="Determine Dates To Process")
def determine_dates_to_process(
    last_processed_dates_df: pd.DataFrame,
    sorted_available_dates: List[str],
) -> Iterator[Dict[str, Any]]:
    """Determine which dates need to be processed for each stream."""
    for _, row in last_processed_dates_df.iterrows():
        stream_id = row["stream_id"]
        source_id = row["source_id"]
        last_processed_date = row["last_date"]
        dates_to_process = [
            date for date in sorted_available_dates if date > last_processed_date
        ]
        yield {
            "stream_id": stream_id,
            "source_id": source_id,
            "date_list": dates_to_process,
        }


@flow(log_prints=True)
def argentina_ingestor_flow(
    source_descriptor_type: PrimitiveSourcesTypeStr,
    trufnetwork_access_block_name: str,
    product_category_map_url: str,
    data_provider: str,
):
    """
    Ingests Argentina's SEPA price data into the Truflation Network (TN).

    This flow orchestrates the retrieval, processing, and insertion of historical
    price data from Argentina's "Sistema Electrónico de Publicidad de Precios
    Argentinos" (SEPA). It leverages Prefect for workflow management and
    Pandera for data validation.

    The process involves:

    1. **Fetching Source Metadata:** Retrieves metadata about available data
       streams from a specified source (URL or GitHub).
    2. **Determining Processed Dates:** Identifies the last processed date for
       each data stream within the TN.
    3. **Scraping SEPA Data:** Extracts historical price data from the SEPA
       website for dates not yet processed.
    4. **Aggregating Prices:** Calculates average prices by product category for
       each date.
    5. **Inserting into TN:** Stores the aggregated price data into the TN,
       organized by stream.
    6. **Identifying Uncategorized Products:** Detects and reports products
       without a defined category.

    Args:
        source_descriptor_type: Specifies the type of source descriptor ('url'
                                 or 'github').
        trufnetwork_access_block_name: The name of the TrufNetwork access block
                                       used for interacting with the TN.
        product_category_map_url: The URL to the product-category mapping file.
        data_provider: The data provider address to use for reading and writing to the TN.
    """
    # --- Initialization ---
    source_descriptor_block_name = "argentina-sepa-source-descriptor"
    trufnetwork_access_block = TNAccessBlock.load(trufnetwork_access_block_name)
    product_category_map_df = SepaProductCategoryMapModel.task_from_url(
        url=product_category_map_url,
        compression="zip",
        sep="|",
    )

    # --- Fetch metadata and determine dates to process ---
    source_metadata_df = fetch_source_metadata(
        source_descriptor_type, source_descriptor_block_name
    )
    last_processed_dates_df = get_last_processed_dates(
        source_metadata_df, trufnetwork_access_block, data_provider
    )

    # --- Get available dates from source ---
    sepa_scraper = SepaPreciosScraper(show_progress_bar=True)
    historical_data_items_by_date = get_available_dates(sepa_scraper)
    sorted_available_dates = sorted(historical_data_items_by_date.keys())

    # --- Determine dates to process for each stream ---
    dates_to_process_by_stream = determine_dates_to_process(
        last_processed_dates_df, sorted_available_dates
    )

    # --- Get unique dates to process ---
    unique_dates_to_process = {
        date
        for stream_data in dates_to_process_by_stream
        for date in stream_data["date_list"]
    }


    # --- Process historical data ---
    aggregated_records, uncategorized_by_date = process_historical_data(
        unique_dates_to_process,
        historical_data_items_by_date,
        product_category_map_df,
    )

    # --- Insert data by stream ---
    insertion_tasks = []
    insertion_summaries = []
    for stream_data in dates_to_process_by_stream:
        stream_id = stream_data["stream_id"]
        stream_records = aggregated_records[
            aggregated_records["stream_id"] == stream_id
        ]

        if not stream_records.empty:
            records_count = len(stream_records)
            date_range = (
                f"{stream_records['date'].min()} to {stream_records['date'].max()}"
            )

            insertion_task = insert_and_wait_for_tx(
                block=trufnetwork_access_block,
                stream_id=stream_id,
                records=stream_records,
                data_provider=data_provider,
            )
            insertion_tasks.append(insertion_task)

            insertion_summaries.append(
                {
                    "stream_id": stream_id,
                    "records_count": records_count,
                    "date_range": date_range,
                    "status": "pending",
                }
            )

    # --- Create processing summary ---
    create_processing_summary(
        source_metadata_df, unique_dates_to_process, uncategorized_by_date
    )

    # Wait for all insertions to complete and update summaries
    for idx, task in enumerate(insertion_tasks):
        try:
            task.result()
            insertion_summaries[idx]["status"] = "completed"
        except Exception as e:
            insertion_summaries[idx]["status"] = f"failed: {str(e)}"

    # Create insertion summary artifact
    insertion_summary_md = [
        "# Data Insertion Summary",
        "",
        "| Stream ID | Records Count | Date Range | Status |",
        "|-----------|---------------|------------|---------|",
    ]

    for summary in insertion_summaries:
        insertion_summary_md.append(
            f"| {summary['stream_id']} | {summary['records_count']} | {summary['date_range']} | {summary['status']} |"
        )

    create_markdown_artifact(
        key="insertion-summary",
        markdown="\n".join(insertion_summary_md),
        description="Summary of data insertions into TrufNetwork streams",
    )


def data_item_cache_key(_, args: Dict[str, Any]) -> Optional[str]:
    """
    Cache key function for the task_get_sepa_data task.

    We're caching the data by date, since we know that the data is not going to change for a given date
    """
    data_item: SepaHistoricalDataItem = args["data_item"]
    return data_item.date


def generate_task_get_sepa_data_name():
    parameters = task_run.parameters
    historical_data_item: SepaHistoricalDataItem = parameters["data_item"]
    return f"task-get-sepa-data-{historical_data_item.date}"


@task(
    cache_key_fn=data_item_cache_key,
    cache_expiration=timedelta(days=7),
    retries=3,
    task_run_name=generate_task_get_sepa_data_name,
)
def task_get_sepa_data(
    data_item: SepaHistoricalDataItem,
) -> PaDataFrame[SepaProductosDataModel]:
    with tempfile.TemporaryDirectory() as temp_workdir:
        temp_zip_path = os.path.join(temp_workdir, "data.zip")
        temp_extracted_path = os.path.join(temp_workdir, "extracted")
        # fetch into a temp dir
        data_item.fetch_into_file(temp_zip_path, show_progress_bar=True)
        # extract the zip
        processor = SepaDirectoryProcessor.from_zip_path(
            temp_zip_path, temp_extracted_path
        )
        # Process the data while the temporary directory is still open
        return processor.get_all_products_data_merged()


if __name__ == "__main__":
    # test run
    argentina_ingestor_flow(
        source_descriptor_type="github",
        trufnetwork_access_block_name="default",
        product_category_map_url="https://drive.usercontent.google.com/u/2/uc?id=1nfcAjCF-BYU5-rrWJW9eFqCw2AjNpc1x&export=download",
        # 000...001
        data_provider="7e5f4552091a69125d5dfcb7b8c2659029395bdf",
    )
