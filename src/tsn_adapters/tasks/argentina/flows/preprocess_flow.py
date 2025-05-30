"""
Preprocessing flow for Argentina SEPA data.

This flow handles:
1. Raw data validation
2. Category mapping
3. Price aggregation
4. Storage of processed data in S3
"""

import asyncio
from collections.abc import Iterator
from typing import cast

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
import prefect.cache_policies as CachePolicies
import prefect.variables as variables  # Import prefect variables
from prefect_aws import S3Bucket

from tsn_adapters.tasks.argentina.aggregate import aggregate_prices_by_category
from tsn_adapters.tasks.argentina.config import ArgentinaFlowVariableNames  # Import config
from tsn_adapters.tasks.argentina.flows.base import ArgentinaFlowController
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import (
    SepaAvgPriceProductModel,
    SepaWeightedAvgPriceProductModel,
)
from tsn_adapters.tasks.argentina.provider.product_averages import ProductAveragesProvider
from tsn_adapters.tasks.argentina.provider.s3 import RawDataProvider
from tsn_adapters.tasks.argentina.task_wrappers import task_load_category_map
from tsn_adapters.tasks.argentina.types import AggregatedPricesDF, CategoryMapDF, DateStr, SepaDF, UncategorizedDF
from tsn_adapters.tasks.argentina.utils.weighted_average import combine_weighted_averages
from tsn_adapters.utils.deroutine import force_sync


@task(name="Process Raw Data in Batches")
def process_raw_data_streaming(
    raw_data_stream: Iterator[SepaDF],
    category_map_df: CategoryMapDF,
) -> tuple[AggregatedPricesDF, UncategorizedDF, DataFrame[SepaWeightedAvgPriceProductModel]]:
    """Process raw SEPA data in batches using streaming to minimize memory usage.

    MEMORY OPTIMIZATION: Implements periodic reset every 50 batches to prevent
    memory buildup that causes SIGKILL at batch 171.

    Args:
        raw_data_stream: Iterator yielding chunks of raw SEPA data.
        category_map_df: Category mapping DataFrame.
        chunk_size: Size of each processing batch.

    Returns:
        Tuple of (category aggregated data, uncategorized products, weighted product average data).
    """
    logger = get_run_logger()

    # Initialize accumulator for weighted averages only
    cumulative_weighted_avg: DataFrame[SepaWeightedAvgPriceProductModel] | None = None

    # MEMORY OPTIMIZATION: Track and reset every 50 batches to prevent memory buildup
    reset_interval = 50
    all_weighted_results = []  # Store intermediate results

    batch_count = 0
    total_processed = 0

    for batch_data in raw_data_stream:
        if batch_data.empty:
            continue

        batch_count += 1
        current_batch_size = len(batch_data)
        total_processed += current_batch_size

        logger.info(f"Processing batch {batch_count} with {current_batch_size} rows")

        # Process current batch to weighted averages
        batch_weighted_avg = SepaWeightedAvgPriceProductModel.from_sepa_product_data(batch_data)

        # IMMEDIATE COMBINE: Combine with cumulative result right away
        if cumulative_weighted_avg is None:
            # First batch - just store it
            cumulative_weighted_avg = batch_weighted_avg
        else:
            # Combine with existing cumulative result immediately
            cumulative_weighted_avg = combine_weighted_averages([cumulative_weighted_avg, batch_weighted_avg])

        # Clean up batch data immediately
        del batch_data
        del batch_weighted_avg

        # MEMORY OPTIMIZATION: Reset every 50 batches to prevent memory buildup
        if batch_count % reset_interval == 0:
            logger.info(f"MEMORY RESET: Saving intermediate result at batch {batch_count}")
            all_weighted_results.append(cumulative_weighted_avg.copy())
            cumulative_weighted_avg = None
            # Force garbage collection after reset
            import gc

            gc.collect()

    logger.info(f"Processed {batch_count} batches with {total_processed} total rows")

    # Combine all intermediate results at the end
    if cumulative_weighted_avg is not None:
        all_weighted_results.append(cumulative_weighted_avg)

    if not all_weighted_results:
        logger.warning("No data processed - returning empty results")
        empty_weighted_avg_df = DataFrame[SepaWeightedAvgPriceProductModel](
            pd.DataFrame(columns=list(SepaWeightedAvgPriceProductModel.to_schema().columns.keys()))
        )
        return cast(AggregatedPricesDF, pd.DataFrame()), cast(UncategorizedDF, pd.DataFrame()), empty_weighted_avg_df

    # Final combine of all intermediate results
    logger.info(f"Final combine of {len(all_weighted_results)} intermediate results")
    final_weighted_avg = combine_weighted_averages(all_weighted_results)

    # Convert weighted averages to standard avg price format for aggregation (done once at the end)
    standard_data = {
        "id_producto": final_weighted_avg["id_producto"],
        "productos_descripcion": final_weighted_avg["productos_descripcion"],
        "productos_precio_lista_avg": final_weighted_avg["productos_precio_lista_avg"],
        "date": final_weighted_avg["date"],
    }
    avg_price_df = DataFrame[SepaAvgPriceProductModel](pd.DataFrame(standard_data))

    # Apply category aggregation once at the end
    categorized, uncategorized = aggregate_prices_by_category(category_map_df, avg_price_df)

    logger.info(
        f"Final results: {len(categorized)} categorized, {len(uncategorized)} uncategorized, {len(final_weighted_avg)} weighted averages"
    )

    return categorized, uncategorized, final_weighted_avg


# Keep the old function for backward compatibility but mark as deprecated
@task(name="Process Raw Data")
def process_raw_data(
    raw_data: SepaDF,
    category_map_df: CategoryMapDF,
) -> tuple[AggregatedPricesDF, UncategorizedDF, DataFrame[SepaWeightedAvgPriceProductModel]]:
    """DEPRECATED: Use process_raw_data_streaming for better memory efficiency."""
    logger = get_run_logger()
    logger.warning("Using deprecated process_raw_data - consider switching to process_raw_data_streaming")

    # Convert single DataFrame to iterator for compatibility
    def single_chunk_iterator():
        yield raw_data

    return process_raw_data_streaming(single_chunk_iterator(), category_map_df)


@task(name="List Available Dates", cache_policy=CachePolicies.RUN_ID)
def task_list_available_dates(raw_provider: RawDataProvider) -> list[DateStr]:
    """List available dates in the raw data provider"""
    return raw_provider.list_available_keys()


class PreprocessFlow(ArgentinaFlowController):
    """Flow for preprocessing SEPA data."""

    def __init__(
        self,
        product_category_map_url: str,
        s3_block: S3Bucket,
    ):
        """Initialize preprocessing flow.

        Args:
            product_category_map_url: URL to product category mapping
            s3_block: Preconfigured S3 block
        """
        super().__init__(s3_block=s3_block)
        self.category_map_url = product_category_map_url
        self.raw_provider = RawDataProvider(s3_block=s3_block)
        # Instantiate the new provider
        self.product_averages_provider = ProductAveragesProvider(s3_block=s3_block)
        # Note: self.processed_provider is inherited from ArgentinaFlowController

    async def run_flow(self) -> None:
        """
        1. Lists all dates in the raw data provider
        2. See if target already exists in the processed data provider
        3. If not, process the date
        """
        logger = get_run_logger()
        
        for date in self.raw_provider.list_available_keys():
            if self.processed_provider.exists(date):
                logger.info(f"Skipping {date} because it already exists")
                continue
            await self.process_date(date)
            
            # Update LAST_PREPROCESS_SUCCESS_DATE immediately after successful processing
            try:
                await variables.Variable.aset(
                    ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE, 
                    date, 
                    overwrite=True
                )
                logger.info(f"Successfully set {ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE} to {date}")
            except Exception as e:
                logger.error(f"Failed to set {ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE}: {e}", exc_info=True)

    async def process_date(self, date_str: DateStr) -> None:
        """Process raw data for a specific date into aggregated prices."""
        logger = get_run_logger()
        self.validate_date(date_str)

        if not self.raw_provider.has_data_for(date_str):
            logger.info(f"No raw data available for {date_str}")
            return

        logger.info(f"Loading category map from {self.category_map_url}")
        category_map_df = task_load_category_map(url=self.category_map_url)

        logger.info(f"Processing data for {date_str}")
        raw_data_stream = self.raw_provider.stream_raw_data_for(date_str)

        processed_data, uncategorized, avg_price_df = process_raw_data_streaming(
            raw_data_stream=raw_data_stream,
            category_map_df=category_map_df,
            return_state=False,
        )

        # Save product averages if they exist (but continue if save fails)
        if not avg_price_df.empty:
            try:
                self.product_averages_provider.save_product_averages(date_str=date_str, data=avg_price_df)
                logger.info(f"Saved {len(avg_price_df)} product averages for {date_str}")
            except Exception as e:
                logger.error(f"Failed to save product averages for {date_str}: {e}")

        # Save processed data
        self.processed_provider.save_processed_data(
            date_str=date_str,
            data=processed_data,
            uncategorized=uncategorized,
            logs=b"",  # TODO: Implement logging collection
        )

        # Generate summary
        self._create_summary(date_str, processed_data, uncategorized)

    def _create_summary(
        self,
        date: DateStr,
        processed_data: AggregatedPricesDF,
        uncategorized: UncategorizedDF,
    ) -> None:
        """Create a markdown summary of preprocessing results.

        Args:
            date: Date processed
            processed_data: Processed data DataFrame
            uncategorized: Uncategorized products DataFrame
        """
        summary = [
            f"# SEPA Preprocessing Summary for {date}\n",
            "## Processing Statistics\n",
            f"- Total records processed: {len(processed_data) + len(uncategorized)}\n",
            f"- Successfully categorized: {len(processed_data)}\n",
            f"- Uncategorized products: {len(uncategorized)}\n",
        ]

        if not uncategorized.empty:
            summary.extend(
                ["\n## Sample Uncategorized Products\n", "| Product ID | Name |\n", "|------------|------|\n"]
            )

            # Show up to 5 examples
            for _, row in uncategorized.head().iterrows():
                summary.append(f"| {row['id_producto']} | {row['productos_descripcion']} |\n")

        create_markdown_artifact(
            key=f"preprocessing-summary-{date}",
            markdown="\n".join(summary),
            description=f"Summary of SEPA data preprocessing for {date}",
        )


@flow(name="Argentina SEPA Preprocessing")
async def preprocess_flow(product_category_map_url: str, s3_block_name: str) -> None:
    """Preprocess Argentina SEPA data.

    Args:
        date: Date to process (YYYY-MM-DD)
        product_category_map_url: URL to product category mapping
        s3_block_name: Optional name of S3 block to use
    """
    # Get S3 block if specified
    s3_block = force_sync(S3Bucket.load)(s3_block_name)

    # Create and run flow
    flow = PreprocessFlow(
        product_category_map_url=product_category_map_url,
        s3_block=s3_block,
    )
    await flow.run_flow()


if __name__ == "__main__":
    asyncio.run(
        preprocess_flow(
            "https://drive.usercontent.google.com/u/2/uc?id=1phvOyaOCjQ_fz-03r00R-podmsG0Ygf4&export=download",
            "argentina-sepa",
        )
    )
