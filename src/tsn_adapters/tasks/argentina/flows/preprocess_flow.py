"""
Preprocessing flow for Argentina SEPA data.

This flow handles:
1. Raw data validation
2. Category mapping
3. Price aggregation
4. Storage of processed data in S3
"""

from typing import cast

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
import prefect.cache_policies as CachePolicies
import prefect.variables as variables
from prefect_aws import S3Bucket

from tsn_adapters.tasks.argentina.aggregate import aggregate_prices_by_category
from tsn_adapters.tasks.argentina.config import ArgentinaFlowVariableNames
from tsn_adapters.tasks.argentina.flows.base import ArgentinaFlowController
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider.product_averages import ProductAveragesProvider
from tsn_adapters.tasks.argentina.provider.s3 import RawDataProvider
from tsn_adapters.tasks.argentina.task_wrappers import task_load_category_map
from tsn_adapters.tasks.argentina.types import AggregatedPricesDF, CategoryMapDF, DateStr, SepaDF, UncategorizedDF
from tsn_adapters.utils import deroutine


@task(name="Process Raw Data")
def process_raw_data(
    raw_data: SepaDF,
    category_map_df: CategoryMapDF,
) -> tuple[AggregatedPricesDF, UncategorizedDF, DataFrame[SepaAvgPriceProductModel]]:
    """Process raw SEPA data, calculate product averages, and aggregate by category.

    Args:
        raw_data: Raw SEPA data DataFrame.
        category_map_df: Category mapping DataFrame.

    Returns:
        Tuple of (category aggregated data, uncategorized products, product average data).
    """
    # Process the raw data
    if raw_data.empty:
        # Return empty DataFrames with correct types if raw data is empty
        empty_avg_price_df = DataFrame[SepaAvgPriceProductModel](
            pd.DataFrame(columns=list(SepaAvgPriceProductModel.to_schema().columns.keys()))
        )
        return cast(AggregatedPricesDF, pd.DataFrame()), cast(UncategorizedDF, pd.DataFrame()), empty_avg_price_df

    # Get average price per product (This is the SepaAvgPriceProductModel DataFrame)
    avg_price_df = SepaAvgPriceProductModel.from_sepa_product_data(raw_data)

    # Apply category mapping and aggregation
    categorized, uncategorized = aggregate_prices_by_category(category_map_df, avg_price_df)

    # Return all three DataFrames
    return categorized, uncategorized, avg_price_df


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

    async def process_date(self, date: DateStr) -> None:
        """Process data for a specific date.

        Args:
            date: Date to process (YYYY-MM-DD)

        Raises:
            ValueError: If date format is invalid
            KeyError: If no data available for date
        """
        logger = get_run_logger()
        self.validate_date(date)

        logger.info(f"Processing {date}")

        # Get raw data
        raw_data = self.raw_provider.get_raw_data_for(date)
        if raw_data.empty:
            logger.warning(f"No data available for date: {date}, skipping processing.")
            return

        # Load category mapping
        logger.info("Loading category mapping")
        category_map_df = task_load_category_map(url=self.category_map_url)

        # Process the data - unpack all three results
        logger.info("Processing raw data and aggregating categories")
        processed_data, uncategorized, avg_price_df = process_raw_data(
            raw_data=raw_data,
            category_map_df=category_map_df,
            return_state=True,
        ).result()

        # --- Save Product Averages ---
        if not avg_price_df.empty:
            logger.info("Saving product averages")
            try:
                self.product_averages_provider.save_product_averages(date_str=date, data=avg_price_df)
                logger.info("Successfully saved product averages")
            except Exception as e:
                logger.error(f"Failed to save product averages for {date}: {e}", exc_info=True)
                # Decide if this error should halt processing or just be logged
                # For now, we log and continue to save category data
        else:
            logger.info("Skipping saving product averages as the DataFrame is empty.")

        # Save category aggregated data to S3 (using self.processed_provider from base class)
        logger.info("Saving processed category data")
        self.processed_provider.save_processed_data(
            date_str=date,
            data=processed_data,
            uncategorized=uncategorized,
            logs=b"Placeholder for logs",
        )

        # --- Set Prefect Variable on Success ---
        try:
            await variables.Variable.aset(ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE, date, overwrite=True)
            logger.info(f"Successfully set {ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE} to {date}")
        except Exception as e:
            # Log error but don't fail the flow just because variable setting failed
            logger.error(
                f"Failed to set {ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE} for date {date}: {e}",
                exc_info=True,
            )
        # --- End Prefect Variable Setting ---

        # Create summary
        logger.info("Creating summary")
        self._create_summary(date, processed_data, uncategorized)

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
    s3_block = deroutine(S3Bucket.load(s3_block_name))

    # Create and run flow
    flow = PreprocessFlow(
        product_category_map_url=product_category_map_url,
        s3_block=s3_block,
    )
    await flow.run_flow()


if __name__ == "__main__":
    preprocess_flow(
        "https://drive.usercontent.google.com/u/2/uc?id=1phvOyaOCjQ_fz-03r00R-podmsG0Ygf4&export=download",
        "argentina-sepa",
    )
