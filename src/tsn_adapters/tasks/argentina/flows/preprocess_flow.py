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
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
import prefect.cache_policies as CachePolicies
from prefect_aws import S3Bucket

from tsn_adapters.tasks.argentina.aggregate import aggregate_prices_by_category
from tsn_adapters.tasks.argentina.flows.base import ArgentinaFlowController
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider.s3 import RawDataProvider
from tsn_adapters.tasks.argentina.task_wrappers import task_load_category_map
from tsn_adapters.tasks.argentina.types import AggregatedPricesDF, CategoryMapDF, DateStr, SepaDF, UncategorizedDF
from tsn_adapters.utils import deroutine


@task(name="Process Raw Data")
def process_raw_data(
    raw_data: SepaDF,
    category_map_df: CategoryMapDF,
) -> tuple[AggregatedPricesDF, UncategorizedDF]:
    """Process raw SEPA data.

    Args:
        data_item: Raw data item to process
        category_map_df: Category mapping DataFrame

    Returns:
        Tuple of (processed data, uncategorized products)
    """
    # Process the raw data
    if raw_data.empty:
        return cast(AggregatedPricesDF, pd.DataFrame()), cast(UncategorizedDF, pd.DataFrame())

    # get average price per product
    avg_price_df = SepaAvgPriceProductModel.from_sepa_product_data(raw_data)

    # Apply category mapping
    categorized, uncategorized = aggregate_prices_by_category(category_map_df, avg_price_df)
    return categorized, uncategorized


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
            s3_block: Optional preconfigured S3 block
        """
        super().__init__(s3_block=s3_block)
        self.category_map_url = product_category_map_url
        self.raw_provider = RawDataProvider(s3_block=s3_block)

    def run_flow(self) -> None:
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
            self.process_date(date)

    def process_date(self, date: DateStr) -> None:
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
            logger.error(f"No data available for date: {date}")
            return

        # Load category mapping
        logger.info("Loading category mapping")
        category_map_df = task_load_category_map(url=self.category_map_url)

        # Process the data
        logger.info("Processing data")
        processed_data, uncategorized = process_raw_data(
            raw_data=raw_data,
            category_map_df=category_map_df,
            return_state=True,
        ).result()

        # Save to S3
        logger.info("Saving processed data")
        self.processed_provider.save_processed_data(
            date_str=date,
            data=processed_data,
            uncategorized=uncategorized,
            logs=b"Placeholder for logs",
        )

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
def preprocess_flow(product_category_map_url: str, s3_block_name: str) -> None:
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
    flow.run_flow()


if __name__ == "__main__":
    preprocess_flow(
        "https://drive.usercontent.google.com/u/2/uc?id=1phvOyaOCjQ_fz-03r00R-podmsG0Ygf4&export=download",
        "argentina-sepa",
    )
