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
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect_aws import S3Bucket

from tsn_adapters.tasks.argentina.aggregate import aggregate_prices_by_category
from tsn_adapters.tasks.argentina.flows.base import ArgentinaFlowController
from tsn_adapters.tasks.argentina.models.sepa import SepaDataItem
from tsn_adapters.tasks.argentina.provider.data_processor import process_sepa_data
from tsn_adapters.tasks.argentina.task_wrappers import task_load_category_map
from tsn_adapters.tasks.argentina.types import AggregatedPricesDF, CategoryMapDF, DateStr, UncategorizedDF
from tsn_adapters.utils import deroutine

from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel


@task(name="Process Raw Data")
def process_raw_data(
    data_item: SepaDataItem,
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
    df = process_sepa_data(data_item=data_item, source_name="s3")

    if df.empty:
        return cast(AggregatedPricesDF, pd.DataFrame()), cast(UncategorizedDF, pd.DataFrame())

    # get average price per product
    df_avg_price = SepaAvgPriceProductModel.from_sepa_product_data(df)

    # Apply category mapping
    categorized, uncategorized = aggregate_prices_by_category(category_map_df, df_avg_price)
    return categorized, uncategorized


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

    def process_date(self, date: DateStr) -> None:
        """Process data for a specific date.

        Args:
            date: Date to process (YYYY-MM-DD)

        Raises:
            ValueError: If date format is invalid
            KeyError: If no data available for date
        """
        self.validate_date(date)

        # Get raw data
        data_item = self.raw_provider.get_data_for(date)
        if not data_item:
            raise KeyError(f"No data available for date: {date}")

        # Load category mapping
        category_map_df = task_load_category_map(url=self.category_map_url)

        # Process the data
        processed_data, uncategorized = process_raw_data(data_item=data_item, category_map_df=category_map_df)

        # Save to S3
        self.processed_provider.save_processed_data(
            date_str=date,
            data=processed_data,
            uncategorized=uncategorized,
            logs=b"Placeholder for logs",
        )

        # Create summary
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
                summary.append(f"| {row['product_id']} | {row['name']} |\n")

        create_markdown_artifact(
            key=f"preprocessing-summary-{date}",
            markdown="\n".join(summary),
            description=f"Summary of SEPA data preprocessing for {date}",
        )


@flow(name="Argentina SEPA Preprocessing")
def preprocess_flow(date: str, product_category_map_url: str, s3_block_name: str) -> None:
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
    flow.process_date(DateStr(date))
