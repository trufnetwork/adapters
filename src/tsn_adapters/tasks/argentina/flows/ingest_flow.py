"""
Ingestion flow for Argentina SEPA data.

This flow handles:
1. Loading preprocessed data from S3
2. Network stream management
3. Batch insertions to Truf.network
"""

from typing import Optional, cast

import pandas as pd
from pandera.typing import DataFrame as PanderaDataFrame
from prefect import flow, transactions
from prefect.artifacts import create_markdown_artifact
from prefect_aws import S3Bucket

from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.tasks.argentina.flows.base import ArgentinaFlowController
from tsn_adapters.tasks.argentina.target import create_trufnetwork_components
from tsn_adapters.tasks.argentina.task_wrappers import (
    task_create_reconciliation_strategy,
    task_create_stream_fetcher,
    task_create_transformer,
    task_determine_needed_keys,
    task_get_streams,
    task_insert_data,
    task_transform_data,
)
from tsn_adapters.tasks.argentina.types import DateStr, SourceId, StreamId, StreamIdMap
from tsn_adapters.utils import deroutine


class IngestFlow(ArgentinaFlowController):
    """Flow for ingesting preprocessed SEPA data."""

    def __init__(
        self,
        source_descriptor_type: str,
        source_descriptor_block_name: str,
        trufnetwork_access_block_name: str,
        s3_block: S3Bucket,
        data_provider: Optional[str] = None,
    ):
        """Initialize ingestion flow.

        Args:
            source_descriptor_type: Type of source descriptor
            source_descriptor_block_name: Name of source descriptor block
            trufnetwork_access_block_name: Name of TrufNetwork access block
            s3_block: Optional preconfigured S3 block
            data_provider: Optional data provider identifier
        """
        super().__init__(s3_block=s3_block)
        self.source_descriptor_type = source_descriptor_type
        self.source_descriptor_block_name = source_descriptor_block_name
        self.trufnetwork_access_block_name = trufnetwork_access_block_name
        self.data_provider = data_provider

    def run(self) -> None:
        """Run the ingestion flow."""
        self.logger.info("Creating components...")

        # Create stream details fetcher and get streams
        fetcher = task_create_stream_fetcher(
            source_type=self.source_descriptor_type,
            block_name=self.source_descriptor_block_name,
        )
        streams_df = task_get_streams(fetcher=fetcher)

        # Create TrufNetwork client
        target_client = create_trufnetwork_components(block_name=self.trufnetwork_access_block_name)

        # Create reconciliation strategy
        recon_strategy = task_create_reconciliation_strategy()

        # Create stream ID map
        stream_id_map: StreamIdMap = {
            cast(SourceId, source_id): cast(StreamId, stream_id)
            for source_id, stream_id in zip(streams_df["source_id"], streams_df["stream_id"])
        }

        # Create transformer
        transformer = task_create_transformer(stream_id_map=stream_id_map)

        # Step 2: Determine what data to fetch
        self.logger.info("Determining what data to fetch...")
        needed_keys = task_determine_needed_keys(
            strategy=recon_strategy,
            streams_df=streams_df,
            provider_getter=self.processed_provider,
            target_client=target_client,
            data_provider=self.data_provider,
            return_state=True,
        ).result()

        # Step 3: Process each date
        dates_processed: list[DateStr] = []

        # Merge all needed dates across streams
        all_needed_dates = set()
        for dates in needed_keys.values():
            all_needed_dates.update(dates)
        sorted_needed_dates = sorted(all_needed_dates)

        self.logger.info(f"Processing {len(all_needed_dates)} unique dates...")

        # Optimization to avoid processing if no new dates
        with transactions.transaction():
            # if task_dates_already_processed(needed_dates=sorted_needed_dates):
            #     self.logger.info("Dates have already been processed in the last day, skipping...")
            #     return

            self.logger.info("Dates have not been processed in the last day, processing...")

            all_data = pd.DataFrame(columns=["date", "value", "stream_id", "data_provider"])
            for date in sorted_needed_dates:
                data = self.processed_provider.get_data_for(date)
                transformed_data = task_transform_data(transformer=transformer, data=data)
                if not transformed_data.empty:
                    all_data = pd.concat([all_data, transformed_data])
                    dates_processed.append(date)

            typed_all_data = PanderaDataFrame[TnDataRowModel](all_data)

            # Process each stream in parallel
            task_insert_data.submit(
                client=target_client,
                data=typed_all_data,
            )

        # Step 4: Create summary
        self.logger.info("Creating processing summary...")
        self._create_summary(streams_df, set(dates_processed))

        self.logger.info("Flow completed successfully!")

    def _create_summary(
        self,
        source_metadata_df: pd.DataFrame,
        dates_processed: set[DateStr],
    ) -> None:
        """Create a markdown summary of the ingestion results.

        Args:
            source_metadata_df: Stream metadata DataFrame
            dates_processed: Set of processed dates
        """
        summary = [
            "# SEPA Data Ingestion Summary\n",
            f"## Streams Processed: {len(source_metadata_df)}\n",
            "### Stream Details:\n",
        ]

        for _, row in source_metadata_df.iterrows():
            summary.append(f"- Stream: {row['stream_id']} (Source: {row['source_id']})\n")

        summary.extend(
            [
                f"\n## Dates Processed: {len(dates_processed)}\n",
                "### Date Details:\n",
            ]
        )

        for date in sorted(dates_processed):
            summary.append(f"- {date}\n")

        create_markdown_artifact(
            key="ingestion-summary",
            markdown="\n".join(summary),
            description="Summary of SEPA data ingestion",
        )


@flow(name="Argentina SEPA Ingestion")
def ingest_flow(
    source_descriptor_type: str,
    source_descriptor_block_name: str,
    trufnetwork_access_block_name: str,
    s3_block_name: str,
    data_provider: Optional[str] = None,
) -> None:
    """Ingest preprocessed Argentina SEPA data.

    Args:
        source_descriptor_type: Type of source descriptor
        source_descriptor_block_name: Name of source descriptor block
        trufnetwork_access_block_name: Name of TrufNetwork access block
        s3_block_name: Optional name of S3 block to use
        data_provider: Optional data provider identifier
    """
    # Get S3 block if specified
    s3_block = deroutine(S3Bucket.load(s3_block_name))

    # Create and run flow
    flow = IngestFlow(
        source_descriptor_type=source_descriptor_type,
        source_descriptor_block_name=source_descriptor_block_name,
        trufnetwork_access_block_name=trufnetwork_access_block_name,
        s3_block=s3_block,
        data_provider=data_provider,
    )
    flow.run()


if __name__ == "__main__":
    ingest_flow(
        source_descriptor_type="github",
        source_descriptor_block_name="argentina-sepa",
        trufnetwork_access_block_name="default",
        s3_block_name="argentina-sepa",
    )
