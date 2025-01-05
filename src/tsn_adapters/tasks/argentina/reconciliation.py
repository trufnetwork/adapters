"""
Reconciliation strategies for determining what data needs to be fetched.
"""

from typing import cast

import pandas as pd
from prefect import task
from prefect.artifacts import create_markdown_artifact
from tsn_adapters.tasks.argentina.interfaces.base import IReconciliationStrategy, ITargetGetter
from tsn_adapters.tasks.argentina.types import DateStr, NeededKeysMap, StreamId


class ByLastInsertedStrategy(IReconciliationStrategy):
    """Strategy that determines needed data based on the last inserted date."""

    def determine_needed_keys(
        self, streams_df: pd.DataFrame, target_getter: ITargetGetter, data_provider: str
    ) -> NeededKeysMap:
        """
        Determine which dates need to be fetched for each stream.

        Args:
            streams_df: DataFrame containing stream metadata with columns:
                - stream_id: StreamId
                - source_id: str
                - available_dates: List[DateStr]
            target_getter: The target system getter
            data_provider: The data provider identifier

        Returns:
            Dict mapping stream_id to list of dates that need to be fetched
        """
        results: NeededKeysMap = {}
        summary: list[str] = []
        summary.append(f"Strategy: {self.__class__.__name__}\n\n")
        for _, row in streams_df.iterrows():
            stream_id = cast(StreamId, row["stream_id"])

            # Get existing data for this stream
            existing_df = target_getter.get_latest(stream_id=stream_id, data_provider=data_provider)

            # Get the last inserted date, or use a very old date if no data exists
            if not existing_df.empty:
                last_inserted = cast(DateStr, existing_df["date"].max())
            else:
                last_inserted = cast(DateStr, "0000-00-00")

            # Get all available dates for this stream
            available_dates = [cast(DateStr, d) for d in row["available_dates"]]

            # Find dates that are newer than the last inserted date
            needed_dates = [d for d in available_dates if d > last_inserted]

            # sort needed dates
            needed_dates.sort()

            # Store the needed dates for this stream
            if needed_dates:
                results[stream_id] = needed_dates

            summary.append(f"## Stream: {stream_id}\n")
            summary.append(f"- Source ID: {row['source_id']}\n")
            summary.append(f"- Last inserted date: {last_inserted}\n")
            summary.append(f"- Total dates to fetch: {len(needed_dates)}\n")

        create_markdown_artifact(
            key="needed-keys-summary",
            markdown="".join(summary),
            description=f"Summary of data requirements using {self.__class__.__name__}"
        )

        return results


@task(name="Create Reconciliation Strategy")
def create_reconciliation_strategy() -> ByLastInsertedStrategy:
    """
    Create a reconciliation strategy instance.

    Returns:
        ByLastInsertedStrategy: The strategy instance
    """
    return ByLastInsertedStrategy()
