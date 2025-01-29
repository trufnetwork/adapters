"""
SEPA data transformer implementation.
"""

from typing import cast

from pandera.typing import DataFrame
from prefect import task

from tsn_adapters.common.interfaces.transformer import IDataTransformer
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.tasks.argentina.models.aggregated_prices import sepa_aggregated_prices_to_tn_records
from tsn_adapters.tasks.argentina.types import (
    AggregatedPricesDF,
    SourceId,
    StreamIdMap,
)


class SepaDataTransformer(IDataTransformer[AggregatedPricesDF]):
    """Transforms SEPA data into TrufNetwork records."""

    def __init__(self, stream_id_map: StreamIdMap):
        """
        Initialize with stream ID mapping.

        Args:
            stream_id_map: Dict mapping category IDs to stream IDs
        """
        self.stream_id_map = stream_id_map

    def transform(self, data: AggregatedPricesDF) -> DataFrame[TnDataRowModel]:
        """
        Transform SEPA data into TrufNetwork records.

        Args:
            data: The SEPA data to transform

        Returns:
            pd.DataFrame: The transformed data ready for TrufNetwork
        """
        # Convert to TN records
        tn_records = sepa_aggregated_prices_to_tn_records(
            data,
            lambda category_id: self.stream_id_map[cast(SourceId, category_id)],
        )

        return tn_records


@task(name="Create SEPA Data Transformer")
def create_sepa_transformer(stream_id_map: StreamIdMap) -> SepaDataTransformer:
    """
    Create a SEPA data transformer instance.

    Args:
        stream_id_map: Dict mapping category IDs to stream IDs

    Returns:
        SepaDataTransformer: The transformer instance
    """
    return SepaDataTransformer(stream_id_map)
