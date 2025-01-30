"""
Type aliases for the Argentina SEPA data ingestion pipeline.
"""

from pandera.typing import DataFrame as PaDataFrame

from tsn_adapters.tasks.argentina.base_types import DateStr, SourceId, StreamId
from tsn_adapters.tasks.argentina.models.aggregated_prices import SepaAggregatedPricesModel
from tsn_adapters.tasks.argentina.models.category_map import SepaProductCategoryMapModel
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import (
    ProductDescriptionModel,
    SepaAvgPriceProductModel,
    SepaProductosDataModel,
)
from tsn_adapters.tasks.argentina.models.stream_source import StreamSourceMetadataModel

# DataFrame type aliases
SepaDF = PaDataFrame[SepaProductosDataModel]
CategoryMapDF = PaDataFrame[SepaProductCategoryMapModel]
AvgPriceDF = PaDataFrame[SepaAvgPriceProductModel]
AggregatedPricesDF = PaDataFrame[SepaAggregatedPricesModel]
UncategorizedDF = PaDataFrame[ProductDescriptionModel]
StreamSourceMapDF = PaDataFrame[StreamSourceMetadataModel]

# Common dictionary types
StreamIdMap = dict[SourceId, StreamId]
NeededKeysMap = dict[StreamId, list[DateStr]]
AvailableKeysMap = dict[StreamId, list[DateStr]]
