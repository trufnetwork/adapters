"""
Pandera schema for stream metadata.
"""

from typing import cast

import pandera as pa
from pandera.typing import Series

from tsn_adapters.tasks.argentina.base_types import SourceId, StreamId


class StreamSourceMetadataModel(pa.DataFrameModel):
    """Schema for stream metadata."""

    stream_id: Series[str]
    source_id: Series[str]

    class Config(pa.DataFrameModel.Config):
        strict = "filter"
        coerce = True

    @pa.check("stream_id")
    def validate_stream_id(cls, series: Series[str]) -> Series[str]:
        """Validate and coerce stream_id to StreamId."""
        # We need to return Series[str] because that's what pandera expects,
        # but the values will be StreamId instances
        result = series.apply(lambda x: StreamId(str(x)))
        return cast(Series[str], result)

    @pa.check("source_id")
    def validate_source_id(cls, series: Series[str]) -> Series[str]:
        """Validate and coerce source_id to SourceId."""
        # We need to return Series[str] because that's what pandera expects,
        # but the values will be SourceId instances
        result = series.apply(lambda x: SourceId(str(x)))
        return cast(Series[str], result)
