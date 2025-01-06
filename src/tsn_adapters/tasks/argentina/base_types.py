"""
Base type definitions for the Argentina SEPA data ingestion pipeline.
"""

from typing import Literal, NewType

# Basic type aliases
StreamId = NewType("StreamId", str)
SourceId = NewType("SourceId", str)
DateStr = NewType("DateStr", str)  # YYYY-MM-DD format

# Source descriptor types
PrimitiveSourcesTypeStr = Literal["url", "github"]
