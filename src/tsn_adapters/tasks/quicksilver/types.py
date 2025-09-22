"""
Type definitions for Quicksilver adapter.
"""

from typing import NewType

from pandera import DataFrameModel, Field
from pandera.typing import DataFrame, Series

QuicksilverKey = NewType("QuicksilverKey", str)


class QuicksilverDataModel(DataFrameModel):
    """
    Designed for ID-filtered queries that return a single, known asset.
    """
    id: Series[str] = Field(description="Asset ID (e.g., 'bitcoin', 'ethereum')")
    symbol: Series[str] = Field(description="Trading symbol (e.g., 'btc', 'eth')")
    current_price: Series[str] = Field(description="Current price as string")
    last_updated: Series[str] = Field(description="Last update timestamp")
    
    class Config(DataFrameModel.Config):
        strict = "filter"  # Filter out unknown columns
        coerce = True      # Coerce data types


QuicksilverDataDF = DataFrame[QuicksilverDataModel]
