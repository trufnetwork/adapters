"""
Type definitions for Quicksilver adapter.
"""

from typing import NewType

from pandera import DataFrameModel, Field
from pandera.typing import DataFrame, Series

QuicksilverKey = NewType("QuicksilverKey", str)


class QuicksilverDataModel(DataFrameModel):
    """Data model for Quicksilver API responses."""
    ticker: Series[str] = Field(description="Ticker symbol")
    price: Series[str] = Field(description="Current price")
    
    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True


QuicksilverDataDF = DataFrame[QuicksilverDataModel]
