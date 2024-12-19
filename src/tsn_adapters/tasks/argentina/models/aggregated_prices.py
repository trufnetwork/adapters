"""
Models for aggregated price data in the SEPA dataset.

This module contains the Pandera model for validating and typing the aggregated
price data from the SEPA dataset.
"""

from pandera.typing import Series
from pandera import DataFrameModel


class SepaAggregatedPricesModel(DataFrameModel):
    """
    Pandera model for aggregated price data by category.

    This model validates the structure of DataFrames containing aggregated price
    information for each category over time.

    Attributes
    ----------
    category_id : Series[str]
        Identifier for the category
    avg_price : Series[float]
        Average price for the category
    date : Series[str]
        Date of the price record in string format
    """
    category_id: Series[str]
    avg_price: Series[float]
    date: Series[str]

    class Config:
        strict = True
        coerce = True 