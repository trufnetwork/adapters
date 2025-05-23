"""
Models for aggregated price data in the SEPA dataset.

This module contains the Pandera model for validating and typing the aggregated
price data from the SEPA dataset.
"""

import pandas as pd

from typing import Callable

from pandera import DataFrameModel
from pandera.typing import DataFrame, Series

from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel


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

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True


def sepa_aggregated_prices_to_tn_records(
    df: DataFrame[SepaAggregatedPricesModel],
    get_stream_id: Callable[[str], str],
) -> DataFrame[TnDataRowModel]:
    new_df = df.copy()
    new_df["value"] = new_df["avg_price"].astype(str)
    new_df["date"] = pd.to_datetime(new_df["date"])
    new_df["date"] = new_df["date"].astype(int) // 10**9 # convert to UNIX
    new_df["stream_id"] = new_df["category_id"].apply(get_stream_id)
    return DataFrame[TnDataRowModel](new_df)
