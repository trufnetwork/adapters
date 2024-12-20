"""
Models for aggregated price data in the SEPA dataset.

This module contains the Pandera model for validating and typing the aggregated
price data from the SEPA dataset.
"""

from typing import Callable
from pandera.typing import Series, DataFrame
from pandera import DataFrameModel
from tsn_adapters.tasks.trufnetwork.models import TnRecordModel
import pandera as pa

from ...trufnetwork.models.tn_models import TnDataRowModel


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
        strict = "filter"
        coerce = True


@pa.check_types
def sepa_aggregated_prices_to_tn_records(
    df: DataFrame[SepaAggregatedPricesModel],
    get_stream_id: Callable[[str], str],
) -> DataFrame[TnDataRowModel]:
    new_df = df.copy()
    new_df["value"] = new_df["avg_price"].astype(str)
    new_df["date"] = new_df["date"].astype(str)
    new_df["stream_id"] = new_df["category_id"].apply(get_stream_id)
    return DataFrame[TnDataRowModel](new_df)
