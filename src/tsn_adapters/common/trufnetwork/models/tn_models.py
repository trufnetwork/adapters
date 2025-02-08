from typing import TypeVar

import pandera as pa
from pandera.typing import DataFrame, Series
from pydantic import BaseModel

from ....tasks.argentina.base_types import StreamId

# Create type variables for the models
T = TypeVar("T", bound="TnRecordModel")
S = TypeVar("S", bound="TnDataRowModel")


class TnRecord(BaseModel):
    date: str
    value: str


class TnRecordModel(pa.DataFrameModel):
    """
    Schema for TN records
    """

    date: Series[str]  # A string here to support both date formats and unix timestamps (seconds)
    value: Series[str]  # Can't use decimal.Decimal in series

    class Config(pa.DataFrameModel.Config):
        coerce = True
        strict = "filter"


class TnDataRowModel(TnRecordModel):
    """
    Schema for TN data rows, which includes the stream_id
    """

    stream_id: Series[str]

    class Config(TnRecordModel.Config):
        coerce = True
        strict = "filter"


def split_data_row(data_row: DataFrame[TnDataRowModel]) -> dict[StreamId, DataFrame[TnRecordModel]]:
    """Split a data row into a dictionary of dataframes for each stream_id"""
    unique_stream_ids = data_row["stream_id"].unique()
    return {
        stream_id: DataFrame[TnRecordModel](data_row[data_row["stream_id"] == stream_id])
        for stream_id in unique_stream_ids
    }
