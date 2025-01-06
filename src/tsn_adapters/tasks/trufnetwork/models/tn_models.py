from typing import TypeVar

import pandera as pa
from pandera.typing import Series

# Create type variables for the models
T = TypeVar("T", bound="TnRecordModel")
S = TypeVar("S", bound="TnDataRowModel")


class TnRecordModel(pa.DataFrameModel):
    """
    Schema for TN records
    """

    date: Series[str]
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
