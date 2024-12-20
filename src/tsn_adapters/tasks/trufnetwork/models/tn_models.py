import pandera as pa
from pandera.typing import Series

class TnRecordModel(pa.DataFrameModel):
    """
    Schema for TN records
    """
    date: Series[str]
    value: Series[str]  # Can't use decimal.Decimal in series

    class Config:
        coerce = True
        strict = 'filter'

class TnDataRowModel(TnRecordModel):
    """
    Schema for TN data rows, which includes the stream_id
    """
    stream_id: Series[str]

    class Config:
        coerce = True
        strict = 'filter'
