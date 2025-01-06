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