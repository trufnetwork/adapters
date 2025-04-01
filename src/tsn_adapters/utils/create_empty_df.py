from typing import TypeVar

from pandera import DataFrameModel
from pandera.typing import DataFrame

T = TypeVar("T", bound=DataFrameModel)


def create_empty_df(model_type: type[T]) -> DataFrame[T]:
    """Create an empty DataFrame with specified columns."""
    schema = model_type.to_schema()
    columns = list(schema.columns.keys())
    return DataFrame[model_type](columns=columns)
