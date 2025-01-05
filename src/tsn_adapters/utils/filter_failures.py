from typing import TypeVar, cast

import pandas as pd
import pandera as pa
from pandera.errors import SchemaErrors
from pandera.typing import DataFrame

U = TypeVar("U", bound=pa.DataFrameModel)


def filter_failures(original: pd.DataFrame, model: type[U]) -> DataFrame[U]:
    """
    Filters out rows from the original DataFrame that fail validation against the provided Pandera model.

    Args:
        original: The original DataFrame.
        model: The Pandera model to validate against.

    Returns:
        A new DataFrame containing only the rows that passed validation.
    """
    try:
        # Attempt lazy validation to catch all failures
        validated = model.validate(original, lazy=True)
        return cast(DataFrame[U], validated)
    except SchemaErrors as exc:
        if exc.failure_cases is not None:
            # Get the failure indices and drop those rows
            failure_indices = exc.failure_cases["index"].unique()
            filtered_df = original.drop(failure_indices)

            # Re-validate the filtered DataFrame
            validated_filtered = model.validate(filtered_df, lazy=False)
            return cast(DataFrame[U], validated_filtered)
        else:
            raise exc
