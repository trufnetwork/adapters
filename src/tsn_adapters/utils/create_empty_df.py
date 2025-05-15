import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame




def create_empty_df[PanderaModel: DataFrameModel](model_type: type[PanderaModel]) -> DataFrame[PanderaModel]:
    """
    Create an empty pandas DataFrame with columns and dtypes (including nullable dtypes)
    derived from the provided Pandera DataFrameModel.

    Args:
        model_type: The Pandera DataFrameModel class.

    Returns:
        An empty DataFrame typed with the PanderaModel, having columns
        with appropriate (nullable) dtypes.
    """
    schema = model_type.to_schema()
    empty_data = {}
    for col_name, pa_column in schema.columns.items():
        # pa_column.dtype is the Pandera Dtype object (e.g., pandera.dtypes.Float)
        # We need to get the corresponding pandas dtype.
        # For nullable dtypes, Pandera's schema (when dtype_backend="numpy_nullable" is set on the model)
        # should guide towards the correct pandas extension dtypes.
        
        # Default to object if Pandera dtype is None, though less common for well-defined schemas.
        pd_dtype = object 
        if pa_column.dtype:
            try:
                # Pandera's internal mapping or direct pandas extension types
                # For example, if model field is Series[float] and nullable=True with numpy_nullable backend,
                # Pandera might internally map this to something that translates to pd.Float64Dtype().
                # A simple way is to rely on the string representation for common types
                # or let pandas infer from an empty series of that type if possible.
                # More robustly, map Pandera dtypes to pandas extension dtypes:
                if pa_column.dtype == pd.Float64Dtype: # Check if it's already a pandas dtype
                     pd_dtype = pa_column.dtype
                elif pa_column.dtype.type is float:
                    pd_dtype = pd.Float64Dtype() if pa_column.nullable else float
                elif pa_column.dtype.type is int:
                    pd_dtype = pd.Int64Dtype() if pa_column.nullable else int # Choose appropriate bit size
                elif pa_column.dtype.type is bool:
                    pd_dtype = pd.BooleanDtype() if pa_column.nullable else bool
                elif pa_column.dtype.type is str:
                    pd_dtype = pd.StringDtype() if pa_column.nullable else str
                # Add more mappings as needed for datetime, timedelta, etc.
                # Example for datetime:
                # elif pa_column.dtype.type == datetime.datetime:
                #     pd_dtype = pd.DatetimeTZDtype(tz="UTC") if pa_column.nullable else 'datetime64[ns, UTC]'

                else: # Fallback for other types
                    pd_dtype = pa_column.dtype.type 
            except AttributeError: # If pa_column.dtype.type is not available for some reason
                 pd_dtype = object # Fallback

        empty_data[col_name] = pd.Series(dtype=pd_dtype)
        
    df = pd.DataFrame(empty_data)
    # Ensure columns are in the order defined in the schema
    df = df.reindex(columns=list(schema.columns.keys()))
    return DataFrame[model_type](df)