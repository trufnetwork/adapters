import pandas as pd
from prefect import task
from datetime import datetime

@task
def reconcile_data(df_base: pd.DataFrame, df_target: pd.DataFrame) -> pd.DataFrame:
    """
    Reconciles two dataframes, keeping only new records from df_target.

    expects the following columns
    - date: datetime or string
    - value: float, int or string
    
    produces the following columns
    - date: string
    - value: float
    """
    # if target is empty, return base
    if df_target.empty:
        return df_base

    # reconcile the data, keeping only new records
    # both should have at least the following columns: date, value
    df_base = df_base.copy()
    df_target = df_target.copy()

    # check columns
    if "date" not in df_base.columns or "date" not in df_target.columns:
        raise ValueError("Date column is missing")
    if "value" not in df_base.columns or "value" not in df_target.columns:
        raise ValueError("Value column is missing")

    # normalize the types
    # we accept dates as strings or datetime, but we convert to string
    df_base["date"] = df_base["date"].apply(lambda x: x.strftime("%Y-%m-%d") if isinstance(x, datetime) else x)
    df_target["date"] = df_target["date"].apply(lambda x: x.strftime("%Y-%m-%d") if isinstance(x, datetime) else x)

    # check if the dates are the same
    if df_base["date"].dtype != df_target["date"].dtype:
        raise ValueError("Date column is not of type string")
    
    # we accept value as string, float or int, but the result should be a float
    df_base["value"] = df_base["value"].apply(lambda x: float(x) if not isinstance(x, float) else x)
    df_target["value"] = df_target["value"].apply(lambda x: float(x) if not isinstance(x, float) else x)

    # check if the values are the same
    if df_base["value"].dtype != df_target["value"].dtype:
        raise ValueError("Value column is not of type float")

    df_result = df_target[~df_target["date"].isin(df_base["date"])]
    return df_result
                         