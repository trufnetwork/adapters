import pandas as pd
from prefect import task
from datetime import datetime

@task
def task_reconcile_data(df_base: pd.DataFrame, df_target: pd.DataFrame) -> pd.DataFrame:
    return reconcile_data(df_base, df_target)

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
    df_base = normalize_columns(df_base) if not df_base.empty else df_base
    df_target = normalize_columns(df_target)

    if df_base.empty:
        return df_target

    # keep only the records that are not in the base
    df_result = df_target[~df_target["date"].isin(df_base["date"])]
    return df_result


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes the columns of a dataframe to the following:
    - date: string
    - value: float
    """
    df = df.copy()
    
    # check columns presence
    if "date" not in df.columns:
        raise ValueError("Date column is missing")
    if "value" not in df.columns:
        raise ValueError("Value column is missing")
    
    # normalize the types
    df["date"] = df["date"].apply(lambda x: x.strftime("%Y-%m-%d") if isinstance(x, datetime) else x)
    df["value"] = df["value"].apply(lambda x: float(x) if not isinstance(x, float) else x)
    
    return df