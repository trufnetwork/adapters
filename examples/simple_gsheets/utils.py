import pandas as pd
from prefect import task


@task
def filter_by_source_id(df: pd.DataFrame, source_id: str) -> pd.DataFrame:
    return df[df["source_id"] == source_id]

@task
def prepare_records_for_tsn(df: pd.DataFrame) -> pd.DataFrame:
    # date must be in YYYY-MM-DD format
    df.loc[:, "date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df