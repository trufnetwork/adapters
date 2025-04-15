import pandas as pd
from prefect import task
import trufnetwork_sdk_py.client as tn_client


@task
def task_filter_by_source_id(df: pd.DataFrame, source_id: str) -> pd.DataFrame:
    return filter_by_source_id(df, source_id)


def filter_by_source_id(df: pd.DataFrame, source_id: str) -> pd.DataFrame:
    return df[df["source_id"] == source_id]


@task
def task_prepare_records_for_tsn(df: pd.DataFrame) -> pd.DataFrame:
    return prepare_records_for_tsn(df)


def prepare_records_for_tsn(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the records for TSN by converting the date to YYYY-MM-DD format.
    """
    df.loc[:, "date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df


@task
def task_deploy_primitive_if_needed(stream_id: str, client: tn_client.TNClient):
    return deploy_primitive_if_needed(stream_id, client)


def deploy_primitive_if_needed(stream_id: str, client: tn_client.TNClient):
    """
    Deploy the primitive if it does not exist.
    """
    try:
        print(f"Deploying stream {stream_id}...")
        client.deploy_stream(stream_id)
        print(f"Stream {stream_id} deployed.")
    except:
        print(f"Stream {stream_id} already exists.")


@task
def task_normalize_source(df: pd.DataFrame) -> pd.DataFrame:
    return normalize_source(df)


def normalize_source(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the source data to match the expected format.
    """

    inputs = df.copy()
    # add default day
    inputs["Day"] = 1

    # Combine Year, Month, and Day into a string and then convert to datetime
    inputs["date"] = pd.to_datetime(
        inputs["Year"].astype(str) + " " + inputs["Month"] + " " + inputs["Day"].astype(str)
    )
    inputs["value"] = inputs["Value"].astype(float)

    # rename columns to match the expected ones
    inputs = inputs.rename({"ID": "source_id"}, axis=1)

    # select the columns we need
    inputs = inputs[["date", "value", "source_id"]]

    return inputs
