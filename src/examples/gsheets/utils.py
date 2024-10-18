import pandas as pd
from prefect import task
import tsn_sdk.client as tsn_client


@task
def filter_by_source_id(df: pd.DataFrame, source_id: str) -> pd.DataFrame:
    return df[df["source_id"] == source_id]

@task
def prepare_records_for_tsn(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare the records for TSN by converting the date to YYYY-MM-DD format.
    """
    df.loc[:, "date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    return df

@task
def deploy_primitive_if_needed(stream_id: str, client: tsn_client.TSNClient):
    """
    Deploy the primitive if it does not exist.
    """
    # check if the stream_id exists in the TSN 
    stream_exists = client.stream_exists(stream_id)

    if not stream_exists:
        print(f"Stream {stream_id} does not exist, deploying...")
        client.deploy_stream(stream_id, wait=True)
        print(f"Stream {stream_id} deployed. Initializing...")
        client.init_stream(stream_id, wait=True)
        print(f"Stream {stream_id} initialized.")
    else:
        print(f"Stream {stream_id} already exists.")

@task
def normalize_source(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the source data to match the expected format.
    """

    inputs = df.copy()
    # add default day
    inputs['Day'] = 1

    # Combine Year, Month, and Day into a string and then convert to datetime
    inputs['date'] = pd.to_datetime(inputs['Year'].astype(str) + ' ' + inputs['Month'] + ' ' + inputs['Day'].astype(str))
    inputs["value"] = inputs["Value"].astype(float)

    # rename columns to match the expected ones
    inputs = inputs.rename({"ID": "source_id"}, axis=1)

    # select the columns we need
    inputs = inputs[["date", "value", "source_id"]]

    return inputs