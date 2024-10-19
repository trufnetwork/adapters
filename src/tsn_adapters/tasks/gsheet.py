from typing import Optional
from prefect import task, flow
from datetime import datetime, timedelta
from truflation.data.connector.gsheet import ConnectorGoogleSheets
import pandas as pd
from prefect.tasks import task_input_hash

# minute cache = it's shared between multiple runs
@task(cache_expiration=timedelta(minutes=1), cache_key_fn=task_input_hash)
def task_read_gsheet(gsheets_id: str, second_column_name: Optional[str] = "value"):
    return read_gsheet(gsheets_id, second_column_name)


def read_gsheet(gsheets_id: str, second_column_name: Optional[str] = "value"):
    """
    Read a Google Sheet and return a DataFrame.

    Args:
        gsheets_id (str): The ID of the Google Sheet.
        second_column_name (Optional[str]): The name of the second column. Defaults to 'value'. It's a workaround to
        be explicit about the second column name, because truflation connector forces its renaming to "value".

    Returns:
        pd.DataFrame: The DataFrame containing the data from the Google Sheet.
    """
    connector = ConnectorGoogleSheets()
    df = connector.read_all(sheet_id=gsheets_id)

    # truflation connector expects that the second column is a value column, and it forces its renaming to "value"
    # we need to rename it back to the original name
    if second_column_name:
        df.columns.values[1] = second_column_name
    return df