from prefect import task, flow
from datetime import datetime, timedelta
from truflation.data.connector.gsheet import ConnectorGoogleSheets
import pandas as pd
from prefect.tasks import task_input_hash

# minute cache = it's shared between multiple runs
@task(cache_expiration=timedelta(minutes=1), cache_key_fn=task_input_hash)
def read_gsheet(gsheets_id: str):
    connector = ConnectorGoogleSheets()
    df = connector.read_all(sheet_id=gsheets_id)
    return df