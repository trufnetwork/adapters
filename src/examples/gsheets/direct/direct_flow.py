import os
from prefect import flow
from examples.gsheets.utils import task_deploy_primitive_if_needed, task_filter_by_source_id, task_normalize_source, task_prepare_records_for_tsn
from tsn_adapters.tasks.trufnetwork import task_insert_tsn_records, task_get_all_tsn_records
from tsn_adapters.tasks.data_manipulation import task_reconcile_data
from tsn_adapters.tasks.gsheet import task_read_gsheet
import trufnetwork_sdk_py.client as tn_client
import trufnetwork_sdk_py.utils as tn_utils

@flow(log_prints=True)
def gsheets_flow(destination_tsn_provider: str):
    """
    This flow ingests data from Google Sheets into TSN, by directly specifying the sheet ID and the source ID to filter by.

    Example publicly available at https://docs.google.com/spreadsheets/d/1WE3Sw_ZZ4IyJmcqG5BTTtAMX6qRX0_k8dBlnH2se7dI/view

    Expects from gsheet:
    - Year: YYYY
    - Month: MM
    - ID: the identification to filter the records by on the source
    - Value: the value to insert into TSN
    
    It will fetch records from the sheet and insert them into TSN, creating the stream if needed.
    """
    source_id = "1.1.01"
    stream_name = "gsheets-direct-flow-stream"
    stream_id = tn_utils.generate_stream_id(stream_name)
    gsheets_id = "1WE3Sw_ZZ4IyJmcqG5BTTtAMX6qRX0_k8dBlnH2se7dI"
    
    # initialize the TSN client
    client = tn_client.TNClient(destination_tsn_provider, token=os.environ["TSN_PRIVATE_KEY"])

    # deploy the source_id if needed
    task_deploy_primitive_if_needed(stream_id, client)

    # Fetch the records from the sheet
    print(f"Fetching records from sheet {gsheets_id}")
    # see read_gsheet for more details about the second_column_name parameter
    records = task_read_gsheet(gsheets_id, second_column_name="Month")

    # Standardize the records
    normalized_records = task_normalize_source(records)

    # Filter the records by source_id
    filtered_records = task_filter_by_source_id(normalized_records, source_id)
    print(f"Found {len(filtered_records)} records for {source_id}")

    # Prepare the records for TSN
    prepared_records = task_prepare_records_for_tsn(filtered_records)

    # Get the existing records from TSN, so we can compare and only insert new or modified records
    existing_records = task_get_all_tsn_records(stream_id, client)

    # Reconcile the records with the existing ones in TSN
    reconciled_records = task_reconcile_data(existing_records, prepared_records)

    # Insert the records into TSN, if needed
    task_insert_tsn_records(stream_id, reconciled_records, client)

if __name__ == "__main__":
    destination_tsn_provider = os.environ["TSN_PROVIDER"]

    gsheets_flow(destination_tsn_provider)
