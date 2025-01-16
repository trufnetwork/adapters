import os

from prefect import flow
from prefect.futures import wait
import trufnetwork_sdk_py.client as tn_client

from examples.gsheets.utils import (
    task_deploy_primitive_if_needed,
    task_filter_by_source_id,
    task_normalize_source,
    task_prepare_records_for_tsn,
)
from tsn_adapters.blocks.github_access import GithubAccess, read_repo_csv_file
from tsn_adapters.common.trufnetwork.tn import task_get_all_tsn_records, task_insert_tsn_records
from tsn_adapters.tasks.data_manipulation import task_reconcile_data
from tsn_adapters.tasks.gsheet import task_read_gsheet
from tsn_adapters.utils import cast_future


@flow(log_prints=True)
def gsheets_flow(repo: str, sources_path: str, destination_tsn_provider: str):
    """
    This flow ingests data from Google Sheets into TSN.
    It reads from a `primitive_sources.csv` file in the repo to know which sheets to ingest data from.

    It expects a CSV file in the repo with the following columns:
    - source_type: the type of source, e.g. gsheets:<gsheets_id>
    - stream_id: the TSN stream_id to insert the records into
    - source_id: the identification to filter the records by on the source

    Example publicly available at https://docs.google.com/spreadsheets/d/1WE3Sw_ZZ4IyJmcqG5BTTtAMX6qRX0_k8dBlnH2se7dI/view

    Expects from gsheet:
    - Year: YYYY
    - Month: MM
    - ID: the identification to filter the records by on the source
    - Value: the value to insert into TSN

    It will fetch records from all the sources and insert them into TSN, creating the stream if needed.
    """

    # don't forget to create the block in the UI first with the name "default"
    github_block = GithubAccess.load("default")

    # Read the sources from the CSV file in the repo
    sources_df = read_repo_csv_file(block=github_block, repo=repo, path=sources_path)
    print(f"Found {len(sources_df)} sources to be ingested")

    # we want to know from which sources we are ingesting data
    # get unique source_types to extract the gsheets_ids
    source_types = sources_df["source_type"].unique().tolist()
    print(f"Found {len(source_types)} source types: {source_types}")

    # extract the gsheets_id from the source_type, ensuring it starts with 'gsheets'
    gsheets_ids = [source_type.split(":")[1] for source_type in source_types if source_type.startswith("gsheets:")]
    print(f"Found {len(gsheets_ids)} gsheets ids: {gsheets_ids}")

    # store insertion tasks
    insert_jobs = []

    # initialize the TSN client
    client = tn_client.TNClient(destination_tsn_provider, token=os.environ["TSN_PRIVATE_KEY"])

    for gsheets_id in gsheets_ids:
        # Fetch the records from the sheet
        print(f"Fetching records from sheet {gsheets_id}")
        # see read_gsheet for more details about the second_column_name parameter
        records = task_read_gsheet(gsheets_id=gsheets_id, second_column_name="Month")

        # for each source, fetch the records and transform until we can insert them into TSN
        # insertions happen concurrently
        for _, row in sources_df.iterrows():
            # deploy the source_id if needed
            deployment_job = task_deploy_primitive_if_needed.submit(stream_id=row["stream_id"], client=client)

            # Standardize the records
            normalized_records = task_normalize_source(df=records)

            # Filter the records by source_id
            filtered_records = task_filter_by_source_id(df=normalized_records, source_id=row["source_id"])
            print(f"Found {len(filtered_records)} records for {row['source_id']}")

            # Prepare the records for TSN
            prepared_records = task_prepare_records_for_tsn(df=filtered_records)

            # Get the existing records from TSN, so we can compare and only insert new or modified records
            existing_records = task_get_all_tsn_records.submit(
                stream_id=row["stream_id"], client=client, data_provider=None, wait_for=[deployment_job]
            )

            # Reconcile the records with the existing ones in TSN
            reconciled_records = task_reconcile_data.submit(df_base=existing_records, df_target=prepared_records)

            # Insert the records into TSN, concurrently, if needed
            insert_job = task_insert_tsn_records.submit(
                stream_id=row["stream_id"], records=cast_future.cast_future(reconciled_records), client=client
            )
            insert_jobs.append(insert_job)

    # Wait for all the insertions to complete
    wait(insert_jobs)


if __name__ == "__main__":
    """
    Run the flow, fetching from the same repository.
    """

    repo = "truflation/tsn-adapters"
    repo_sources_path = "src/examples/gsheets/dynamic/primitive_sources.csv"
    destination_tsn_provider = os.environ["TSN_PROVIDER"]

    gsheets_flow(repo, repo_sources_path, destination_tsn_provider)
