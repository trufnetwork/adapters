import os

import pandas as pd
from prefect import flow, get_run_logger
from prefect.futures import wait
import trufnetwork_sdk_py.client as tn_client

from examples.gsheets.utils import (
    task_filter_by_source_id,
    task_normalize_source,
    task_prepare_records_for_tsn,
)
from tsn_adapters.blocks.github_access import GithubAccess, read_repo_csv_file
from tsn_adapters.blocks.tn_access import TNAccessBlock, task_wait_for_tx
from tsn_adapters.common.trufnetwork.tn import (
    task_batch_deploy_streams,
    task_batch_filter_streams_by_existence,
    task_get_all_tsn_records,
    task_insert_tsn_records,
)
from tsn_adapters.tasks.data_manipulation import task_reconcile_data
from tsn_adapters.tasks.gsheet import task_read_gsheet
from tsn_adapters.utils import cast_future

from ....tsn_adapters.utils.deroutine import force_sync


@flow(log_prints=True)
def gsheets_flow(repo: str, sources_path: str, destination_tsn_provider: str):
    """
    This flow ingests data from Google Sheets into TSN.
    It reads from a `primitive_sources.csv` file in the repo to know which sheets to ingest data from.
    Streams are batch deployed if they don't exist before data ingestion.

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

    logger = get_run_logger()

    # Block.load() is synchronous. Type hinting removed to avoid linter confusion if it misinterprets.
    github_block = force_sync(GithubAccess.load)("default")
    logger.info("Loaded GithubAccess block.")

    sources_df: pd.DataFrame = read_repo_csv_file(block=github_block, repo=repo, path=sources_path)
    logger.info(f"Found {len(sources_df)} sources to be ingested from {repo}/{sources_path}")

    if sources_df.empty:
        logger.info("No sources found in the descriptor file. Exiting flow.")
        return

    try:
        tna_block = force_sync(TNAccessBlock.load)("default-tn-access")
        logger.info(f"Loaded TNAccessBlock 'default-tn-access' for provider: {tna_block.tn_provider}")
    except Exception as e:
        logger.error(
            f"Failed to load TNAccessBlock 'default-tn-access'. Ensure it exists and is configured. Error: {e}"
        )
        logger.error("Halting flow as TNAccessBlock is required.")
        return

    # SECTION: Batch Deployment Pre-Step
    all_stream_ids_from_sources = sources_df["stream_id"].astype(str).unique().tolist()
    if not all_stream_ids_from_sources:
        logger.info("No unique stream IDs found in sources. Skipping deployment pre-step.")
    else:
        logger.info(f"Preparing to check/deploy {len(all_stream_ids_from_sources)} unique streams from sources.")

        locators_to_check: list[tn_client.StreamLocatorInput] = []
        data_provider_for_check = tna_block.current_account
        for sid in all_stream_ids_from_sources:
            locators_to_check.append(tn_client.StreamLocatorInput(stream_id=sid, data_provider=data_provider_for_check))

        try:
            logger.info(f"Checking existence of {len(locators_to_check)} streams on TN.")
            non_existent_locators = task_batch_filter_streams_by_existence.submit(
                block=tna_block, locators=locators_to_check, return_existing=False
            ).result()

            streams_to_deploy_ids = [str(loc["stream_id"]) for loc in non_existent_locators]
            logger.info(f"{len(streams_to_deploy_ids)} streams need deployment.")

            if streams_to_deploy_ids:
                deployment_definitions: list[tn_client.StreamDefinitionInput] = []
                for sid_to_deploy in streams_to_deploy_ids:
                    deployment_definitions.append(
                        tn_client.StreamDefinitionInput(
                            stream_id=sid_to_deploy, stream_type=tn_client.STREAM_TYPE_PRIMITIVE
                        )
                    )

                logger.info(f"Submitting batch deployment for {len(deployment_definitions)} streams.")
                batch_tx_hash_future = task_batch_deploy_streams.submit(
                    block=tna_block, definitions=deployment_definitions, wait=False
                )
                batch_tx_hash = batch_tx_hash_future.result()

                if batch_tx_hash:
                    logger.info(f"Batch deployment transaction submitted: {batch_tx_hash}. Waiting for confirmation.")
                    task_wait_for_tx.submit(block=tna_block, tx_hash=batch_tx_hash).wait()
                    logger.info(f"Batch deployment transaction {batch_tx_hash} confirmed.")
                else:
                    logger.error("Did not receive a transaction hash for batch deployment. Check logs from the task.")
                    logger.warning("Proceeding with data ingestion despite potential batch deployment issues.")
            else:
                logger.info("No streams require deployment based on TN existence check.")

        except Exception as e:
            logger.error(
                f"Error during batch stream deployment pre-step: {e!s}. Proceeding with caution.", exc_info=True
            )

    # Data Ingestion Loop
    source_types = sources_df["source_type"].astype(str).unique().tolist()
    gsheets_ids = [st.split(":")[1] for st in source_types if st.startswith("gsheets:")]
    logger.info(f"Processing gsheets_ids: {gsheets_ids}")

    insert_jobs = []
    # client for old tasks, assuming TNAccessBlock provides a compatible client property
    # or tasks are updated. For this example, use tna_block.client
    # The tasks `task_get_all_tsn_records` and `task_insert_tsn_records` from `tn.py` expect `TNClient`
    tn_sdk_client_instance = tna_block.client

    for gsheets_id in gsheets_ids:
        logger.info(f"Fetching records from sheet {gsheets_id}")
        records_df: pd.DataFrame = task_read_gsheet(gsheets_id=gsheets_id, second_column_name="Month")

        for _, row in sources_df[sources_df["source_type"] == f"gsheets:{gsheets_id}"].iterrows():
            stream_id_for_row = str(row["stream_id"])
            source_id_for_row = str(row["source_id"])
            logger.info(f"Processing stream_id: {stream_id_for_row}, source_id: {source_id_for_row}")

            normalized_records_df = task_normalize_source(df=records_df)
            filtered_records_df = task_filter_by_source_id(df=normalized_records_df, source_id=source_id_for_row)
            logger.info(f"Found {len(filtered_records_df)} records for {source_id_for_row}")

            prepared_records_df = task_prepare_records_for_tsn(df=filtered_records_df)

            existing_records_future = task_get_all_tsn_records.submit(
                stream_id=stream_id_for_row, client=tn_sdk_client_instance, data_provider=tna_block.current_account
            )

            reconciled_records_future = task_reconcile_data.submit(
                df_base=cast_future(existing_records_future), df_target=prepared_records_df
            )

            insert_job = task_insert_tsn_records.submit(
                stream_id=stream_id_for_row,
                records=cast_future(reconciled_records_future),
                client=tn_sdk_client_instance,
            )
            insert_jobs.append(insert_job)

    if insert_jobs:
        logger.info(f"Waiting for {len(insert_jobs)} insert jobs to complete.")
        wait(insert_jobs)
        logger.info("All insert jobs completed.")
    else:
        logger.info("No insert jobs were submitted.")

    logger.info("gsheets_flow completed.")


if __name__ == "__main__":
    """
    Run the flow, fetching from the same repository.
    """

    repo = "truflation/tsn-adapters"
    repo_sources_path = "src/examples/gsheets/dynamic/primitive_sources.csv"
    destination_tsn_provider = os.environ.get("TSN_PROVIDER")
    if not destination_tsn_provider:
        raise ValueError("TSN_PROVIDER environment variable not set.")
    if not os.environ.get("TSN_PRIVATE_KEY"):
        raise ValueError("TSN_PRIVATE_KEY environment variable not set for TNAccessBlock/TNClient.")

    gsheets_flow(repo, repo_sources_path, destination_tsn_provider)
