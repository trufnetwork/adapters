"""
Prefect flow for inserting Argentina SEPA product data into TrufNetwork.

This flow reads daily average product prices, maps them to TN streams,
transforms the data, inserts it into TN, and manages state.
"""

from itertools import batched
from typing import Generator, Sequence

from pandera.typing import DataFrame
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact
import prefect.variables as variables  # Import prefect variables
from prefect_aws import S3Bucket

from tsn_adapters.blocks.deployment_state import DeploymentStateBlock
from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel, PrimitiveSourcesDescriptorBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock, extract_stream_locators
from tsn_adapters.common.trufnetwork.tasks.insert import task_split_and_insert_records
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.common.trufnetwork.tn import task_batch_filter_streams_by_existence
import trufnetwork_sdk_py.client as tn_client
from tsn_adapters.tasks.argentina.config import ArgentinaFlowVariableNames  # Import config
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks import (
    determine_dates_to_insert,
    load_daily_averages,
    transform_product_data,
)

StreamLocatorBatches = Sequence[Sequence[tn_client.StreamLocatorInput]]


class DeploymentCheckError(Exception):
    """Raised when required streams for a date are not deployed."""

    pass


def process_stream_batches(
    tn_block: TNAccessBlock,
    batches: StreamLocatorBatches,
) -> Generator[tn_client.StreamLocatorInput, None, None]:
    """
    Generator that processes stream existence batches and yields missing locators.
    
    Extracted as a standalone function for better testability and reusability.
    Uses yield from to flatten batch results efficiently without accumulating
    all results in memory before returning.
    
    Args:
        tn_block: TNAccessBlock instance for API calls
        batches: Sequence of batches, each containing stream locators to check
        
    Yields:
        StreamLocatorInput: Individual missing stream locators
        
    Raises:
        Exception: If any batch existence check fails
    """
    logger = get_run_logger()
    
    for batch_num, current_filter_batch in enumerate(batches, 1):
        logger.debug(
            f"Checking existence batch {batch_num}/{len(batches)} ({len(current_filter_batch)} locators)..."
        )
        
        try:
            missing_locators_in_batch = task_batch_filter_streams_by_existence(
                block=tn_block, 
                locators=list(current_filter_batch), 
                return_existing=False  # Inverted: returns non-existent streams
            )
            
            logger.debug(f"Batch {batch_num}: Found {len(missing_locators_in_batch)} non-existent streams.")
            yield from missing_locators_in_batch
            
        except Exception as e:
            logger.error(f"Failed to check existence for batch {batch_num}: {e}", exc_info=True)
            raise


@task(name="Batch Check Missing Streams")
def task_batch_check_missing_streams(
    tn_block: TNAccessBlock,
    required_locators: list[tn_client.StreamLocatorInput],
    max_filter_size: int,
    date_str: str,
    ) -> list[tn_client.StreamLocatorInput]:
    """
    Check which streams from the required list do not exist on TN using batched API calls.
    
    Uses a functional approach with generators to avoid API rate limits while maintaining
    memory efficiency for large stream sets (65k+ streams observed in production).
    
    Args:
        tn_block: TNAccessBlock instance for API calls
        required_locators: List of stream locators to check
        max_filter_size: Maximum number of streams per API batch (typically 500 to avoid rate limits)
        date_str: Date string for logging context
        
    Returns:
        List of missing stream locators (empty if all exist)
        
    Raises:
        Exception: If any batch check fails
    """
    logger = get_run_logger()
    logger.info(f"Checking existence for {len(required_locators)} streams for date {date_str}")
    
    locator_batches = list(batched(required_locators, max_filter_size))
    logger.info(f"Processing {len(locator_batches)} batches (batch size: {max_filter_size}) for date {date_str}")
    
    all_missing_locators = list(process_stream_batches(tn_block, locator_batches))
    logger.info(f"Found {len(all_missing_locators)} missing streams out of {len(required_locators)} total")
    return all_missing_locators


@flow(name="Insert Argentina SEPA Products to TN")
async def insert_argentina_products_flow(
    s3_block: S3Bucket,
    tn_block: TNAccessBlock,
    descriptor_block: PrimitiveSourcesDescriptorBlock,
    deployment_state: DeploymentStateBlock,
    batch_size: int = 1000,  # Default batch size (empirically chosen to balance API load and memory)
    max_filter_size: int = 500,  # Max number of streams per batch when filtering deployed streams
):
    """
    Inserts pre-calculated Argentina SEPA daily average product prices into TN streams.

    This flow:
      1) Loads product-to-stream mapping (descriptor).
      2) Determines new dates since the last run.
      3) For each date:
         - Loads raw SEPA average price data.
         - Checks TN stream deployment status.
         - Transforms data into TnDataRowModel.
         - Submits batched insert tasks to TN.
      4) Updates Prefect Variables and creates summary artifacts.

    Args:
        s3_block (S3Bucket): Block for accessing S3 data and metadata.
        tn_block (TNAccessBlock): Block for TN insert API access.
        descriptor_block (PrimitiveSourcesDescriptorBlock): Provides product-to-stream mappings.
        deployment_state (DeploymentStateBlock): Verifies TN stream deployment status.
        batch_size (int): Number of records per TN insert batch.
        max_filter_size (int): Max number of streams per batch during stream filtering.

    Returns:
        None: Flow does not return; outputs artifacts and updates Prefect Variables.

    Raises:
        DeploymentCheckError: If required TN streams are not deployed.
        RuntimeError: If loading descriptor fails.
        Exception: On fatal errors during provider initialization, date determination, load, transform, or insert.
    """
    logger = get_run_logger()
    logger.info(f"Starting Argentina product insertion flow. Batch size: {batch_size}")

    # 1. Instantiate Provider
    try:
        product_averages_provider = ProductAveragesProvider(s3_block=s3_block)
        logger.info("ProductAveragesProvider instantiated.")
    except Exception as e:
        logger.critical(f"Fatal Error: Failed to instantiate ProductAveragesProvider: {e}", exc_info=True)
        raise  # Fatal: Cannot proceed without the provider

    logger.info("S3 state metadata loading removed. State will be managed by Prefect Variables.")

    # 2. Load Product Descriptor
    try:
        descriptor_df: DataFrame[PrimitiveSourceDataModel] = descriptor_block.get_descriptor()
        logger.info(f"Successfully loaded product descriptor with {len(descriptor_df)} entries from block.")
    except Exception as e_desc:
        # load_product_descriptor already logs critically and raises DescriptorError
        logger.critical(
            f"Fatal Error: Could not load product descriptor from block: {e_desc}. Flow cannot proceed.", exc_info=True
        )
        raise RuntimeError("Failed to load product descriptor") from e_desc  # Fatal: Cannot proceed without descriptor

    # 3. Determine Dates to Process using the task
    try:
        dates_to_process = await determine_dates_to_insert(provider=product_averages_provider)

        if not dates_to_process:
            logger.info("No new dates to process based on insertion/aggregation state variables. Flow finished.")
            # Create artifact even if no dates processed
            # Fetch current state variables for reporting
            last_agg_date = variables.Variable.get(
                ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE,
                default=ArgentinaFlowVariableNames.DEFAULT_DATE,
            )
            last_ins_date = variables.Variable.get(
                ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE, default=ArgentinaFlowVariableNames.DEFAULT_DATE
            )
            summary_md = f"""# Argentina Product Insertion Summary

No new dates available to process.
* Last Processed Aggregation Date: `{last_agg_date}`
* Last Processed Insertion Date: `{last_ins_date}`

State is managed by Prefect Variables.
"""
            create_markdown_artifact(
                key="argentina-product-insertion-summary",
                markdown=summary_md.strip(),
                description="Summary of the Argentina SEPA product insertion flow run.",
            )
            return  # Exit gracefully if no dates

        logger.info(
            f"Determined {len(dates_to_process)} dates to process via task: "
            f"{dates_to_process[0]} to {dates_to_process[-1]}"
        )
    except Exception as e:
        logger.critical(f"Fatal Error: Failed to determine dates to process using task: {e}", exc_info=True)
        raise  # Fatal: Cannot proceed if date determination fails

    logger.info("Initial setup complete. Proceeding to process dates.")

    # --- Loop, Load/Transform, Insert, Save State, Report ---
    processed_dates_count = 0
    total_records_transformed = 0
    last_processed_date_str = (
        ArgentinaFlowVariableNames.DEFAULT_DATE
    )  # Variable to track the last successfully processed date
    first_processed_date_str = dates_to_process[0]

    for date_str in dates_to_process:
        logger.info(f"--- Processing date: {date_str} ---")
        try:
            # Step 10a: Load daily averages for the current date
            daily_avg_df: DataFrame[SepaAvgPriceProductModel] = await load_daily_averages(
                provider=product_averages_provider, date_str=date_str
            )

            if daily_avg_df.empty:
                # This might indicate an issue if a date was determined but has no data
                logger.warning(
                    f"No daily averages found for determined date {date_str}. Skipping, but check source data."
                )
                continue  # Continue to next date, but log warning

            # --- Deployment Status Check (Moved Before Transformation/Insertion) ---
            # Get unique product IDs for this date
            product_ids_for_date: set[str] = set(daily_avg_df["id_producto"].unique())

            # Business mapping: external 'id_producto' -> TN 'stream_id' (per ARG SEPA spec)
            descriptor_subset = descriptor_df[descriptor_df["source_id"].isin(product_ids_for_date)]
            required_stream_ids: list[str] = descriptor_subset["stream_id"].tolist()

            if not required_stream_ids:
                # This indicates missing mappings in the descriptor for products present in daily data
                logger.error(
                    f"Skipping date {date_str}: No stream IDs found in descriptor for product IDs present in daily data. Product IDs sample: {list(product_ids_for_date)[:10]}..."
                )
                continue  # Skip this date if crucial mappings are missing

            # Check if all required streams are deployed
            try:
                logger.debug(
                    f"Checking deployment status for {len(required_stream_ids)} required streams for date {date_str}..."
                )
                stream_deployment_status: dict[str, bool] = deployment_state.check_multiple_streams(required_stream_ids)

                undeployed_streams = [
                    sid for sid in required_stream_ids if not stream_deployment_status.get(sid, False)
                ]

                if undeployed_streams:
                    error_msg = (
                        f"Halting flow: Date {date_str} cannot be processed because the following required streams "
                        f"are not marked as deployed: {undeployed_streams[:20]}..."
                    )
                    logger.error(error_msg)
                    raise DeploymentCheckError(error_msg)  # Raise exception to stop flow
                else:
                    logger.info(
                        f"All {len(required_stream_ids)} required streams for date {date_str} are deployed. Proceeding."
                    )

            except Exception as e_state:
                error_msg = f"Halting flow: Failed to check deployment status for date {date_str}: {e_state}."
                logger.error(error_msg, exc_info=True)
                raise DeploymentCheckError(error_msg) from e_state  # Re-raise to stop flow
            # --- End Deployment Status Check ---

            # Step 10b: Transform the loaded data (Only if deployment check passed)
            transformed_data: DataFrame[TnDataRowModel] = await transform_product_data(
                daily_avg_df=daily_avg_df,
                descriptor_df=descriptor_df,
                date_str=date_str,  # Pass date_str for logging within the task
            )
            # Ensure data_provider is populated for filtering: StreamLocatorModel requires non-null data_provider
            transformed_data["data_provider"] = transformed_data["data_provider"].fillna(tn_block.current_account)
            num_transformed = len(transformed_data)
            total_records_transformed += num_transformed
            logger.info(f"Transformed {num_transformed} records for date {date_str}.")

            # Step 11: Pre-insertion Existence Check & Insert Transformed Data to TN 
            if not transformed_data.empty:
                
                # === Pre-insertion Stream Existence Check ===
                logger.info(f"Performing pre-insertion existence check for streams in date {date_str} data.")
                required_locators_df = extract_stream_locators(transformed_data)
                required_locators_list: list[tn_client.StreamLocatorInput] = [
                    tn_client.StreamLocatorInput(stream_id=str(row["stream_id"]), data_provider=str(row["data_provider"]))
                    for _, row in required_locators_df.iterrows()
                ]
                required_locators_set = {
                    (str(loc["data_provider"]), str(loc["stream_id"])) 
                    for loc in required_locators_list
                }

                if not required_locators_list:
                    logger.warning(f"No stream locators found in transformed data for {date_str}. Skipping insertion.")
                    continue
                
                try:
                    # Check for missing streams using dedicated task
                    missing_locators = task_batch_check_missing_streams(
                        tn_block=tn_block,
                        required_locators=required_locators_list,
                        max_filter_size=max_filter_size,
                        date_str=date_str
                    )
                    
                    # Check if any streams are missing
                    if missing_locators:
                        missing_list_sample = [
                            (loc.get("data_provider", "N/A"), loc.get("stream_id", "N/A")) 
                            for loc in missing_locators[:20]
                        ]
                        error_msg = (
                            f"Halting flow: Date {date_str} cannot be processed because the following required streams "
                            f"do not exist on TN (or check failed): {missing_list_sample}..."
                        )
                        logger.error(error_msg)
                        raise DeploymentCheckError(error_msg) 
                    else:
                        logger.info(f"All {len(required_locators_set)} required streams exist for date {date_str}. Proceeding to insertion.")
                        
                except Exception as e_exist:
                    error_msg = f"Halting flow: Failed during pre-insertion stream existence check for date {date_str}: {e_exist}."
                    logger.error(error_msg, exc_info=True)
                    raise DeploymentCheckError(error_msg) from e_exist
                # === End Pre-insertion Stream Existence Check ===

                # Proceed to insertion only if the check passed
                logger.info(
                    f"Submitting {num_transformed} transformed records for date {date_str} to TN insertion task..."
                )
                results = task_split_and_insert_records(
                    block=tn_block,
                    records=transformed_data,
                    max_batch_size=batch_size,
                    wait=True, 
                    filter_deployed_streams=False, # Disable internal filter
                )
                if results["failed_records"].empty:
                    logger.info(f"Successfully submitted records for date {date_str} to TN insertion task.")
                else:
                    logger.error(f"Failed to submit records for date {date_str} to TN insertion task.")
                    failed_count = len(results["failed_records"])
                    logger.error(f"Failed count: {failed_count}")
                    reasons_sample = results["failed_reasons"][:10]
                    logger.error(f"Failed reasons sample: {reasons_sample}")
                    raise Exception(f"Failed to submit records for date {date_str} to TN insertion task.")
            else:
                logger.info(f"No records to insert for date {date_str} after transformation, skipping TN insertion.")

            logger.info(f"Processing for date {date_str} complete.")

            # Increment processed count only after successful processing of the date
            processed_dates_count += 1
            last_processed_date_str = date_str  # Update last successful date

            # Update last successful INSERTION date variable *after* processing the date
            await variables.Variable.aset(
                ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE, last_processed_date_str, overwrite=True
            )
            logger.info(
                f"Updated {ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE} to {last_processed_date_str}"
            )

        except DeploymentCheckError:
            # Logged sufficiently above, re-raise to halt the flow run
            logger.error("Deployment check failed. Halting flow run.")
            raise
        except Exception as e:
            # Catch other errors during load, transform, insert for this specific date
            logger.critical(f"Fatal Error processing date {date_str}: {e}", exc_info=True)
            # It might be desirable to continue to the next date depending on error handling strategy
            # For now, re-raise to fail the flow run on any error within the loop
            raise

    logger.info(f"--- Finished processing loop for {processed_dates_count} dates. --- ")
    logger.info(f"Total records transformed across all dates: {total_records_transformed}")

    # Step 14: Final Reporting
    if processed_dates_count > 0:
        summary_md = f"""# Argentina Product Insertion Summary

Successfully processed data and submitted for insertion.

*   **Processed Date Range:** {first_processed_date_str} to {last_processed_date_str} ({processed_dates_count} dates)
*   **Total Records Transformed & Submitted:** {total_records_transformed}
*   **Final Last Processed Insertion Date (Variable Updated):** {last_processed_date_str}
"""
        create_markdown_artifact(
            key="argentina-product-insertion-summary",
            markdown=summary_md.strip(),
            description="Summary of the Argentina SEPA product insertion flow run.",
        )
        logger.info("Created final summary artifact.")
    else:
        # This case should ideally be covered by the early exit, but log just in case
        logger.info(
            "No dates were successfully processed in this run (loop may have been skipped or failed early). Check logs."
        )
