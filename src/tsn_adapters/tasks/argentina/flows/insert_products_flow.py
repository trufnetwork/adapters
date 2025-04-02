"""
Prefect flow for inserting Argentina SEPA product data into TrufNetwork.

This flow reads daily average product prices, maps them to TN streams,
transforms the data, inserts it into TN, and manages state.
"""

from pandera.typing import DataFrame
from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
import prefect.variables as variables  # Import prefect variables
from prefect_aws import S3Bucket

from tsn_adapters.blocks.deployment_state import DeploymentStateBlock
from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel, PrimitiveSourcesDescriptorBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock, task_split_and_insert_records
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.tasks.argentina.config import ArgentinaFlowVariableNames  # Import config
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks import (
    determine_dates_to_insert, # Now using the task
    load_daily_averages,
    transform_product_data,
)


# Custom Exception for Deployment Check Failures
class DeploymentCheckError(Exception):
    """Raised when required streams for a date are not deployed."""

    pass


@flow(name="Insert Argentina SEPA Products to TN")
async def insert_argentina_products_flow(
    s3_block: S3Bucket,
    tn_block: TNAccessBlock,
    descriptor_block: PrimitiveSourcesDescriptorBlock,
    deployment_state: DeploymentStateBlock,
    batch_size: int = 10000,
):
    """
    Inserts pre-calculated Argentina SEPA daily average product prices into TN streams.

    Reads daily averages, maps products using a descriptor, transforms data,
    inserts using batching, and manages state via Prefect Variables.

    Args:
        s3_block: Prefect S3Bucket block for accessing daily averages and state file.
        tn_block: Prefect TNAccessBlock block for TN insertion.
        descriptor_block: Prefect PrimitiveSourcesDescriptorBlock for reading the product descriptor.
        deployment_state: Prefect DeploymentStateBlock for checking stream deployment status.
        batch_size: Size of record batches for the TN insertion task.
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
            last_agg_date = variables.get(ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE, default="N/A")
            last_ins_date = variables.get(ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE, default="N/A")
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

    # --- Implement Steps 10-13 (Loop, Load/Transform, Insert, Save State, Report) ---
    processed_dates_count = 0
    total_records_transformed = 0
    last_processed_date_str = "N/A"  # Variable to track the last successfully processed date
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
                logger.warning(f"No daily averages found for determined date {date_str}. Skipping, but check source data.")
                continue # Continue to next date, but log warning

            # --- Deployment Status Check (Moved Before Transformation/Insertion) ---
            # Get unique product IDs for this date
            product_ids_for_date: set[str] = set(daily_avg_df["id_producto"].unique())

            # Map product IDs to required stream IDs using the descriptor
            descriptor_subset = descriptor_df[descriptor_df["source_id"].isin(product_ids_for_date)]
            required_stream_ids: list[str] = descriptor_subset["stream_id"].tolist()

            if not required_stream_ids:
                # This indicates missing mappings in the descriptor for products present in daily data
                logger.error(
                    f"Skipping date {date_str}: No stream IDs found in descriptor for product IDs present in daily data. Product IDs sample: {list(product_ids_for_date)[:10]}..."
                )
                continue # Skip this date if crucial mappings are missing

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
                raise DeploymentCheckError(error_msg) from e_state # Re-raise to stop flow
            # --- End Deployment Status Check ---

            # Step 10b: Transform the loaded data (Only if deployment check passed)
            transformed_data: DataFrame[TnDataRowModel] = await transform_product_data(
                daily_avg_df=daily_avg_df,
                descriptor_df=descriptor_df,
                date_str=date_str,  # Pass date_str for logging within the task
            )

            num_transformed = len(transformed_data)
            total_records_transformed += num_transformed
            logger.info(f"Transformed {num_transformed} records for date {date_str}.")

            # Step 11: Insert Transformed Data to TN
            if not transformed_data.empty:
                logger.info(
                    f"Submitting {num_transformed} transformed records for date {date_str} to TN insertion task..."
                )
                # No need to await here if the task runs concurrently
                task_split_and_insert_records(
                    block=tn_block,
                    records=transformed_data,
                    batch_size=batch_size,
                )
                logger.info(f"Successfully submitted records for date {date_str} to TN insertion task.")
            else:
                logger.info(f"No records to insert for date {date_str} after transformation, skipping TN insertion.")

            logger.info(f"Processing for date {date_str} complete.")

            # Increment processed count only after successful processing of the date
            processed_dates_count += 1
            last_processed_date_str = date_str  # Update last successful date

            # Update last successful INSERTION date variable *after* processing the date
            variables.Variable.set(ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE, last_processed_date_str)
            logger.info(f"Updated {ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE} to {last_processed_date_str}")

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
        logger.info("No dates were successfully processed in this run (loop may have been skipped or failed early). Check logs.")
