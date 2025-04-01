"""
Prefect flow for inserting Argentina SEPA product data into TrufNetwork.

This flow reads daily average product prices, maps them to TN streams,
transforms the data, inserts it into TN, and manages state.
"""

from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
from prefect_aws import S3Bucket

from tsn_adapters.blocks.primitive_source_descriptor import S3SourceDescriptor
from tsn_adapters.blocks.tn_access import TNAccessBlock, task_split_and_insert_records
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks import (
    determine_dates_to_insert,
    load_daily_averages,
    load_product_descriptor,
    load_insertion_metadata,
    save_insertion_metadata,
    transform_product_data,
)


@flow(name="Insert Argentina SEPA Products to TN")
async def insert_argentina_products_flow(
    s3_block: S3Bucket,
    tn_block: TNAccessBlock,
    descriptor_block: S3SourceDescriptor,
    batch_size: int = 10000,
):
    """
    Inserts pre-calculated Argentina SEPA daily average product prices into TN streams.

    Reads daily averages, maps products using a descriptor, transforms data,
    inserts using batching, and manages state via a shared metadata file.

    Args:
        s3_block: Prefect S3Bucket block for accessing daily averages and state file.
        tn_block: Prefect TNAccessBlock block for TN insertion.
        descriptor_block: Prefect S3SourceDescriptor block for reading the product descriptor.
        batch_size: Size of record batches for the TN insertion task.
    """
    logger = get_run_logger()
    logger.info(f"Starting Argentina product insertion flow. Batch size: {batch_size}")

    # Define state file path (consistent with spec)
    state_base_path = "aggregated"
    # Note: aggregate_products_tasks uses 'argentina_products_metadata.json'
    # Spec uses 'argentina_product_state.json'.
    state_file_path = f"{state_base_path}/argentina_product_state.json"

    # 1. Instantiate Provider
    try:
        product_averages_provider = ProductAveragesProvider(s3_block=s3_block)
        logger.info("ProductAveragesProvider instantiated.")
    except Exception as e:
        logger.critical(f"Fatal Error: Failed to instantiate ProductAveragesProvider: {e}", exc_info=True)
        raise  # Fatal: Cannot proceed without the provider

    # 2. Load Shared State (using new task)
    try:
        # Use the new task specific for loading insertion metadata
        metadata = await load_insertion_metadata(
            s3_block=s3_block,
            state_file_path=state_file_path,
        )
        logger.info(f"Loaded shared state metadata: {metadata}")
    except Exception as e:
        logger.critical(f"Fatal Error: Failed to load shared state: {e}", exc_info=True)
        raise  # Fatal: Cannot proceed without state

    # 3. Load Product Descriptor
    try:
        descriptor_df = await load_product_descriptor(descriptor_block=descriptor_block)
        logger.info(f"Successfully loaded product descriptor with {len(descriptor_df)} entries.")
    except Exception:
        # load_product_descriptor already logs critically and raises DescriptorError
        logger.critical("Fatal Error: Could not load product descriptor. Flow cannot proceed.")
        raise  # Fatal: Cannot proceed without descriptor

    # 4. Determine Dates to Process
    try:
        dates_to_process = await determine_dates_to_insert(
            metadata=metadata,
            provider=product_averages_provider
        )
        if not dates_to_process:
            logger.info("No new dates to process based on current state. Flow finished.")
            # TODO: Add artifact creation here later (Step 13)
            # Create artifact even if no dates processed
            summary_md = """# Argentina Product Insertion Summary

No new dates found to process based on the current state.

*   **Last Processed Insertion Date:** `{metadata.last_insertion_processed_date}`
*   **Last Product Deployment Date:** `{metadata.last_product_deployment_date}`
"""
            create_markdown_artifact(
                key="argentina-product-insertion-summary",
                markdown=summary_md.strip(),
                description="Summary of the Argentina SEPA product insertion flow run.",
            )
            return  # Exit gracefully if no dates

        logger.info(
            f"Determined {len(dates_to_process)} dates to process: " f"{dates_to_process[0]} to {dates_to_process[-1]}"
        )
    except Exception as e:
        logger.critical(f"Fatal Error: Failed to determine dates to process: {e}", exc_info=True)
        raise  # Fatal: Cannot proceed if date determination fails

    logger.info("Initial setup complete. Proceeding to process dates.")

    # --- Implement Steps 10-13 (Loop, Load/Transform, Insert, Save State, Report) ---
    processed_dates_count = 0
    total_records_transformed = 0

    for date_str in dates_to_process:
        logger.info(f"--- Processing date: {date_str} ---")
        try:
            # Step 10a: Load daily averages for the current date
            daily_avg_df = await load_daily_averages(
                provider=product_averages_provider,
                date_str=date_str
            )

            # Step 10b: Transform the loaded data
            transformed_data = await transform_product_data(
                daily_avg_df=daily_avg_df,
                descriptor_df=descriptor_df,
                date_str=date_str,  # Pass date_str for logging within the task
                return_state=False,  # this makes the typing work better
            )

            num_transformed = len(transformed_data)
            total_records_transformed += num_transformed
            logger.info(f"Transformed {num_transformed} records for date {date_str}.")

            # Step 11: Insert Transformed Data to TN
            if not transformed_data.empty:
                logger.info(
                    f"Submitting {num_transformed} transformed records for date {date_str} to TN insertion task..."
                )
                # Use the imported task directly, passing the block instance and other params
                task_split_and_insert_records(
                    block=tn_block,
                    records=transformed_data,
                    batch_size=batch_size,
                )
                logger.info(f"Successfully submitted records for date {date_str} to TN insertion task.")
            else:
                logger.info(f"No records to insert for date {date_str}, skipping TN insertion.")

            # Step 12: Update and Save State
            # Update metadata only after successful processing (including insertion or skip)
            metadata.last_insertion_processed_date = date_str
            logger.info(f"Updating metadata: last_insertion_processed_date set to {date_str}.")

            # Save the updated state back to S3
            await save_insertion_metadata(
                s3_block=s3_block,
                metadata=metadata,
                state_file_path=state_file_path,
            )
            logger.info(f"Successfully saved updated state for date {date_str}.")

            # Increment processed count only after successful save
            processed_dates_count += 1

        except Exception as e:
            # This implements part of Step 13 (Error Handling)
            # If any task within the loop fails (load, transform, insert, save state), log critically and re-raise.
            # This ensures the flow run fails and state is NOT updated for the problematic date.
            logger.critical(f"Fatal Error processing date {date_str}: {e}", exc_info=True)
            raise  # Re-raise to fail the flow run

    logger.info(f"--- Finished processing loop for {processed_dates_count} dates. --- ")
    logger.info(f"Total records transformed across all dates: {total_records_transformed}")

    # Step 14: Final Reporting
    if processed_dates_count > 0:
        first_processed = dates_to_process[0]
        last_processed = metadata.last_insertion_processed_date # Final date successfully processed and saved
        summary_md = f"""# Argentina Product Insertion Summary

Successfully processed data and submitted for insertion.

*   **Processed Date Range:** {first_processed} to {last_processed} ({processed_dates_count} dates)
*   **Total Records Transformed & Submitted:** {total_records_transformed}
*   **Final Last Processed Insertion Date:** {last_processed}
"""
        create_markdown_artifact(
            key="argentina-product-insertion-summary",
            markdown=summary_md.strip(),
            description="Summary of the Argentina SEPA product insertion flow run.",
        )
        logger.info("Created final summary artifact.")
    else:
        # This case should ideally be covered by the early exit, but log just in case
        logger.info("No dates were processed in this run (loop did not execute). No final artifact created.")
