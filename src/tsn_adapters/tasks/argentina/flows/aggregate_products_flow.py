"""
Prefect flow for aggregating Argentina SEPA product data.

This flow loads the current aggregation state, determines which dates need processing,
iterates through those dates calling the processing logic, and placeholders for
saving state and reporting.
"""

from pandera.typing import DataFrame
from prefect import flow, get_run_logger

# Import artifact creation function
from prefect.artifacts import create_markdown_artifact
from prefect_aws import S3Bucket  # type: ignore

# Add back model imports
from tsn_adapters.tasks.argentina.models import (
    DynamicPrimitiveSourceModel,
)
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks import (
    determine_date_range_to_process,
    load_aggregation_state,  # Imported for later use
    save_aggregation_state,  # Import save_aggregation_state for Step 7
)

# Import the RENAMED helper function directly, not as a task
from tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks import process_single_date_products


@flow(name="Aggregate Argentina SEPA Products")
async def aggregate_argentina_products_flow(
    s3_block: S3Bucket,
    force_reprocess: bool = False,
):
    """
    Aggregates Argentina SEPA product data, tracking first appearance dates.

    Loads existing aggregated data and metadata from S3, determines the range
    of dates to process based on available daily data and the last processed date
    (or forces reprocessing), processes each date to find new products,
    and logs placeholders for state updates and reporting.

    Args:
        s3_block: Configured Prefect S3Bucket block for accessing S3.
        force_reprocess: If True, ignores the last processed date and reprocesses all
                         available daily data. Defaults to False.
    """
    logger = get_run_logger()
    logger.info(f"Starting Argentina product aggregation flow. Force reprocess: {force_reprocess}")

    # 1. Instantiate Provider
    try:
        product_averages_provider = ProductAveragesProvider(s3_block=s3_block)
        logger.info("ProductAveragesProvider instantiated.")
    except Exception as e:
        logger.error(f"Failed to instantiate ProductAveragesProvider: {e}", exc_info=True)
        raise  # Cannot proceed without the provider

    # 2. Load Initial State
    try:
        # Use deroutine and ignore type check errors for this specific assignment
        aggregated_data, metadata = await load_aggregation_state(
            s3_block=s3_block,
            wait_for=[product_averages_provider],
            # This ensures the correct type is returned
            return_state=False,
        )

        logger.info(
            f"Initial state loaded. Last processed: {metadata.last_processed_date}, Total products: {metadata.total_products_count}"
        )
    except Exception as e:
        logger.error(f"Failed to load initial aggregation state: {e}", exc_info=True)
        raise  # Cannot proceed without initial state

    # 3. Determine Date Range
    try:
        # Pass provider instance, not just the block
        dates_to_process = determine_date_range_to_process(
            product_averages_provider=product_averages_provider,
            metadata=metadata,
            force_reprocess=force_reprocess,
            # wait_for=[aggregated_data, metadata] # wait for state load
        )
        if not dates_to_process:
            logger.info("No new dates to process. Flow finished early.")
            # Create artifact even if no dates processed
            create_markdown_artifact(
                key="argentina-product-aggregation-summary",
                markdown="# Argentina Product Aggregation Summary\n\nNo new dates found to process.",
                description="Summary of the Argentina SEPA product aggregation flow run.",
            )
            return
        logger.info(
            f"Determined {len(dates_to_process)} dates to process: {dates_to_process[0]} to {dates_to_process[-1]}"
        )
    except Exception as e:
        logger.error(f"Failed to determine date range to process: {e}", exc_info=True)
        raise  # Cannot proceed if date range fails

    # 4. Initialize Reporting Counters (Placeholder)
    new_products_added_total = 0
    processed_dates_count = 0
    logger.debug("Reporting counters initialized.")

    # 5. Loop Through Dates
    logger.info(f"Starting processing loop for {len(dates_to_process)} dates...")
    # Start with the correctly typed aggregated_data from state load
    processed_aggregated_data: DataFrame[DynamicPrimitiveSourceModel] = aggregated_data
    for date_str in dates_to_process:
        logger.debug(f"Processing date: {date_str}...")
        try:
            # Store pre-processing count for comparison
            count_before = len(processed_aggregated_data)

            # Call the RENAMED helper function (NOT as a task)
            processed_aggregated_data = process_single_date_products(
                date_to_process=date_str,
                current_aggregated_data=processed_aggregated_data,  # Should be correctly typed now
                product_averages_provider=product_averages_provider,
            )
            count_after = len(processed_aggregated_data)
            new_this_date = count_after - count_before
            new_products_added_total += new_this_date
            processed_dates_count += 1

            # Update Metadata
            metadata.last_processed_date = date_str
            metadata.total_products_count = count_after
            logger.info(
                f"Processed date {date_str}. Found {new_this_date} new products. Total products: {count_after}."
            )

            # Save state after processing each date
            await save_aggregation_state(
                s3_block=s3_block,
                aggregated_data=processed_aggregated_data,
                metadata=metadata,
                # No explicit wait_for needed here as loop is sequential
            )
            logger.debug(f"Saved intermediate state for date {date_str}.")

            # Placeholders removed as logic is implemented above

        except Exception as e:
            logger.error(f"Failed to process date {date_str}: {e}", exc_info=True)
            # Decide on error strategy: continue, break, or raise?
            # For now, log error and continue to next date
            continue

    # 6. Final State Save - Removed (Done within the loop now)
    logger.info("Finished processing all dates.")

    # 7. Reporting
    final_total_products = metadata.total_products_count
    logger.info(
        f"Flow finished. Processed {processed_dates_count} dates. Added {new_products_added_total} new products in total. Final product count: {final_total_products}."
    )
    # Create final summary artifact
    summary_md = f"""# Argentina Product Aggregation Summary

*   **Processed Date Range:** {dates_to_process[0]} to {dates_to_process[-1]} ({processed_dates_count} dates)
*   **New Products Added:** {new_products_added_total}
*   **Total Unique Products:** {final_total_products}
*   **Force Reprocess Flag:** {force_reprocess}
"""
    create_markdown_artifact(
        key="argentina-product-aggregation-summary",
        markdown=summary_md.strip(),
        description="Summary of the Argentina SEPA product aggregation flow run.",
    )
    logger.info("Created summary artifact.")
