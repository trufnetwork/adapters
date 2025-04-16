"""
Prefect flow for aggregating Argentina SEPA product data.

This flow loads the current aggregation state, determines which dates need processing,
iterates through those dates calling the processing logic, and updates flow state variables and reporting.
"""

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow, get_run_logger
from prefect.artifacts import create_markdown_artifact
import prefect.variables as variables  # Import prefect variables
from prefect_aws import S3Bucket

from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel, WritableSourceDescriptorBlock
from tsn_adapters.tasks.argentina.config import ArgentinaFlowVariableNames  # Import config
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks.aggregate_products_tasks import (
    determine_aggregation_dates,
    process_single_date_products,
)


@flow(name="Aggregate Argentina SEPA Products")
async def aggregate_argentina_products_flow(
    s3_block: S3Bucket,
    descriptor_block: WritableSourceDescriptorBlock,
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
        descriptor_block: Configured WritableSourceDescriptorBlock for upserting products.
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

    # Load Initial Descriptor State
    logger.info("Descriptor state will be loaded from the provided block.")

    # 3. Determine Date Range
    try:
        task_result = determine_aggregation_dates(
            product_averages_provider=product_averages_provider,
            force_reprocess=force_reprocess,
        )
        # Unpack the results from the task
        dates_to_process, last_agg_date_used, last_prep_date_used = task_result

        # Check if any dates remain after filtering by the task
        if not dates_to_process:
            # Use the context dates returned by the task
            logger.info(
                f"Task determine_aggregation_dates returned no new dates to process between {last_agg_date_used} ({ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE}) and {last_prep_date_used} ({ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE}). Flow finished early."
            )
            # Create artifact even if no dates processed
            create_markdown_artifact(
                key="argentina-product-aggregation-summary",
                markdown=f"# Argentina Product Aggregation Summary\\n\\nNo new dates found to process between {last_agg_date_used} ({ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE}) and {last_prep_date_used} ({ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE}).",
                description="Summary of the Argentina SEPA product aggregation flow run.",
            )
            return

        # Log the result from the task
        logger.info(
            f"Task determine_aggregation_dates determined {len(dates_to_process)} dates to process: {dates_to_process[0]} to {dates_to_process[-1]}"
        )

    except Exception as e:
        logger.error(f"Failed during date determination task: {e}", exc_info=True)
        raise  # Cannot proceed if date range fails

    # 4. Initialize Reporting Counters
    new_products_added_total = 0
    processed_dates_count = 0
    final_total_products = 0  # Initialize counter here
    logger.debug("Reporting counters initialized.")

    # 5. Loop Through Dates
    logger.info(f"Starting processing loop for {len(dates_to_process)} dates...")
    # Load the initial/current full descriptor once before the loop
    try:
        current_descriptor_df: DataFrame[PrimitiveSourceDataModel] = descriptor_block.get_descriptor()
        logger.info(f"Loaded initial descriptor with {len(current_descriptor_df)} entries.")
    except Exception as e_desc:
        logger.error(f"Failed to load initial descriptor: {e_desc}", exc_info=True)
        raise  # Cannot proceed without descriptor

    # We pass the full current descriptor state to the task each time for comparison.
    # The task now returns ONLY the new products.
    processed_descriptor_state: DataFrame[PrimitiveSourceDataModel] = current_descriptor_df

    # avoid unbound variable error
    date_str = ""
    for date_str in dates_to_process:
        logger.debug(f"Processing date: {date_str}...")
        try:
            # Pass the current full descriptor state for comparison
            new_products_df: DataFrame[PrimitiveSourceDataModel] = await process_single_date_products(
                date_to_process=date_str,
                current_aggregated_data=processed_descriptor_state,  # Pass current full state
                product_averages_provider=product_averages_provider,
            )

            # Upsert new products if any were found
            if not new_products_df.empty:
                new_this_date = len(new_products_df)
                logger.info(f"Found {new_this_date} new products for date {date_str}. Upserting...")
                try:
                    descriptor_block.upsert_sources(new_products_df)
                    logger.debug(f"Upsert successful for date {date_str}.")
                    # Update the locally tracked descriptor state *after* successful upsert
                    # This ensures the *next* iteration compares against the newly added products
                    processed_descriptor_state = pd.concat(  # type: ignore
                        [processed_descriptor_state, new_products_df], ignore_index=True
                    ).drop_duplicates(subset=["stream_id"], keep="last")  # Keep last in case of re-run/overlap

                    # Update reporting counters *after* successful upsert
                    new_products_added_total += new_this_date
                    final_total_products = len(processed_descriptor_state)  # Update total count

                except Exception as e_upsert:
                    logger.error(f"Failed to upsert new products for date {date_str}: {e_upsert}", exc_info=True)
                    # Halt flow on upsert failure to prevent inconsistent state
                    raise ValueError(f"Halting flow due to upsert failure for date {date_str}.") from e_upsert

            else:
                logger.info(f"No new products found for date {date_str}.")
                new_this_date = 0

            processed_dates_count += 1

            logger.info(
                f"Processed date {date_str}. Found {new_this_date} new products. Total products in descriptor: {final_total_products}."
            )

        except Exception as e:
            # Catch errors from process_single_date_products or other issues
            logger.error(f"Failed to process date {date_str}: {e}", exc_info=True)
            # Halt flow if a date fails, otherwise subsequent dates might rely on its output
            raise ValueError(f"Halting flow due to failure processing date {date_str}.") from e

    logger.info("Finished processing all dates.")

    # --- Set Prefect Variable on Success (if any dates were processed) ---
    if processed_dates_count > 0:
        # 'date_str' will hold the last successfully processed date from the loop
        last_processed_date = date_str
        try:
            # Set Aggregation Date
            await variables.Variable.aset(
                ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE, last_processed_date, overwrite=True
            )
            logger.info(
                f"Successfully set {ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE} to {last_processed_date}"
            )

        except Exception as e:
            logger.error(
                f"Failed to set {ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE} for date {last_processed_date}: {e}",
                exc_info=True,
            )
    else:
        logger.info("No dates were processed successfully, skipping Prefect Variable update.")
    # --- End Prefect Variable Setting ---

    # 6. Reporting
    logger.info(
        f"Flow finished. Processed {processed_dates_count} dates. Added {new_products_added_total} new products in total. Final product count: {final_total_products}."
    )
    # Create final summary artifact
    # Determine date range safely
    processed_date_range_str = "N/A"
    if dates_to_process and processed_dates_count > 0:
        processed_date_range_str = (
            f"{dates_to_process[0]} to {date_str}"  # date_str holds the last processed date from the loop
        )
    elif dates_to_process:
        processed_date_range_str = f"{dates_to_process[0]} to {dates_to_process[-1]} (0 dates processed successfully)"
    elif processed_dates_count == 0:
        processed_date_range_str = "No dates processed"

    summary_md = f"""# Argentina Product Aggregation Summary

*   **Processed Date Range:** {processed_date_range_str} ({processed_dates_count} dates processed)
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
