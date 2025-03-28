"""
Integration tests for the Argentina SEPA Product Aggregation Flow.
"""

import asyncio
from unittest.mock import (
    AsyncMock,
    MagicMock,
    patch,
)

import pandas as pd
from prefect_aws import S3Bucket  # type: ignore
import pytest

from tsn_adapters.tasks.argentina.flows.aggregate_products_flow import aggregate_argentina_products_flow
from tsn_adapters.tasks.argentina.models import DynamicPrimitiveSourceModel, ProductAggregationMetadata
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider


# Fixture for mock S3Bucket
@pytest.fixture
def mock_s3_block() -> MagicMock:
    return MagicMock(spec=S3Bucket)


# Fixture for mock ProductAveragesProvider
@pytest.fixture
def mock_provider() -> MagicMock:
    return MagicMock(spec=ProductAveragesProvider)


# Fixture for empty aggregated data DataFrame
@pytest.fixture
def empty_aggregated_df() -> pd.DataFrame:
    columns = list(DynamicPrimitiveSourceModel.to_schema().columns.keys())
    df = pd.DataFrame(columns=columns)
    # Attempt setting dtypes on the empty DataFrame based on the model
    for col, props in DynamicPrimitiveSourceModel.to_schema().columns.items():
        if col in df.columns and props.dtype:
            try:
                df[col] = df[col].astype(props.dtype.type)  # type: ignore
            except Exception:
                df[col] = df[col].astype(object)  # type: ignore
    return df


# Fixture for default metadata
@pytest.fixture
def default_metadata() -> ProductAggregationMetadata:
    return ProductAggregationMetadata()


# Updated integration test to include state saving and artifacts
@pytest.mark.usefixtures("prefect_test_fixture")
def test_aggregate_flow_with_state_and_artifacts(
    mock_s3_block: MagicMock,
    mock_provider: MagicMock,
    empty_aggregated_df: pd.DataFrame,
    default_metadata: ProductAggregationMetadata,
):
    """
    Tests the aggregation flow including state saving within the loop and artifact creation.

    Mocks tasks and helpers, verifies calls to save_aggregation_state and create_markdown_artifact.
    Simulates adding new products.
    """
    dates_to_process = ["2024-01-01", "2024-01-02"]
    # Simulate finding 1 new product on the first date, 2 on the second
    product_counts_after = [1, 3]

    # --- Mock Setup ---
    # Mock ProductAveragesProvider instantiation
    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.ProductAveragesProvider",
            return_value=mock_provider,
        ) as mock_provider_init,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.load_aggregation_state", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.determine_date_range_to_process"
        ) as mock_determine_range,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.process_single_date_products"
        ) as mock_process_single_date,
        # Mock the save state task
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.save_aggregation_state", new_callable=AsyncMock
        ) as mock_save_state,
        # Mock the artifact creation function - patch where it's used in the flow module
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.create_markdown_artifact",
            new_callable=MagicMock,
        ) as mock_create_artifact,
    ):
        # Configure mock return values
        mock_load_state.return_value = (empty_aggregated_df.copy(), default_metadata.model_copy(deep=True))
        mock_determine_range.return_value = dates_to_process

        # Simulate process_single_date_products returning a modified DataFrame
        # We need a mutable object to track the state across calls
        current_df_state = {"df": empty_aggregated_df.copy()}
        current_count_state = {"count": 0}

        def mock_process_side_effect(
            date_to_process: str,
            current_aggregated_data: pd.DataFrame,
            product_averages_provider: ProductAveragesProvider,
        ) -> pd.DataFrame:
            if date_to_process == "2024-01-01":
                new_count = product_counts_after[0]
                # Simulate adding rows - actual content doesn't matter for this mock
                new_rows = pd.DataFrame([{"source_id": f"prod_{i}"} for i in range(new_count)])
            elif date_to_process == "2024-01-02":
                new_count = product_counts_after[1]
                new_rows = pd.DataFrame([{"source_id": f"prod_{i}"} for i in range(new_count)])
            else:
                new_rows = pd.DataFrame()
                new_count = len(current_aggregated_data)

            # Update the state for the next call
            current_df_state["df"] = new_rows  # Simplified: just return the expected final shape
            current_count_state["count"] = new_count
            return current_df_state["df"]

        mock_process_single_date.side_effect = mock_process_side_effect

        # --- Flow Execution ---
        asyncio.run(aggregate_argentina_products_flow(s3_block=mock_s3_block, force_reprocess=False))

        # --- Assertions ---
        # 1. Provider Instantiation
        mock_provider_init.assert_called_once_with(s3_block=mock_s3_block)

        # 2. Load State Call
        mock_load_state.assert_awaited_once()
        # Check args if necessary, e.g., mock_load_state.assert_awaited_once_with(s3_block=mock_s3_block, ...) # wait_for needs care

        # 3. Determine Date Range Call
        mock_determine_range.assert_called_once()
        # Check args if necessary

        # 4. Process Single Date Calls
        assert mock_process_single_date.call_count == len(dates_to_process)

        # 5. Save State Calls (Inside the loop)
        assert mock_save_state.await_count == len(dates_to_process)

        # Verify the arguments of the LAST call match the final expected state
        final_date = dates_to_process[-1]
        final_count = product_counts_after[-1]
        expected_final_metadata = default_metadata.model_copy(deep=True)
        expected_final_metadata.last_processed_date = final_date
        expected_final_metadata.total_products_count = final_count
        # Reconstruct the final expected DataFrame based on the mock logic
        expected_final_df = pd.DataFrame([{"source_id": f"prod_{j}"} for j in range(final_count)])

        last_call_args = mock_save_state.await_args_list[-1].kwargs
        assert last_call_args["s3_block"] == mock_s3_block
        assert last_call_args["metadata"] == expected_final_metadata
        pd.testing.assert_frame_equal(last_call_args["aggregated_data"], expected_final_df, check_dtype=False)

        # 6. Create Artifact Call (After the loop)
        mock_create_artifact.assert_called_once()
        # Add assertion to satisfy linter
        assert mock_create_artifact.call_args is not None
        artifact_call_args = mock_create_artifact.call_args.kwargs
        assert artifact_call_args["key"] == "argentina-product-aggregation-summary"

        assert "**Processed Date Range:** 2024-01-01 to 2024-01-02" in artifact_call_args["markdown"]
        assert "**New Products Added:** 3" in artifact_call_args["markdown"]  # 1 + 2
        assert f"**Total Unique Products:** {product_counts_after[-1]}" in artifact_call_args["markdown"]
        assert "**Force Reprocess Flag:** False" in artifact_call_args["markdown"]


@pytest.mark.usefixtures("prefect_test_fixture")
def test_aggregate_flow_no_dates_to_process_with_artifact(
    mock_s3_block: MagicMock,
    mock_provider: MagicMock,
    empty_aggregated_df: pd.DataFrame,
    default_metadata: ProductAggregationMetadata,
):
    """
    Tests the flow behavior when determine_date_range_to_process returns an empty list.
    """
    # --- Mock Setup ---
    with (
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.ProductAveragesProvider",
            return_value=mock_provider,
        ) as mock_provider_init,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.load_aggregation_state", new_callable=AsyncMock
        ) as mock_load_state,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.determine_date_range_to_process"
        ) as mock_determine_range,
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.process_single_date_products"
        ) as mock_process_single_date,
        # Mock save state (should not be called)
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.save_aggregation_state", new_callable=AsyncMock
        ) as mock_save_state,
        # Mock artifact creation - patch where it's used in the flow module
        patch(
            "tsn_adapters.tasks.argentina.flows.aggregate_products_flow.create_markdown_artifact",
            new_callable=MagicMock,
        ) as mock_create_artifact,
    ):
        mock_load_state.return_value = (empty_aggregated_df, default_metadata)
        mock_determine_range.return_value = []  # Return empty list

        # --- Flow Execution ---
        asyncio.run(aggregate_argentina_products_flow(s3_block=mock_s3_block, force_reprocess=False))

        # --- Assertions ---
        mock_provider_init.assert_called_once()
        mock_load_state.assert_awaited_once()
        mock_determine_range.assert_called_once()

        # Assert that the processing loop and save state were NOT entered
        mock_process_single_date.assert_not_called()
        mock_save_state.assert_not_called()

        # Assert that the artifact was created with the 'no dates' message
        mock_create_artifact.assert_called_once()
        # Add assertion to satisfy linter
        assert mock_create_artifact.call_args is not None
        artifact_call_args = mock_create_artifact.call_args.kwargs
        assert artifact_call_args["key"] == "argentina-product-aggregation-summary"
        assert "No new dates found to process." in artifact_call_args["markdown"]
