"""
Unit tests for Argentina SEPA date processing tasks.
"""

from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
from pandera.typing import DataFrame
import pytest

from tsn_adapters.tasks.argentina.models import ArgentinaProductStateMetadata
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks.date_processing_tasks import (
    DailyAverageLoadingError,
    determine_dates_to_insert,
    load_daily_averages,
)


@pytest.fixture
def mock_product_averages_provider() -> MagicMock:
    """Fixture for a mocked ProductAveragesProvider."""
    mock_provider = MagicMock(spec=ProductAveragesProvider)
    return mock_provider


# --- Test Cases for determine_dates_to_process ---

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "available_dates, metadata_dict, expected_dates",
    [
        # Scenario 1: Initial run (defaults in metadata)
        (
            ["2023-01-01", "2023-01-02", "2023-01-03"],
            {"last_insertion_processed_date": "1970-01-01", "last_product_deployment_date": "2023-01-03"},
            ["2023-01-01", "2023-01-02", "2023-01-03"],
        ),
        # Scenario 2: Resumed run
        (
            ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
            {"last_insertion_processed_date": "2023-01-02", "last_product_deployment_date": "2023-01-04"},
            ["2023-01-03", "2023-01-04"],
        ),
        # Scenario 3: Deployment date restriction
        (
            ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
            {"last_insertion_processed_date": "2023-01-01", "last_product_deployment_date": "2023-01-03"},
            ["2023-01-02", "2023-01-03"],
        ),
        # Scenario 4: No new dates after last insertion
        (
            ["2023-01-01", "2023-01-02"],
            {"last_insertion_processed_date": "2023-01-02", "last_product_deployment_date": "2023-01-03"},
            [],
        ),
        # Scenario 5: No dates within deployment window (deployment before insertion)
        (
            ["2023-01-01", "2023-01-02", "2023-01-03"],
            {"last_insertion_processed_date": "2023-01-02", "last_product_deployment_date": "2023-01-01"},
            [], # Expect empty list as deployment date < insertion date
        ),
        # Scenario 6: No available dates from provider
        (
            [],
            {"last_insertion_processed_date": "2023-01-01", "last_product_deployment_date": "2023-01-03"},
            [],
        ),
        # Scenario: Available dates contain invalid formats (should be skipped)
        # note: currently doesn't work like this. invalid dates are fatal for now, to be simplified
        # (
        #     ["2023-01-01", "invalid-date", "2023-01-03"],
        #     {"last_insertion_processed_date": "1970-01-01", "last_product_deployment_date": "2023-01-04"},
        #     ["2023-01-01", "2023-01-03"],
        # ),
        # Scenario 7: Dates are exactly on the boundaries
        (
            ["2023-01-01", "2023-01-02", "2023-01-03"],
            {"last_insertion_processed_date": "2023-01-01", "last_product_deployment_date": "2023-01-03"},
            ["2023-01-02", "2023-01-03"], # Should include end boundary, exclude start boundary
        ),
    ],
)
async def test_determine_dates_to_process_scenarios(
    mock_product_averages_provider: MagicMock,
    available_dates: list[str],
    metadata_dict: dict[str, str],
    expected_dates: list[str],
    caplog: pytest.LogCaptureFixture,
):
    """Test determine_dates_to_process with various scenarios."""
    # Arrange
    mock_product_averages_provider.list_available_keys.return_value = available_dates
    # Create metadata instance, handling potential validation errors during test setup if needed
    try:
        metadata = ArgentinaProductStateMetadata(**metadata_dict)
    except ValueError as e:
        # If metadata itself is invalid in the test parameters, fail the test setup
        pytest.fail(f"Invalid metadata provided in test parameters: {metadata_dict} - {e}")

    # Act
    result_dates = await determine_dates_to_insert.fn(metadata=metadata, provider=mock_product_averages_provider)

    # Assert
    mock_product_averages_provider.list_available_keys.assert_called_once()
    assert result_dates == expected_dates

    # Optional: Check logs for specific messages in certain scenarios
    if "invalid-date" in available_dates:
        assert "Skipping available key 'invalid-date'" in caplog.text
    if metadata_dict.get("last_insertion_processed_date") == "invalid":
        assert "Invalid last_insertion_processed_date 'invalid'" in caplog.text
    # Check log for Scenario 5: deployment date < insertion date
    try:
        insertion_dt = datetime.strptime(metadata_dict.get("last_insertion_processed_date", "1970-01-01"), "%Y-%m-%d").date()
        deployment_dt = datetime.strptime(metadata_dict.get("last_product_deployment_date", "1970-01-01"), "%Y-%m-%d").date()
        if deployment_dt < insertion_dt:
            assert "last_product_deployment_date" in caplog.text and "is before" in caplog.text and "last_insertion_processed_date" in caplog.text
    except ValueError:
        pass # Ignore if dates in metadata_dict are invalid for this specific check


@pytest.mark.asyncio
async def test_determine_dates_to_process_provider_error(mock_product_averages_provider):
    """Test that an exception from the provider's list_available_keys is propagated."""
    # Arrange
    test_exception = OSError("Simulated S3 connection error")
    mock_product_averages_provider.list_available_keys.side_effect = test_exception
    metadata = ArgentinaProductStateMetadata() # Default metadata

    # Act & Assert
    with pytest.raises(IOError) as exc_info:
        await determine_dates_to_insert.fn(metadata=metadata, provider=mock_product_averages_provider)

    # Check that the original exception is raised
    assert exc_info.value is test_exception
    mock_product_averages_provider.list_available_keys.assert_called_once()


# --- Test Cases for load_daily_averages ---

# Sample valid daily averages data
SAMPLE_DAILY_AVG_DATA = {
    'id_producto': ['p1', 'p2'],
    'productos_descripcion': ['Product 1', 'Product 2'],
    'productos_precio_lista_avg': [100.5, 200.0],
    'date': ['2023-01-10'] * 2,
}


@pytest.mark.asyncio
async def test_load_daily_averages_success(mock_product_averages_provider):
    """Test successful loading of daily averages."""
    # Arrange
    test_date = "2023-01-10"
    valid_df = DataFrame[SepaAvgPriceProductModel](pd.DataFrame(SAMPLE_DAILY_AVG_DATA))
    mock_product_averages_provider.get_product_averages_for.return_value = valid_df

    # Act
    result_df = await load_daily_averages.fn(provider=mock_product_averages_provider, date_str=test_date)

    # Assert
    mock_product_averages_provider.get_product_averages_for.assert_called_once_with(test_date)
    pd.testing.assert_frame_equal(result_df, valid_df)


@pytest.mark.asyncio
async def test_load_daily_averages_provider_error(mock_product_averages_provider, caplog):
    """Test that an exception from the provider is caught and re-raised as DailyAverageLoadingError."""
    # Arrange
    test_date = "2023-01-11"
    test_exception = FileNotFoundError(f"File for {test_date} not found")
    mock_product_averages_provider.get_product_averages_for.side_effect = test_exception

    # Act & Assert
    with pytest.raises(DailyAverageLoadingError, match=f"Fatal error loading daily averages for date {test_date}") as exc_info:
        await load_daily_averages.fn(provider=mock_product_averages_provider, date_str=test_date)

    mock_product_averages_provider.get_product_averages_for.assert_called_once_with(test_date)
    assert f"Fatal error loading daily averages for date {test_date}" in caplog.text
    assert str(test_exception) in caplog.text # Check original error message is logged
    assert "ERROR" in caplog.text
    # Check that the original exception is chained
    assert exc_info.value.__cause__ is test_exception 