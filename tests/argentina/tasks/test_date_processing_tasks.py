"""
Unit tests for Argentina SEPA date processing tasks.
"""

from datetime import datetime
import logging
from unittest.mock import MagicMock

import pandas as pd
from pandera.typing import DataFrame
import pytest

from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.tasks.argentina.models import ArgentinaProductStateMetadata, DynamicPrimitiveSourceModel
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider import ProductAveragesProvider
from tsn_adapters.tasks.argentina.tasks.date_processing_tasks import (
    DailyAverageLoadingError,
    MappingIntegrityError,
    determine_dates_to_insert,
    load_daily_averages,
    transform_product_data,
)
from tsn_adapters.tasks.argentina.types import DateStr
from tsn_adapters.utils import create_empty_df


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
            [],  # Expect empty list as deployment date < insertion date
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
            ["2023-01-02", "2023-01-03"],  # Should include end boundary, exclude start boundary
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
        insertion_dt = datetime.strptime(
            metadata_dict.get("last_insertion_processed_date", "1970-01-01"), "%Y-%m-%d"
        ).date()
        deployment_dt = datetime.strptime(
            metadata_dict.get("last_product_deployment_date", "1970-01-01"), "%Y-%m-%d"
        ).date()
        if deployment_dt < insertion_dt:
            assert (
                "last_product_deployment_date" in caplog.text
                and "is before" in caplog.text
                and "last_insertion_processed_date" in caplog.text
            )
    except ValueError:
        pass  # Ignore if dates in metadata_dict are invalid for this specific check


@pytest.mark.asyncio
async def test_determine_dates_to_process_provider_error(mock_product_averages_provider: MagicMock):
    """Test that an exception from the provider's list_available_keys is propagated."""
    # Arrange
    test_exception = OSError("Simulated S3 connection error")
    mock_product_averages_provider.list_available_keys.side_effect = test_exception
    metadata = ArgentinaProductStateMetadata()  # Default metadata

    # Act & Assert
    with pytest.raises(IOError) as exc_info:
        await determine_dates_to_insert.fn(metadata=metadata, provider=mock_product_averages_provider)

    # Check that the original exception is raised
    assert exc_info.value is test_exception
    mock_product_averages_provider.list_available_keys.assert_called_once()


# --- Test Cases for load_daily_averages ---

# Sample valid daily averages data
SAMPLE_DAILY_AVG_DATA = {
    "id_producto": ["p1", "p2"],
    "productos_descripcion": ["Product 1", "Product 2"],
    "productos_precio_lista_avg": [100.5, 200.0],
    "date": ["2023-01-10"] * 2,
}


@pytest.mark.asyncio
async def test_load_daily_averages_success(mock_product_averages_provider: MagicMock):
    """Test successful loading of daily averages."""
    # Arrange
    test_date = "2023-01-10"
    valid_df = DataFrame[SepaAvgPriceProductModel](pd.DataFrame(SAMPLE_DAILY_AVG_DATA))
    mock_product_averages_provider.get_product_averages_for.return_value = valid_df

    # Act
    result_df = await load_daily_averages.fn(provider=mock_product_averages_provider, date_str=DateStr(test_date))

    # Assert
    mock_product_averages_provider.get_product_averages_for.assert_called_once_with(test_date)
    pd.testing.assert_frame_equal(result_df, valid_df)


@pytest.mark.asyncio
async def test_load_daily_averages_provider_error(
    mock_product_averages_provider: MagicMock, caplog: pytest.LogCaptureFixture
):
    """Test that an exception from the provider is caught and re-raised as DailyAverageLoadingError."""
    # Arrange
    test_date = "2023-01-11"
    test_exception = FileNotFoundError(f"File for {test_date} not found")
    mock_product_averages_provider.get_product_averages_for.side_effect = test_exception

    # Act & Assert
    with pytest.raises(
        DailyAverageLoadingError, match=f"Fatal error loading daily averages for date {test_date}"
    ) as exc_info:
        await load_daily_averages.fn(provider=mock_product_averages_provider, date_str=DateStr(test_date))

    mock_product_averages_provider.get_product_averages_for.assert_called_once_with(test_date)
    assert f"Fatal error loading daily averages for date {test_date}" in caplog.text
    assert str(test_exception) in caplog.text  # Check original error message is logged
    assert "ERROR" in caplog.text
    # Check that the original exception is chained
    assert exc_info.value.__cause__ is test_exception


# --- Test Cases for transform_product_data ---


# Fixture for sample Daily Average DataFrame
@pytest.fixture
def sample_daily_avg_df() -> DataFrame[SepaAvgPriceProductModel]:
    data = {
        "id_producto": ["p1", "p2", "p3"],
        "productos_descripcion": ["Product 1", "Product 2", "Product 3"],
        "productos_precio_lista_avg": [10.0, 20.0, 30.0],
        "date": ["2023-01-15"] * 3,
    }
    return SepaAvgPriceProductModel.validate(pd.DataFrame(data), lazy=True)


# Fixture for sample Descriptor DataFrame
@pytest.fixture
def sample_descriptor_df() -> DataFrame[DynamicPrimitiveSourceModel]:
    data = {
        "stream_id": ["stream-1", "stream-2", "stream-3"],
        "source_id": ["p1", "p2", "p3"],  # Matches sample_daily_avg_df for success case
        "source_type": ["arg_sepa_prod"] * 3,
        "productos_descripcion": ["Desc 1", "Desc 2", "Desc 3"],
        "first_shown_at": ["2023-01-01"] * 3,
    }
    return DynamicPrimitiveSourceModel.validate(pd.DataFrame(data), lazy=True)


# Fixture for sample Descriptor DataFrame with missing mappings
@pytest.fixture
def sample_descriptor_missing_df() -> DataFrame[DynamicPrimitiveSourceModel]:
    data = {
        "stream_id": ["stream-1", "stream-2"],  # Only p1 and p2 are mapped
        "source_id": ["p1", "p2"],
        "source_type": ["arg_sepa_prod"] * 2,
        "productos_descripcion": ["Desc 1", "Desc 2"],
        "first_shown_at": ["2023-01-01"] * 2,
    }
    return DynamicPrimitiveSourceModel.validate(pd.DataFrame(data), lazy=True)


@pytest.mark.asyncio
async def test_transform_product_data_full_success(
    sample_daily_avg_df: DataFrame[SepaAvgPriceProductModel],
    sample_descriptor_df: DataFrame[DynamicPrimitiveSourceModel],
):
    """Test the full successful transformation process."""
    # Arrange
    test_date = "2023-01-15"
    # Expected timestamp for 2023-01-15 00:00:00 UTC
    expected_timestamp = 1673740800

    # Act
    result_df = await transform_product_data.fn(
        daily_avg_df=sample_daily_avg_df,
        descriptor_df=sample_descriptor_df,
        date_str=DateStr(test_date),
    )

    # Assert
    assert isinstance(result_df, pd.DataFrame)
    # Check structure against TnDataRowModel
    assert list(result_df.columns) == ["stream_id", "date", "value", "data_provider"]
    assert len(result_df) == 3

    # Verify content and types
    assert result_df["stream_id"].tolist() == ["stream-1", "stream-2", "stream-3"]
    # Check content is the string representation of the timestamp
    assert result_df["date"].tolist() == [str(expected_timestamp)] * 3 
    # Check the dtype is object (as pandas often represents strings)
    assert result_df["date"].dtype == "object" 
    assert result_df["value"].tolist() == ["10.0", "20.0", "30.0"]
    assert result_df["value"].dtype == "object"  # Pandas uses object for strings
    # Verify data_provider was added and is None/NaN
    assert "data_provider" in result_df.columns
    assert result_df["data_provider"].isnull().all() # type: ignore[arg-type]

    # Explicitly validate with Pandera model again
    try:
        TnDataRowModel.validate(result_df, lazy=True)
    except Exception as e:
        pytest.fail(f"Final DataFrame validation failed: {e}")


@pytest.mark.asyncio
async def test_transform_product_data_mapping_failure(
    sample_daily_avg_df: DataFrame[SepaAvgPriceProductModel],
    sample_descriptor_missing_df: DataFrame[DynamicPrimitiveSourceModel],
    caplog: pytest.LogCaptureFixture,
):
    """Test mapping integrity check failure in transform_product_data."""
    # Arrange
    test_date = "2023-01-15"

    # Act & Assert
    with pytest.raises(MappingIntegrityError) as exc_info:
        await transform_product_data.fn(
            daily_avg_df=sample_daily_avg_df,
            descriptor_df=sample_descriptor_missing_df,
            date_str=DateStr(test_date),
        )

    # Check the exception message and log content
    assert f"Mapping integrity check failed for date {test_date}" in str(exc_info.value)
    assert "1 product IDs from daily averages were not found" in str(exc_info.value)
    assert "Missing IDs sample: ['p3']" in str(exc_info.value)
    assert "Mapping integrity check failed" in caplog.text
    assert "Missing IDs sample: ['p3']" in caplog.text
    assert "ERROR" in caplog.text


@pytest.mark.asyncio
async def test_transform_product_data_empty_input(
    sample_descriptor_df: DataFrame[DynamicPrimitiveSourceModel],
    caplog: pytest.LogCaptureFixture,
):
    """Test transform_product_data with an empty input daily average DataFrame."""
    # Arrange
    test_date = "2023-01-16"
    empty_daily_avg_df = create_empty_df(SepaAvgPriceProductModel)

    # Act
    with caplog.at_level(logging.INFO):
        result_df = await transform_product_data.fn(
            daily_avg_df=empty_daily_avg_df,
            descriptor_df=sample_descriptor_df,
            date_str=DateStr(test_date),
        )

    # Assert
    assert isinstance(result_df, pd.DataFrame)
    assert result_df.empty
    # Check that the empty dataframe conforms to the *target* model structure
    assert list(result_df.columns) == list(TnDataRowModel.to_schema().columns.keys())
    assert f"Input daily average DataFrame for {test_date} is empty" in caplog.text
    assert "INFO" in caplog.text


@pytest.mark.asyncio
async def test_transform_product_data_invalid_date_format(
    sample_descriptor_df: DataFrame[DynamicPrimitiveSourceModel],
    caplog: pytest.LogCaptureFixture,
):
    """Test transformation failure when date format is invalid."""
    # Arrange
    test_date = "invalid-date-format"
    data = {
        "id_producto": ["p1"],
        "productos_descripcion": ["Product 1"],
        "productos_precio_lista_avg": [10.0],
        "date": [test_date],  # Invalid date
    }
    invalid_date_df = DataFrame[SepaAvgPriceProductModel](pd.DataFrame(data))

    # Act & Assert
    with pytest.raises(ValueError, match="Failed to parse date strings"):
        await transform_product_data.fn(
            daily_avg_df=invalid_date_df,
            descriptor_df=sample_descriptor_df,
            date_str=DateStr("2023-01-17"),
        )
    assert "Error during data conversion (timestamp/value)" in caplog.text
    assert "ERROR" in caplog.text


@pytest.mark.asyncio
async def test_transform_product_data_timestamp_out_of_range(
    sample_descriptor_df: DataFrame[DynamicPrimitiveSourceModel],
    caplog: pytest.LogCaptureFixture,
):
    """Test transformation failure when date results in out-of-range timestamp."""
    # Arrange
    test_date = "1960-01-01"  # Date before Unix epoch start
    data = {
        "id_producto": ["p1"],
        "productos_descripcion": ["Product 1"],
        "productos_precio_lista_avg": [10.0],
        "date": [test_date],
    }
    out_of_range_df = DataFrame[SepaAvgPriceProductModel](pd.DataFrame(data))

    # Act & Assert
    with pytest.raises(ValueError, match="Converted timestamps outside valid range"):
        await transform_product_data.fn(
            daily_avg_df=out_of_range_df,
            descriptor_df=sample_descriptor_df,
            date_str=DateStr(test_date),
        )
    assert "Error during data conversion (timestamp/value)" in caplog.text
    assert "ERROR" in caplog.text
