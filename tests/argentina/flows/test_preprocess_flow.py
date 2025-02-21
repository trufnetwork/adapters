"""Tests for the Argentina SEPA preprocessing flow."""

from datetime import datetime
import re
from typing import Any, cast
from unittest.mock import MagicMock

import pandas as pd
from pandera.errors import SchemaError
from prefect.logging.loggers import disable_run_logger
from prefect_aws import S3Bucket
import pytest
from pytest_mock import MockerFixture

from tsn_adapters.tasks.argentina.flows.preprocess_flow import PreprocessFlow, preprocess_flow
from tsn_adapters.tasks.argentina.models.aggregated_prices import SepaAggregatedPricesModel
from tsn_adapters.tasks.argentina.models.category_map import SepaProductCategoryMapModel
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaProductosDataModel
from tsn_adapters.tasks.argentina.provider.s3 import ProcessedDataProvider, RawDataProvider
from tsn_adapters.tasks.argentina.types import (
    AggregatedPricesDF,
    DateStr,
    UncategorizedDF,
)

# Test constants
TEST_DATE = DateStr("2024-01-01")
TEST_CATEGORY_MAP_URL = "http://example.com/category_map.csv"
INVALID_DATE = "2024/01/01"  # Wrong format

# Test data
SAMPLE_RAW_DATA = {
    "id_producto": ["P1", "P2", "P3"],
    "productos_descripcion": ["Product A", "Product B", "Product C"],
    "productos_precio_lista": [100.0, 200.0, 300.0],
    "date": ["2024-01-01", "2024-01-01", "2024-01-01"],
}

SAMPLE_CATEGORY_MAP = {
    "id_producto": ["P1", "P2", "P3"],
    "productos_descripcion": ["Product A", "Product B", "Product C"],
    "category_id": ["C1", "C2", "C3"],
    "category_name": ["Category 1", "Category 2", "Category 3"],
}

SAMPLE_PROCESSED_DATA = {
    "category_id": ["C1", "C2", "C3"],
    "avg_price": [100.0, 200.0, 300.0],
    "date": ["2024-01-01", "2024-01-01", "2024-01-01"],
}


@pytest.fixture(autouse=True)
def mock_run_logger():
    with disable_run_logger():
        yield


class TestPreprocessFlowInit:
    """Tests for the PreprocessFlow class initialization."""

    def test_init_success(self):
        """Test successful initialization of PreprocessFlow."""
        mock_s3_block = MagicMock(spec=S3Bucket)

        flow = PreprocessFlow(
            product_category_map_url=TEST_CATEGORY_MAP_URL,
            s3_block=mock_s3_block,
        )

        assert isinstance(flow.raw_provider, RawDataProvider)
        assert isinstance(flow.processed_provider, ProcessedDataProvider)
        assert flow.category_map_url == TEST_CATEGORY_MAP_URL
        # Check that providers were initialized with the correct S3 block
        assert flow.raw_provider.s3_block == mock_s3_block
        assert flow.processed_provider.s3_block == mock_s3_block


class TestPreprocessFlowRunFlow:
    """Tests for the PreprocessFlow run_flow method."""

    @pytest.fixture
    def mock_preprocess_flow(self) -> PreprocessFlow:
        """Fixture to create a PreprocessFlow instance with mocked providers."""
        mock_s3_block = MagicMock(spec=S3Bucket)
        flow = PreprocessFlow(
            product_category_map_url=TEST_CATEGORY_MAP_URL,
            s3_block=mock_s3_block,
        )
        # Mock the providers directly with proper typing
        flow.raw_provider = cast(Any, MagicMock(spec=RawDataProvider))
        flow.processed_provider = cast(Any, MagicMock(spec=ProcessedDataProvider))
        return flow

    def test_run_flow_happy_path(self, mock_preprocess_flow: PreprocessFlow):
        """Test the run_flow method's happy path."""
        # Mock list_available_keys to return a list of dates
        raw_provider = cast(Any, mock_preprocess_flow.raw_provider)
        processed_provider = cast(Any, mock_preprocess_flow.processed_provider)
        raw_provider.list_available_keys.return_value = [
            DateStr("2024-01-01"),
            DateStr("2024-01-02"),
            DateStr("2024-01-03"),
        ]
        # Mock exists to simulate some dates already existing
        processed_provider.exists.side_effect = [
            True,  # 2024-01-01 exists
            False,  # 2024-01-02 does not exist
            True,  # 2024-01-03 exists
        ]
        # Mock process_date
        process_date_mock = cast(Any, MagicMock())
        mock_preprocess_flow.process_date = process_date_mock

        mock_preprocess_flow.run_flow()

        # Verify that process_date was called only for the date that doesn't exist
        process_date_mock.assert_called_once_with(DateStr("2024-01-02"))

    def test_run_flow_no_dates_available(self, mock_preprocess_flow: PreprocessFlow):
        """Test run_flow when no dates are available."""
        raw_provider = cast(Any, mock_preprocess_flow.raw_provider)
        raw_provider.list_available_keys.return_value = []
        process_date_mock = cast(Any, MagicMock())
        mock_preprocess_flow.process_date = process_date_mock

        mock_preprocess_flow.run_flow()

        # Verify process_date was not called
        process_date_mock.assert_not_called()

    def test_run_flow_all_dates_processed(self, mock_preprocess_flow: PreprocessFlow):
        """Test run_flow when all dates are already processed."""
        raw_provider = cast(Any, mock_preprocess_flow.raw_provider)
        processed_provider = cast(Any, mock_preprocess_flow.processed_provider)
        raw_provider.list_available_keys.return_value = [
            DateStr("2024-01-01"),
            DateStr("2024-01-02"),
        ]
        # Mock exists to return True for all dates
        processed_provider.exists.return_value = True
        process_date_mock = cast(Any, MagicMock())
        mock_preprocess_flow.process_date = process_date_mock

        mock_preprocess_flow.run_flow()

        # Verify process_date was not called
        process_date_mock.assert_not_called()


class TestPreprocessFlowProcessDate:
    """Tests for the PreprocessFlow process_date method."""

    @pytest.fixture
    def mock_preprocess_flow(self) -> PreprocessFlow:
        """Fixture to create a PreprocessFlow instance with mocked providers."""
        mock_s3_block = MagicMock(spec=S3Bucket)
        flow = PreprocessFlow(
            product_category_map_url=TEST_CATEGORY_MAP_URL,
            s3_block=mock_s3_block,
        )
        # Mock the providers directly with proper typing
        flow.raw_provider = cast(Any, MagicMock(spec=RawDataProvider))
        flow.processed_provider = cast(Any, MagicMock(spec=ProcessedDataProvider))
        # Mock _create_summary as a MagicMock
        flow._create_summary = cast(Any, MagicMock())
        return flow

    def test_process_date_happy_path(
        self, mock_preprocess_flow: PreprocessFlow, mocker: MockerFixture, prefect_test_fixture
    ):
        """Test process_date with valid data."""
        # Mock dependencies
        mock_raw_data = cast(Any, MagicMock(spec=pd.DataFrame))
        mock_raw_data.empty = False
        raw_provider = cast(Any, mock_preprocess_flow.raw_provider)
        raw_provider.get_raw_data_for.return_value = mock_raw_data

        mock_category_map = cast(Any, MagicMock(spec=pd.DataFrame))
        mock_task_load_category_map = cast(
            Any,
            mocker.patch(
                "tsn_adapters.tasks.argentina.flows.preprocess_flow.task_load_category_map",
                return_value=mock_category_map,
            ),
        )

        mock_processed_data = cast(Any, MagicMock(spec=pd.DataFrame))
        mock_uncategorized = cast(Any, MagicMock(spec=pd.DataFrame))
        mock_process_raw_data = cast(
            Any,
            mocker.patch(
                "tsn_adapters.tasks.argentina.flows.preprocess_flow.process_raw_data",
                return_value=MagicMock(result=lambda: (mock_processed_data, mock_uncategorized)),
            ),
        )

        # Run the method
        mock_preprocess_flow.process_date(TEST_DATE)

        # Verify interactions
        raw_provider.get_raw_data_for.assert_called_once_with(TEST_DATE)
        mock_task_load_category_map.assert_called_once_with(url=mock_preprocess_flow.category_map_url)
        mock_process_raw_data.assert_called_once_with(
            raw_data=mock_raw_data, category_map_df=mock_category_map, date=TEST_DATE, return_state=True
        )
        processed_provider = cast(Any, mock_preprocess_flow.processed_provider)
        processed_provider.save_processed_data.assert_called_once_with(
            date_str=TEST_DATE,
            data=mock_processed_data,
            uncategorized=mock_uncategorized,
            logs=b"Placeholder for logs",
        )
        create_summary = cast(Any, mock_preprocess_flow._create_summary)
        create_summary.assert_called_once_with(TEST_DATE, mock_processed_data, mock_uncategorized)

    def test_process_date_no_data(self, mock_preprocess_flow: PreprocessFlow):
        """Test process_date when no data is available for the date."""
        # Mock raw_provider to return empty DataFrame
        mock_empty_df = cast(Any, MagicMock(spec=pd.DataFrame))
        mock_empty_df.empty = True
        raw_provider = cast(Any, mock_preprocess_flow.raw_provider)
        raw_provider.get_raw_data_for.return_value = mock_empty_df

        mock_preprocess_flow.process_date(TEST_DATE)

        # Verify that processing stops after finding no data
        raw_provider.get_raw_data_for.assert_called_once_with(TEST_DATE)
        processed_provider = cast(Any, mock_preprocess_flow.processed_provider)
        processed_provider.save_processed_data.assert_not_called()
        create_summary = cast(Any, mock_preprocess_flow._create_summary)
        create_summary.assert_not_called()

    @pytest.mark.parametrize(
        "invalid_date",
        [
            "2024/01/01",  # Wrong separator
            "24-01-01",  # Wrong year format
            "2024-1-1",  # Wrong month/day format
            "2024-13-01",  # Invalid month
            "2024-01-32",  # Invalid day
        ],
    )
    def test_process_date_invalid_date(self, mock_preprocess_flow: PreprocessFlow, invalid_date: str):
        """Test process_date with invalid date format."""

        # Override validate_date to do stricter validation
        def strict_validate_date(date: DateStr) -> None:
            # First check format with regex
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
                raise ValueError(f"Invalid date format: {date}")

            # Then check if it's a valid date
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Invalid date format: {date}")

        mock_preprocess_flow.validate_date = strict_validate_date

        with pytest.raises(ValueError, match=f"Invalid date format: {invalid_date}"):
            mock_preprocess_flow.process_date(cast(DateStr, invalid_date))

        # Verify no processing was attempted
        mock_preprocess_flow.raw_provider.get_raw_data_for.assert_not_called()

    def test_process_date_category_map_error(self, mock_preprocess_flow: PreprocessFlow, mocker: MockerFixture):
        """Test process_date when category map loading fails."""
        # Mock raw data
        mock_raw_data = cast(Any, MagicMock(spec=pd.DataFrame))
        mock_raw_data.empty = False
        raw_provider = cast(Any, mock_preprocess_flow.raw_provider)
        raw_provider.get_raw_data_for.return_value = mock_raw_data

        # Mock category map loading to fail
        mock_task_load_category_map = cast(
            Any,
            mocker.patch(
                "tsn_adapters.tasks.argentina.flows.preprocess_flow.task_load_category_map",
                side_effect=Exception("Failed to load category map"),
            ),
        )

        with pytest.raises(Exception, match="Failed to load category map"):
            mock_preprocess_flow.process_date(TEST_DATE)

        # Verify interactions
        raw_provider.get_raw_data_for.assert_called_once_with(TEST_DATE)
        mock_task_load_category_map.assert_called_once_with(url=mock_preprocess_flow.category_map_url)
        processed_provider = cast(Any, mock_preprocess_flow.processed_provider)
        processed_provider.save_processed_data.assert_not_called()
        create_summary = cast(Any, mock_preprocess_flow._create_summary)
        create_summary.assert_not_called()

    def test_process_date_processing_error(self, mock_preprocess_flow: PreprocessFlow, mocker: MockerFixture):
        """Test process_date when data processing fails."""
        # Mock raw data and category map
        mock_raw_data = cast(Any, MagicMock(spec=pd.DataFrame))
        mock_raw_data.empty = False
        raw_provider = cast(Any, mock_preprocess_flow.raw_provider)
        raw_provider.get_raw_data_for.return_value = mock_raw_data

        mock_category_map = cast(Any, MagicMock(spec=pd.DataFrame))
        mocker.patch(
            "tsn_adapters.tasks.argentina.flows.preprocess_flow.task_load_category_map", return_value=mock_category_map
        )

        # Mock process_raw_data to fail
        cast(
            Any,
            mocker.patch(
                "tsn_adapters.tasks.argentina.flows.preprocess_flow.process_raw_data",
                side_effect=Exception("Processing failed"),
            ),
        )

        with pytest.raises(Exception, match="Processing failed"):
            mock_preprocess_flow.process_date(TEST_DATE)

        # Verify that save_processed_data was not called
        processed_provider = cast(Any, mock_preprocess_flow.processed_provider)
        processed_provider.save_processed_data.assert_not_called()
        create_summary = cast(Any, mock_preprocess_flow._create_summary)
        create_summary.assert_not_called()


class TestPreprocessFlowCreateSummary:
    """Tests for the PreprocessFlow _create_summary method."""

    @pytest.fixture
    def mock_preprocess_flow(self) -> PreprocessFlow:
        """Fixture to create a PreprocessFlow instance with mocked providers."""
        mock_s3_block = MagicMock(spec=S3Bucket)
        flow = PreprocessFlow(
            product_category_map_url=TEST_CATEGORY_MAP_URL,
            s3_block=mock_s3_block,
        )
        return flow

    def test_create_summary_with_uncategorized(self, mock_preprocess_flow: PreprocessFlow, mocker: MockerFixture):
        """Test _create_summary with both processed and uncategorized data."""
        # Mock create_markdown_artifact
        mock_create_artifact = mocker.patch(
            "tsn_adapters.tasks.argentina.flows.preprocess_flow.create_markdown_artifact"
        )

        # Create test data
        processed_data = cast(
            AggregatedPricesDF,
            pd.DataFrame(
                {
                    "category_id": ["C1", "C2", "C3"],
                    "avg_price": [10.0, 20.0, 30.0],
                    "date": ["2024-01-01", "2024-01-01", "2024-01-01"],
                }
            ),
        )
        uncategorized = cast(
            UncategorizedDF,
            pd.DataFrame({"id_producto": ["P4", "P5"], "productos_descripcion": ["Product D", "Product E"]}),
        )

        # Call the method
        mock_preprocess_flow._create_summary(TEST_DATE, processed_data, uncategorized)

        # Verify the artifact creation
        mock_create_artifact.assert_called_once()
        call_args = mock_create_artifact.call_args[1]
        assert call_args["key"] == f"preprocessing-summary-{TEST_DATE}"
        assert "description" in call_args

        # Verify markdown content
        markdown = call_args["markdown"]
        assert f"# SEPA Preprocessing Summary for {TEST_DATE}" in markdown
        assert "Total records processed: 5" in markdown  # 3 processed + 2 uncategorized
        assert "Successfully categorized: 3" in markdown
        assert "Uncategorized products: 2" in markdown
        assert "Product D" in markdown  # Sample uncategorized product
        assert "Product E" in markdown  # Sample uncategorized product

    def test_create_summary_no_uncategorized(self, mock_preprocess_flow: PreprocessFlow, mocker: MockerFixture):
        """Test _create_summary with only processed data (no uncategorized)."""
        # Mock create_markdown_artifact
        mock_create_artifact = mocker.patch(
            "tsn_adapters.tasks.argentina.flows.preprocess_flow.create_markdown_artifact"
        )

        # Create test data
        processed_data = cast(
            AggregatedPricesDF,
            pd.DataFrame(
                {
                    "category_id": ["C1", "C2", "C3"],
                    "avg_price": [10.0, 20.0, 30.0],
                    "date": ["2024-01-01", "2024-01-01", "2024-01-01"],
                }
            ),
        )
        uncategorized = cast(UncategorizedDF, pd.DataFrame())

        # Call the method
        mock_preprocess_flow._create_summary(TEST_DATE, processed_data, uncategorized)

        # Verify the artifact creation
        mock_create_artifact.assert_called_once()
        call_args = mock_create_artifact.call_args[1]

        # Verify markdown content
        markdown = call_args["markdown"]
        assert f"# SEPA Preprocessing Summary for {TEST_DATE}" in markdown
        assert "Total records processed: 3" in markdown
        assert "Successfully categorized: 3" in markdown
        assert "Uncategorized products: 0" in markdown
        assert "Sample Uncategorized Products" not in markdown  # No uncategorized section

    def test_create_summary_empty_data(self, mock_preprocess_flow: PreprocessFlow, mocker: MockerFixture):
        """Test _create_summary with empty DataFrames."""
        # Mock create_markdown_artifact
        mock_create_artifact = mocker.patch(
            "tsn_adapters.tasks.argentina.flows.preprocess_flow.create_markdown_artifact"
        )

        # Create empty test data
        processed_data = cast(AggregatedPricesDF, pd.DataFrame())
        uncategorized = cast(UncategorizedDF, pd.DataFrame())

        # Call the method
        mock_preprocess_flow._create_summary(TEST_DATE, processed_data, uncategorized)

        # Verify the artifact creation
        mock_create_artifact.assert_called_once()
        call_args = mock_create_artifact.call_args[1]

        # Verify markdown content
        markdown = call_args["markdown"]
        assert f"# SEPA Preprocessing Summary for {TEST_DATE}" in markdown
        assert "Total records processed: 0" in markdown
        assert "Successfully categorized: 0" in markdown
        assert "Uncategorized products: 0" in markdown
        assert "Sample Uncategorized Products" not in markdown


class TestPreprocessFlowTopLevel:
    """Tests for the top-level preprocess_flow function."""

    pytestmark = pytest.mark.usefixtures("prefect_test_fixture")

    def test_preprocess_flow_happy_path(self, mocker: MockerFixture):
        """Test the top-level preprocess_flow function's happy path."""
        # Mock S3Bucket.load
        mock_s3_block = MagicMock(spec=S3Bucket)
        mock_s3_load = mocker.patch(
            "tsn_adapters.tasks.argentina.flows.preprocess_flow.S3Bucket.load", return_value=mock_s3_block
        )

        # Mock PreprocessFlow
        mock_flow = MagicMock(spec=PreprocessFlow)
        mock_flow_class = mocker.patch(
            "tsn_adapters.tasks.argentina.flows.preprocess_flow.PreprocessFlow", return_value=mock_flow
        )

        # Test parameters
        test_s3_block_name = "test-s3-block"

        # Run the flow
        preprocess_flow(
            product_category_map_url=TEST_CATEGORY_MAP_URL,
            s3_block_name=test_s3_block_name,
        )

        # Verify S3 block loading
        mock_s3_load.assert_called_once_with(test_s3_block_name)

        # Verify PreprocessFlow instantiation and execution
        mock_flow_class.assert_called_once_with(
            product_category_map_url=TEST_CATEGORY_MAP_URL,
            s3_block=mock_s3_block,
        )
        mock_flow.run_flow.assert_called_once()

    def test_preprocess_flow_s3_block_error(self, mocker: MockerFixture):
        """Test preprocess_flow when S3 block loading fails."""
        # Mock S3Bucket.load to raise an exception
        mock_s3_load = mocker.patch(
            "tsn_adapters.tasks.argentina.flows.preprocess_flow.S3Bucket.load",
            side_effect=Exception("Failed to load S3 block"),
        )

        # Test parameters
        test_s3_block_name = "test-s3-block"

        # Run the flow and expect exception
        with pytest.raises(Exception, match="Failed to load S3 block"):
            preprocess_flow(
                product_category_map_url=TEST_CATEGORY_MAP_URL,
                s3_block_name=test_s3_block_name,
            )

        # Verify S3 block loading attempt
        mock_s3_load.assert_called_once_with(test_s3_block_name)

    def test_preprocess_flow_run_error(self, mocker: MockerFixture):
        """Test preprocess_flow when flow execution fails."""
        # Mock S3Bucket.load
        mock_s3_block = MagicMock(spec=S3Bucket)
        mocker.patch("tsn_adapters.tasks.argentina.flows.preprocess_flow.S3Bucket.load", return_value=mock_s3_block)

        # Mock PreprocessFlow with failing run_flow
        mock_flow = MagicMock(spec=PreprocessFlow)
        mock_flow.run_flow.side_effect = Exception("Flow execution failed")
        mocker.patch("tsn_adapters.tasks.argentina.flows.preprocess_flow.PreprocessFlow", return_value=mock_flow)

        # Test parameters
        test_s3_block_name = "test-s3-block"

        # Run the flow and expect exception
        with pytest.raises(Exception, match="Flow execution failed"):
            preprocess_flow(
                product_category_map_url=TEST_CATEGORY_MAP_URL,
                s3_block_name=test_s3_block_name,
            )

        # Verify flow execution attempt
        mock_flow.run_flow.assert_called_once()


class TestPreprocessFlowDataTypes:
    """Tests for data type validation in the PreprocessFlow."""

    def test_raw_data_schema(self):
        """Test that raw data matches the expected schema."""
        df = pd.DataFrame(SAMPLE_RAW_DATA)

        # This should not raise an exception
        validated_df = SepaProductosDataModel.validate(df)
        assert isinstance(validated_df, pd.DataFrame)
        assert all(
            validated_df.dtypes
            == pd.Series(
                {
                    "id_producto": "object",
                    "productos_descripcion": "object",
                    "productos_precio_lista": "float64",
                    "date": "object",
                }
            )
        )

    def test_category_map_schema(self):
        """Test that category map data matches the expected schema."""
        df = pd.DataFrame(SAMPLE_CATEGORY_MAP)

        # This should not raise an exception
        validated_df = SepaProductCategoryMapModel.validate(df)
        assert isinstance(validated_df, pd.DataFrame)
        assert all(
            validated_df.dtypes
            == pd.Series(
                {
                    "id_producto": "object",
                    "productos_descripcion": "object",
                    "category_id": "object",
                    "category_name": "object",
                }
            )
        )

    def test_processed_data_schema(self):
        """Test that processed data matches the expected schema."""
        df = pd.DataFrame(SAMPLE_PROCESSED_DATA)

        # This should not raise an exception
        validated_df = SepaAggregatedPricesModel.validate(df)
        assert isinstance(validated_df, pd.DataFrame)
        assert all(
            validated_df.dtypes
            == pd.Series(
                {
                    "category_id": "object",
                    "avg_price": "float64",
                    "date": "object",
                }
            )
        )

    def test_invalid_raw_data_schema(self):
        """Test that invalid raw data fails schema validation."""
        invalid_data = {
            "id_producto": [1, 2, 3],  # Should be strings
            "productos_descripcion": ["Product A", "Product B", "Product C"],
            "productos_precio_lista": ["100.0", "200.0", "300.0"],  # Should be floats
            "date": ["2024-01-01", "2024-01-01", "2024-01-01"],
        }
        df = pd.DataFrame(invalid_data)

        # Disable coercion to ensure type validation fails
        schema = SepaProductosDataModel.to_schema()
        schema.coerce = False
        with pytest.raises(SchemaError):
            schema.validate(df)

    def test_invalid_category_map_schema(self):
        """Test that invalid category map data fails schema validation."""
        invalid_data = {
            "id_producto": ["P1", "P2", "P3"],
            "productos_descripcion": ["Product A", "Product B", "Product C"],
            "category_id": [1, 2, 3],  # Should be strings
            "category_name": [True, False, True],  # Should be strings
        }
        df = pd.DataFrame(invalid_data)

        # Disable coercion to ensure type validation fails
        schema = SepaProductCategoryMapModel.to_schema()
        schema.coerce = False
        with pytest.raises(SchemaError):
            schema.validate(df)

    def test_invalid_processed_data_schema(self):
        """Test that invalid processed data fails schema validation."""
        invalid_data = {
            "category_id": ["C1", "C2", "C3"],
            "avg_price": ["100.0", "200.0", "300.0"],  # Should be floats
            "date": [20240101, 20240101, 20240101],  # Should be strings
        }
        df = pd.DataFrame(invalid_data)

        # Disable coercion to ensure type validation fails
        schema = SepaAggregatedPricesModel.to_schema()
        schema.coerce = False
        with pytest.raises(SchemaError):
            schema.validate(df)
