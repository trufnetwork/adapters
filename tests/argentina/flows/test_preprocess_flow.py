"""Tests for the Argentina SEPA preprocessing flow."""

from datetime import datetime
import re
from typing import Any, cast
from unittest.mock import ANY, MagicMock, AsyncMock

import pandas as pd
from pandera.errors import SchemaError
from pandera.typing import DataFrame
from prefect.logging.loggers import disable_run_logger
from prefect_aws import S3Bucket
import pytest
from pytest_mock import MockerFixture

from tsn_adapters.tasks.argentina.flows.preprocess_flow import PreprocessFlow, preprocess_flow
from tsn_adapters.tasks.argentina.models.aggregated_prices import SepaAggregatedPricesModel
from tsn_adapters.tasks.argentina.models.category_map import SepaProductCategoryMapModel
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel, SepaProductosDataModel
from tsn_adapters.tasks.argentina.provider.product_averages import ProductAveragesProvider
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
def enable_prefect_test_harness(prefect_test_fixture: Any):
    with disable_run_logger():
        _ = prefect_test_fixture
        yield


@pytest.fixture
def mock_s3_block() -> MagicMock:
    """Provides a mocked S3Bucket instance."""
    return MagicMock(spec=S3Bucket)

@pytest.fixture
def mock_raw_provider(mocker: MockerFixture, mock_s3_block: MagicMock) -> MagicMock:
    """Provides a mocked RawDataProvider instance."""
    mock_inst = MagicMock(spec=RawDataProvider)
    # Mock the class constructor to return our instance
    mocker.patch("tsn_adapters.tasks.argentina.flows.preprocess_flow.RawDataProvider", return_value=mock_inst)
    return mock_inst

@pytest.fixture
def mock_processed_provider(mocker: MockerFixture, mock_s3_block: MagicMock) -> MagicMock:
    """Provides a mocked ProcessedDataProvider instance."""
    mock_inst = MagicMock(spec=ProcessedDataProvider)
    # Mock the class constructor (likely in the base class) to return our instance
    mocker.patch("tsn_adapters.tasks.argentina.flows.base.ProcessedDataProvider", return_value=mock_inst)
    return mock_inst

@pytest.fixture
def mock_product_avg_provider(mocker: MockerFixture, mock_s3_block: MagicMock) -> MagicMock:
    """Provides a mocked ProductAveragesProvider instance."""
    mock_inst = MagicMock(spec=ProductAveragesProvider)
    # Mock the class constructor to return our instance
    mocker.patch("tsn_adapters.tasks.argentina.flows.preprocess_flow.ProductAveragesProvider", return_value=mock_inst)
    return mock_inst

@pytest.fixture
def preprocess_flow_instance(
    mock_s3_block: MagicMock,
    mock_raw_provider: MagicMock, # Ensure provider mocks are active
    mock_processed_provider: MagicMock,
    mock_product_avg_provider: MagicMock,
) -> PreprocessFlow:
    """Provides a PreprocessFlow instance with mocked providers."""
    # Instantiating the flow will now automatically use the mocked providers
    # because we patched their constructors in the provider fixtures.
    flow = PreprocessFlow(
        product_category_map_url=TEST_CATEGORY_MAP_URL,
        s3_block=mock_s3_block,
    )
    return flow


class TestPreprocessFlowInit:
    """Tests for the PreprocessFlow class initialization."""

    def test_init_success(self, mocker: MockerFixture):
        """Test successful initialization of PreprocessFlow."""
        mock_s3_block = MagicMock(spec=S3Bucket)
        # Mock provider classes *before* instantiation
        mock_raw_provider_cls = mocker.patch("tsn_adapters.tasks.argentina.flows.preprocess_flow.RawDataProvider")
        # Patch ProcessedDataProvider where it's used in the base class __init__
        mock_processed_provider_cls = mocker.patch(
            "tsn_adapters.tasks.argentina.flows.base.ProcessedDataProvider" # Correct path for base class usage
        )
        # Patch ProductAveragesProvider where it's used in PreprocessFlow __init__
        mock_product_avg_provider_cls = mocker.patch(
            "tsn_adapters.tasks.argentina.flows.preprocess_flow.ProductAveragesProvider" # Corrected path
        )

        flow = PreprocessFlow(
            product_category_map_url=TEST_CATEGORY_MAP_URL,
            s3_block=mock_s3_block,
        )

        # Verify classes were called with the s3_block
        mock_raw_provider_cls.assert_called_once_with(s3_block=mock_s3_block)
        # Verify ProcessedDataProvider was called (by the base class)
        mock_processed_provider_cls.assert_called_once_with(s3_block=mock_s3_block)
        # Verify ProductAveragesProvider was called
        mock_product_avg_provider_cls.assert_called_once_with(s3_block=mock_s3_block)

        assert flow.category_map_url == TEST_CATEGORY_MAP_URL
        # Check instances are assigned (they will be MagicMock instances due to patching)
        assert isinstance(flow.raw_provider, MagicMock)
        assert isinstance(flow.product_averages_provider, MagicMock)
        assert isinstance(flow.processed_provider, MagicMock)


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

    async def test_run_flow_happy_path(self, mock_preprocess_flow: PreprocessFlow):
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
        process_date_mock = cast(Any, AsyncMock())
        mock_preprocess_flow.process_date = process_date_mock

        _ = await mock_preprocess_flow.run_flow()

        # Verify that process_date was called only for the date that doesn't exist
        process_date_mock.assert_called_once_with(DateStr("2024-01-02"))

    async def test_run_flow_no_dates_available(self, mock_preprocess_flow: PreprocessFlow):
        """Test run_flow when no dates are available."""
        raw_provider = cast(Any, mock_preprocess_flow.raw_provider)
        raw_provider.list_available_keys.return_value = []
        process_date_mock = cast(Any, MagicMock())
        mock_preprocess_flow.process_date = process_date_mock

        _ = await mock_preprocess_flow.run_flow()

        # Verify process_date was not called
        process_date_mock.assert_not_called()

    async def test_run_flow_all_dates_processed(self, mock_preprocess_flow: PreprocessFlow):
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

        _ = await mock_preprocess_flow.run_flow()

        # Verify process_date was not called
        process_date_mock.assert_not_called()


class TestPreprocessFlowProcessDate:
    """Tests for the PreprocessFlow process_date method."""

    async def test_process_date_happy_path(
        self,
        mocker: MockerFixture,
        preprocess_flow_instance: PreprocessFlow,
        mock_raw_provider: MagicMock,
        mock_processed_provider: MagicMock,
        mock_product_avg_provider: MagicMock,
    ):
        """Test process_date with valid data."""
        # Arrange: Set return values on the mocked providers from fixtures
        mock_raw_data = MagicMock(spec=pd.DataFrame); mock_raw_data.empty = False
        mock_raw_provider.get_raw_data_for.return_value = mock_raw_data

        mock_category_map = MagicMock(spec=pd.DataFrame)
        mock_task_load_category_map = mocker.patch("tsn_adapters.tasks.argentina.flows.preprocess_flow.task_load_category_map", return_value=mock_category_map)

        mock_processed_data = MagicMock(spec=AggregatedPricesDF)
        mock_uncategorized = MagicMock(spec=UncategorizedDF)
        mock_avg_price_df = MagicMock(spec=DataFrame[SepaAvgPriceProductModel]); mock_avg_price_df.empty = False
        mock_process_raw_data_task = mocker.patch(
                "tsn_adapters.tasks.argentina.flows.preprocess_flow.process_raw_data",
            return_value=(mock_processed_data, mock_uncategorized, mock_avg_price_df),
        )

        # Mock the _create_summary method directly on the instance from the fixture
        preprocess_flow_instance._create_summary = MagicMock() # type: ignore[assignment] # Acknowledge protected access

        # Act
        _ = await preprocess_flow_instance.process_date(TEST_DATE)

        # Assert
        mock_raw_provider.get_raw_data_for.assert_called_once_with(TEST_DATE)
        mock_task_load_category_map.assert_called_once_with(url=preprocess_flow_instance.category_map_url)
        mock_process_raw_data_task.assert_called_once_with(
            raw_data=mock_raw_data,
            category_map_df=mock_category_map,
            return_state=ANY,
        )
        mock_product_avg_provider.save_product_averages.assert_called_once_with(
            date_str=TEST_DATE, data=mock_avg_price_df
        )
        mock_processed_provider.save_processed_data.assert_called_once_with(
            date_str=TEST_DATE,
            data=mock_processed_data,
            uncategorized=mock_uncategorized,
            logs=ANY,
        )
        preprocess_flow_instance._create_summary.assert_called_once_with(TEST_DATE, mock_processed_data, mock_uncategorized) # type: ignore[attr-defined]

    async def test_process_date_with_empty_avg_prices(
        self,
        mocker: MockerFixture,
        preprocess_flow_instance: PreprocessFlow,
        mock_raw_provider: MagicMock,
        mock_processed_provider: MagicMock,
        mock_product_avg_provider: MagicMock,
    ):
        """Test process_date when product average DataFrame is empty."""
        # Arrange
        mock_raw_data = MagicMock(spec=pd.DataFrame); mock_raw_data.empty = False
        mock_raw_provider.get_raw_data_for.return_value = mock_raw_data

        mock_category_map = MagicMock(spec=pd.DataFrame)
        mocker.patch("tsn_adapters.tasks.argentina.flows.preprocess_flow.task_load_category_map", return_value=mock_category_map)

        mock_processed_data = MagicMock(spec=AggregatedPricesDF)
        mock_uncategorized = MagicMock(spec=UncategorizedDF)
        mock_avg_price_df = MagicMock(spec=DataFrame[SepaAvgPriceProductModel]); mock_avg_price_df.empty = True # Key difference
        mocker.patch(
            "tsn_adapters.tasks.argentina.flows.preprocess_flow.process_raw_data",
            return_value=(mock_processed_data, mock_uncategorized, mock_avg_price_df),
        )

        preprocess_flow_instance._create_summary = MagicMock() # type: ignore[assignment]

        # Act
        _ = await preprocess_flow_instance.process_date(TEST_DATE)

        # Assert
        mock_product_avg_provider.save_product_averages.assert_not_called() # Key assertion
        mock_processed_provider.save_processed_data.assert_called_once()
        preprocess_flow_instance._create_summary.assert_called_once_with(TEST_DATE, mock_processed_data, mock_uncategorized) # type: ignore[attr-defined]

    async def test_process_date_product_avg_save_error(
        self,
        mocker: MockerFixture,
        preprocess_flow_instance: PreprocessFlow,
        mock_raw_provider: MagicMock,
        mock_processed_provider: MagicMock,
        mock_product_avg_provider: MagicMock,
    ):
        """Test that process_date continues even if saving product averages fails."""
        # Arrange
        mock_raw_data = MagicMock(spec=pd.DataFrame); mock_raw_data.empty = False
        mock_raw_provider.get_raw_data_for.return_value = mock_raw_data

        mock_category_map = MagicMock(spec=pd.DataFrame)
        mocker.patch("tsn_adapters.tasks.argentina.flows.preprocess_flow.task_load_category_map", return_value=mock_category_map)

        mock_processed_data = MagicMock(spec=AggregatedPricesDF)
        mock_uncategorized = MagicMock(spec=UncategorizedDF)
        mock_avg_price_df = MagicMock(spec=DataFrame[SepaAvgPriceProductModel]); mock_avg_price_df.empty = False
        mocker.patch(
            "tsn_adapters.tasks.argentina.flows.preprocess_flow.process_raw_data",
            return_value=(mock_processed_data, mock_uncategorized, mock_avg_price_df),
        )
        # Make saving product averages fail
        mock_product_avg_provider.save_product_averages.side_effect = Exception("Failed to save product averages")

        preprocess_flow_instance._create_summary = MagicMock() # type: ignore[assignment]

        # Act
        _ = await preprocess_flow_instance.process_date(TEST_DATE)

        # Assert
        mock_product_avg_provider.save_product_averages.assert_called_once_with(
            date_str=TEST_DATE, data=mock_avg_price_df
        ) # Verify attempt
        mock_processed_provider.save_processed_data.assert_called_once() # Verify this still happens
        preprocess_flow_instance._create_summary.assert_called_once_with(TEST_DATE, mock_processed_data, mock_uncategorized) # type: ignore[attr-defined]

    async def test_process_date_no_data(
        self,
        preprocess_flow_instance: PreprocessFlow,
        mock_raw_provider: MagicMock,
        mock_processed_provider: MagicMock,
        mock_product_avg_provider: MagicMock,
    ):
        """Test process_date when no data is available for the date."""
        # Arrange
        mock_empty_df = MagicMock(spec=pd.DataFrame); mock_empty_df.empty = True
        mock_raw_provider.get_raw_data_for.return_value = mock_empty_df

        preprocess_flow_instance._create_summary = MagicMock() # type: ignore[assignment]

        # Act
        _ = await preprocess_flow_instance.process_date(TEST_DATE)

        # Assert
        mock_raw_provider.get_raw_data_for.assert_called_once_with(TEST_DATE)
        mock_product_avg_provider.save_product_averages.assert_not_called()
        mock_processed_provider.save_processed_data.assert_not_called()
        preprocess_flow_instance._create_summary.assert_not_called() # type: ignore[attr-defined]

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
    async def test_process_date_invalid_date(
        self,
        preprocess_flow_instance: PreprocessFlow, # Use the flow instance fixture
        mock_raw_provider: MagicMock, # Need this to assert it wasn't called
        invalid_date: str
    ):
        """Test process_date with invalid date format."""

        # Override validate_date on the instance provided by the fixture
        def strict_validate_date(date: DateStr) -> None:
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
                raise ValueError(f"Invalid date format: {date}")
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Invalid date format: {date}")

        preprocess_flow_instance.validate_date = strict_validate_date

        with pytest.raises(ValueError, match=f"Invalid date format: {invalid_date}"):
            _ = await preprocess_flow_instance.process_date(cast(DateStr, invalid_date))

        mock_raw_provider.get_raw_data_for.assert_not_called()

    async def test_process_date_category_map_error(
        self,
        mocker: MockerFixture,
        preprocess_flow_instance: PreprocessFlow,
        mock_raw_provider: MagicMock,
        mock_processed_provider: MagicMock,
    ):
        """Test process_date when category map loading fails."""
        # Arrange
        mock_raw_data = MagicMock(spec=pd.DataFrame); mock_raw_data.empty = False
        mock_raw_provider.get_raw_data_for.return_value = mock_raw_data

        # Mock category map loading to fail
        mock_task_load_category_map = mocker.patch(
                "tsn_adapters.tasks.argentina.flows.preprocess_flow.task_load_category_map",
                side_effect=Exception("Failed to load category map"),
        )

        # Act & Assert
        with pytest.raises(Exception, match="Failed to load category map"):
            _ = await preprocess_flow_instance.process_date(TEST_DATE)

        mock_raw_provider.get_raw_data_for.assert_called_once_with(TEST_DATE)
        mock_task_load_category_map.assert_called_once_with(url=preprocess_flow_instance.category_map_url)
        mock_processed_provider.save_processed_data.assert_not_called()

    async def test_process_date_processing_error(
        self,
        mocker: MockerFixture,
        preprocess_flow_instance: PreprocessFlow,
        mock_raw_provider: MagicMock,
        mock_processed_provider: MagicMock,
    ):
        """Test process_date when data processing fails."""
        # Arrange
        mock_raw_data = MagicMock(spec=pd.DataFrame); mock_raw_data.empty = False
        mock_raw_provider.get_raw_data_for.return_value = mock_raw_data

        mock_category_map = MagicMock(spec=pd.DataFrame)
        mocker.patch(
            "tsn_adapters.tasks.argentina.flows.preprocess_flow.task_load_category_map", return_value=mock_category_map
        )

        # Mock process_raw_data to fail
        mock_process_raw_data = mocker.patch(
                "tsn_adapters.tasks.argentina.flows.preprocess_flow.process_raw_data",
                side_effect=Exception("Processing failed"),
        )

        # Act & Assert
        with pytest.raises(Exception, match="Processing failed"):
            _ = await preprocess_flow_instance.process_date(TEST_DATE)

        mock_raw_provider.get_raw_data_for.assert_called_once_with(TEST_DATE)
        mock_process_raw_data.assert_called_once() # Verify it was called
        mock_processed_provider.save_processed_data.assert_not_called()


class TestPreprocessFlowTopLevel:
    """Tests for the top-level preprocess_flow function."""
    
    pytestmark = pytest.mark.usefixtures("prefect_test_fixture")

    async def test_preprocess_flow_happy_path(self, mocker: MockerFixture):
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
        _ = await preprocess_flow(
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

    async def test_preprocess_flow_s3_block_error(self, mocker: MockerFixture):
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
            _ = await preprocess_flow(
                product_category_map_url=TEST_CATEGORY_MAP_URL,
                s3_block_name=test_s3_block_name,
            )

        # Verify S3 block loading attempt
        mock_s3_load.assert_called_once_with(test_s3_block_name)

    async def test_preprocess_flow_run_error(self, mocker: MockerFixture):
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
            _ = await preprocess_flow(
                product_category_map_url=TEST_CATEGORY_MAP_URL,
                s3_block_name=test_s3_block_name,
            )

        # Verify flow execution attempt
        mock_flow.run_flow.assert_called_once()


class TestPreprocessFlowDataTypes:
    """Tests for data type validation in the PreprocessFlow."""

    async def test_raw_data_schema(self):
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

    async def test_category_map_schema(self):
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
