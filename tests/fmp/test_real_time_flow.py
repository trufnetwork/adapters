"""
Tests for the real-time market data sync flow.

This module contains both unit tests for individual components and
integration tests for the complete flow, using mock objects to avoid
actual network calls.
"""

from datetime import timedelta
from typing import Any, Union

import pandas as pd
from pandera.typing import DataFrame
from pydantic import SecretStr
import pytest
from tests.utils.fake_tn_access import FakeTNAccessBlock

from tsn_adapters.blocks.fmp import BatchQuoteShort, FMPBlock
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)
from tsn_adapters.blocks.tn_access import convert_to_typed_df
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.flows.fmp.real_time_flow import (
    batch_symbols,
    combine_batch_results,
    convert_quotes_to_tn_data,
    fetch_quotes_for_batch,
    get_symbols_from_descriptor,
    process_data,
    real_time_flow,
)

# --- Helper and Common Assertions ---

EXPECTED_TN_DATA_COLUMNS = {"data_provider", "stream_id", "date", "value"}


def assert_tn_data_schema(df: DataFrame[TnDataRowModel]) -> None:
    """Helper to assert that a DataFrame has the TN data schema."""
    assert set(df.columns) == EXPECTED_TN_DATA_COLUMNS, f"Got columns: {df.columns}"


# --- Fixtures and Test Classes ---


@pytest.fixture
def sample_descriptor_df() -> DataFrame[PrimitiveSourceDataModel]:
    """Create a sample descriptor DataFrame with source_id to stream_id mappings."""
    data = {
        "source_id": ["AAPL", "GOOGL", "MSFT"],
        "stream_id": ["stream_aapl", "stream_googl", "stream_msft"],
        "source_type": ["stock", "stock", "stock"],
    }
    return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(data))


@pytest.fixture
def sample_quotes_df() -> DataFrame[BatchQuoteShort]:
    """Create a sample quotes DataFrame with price and volume data."""
    data = {
        "symbol": ["AAPL", "GOOGL", "MSFT"],
        "price": [150.0, 2500.0, 300.0],
        "volume": [1000000, 500000, 750000],
    }
    return DataFrame[BatchQuoteShort](pd.DataFrame(data))


class FakeFMPBlock(FMPBlock):
    """Mock FMPBlock that returns predefined data."""

    def get_batch_quote(self, symbols: list[str]) -> DataFrame[BatchQuoteShort]:
        """Return fake quote data for the requested symbols."""
        data = {
            "symbol": symbols,
            "price": [150.0] * len(symbols),
            "volume": [1000000] * len(symbols),
        }
        return DataFrame[BatchQuoteShort](pd.DataFrame(data))


class FakePrimitiveSourcesDescriptorBlock(PrimitiveSourcesDescriptorBlock):
    """Mock descriptor block for testing."""

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        """Return a mock descriptor."""
        data = {
            "source_type": ["fmp", "fmp", "fmp"],
            "source_id": ["AAPL", "GOOGL", "MSFT"],
            "stream_id": ["stream_aapl", "stream_googl", "stream_msft"],
        }
        return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(data))


class ErrorFMPBlock(FMPBlock):
    """Mock FMPBlock that simulates API errors."""

    def get_batch_quote(self, symbols: list[str]) -> DataFrame[BatchQuoteShort]:
        """Simulate API error."""
        raise RuntimeError("API Error")


@pytest.fixture
def error_fmp_block():
    """Fixture for error-raising FMP block with retries disabled."""
    return ErrorFMPBlock(api_key=SecretStr("fake"))


class NullPriceFMPBlock(FMPBlock):
    """Mock FMP block that returns quotes with null prices."""

    def __init__(self) -> None:
        """Initialize with fake API key."""
        super().__init__(api_key=SecretStr("fake"))

    def get_batch_quote(self, symbols: list[str]) -> DataFrame[BatchQuoteShort]:
        """Return quote data with some null prices."""
        data = {
            "symbol": ["AAPL", "GOOGL", "MSFT"],
            "price": [150.0, None, 300.0],
            "volume": [1000000, 500000, 750000],
        }
        df = pd.DataFrame(data)
        return DataFrame[BatchQuoteShort](df)


@pytest.fixture(autouse=True, scope="session")
def prefect_on_all_tests(prefect_test_fixture: Any):
    """Fixture to ensure Prefect is configured for testing."""
    pass


class TestRealTimeFlow:
    """Tests for the real-time flow functionality."""

    def test_real_time_flow_success(self):
        """Test the complete real-time flow with mock blocks."""
        fmp_block = FakeFMPBlock(api_key=SecretStr("fake"))
        psd_block = FakePrimitiveSourcesDescriptorBlock()
        tn_block = FakeTNAccessBlock()
        tn_block.set_deployed_streams({"stream_aapl", "stream_googl", "stream_msft"})

        real_time_flow(
            fmp_block=fmp_block,
            psd_block=psd_block,
            tn_block=tn_block,
            tickers_per_request=1,
        )

        # Verify that data was processed and inserted
        assert len(tn_block.inserted_records) > 0
        inserted_df = tn_block.inserted_records[0]
        assert_tn_data_schema(inserted_df)
        assert "stream_aapl" in inserted_df["stream_id"].values

    @pytest.mark.timeout(5, func_only=True)
    def test_real_time_flow_api_error(self, error_fmp_block: ErrorFMPBlock):
        """Test the flow's behavior when FMP API calls fail."""
        # Test direct error from the FMP block
        with pytest.raises(RuntimeError, match="API Error"):
            error_fmp_block.get_batch_quote(["AAPL"])

        # Also verify the flow-level error handling with minimal setup
        psd_block = FakePrimitiveSourcesDescriptorBlock()
        tn_block = FakeTNAccessBlock()
        # Streams need to be 'deployed' in the fake for the flow to attempt processing
        tn_block.set_deployed_streams({"stream_aapl"})

        # The flow should capture the error and return a failed result
        result = real_time_flow(
            fmp_block=error_fmp_block,
            psd_block=psd_block,
            tn_block=tn_block,
            tickers_per_request=1,
            fetch_task=fetch_quotes_for_batch.with_options(retries=0),
        )
        assert result["success"] is False
        assert "API Error" in result["errors"][0]


class TestProcessDataAndDescriptor:
    """Tests for data processing and descriptor handling."""

    def test_process_data(
        self, sample_quotes_df: DataFrame[BatchQuoteShort], sample_descriptor_df: DataFrame[PrimitiveSourceDataModel]
    ):
        """Test processing quote data into TN format."""
        # Convert descriptor_df to plain pandas DataFrame as expected by process_data
        descriptor_df = convert_to_typed_df(pd.DataFrame(sample_descriptor_df), PrimitiveSourceDataModel)

        result = process_data(quotes_df=sample_quotes_df, descriptor_df=descriptor_df)

        assert_tn_data_schema(result)
        assert len(result) == len(sample_quotes_df)
        assert "stream_aapl" in result["stream_id"].values

    def test_process_data_with_none(self, sample_descriptor_df: DataFrame[PrimitiveSourceDataModel]):
        """Test that process_data raises RuntimeError with detailed error messages when quotes_df is None."""
        with pytest.raises(RuntimeError, match="Cannot process data: quotes DataFrame is None"):
            process_data(None, sample_descriptor_df)  # type: ignore

    def test_process_data_with_exception(self, sample_descriptor_df: DataFrame[PrimitiveSourceDataModel]):
        """Test that process_data raises RuntimeError with exception details when quotes_df is an Exception."""
        test_error = RuntimeError("Test API Error")
        with pytest.raises(RuntimeError, match=f"Cannot process data: quotes DataFrame is {test_error}"):
            process_data(test_error, sample_descriptor_df)  # type: ignore

    def test_get_symbols_from_descriptor(self):
        """Test extracting symbols from descriptor block."""
        psd_block = FakePrimitiveSourcesDescriptorBlock()

        result = get_symbols_from_descriptor(psd_block)  # type: ignore

        assert isinstance(result, DataFrame)
        assert len(result) == 3  # Our fake block returns 3 symbols
        assert all(col in result.columns for col in ["source_id", "stream_id", "source_type"])
        assert "AAPL" in result["source_id"].values
        assert "stream_aapl" in result["stream_id"].values


class TestBatching:
    """Tests for batching functionality."""

    def test_batch_symbols(self, sample_descriptor_df: DataFrame[PrimitiveSourceDataModel]):
        """Test symbol batching logic."""
        batch_size = 2
        batches = batch_symbols(sample_descriptor_df, batch_size)  # type: ignore

        assert len(batches) == 2  # 3 symbols with batch_size=2 should yield 2 batches
        assert len(batches[0]) == 2
        assert len(batches[1]) == 1
        assert all(isinstance(batch, list) for batch in batches)

    def test_combine_batch_results(self, sample_quotes_df: DataFrame[BatchQuoteShort]):
        """Test combining multiple quote batches."""
        batch1 = sample_quotes_df.iloc[0:2]
        batch2 = sample_quotes_df.iloc[2:]

        combined = combine_batch_results(batch1, batch2)  # type: ignore

        assert isinstance(combined, DataFrame)
        assert len(combined) == len(sample_quotes_df)
        assert all(combined.columns == sample_quotes_df.columns)

    def test_combine_batch_results_with_none(self, sample_quotes_df: DataFrame[BatchQuoteShort]):
        """Test that combine_batch_results raises RuntimeError with detailed error messages when any batch is None."""
        batch1 = sample_quotes_df.iloc[0:2]
        batch2 = None  # type: ignore

        with pytest.raises(RuntimeError, match="Failed fetching quote batches: Batch 2 is None"):
            combine_batch_results(batch1, batch2)  # type: ignore

    def test_combine_batch_results_with_exception(self, sample_quotes_df: DataFrame[BatchQuoteShort]):
        """Test that combine_batch_results raises RuntimeError with exception details when a batch is an Exception."""
        batch1 = sample_quotes_df.iloc[0:2]
        test_error = RuntimeError("Test API Error")
        batch2 = test_error

        with pytest.raises(RuntimeError, match=f"Failed fetching quote batches: Batch 2: {test_error}"):
            combine_batch_results(batch1, batch2)  # type: ignore


class TestQuoteConversion:
    """Tests for quote data conversion functionality."""

    @pytest.mark.parametrize(
        "quotes_data, id_mapping, fixed_timestamp, expected_length",
        [
            # Valid case: matching mapping
            (
                {"symbol": ["AAPL", "GOOGL"], "price": [150.0, 2500.0], "volume": [1000000, 500000]},
                {"AAPL": "stream_aapl", "GOOGL": "stream_googl"},
                1704086400,
                2,
            ),
            # Empty DataFrame
            (
                {"symbol": [], "price": [], "volume": []},
                {"AAPL": "stream_aapl"},
                1704086400,
                0,
            ),
            # Missing mapping: the row should be dropped
            (
                {"symbol": ["UNKNOWN"], "price": [100.0], "volume": [1000]},
                {"AAPL": "stream_aapl"},
                1704086400,
                0,
            ),
        ],
    )
    def test_convert_quotes_to_tn_data_parametrized(
        self,
        quotes_data: dict[str, list[Union[str, float, int]]],
        id_mapping: dict[str, str],
        fixed_timestamp: int,
        expected_length: int,
    ):
        """Test conversion from quotes to TN data under different scenarios."""
        quotes_df = DataFrame[BatchQuoteShort](pd.DataFrame(quotes_data))
        result = convert_quotes_to_tn_data(quotes_df, id_mapping, fixed_timestamp)
        assert_tn_data_schema(result)
        assert len(result) == expected_length
        if expected_length:
            assert all(result["date"] == fixed_timestamp)


class TestNullPriceHandling:
    """Test handling of null prices in the real-time flow.

    The implementation should:
    1. Accept null prices in the BatchQuoteShort model
    2. Filter out null prices during processing
    3. Log which symbols had null prices
    4. Continue processing valid prices
    5. Report filtering statistics in FlowResult
    """

    @pytest.mark.usefixtures("prefect_test_fixture")
    def test_batch_quote_with_null_prices(self):
        """Test that implementation accepts null prices and processes valid ones."""
        block = NullPriceFMPBlock()
        quotes = block.get_batch_quote(["AAPL", "GOOGL", "MSFT"])

        # Verify that quotes contains all symbols including null prices
        assert len(quotes) == 3
        assert quotes["symbol"].tolist() == ["AAPL", "GOOGL", "MSFT"]

        # Check prices directly from the DataFrame
        assert quotes.iloc[0]["price"] == 150.0  # First price
        assert quotes["price"].isna().iloc[1]  # Second price should be null
        assert quotes.iloc[2]["price"] == 300.0  # Third price

        assert quotes["volume"].tolist() == [1000000, 500000, 750000]

    @pytest.mark.usefixtures("prefect_test_fixture")
    def test_real_time_flow_with_null_prices(self):
        """Test that real-time flow handles null prices correctly.

        The flow should:
        1. Continue processing when encountering null prices
        2. Successfully process valid prices
        3. Report filtering statistics
        4. Maintain data quality
        """
        block = NullPriceFMPBlock()
        psd_block = FakePrimitiveSourcesDescriptorBlock()
        tn_block = FakeTNAccessBlock()
        tn_block.set_deployed_streams({"stream_aapl", "stream_googl", "stream_msft"})

        result = real_time_flow(
            fmp_block=block,
            psd_block=psd_block,
            tn_block=tn_block,
            tickers_per_request=3,
        )

        # Verify flow success
        assert result["success"] is True
        assert result["processed_quotes"] == 2
        assert result["filtered_quotes"] == 1
        assert result["failed_batches"] == 0
        assert len(result["errors"]) == 0

        # Verify data quality
        assert len(tn_block.inserted_records) == 1
        inserted_df = tn_block.inserted_records[0]
        assert_tn_data_schema(inserted_df)
        assert len(inserted_df) == 2  # Only AAPL and MSFT should be processed
        assert "stream_aapl" in inserted_df["stream_id"].values
        assert all(pd.notna(inserted_df["value"].values))  # All values should be non-null

    @pytest.mark.usefixtures("prefect_test_fixture")
    def test_real_time_flow_all_null_prices(self):
        """Test flow behavior when all prices are null."""

        class AllNullPricesFMPBlock(FMPBlock):
            def get_batch_quote(self, symbols: list[str]) -> DataFrame[BatchQuoteShort]:
                """Return quote data with all null prices."""
                data = {
                    "symbol": ["AAPL", "GOOGL"],
                    "price": [None, None],
                    "volume": [1000000, 500000],
                }
                df = pd.DataFrame(data)
                return DataFrame[BatchQuoteShort](df)

        block = AllNullPricesFMPBlock(api_key=SecretStr("fake"))
        psd_block = FakePrimitiveSourcesDescriptorBlock()
        tn_block = FakeTNAccessBlock()
        # Need to set deployed streams for filter to pass even if no data is inserted
        tn_block.set_deployed_streams({"stream_aapl", "stream_googl", "stream_msft"})

        result = real_time_flow(
            fmp_block=block,
            psd_block=psd_block,
            tn_block=tn_block,
            tickers_per_request=2,
        )

        # Verify flow reports correctly
        assert result["success"] is True  # Flow should succeed even with all nulls
        assert result["processed_quotes"] == 0
        assert result["filtered_quotes"] == 3
        assert result["failed_batches"] == 0
        assert len(result["errors"]) == 0

        # Verify no data was inserted
        assert len(tn_block.inserted_records) == 0


if __name__ == "__main__":
    pytest.main(["-v", __file__])
