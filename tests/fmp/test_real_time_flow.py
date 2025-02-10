"""
Tests for the real-time market data sync flow.

This module contains both unit tests for individual components and
integration tests for the complete flow, using mock objects to avoid
actual network calls.
"""

from typing import Optional

import pandas as pd
from pandera.typing import DataFrame
from pydantic import SecretStr
import pytest

from tsn_adapters.blocks.fmp import BatchQuoteShort, FMPBlock
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)
from tsn_adapters.blocks.tn_access import TNAccessBlock
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

EXPECTED_TN_DATA_COLUMNS = {"stream_id", "date", "value"}


def assert_tn_data_schema(df: DataFrame):
    """Helper to assert that a DataFrame has the TN data schema."""
    assert set(df.columns) == EXPECTED_TN_DATA_COLUMNS, f"Got columns: {df.columns}"


# --- Fixtures and Test Classes ---


@pytest.fixture(scope="session", autouse=True)
def include_prefect_in_all_tests(prefect_test_fixture):
    """Include Prefect test harness in all tests."""
    yield prefect_test_fixture


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
    """Mock descriptor block that returns predefined mappings."""

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        """Return fake source to stream mappings."""
        data = {
            "source_id": ["AAPL", "GOOGL"],
            "stream_id": ["stream_aapl", "stream_googl"],
            "source_type": ["stock", "stock"],
        }
        return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(data))


class FakeTNAccessBlock(TNAccessBlock):
    """Mock TNAccessBlock that tracks insertions without making network calls."""

    def __init__(self):
        super().__init__(tn_provider="fake", tn_private_key=SecretStr("fake"))
        self.inserted_records = []

    def batch_insert_tn_records(
        self, records: DataFrame[TnDataRowModel], data_provider: str | None = None
    ) -> Optional[str]:
        """Track inserted records and return a fake BatchInsertResults."""
        self.inserted_records.append(records)
        return "fake_tx_hash"

    def wait_for_tx(self, tx_hash: str) -> None:
        """Mock waiting for transaction - do nothing."""
        pass  # Skip actual transaction waiting in tests


class ErrorFMPBlock(FMPBlock):
    """Mock FMPBlock that simulates API errors."""

    def get_batch_quote(self, symbols: list[str]) -> DataFrame[BatchQuoteShort]:
        """Simulate API error."""
        raise RuntimeError("API Error")


@pytest.fixture
def error_fmp_block():
    """Fixture for error-raising FMP block with retries disabled."""
    return ErrorFMPBlock(api_key=SecretStr("fake"))


class TestRealTimeFlow:
    """Tests for the real-time flow functionality."""

    def test_real_time_flow_success(self):
        """Test the complete real-time flow with mock blocks."""
        fmp_block = FakeFMPBlock(api_key=SecretStr("fake"))
        psd_block = FakePrimitiveSourcesDescriptorBlock()
        tn_block = FakeTNAccessBlock()

        real_time_flow(fmp_block=fmp_block, psd_block=psd_block, tn_block=tn_block, tickers_per_request=1)

        # Verify that data was processed and inserted
        assert len(tn_block.inserted_records) > 0
        inserted_df = tn_block.inserted_records[0]
        assert_tn_data_schema(inserted_df)
        assert "stream_aapl" in inserted_df["stream_id"].values

    @pytest.mark.timeout(5, func_only=True)
    def test_real_time_flow_api_error(self, error_fmp_block):
        """Test the flow's behavior when FMP API calls fail."""
        # Test direct error from the FMP block
        with pytest.raises(RuntimeError, match="API Error"):
            error_fmp_block.get_batch_quote(["AAPL"])

        # Also verify the flow-level error handling with minimal setup
        psd_block = FakePrimitiveSourcesDescriptorBlock()
        tn_block = FakeTNAccessBlock()

        # The flow should fail due to API error
        with pytest.raises(RuntimeError, match="API Error"):
            real_time_flow(
                fmp_block=error_fmp_block,
                psd_block=psd_block,
                tn_block=tn_block,
                tickers_per_request=1,
                fetch_task=fetch_quotes_for_batch.with_options(retries=0),
            )


class TestProcessDataAndDescriptor:
    """Tests for data processing and descriptor handling."""

    def test_process_data(self, sample_quotes_df, sample_descriptor_df):
        """Test processing quote data into TN format."""
        # Convert descriptor_df to plain pandas DataFrame as expected by process_data
        descriptor_df = pd.DataFrame(sample_descriptor_df)

        result = process_data(quotes_df=sample_quotes_df, descriptor_df=descriptor_df)  # type: ignore

        assert_tn_data_schema(result)
        assert len(result) == len(sample_quotes_df)
        assert "stream_aapl" in result["stream_id"].values

    def test_process_data_with_none(self, sample_descriptor_df):
        """Test that process_data raises RuntimeError with detailed error messages when quotes_df is None."""
        with pytest.raises(RuntimeError, match="Cannot process data: quotes DataFrame is None"):
            process_data(None, sample_descriptor_df)  # type: ignore

    def test_process_data_with_exception(self, sample_descriptor_df):
        """Test that process_data raises RuntimeError with exception details when quotes_df is an Exception."""
        test_error = RuntimeError("Test API Error")
        with pytest.raises(RuntimeError, match=f"Cannot process data: quotes DataFrame is {test_error}"):
            process_data(test_error, sample_descriptor_df)  # type: ignore

    def test_get_symbols_from_descriptor(self):
        """Test extracting symbols from descriptor block."""
        psd_block = FakePrimitiveSourcesDescriptorBlock()

        result = get_symbols_from_descriptor(psd_block)  # type: ignore

        assert isinstance(result, DataFrame)
        assert len(result) == 2  # Our fake block returns 2 symbols
        assert all(col in result.columns for col in ["source_id", "stream_id", "source_type"])
        assert "AAPL" in result["source_id"].values
        assert "stream_aapl" in result["stream_id"].values


class TestBatching:
    """Tests for batching functionality."""

    def test_batch_symbols(self, sample_descriptor_df):
        """Test symbol batching logic."""
        batch_size = 2
        batches = batch_symbols(sample_descriptor_df, batch_size)  # type: ignore

        assert len(batches) == 2  # 3 symbols with batch_size=2 should yield 2 batches
        assert len(batches[0]) == 2
        assert len(batches[1]) == 1
        assert all(isinstance(batch, list) for batch in batches)

    def test_combine_batch_results(self, sample_quotes_df):
        """Test combining multiple quote batches."""
        batch1 = sample_quotes_df.iloc[0:2]
        batch2 = sample_quotes_df.iloc[2:]

        combined = combine_batch_results([batch1, batch2])  # type: ignore

        assert isinstance(combined, DataFrame)
        assert len(combined) == len(sample_quotes_df)
        assert all(combined.columns == sample_quotes_df.columns)

    def test_combine_batch_results_with_none(self, sample_quotes_df):
        """Test that combine_batch_results raises RuntimeError with detailed error messages when any batch is None."""
        batch1 = sample_quotes_df.iloc[0:2]
        batch2 = None  # type: ignore

        with pytest.raises(RuntimeError, match="Failed fetching quote batches: Batch 1 is None"):
            combine_batch_results([batch1, batch2])  # type: ignore

    def test_combine_batch_results_with_exception(self, sample_quotes_df):
        """Test that combine_batch_results raises RuntimeError with exception details when a batch is an Exception."""
        batch1 = sample_quotes_df.iloc[0:2]
        test_error = RuntimeError("Test API Error")
        batch2 = test_error

        with pytest.raises(RuntimeError, match=f"Failed fetching quote batches: Batch 1: {test_error}"):
            combine_batch_results([batch1, batch2])  # type: ignore


class TestQuoteConversion:
    """Tests for quote data conversion functionality."""

    @pytest.mark.parametrize(
        "quotes_data, id_mapping, fixed_timestamp, expected_length",
        [
            # Valid case: matching mapping
            (
                {"symbol": ["AAPL", "GOOGL"], "price": [150.0, 2500.0], "volume": [1000000, 500000]},
                {"AAPL": "stream_aapl", "GOOGL": "stream_googl"},
                1234567890,
                2,
            ),
            # Empty DataFrame
            (
                {"symbol": [], "price": [], "volume": []},
                {"AAPL": "stream_aapl"},
                1234567890,
                0,
            ),
            # Missing mapping: the row should be dropped
            (
                {"symbol": ["UNKNOWN"], "price": [100.0], "volume": [1000]},
                {"AAPL": "stream_aapl"},
                1234567890,
                0,
            ),
        ],
    )
    def test_convert_quotes_to_tn_data_parametrized(self, quotes_data, id_mapping, fixed_timestamp, expected_length):
        """Test conversion from quotes to TN data under different scenarios."""
        quotes_df = DataFrame[BatchQuoteShort](pd.DataFrame(quotes_data))
        result = convert_quotes_to_tn_data(quotes_df, id_mapping, fixed_timestamp)
        assert_tn_data_schema(result)
        assert len(result) == expected_length
        if expected_length:
            assert all(result["date"] == str(fixed_timestamp))


if __name__ == "__main__":
    pytest.main(["-v", __file__])
