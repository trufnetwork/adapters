"""
Tests for the historical market data sync flow.

This module contains both unit tests for individual components and
integration tests for the complete flow, using mock objects to avoid
actual network calls.
"""

import datetime
import time
from typing import Optional

import pandas as pd
from pandera.typing import DataFrame
from prefect.testing.utilities import prefect_test_harness
from pydantic import SecretStr
import pytest
from trufnetwork_sdk_py.client import TNClient

from tsn_adapters.blocks.fmp import EODData, FMPBlock
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.flows.fmp.historical_flow import (
    convert_eod_to_tn_df,
    fetch_historical_data,
    get_earliest_data_date,
    historical_flow,
)

# Configure pytest-asyncio
pytestmark = pytest.mark.asyncio

# --- Helper and Common Assertions ---

EXPECTED_TN_DATA_COLUMNS = {"stream_id", "date", "value"}


@pytest.fixture(scope="session", autouse=True)
def include_prefect_in_all_tests(prefect_test_fixture):
    """Include Prefect test harness in all tests."""
    yield prefect_test_fixture

def assert_tn_data_schema(df: pd.DataFrame):
    """Helper to assert that a DataFrame has the TN data schema."""
    assert set(df.columns) == EXPECTED_TN_DATA_COLUMNS, f"Got columns: {df.columns}"


# --- Fixtures and Test Classes ---


@pytest.fixture
def sample_eod_data() -> DataFrame[EODData]:
    """Create a sample EOD data DataFrame."""
    data = {
        "date": ["2024-01-01", "2024-01-02"],
        "symbol": ["AAPL", "AAPL"],
        "price": [151.0, 152.0],
        "volume": [1000000, 1100000],
    }
    return DataFrame[EODData](pd.DataFrame(data))


class FakeFMPBlock(FMPBlock):
    """Mock FMPBlock that returns predefined data."""

    def get_historical_eod_data(
        self, symbol: str, start_date: str | None = None, end_date: str | None = None
    ) -> DataFrame[EODData]:
        """Return fake EOD data."""
        if symbol == "AAPL":
            data = {
                "date": ["2024-01-01", "2024-01-02"],
                "symbol": ["AAPL", "AAPL"],
                "price": [151.0, 152.0],
                "volume": [1000000, 1100000],
            }
            return DataFrame[EODData](pd.DataFrame(data))
        # Return empty DataFrame with correct columns
        return DataFrame[EODData](
            pd.DataFrame(
                {
                    "date": [],
                    "symbol": [],
                    "price": [],
                    "volume": [],
                }
            )
        )


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
    """Fake TN access block that tracks inserted records."""

    def __init__(self):
        # Initialize Pydantic model with required fields
        super().__init__(tn_provider="fake", tn_private_key=SecretStr("fake"))
        # Initialize our tracking variables
        self._inserted_records = []
        self._insert_times = []
        self._batch_sizes = []

    @property
    def inserted_records(self):
        return self._inserted_records

    @property
    def insert_times(self):
        return self._insert_times

    @property
    def batch_sizes(self):
        return self._batch_sizes

    def batch_insert_unix_tn_records(
        self, records: DataFrame[TnDataRowModel], data_provider: str | None = None
    ) -> Optional[str]:
        """Track inserted records and return a fake BatchInsertResults."""
        self._inserted_records.append(records)
        self._insert_times.append(time.time())
        self._batch_sizes.append(len(records))
        return "fake_tx_hash"

    def wait_for_tx(self, tx_hash: str) -> None:
        """Mock waiting."""
        pass

    def get_earliest_date(self, stream_id: str, data_provider: str | None = None) -> datetime.datetime | None:
        """Mock getting earliest date, raising StreamNotFoundError for unknown streams."""
        if stream_id == "unknown":
            raise TNAccessBlock.StreamNotFoundError(f"Stream {stream_id} not found")
        if stream_id.startswith("stream_"):
            return datetime.datetime(2024, 1, 1)
        return None

    def get_client(self) -> TNClient:
        """Mock to prevent real client creation."""
        return None  # type: ignore


class ErrorFMPBlock(FMPBlock):
    """Mock FMPBlock that simulates API errors."""

    def get_historical_eod_data(
        self, symbol: str, start_date: str | None = None, end_date: str | None = None
    ) -> DataFrame[EODData]:
        """Simulate API error."""
        raise RuntimeError("API Error")


@pytest.fixture
def error_fmp_block():
    """Fixture for error-raising FMP block."""
    return ErrorFMPBlock(api_key=SecretStr("fake"))


class TestHistoricalFlow:
    """Tests for the historical flow functionality."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(5, func_only=True)
    async def test_historical_flow_success(self):
        """Test the complete historical flow with mock blocks."""
        fmp_block = FakeFMPBlock(api_key=SecretStr("fake"))
        psd_block = FakePrimitiveSourcesDescriptorBlock()
        tn_block = FakeTNAccessBlock()

        await historical_flow(fmp_block=fmp_block, psd_block=psd_block, tn_block=tn_block)

        # Verify that data was processed and inserted
        assert len(tn_block.inserted_records) > 0
        inserted_df = tn_block.inserted_records[0]
        assert_tn_data_schema(inserted_df)
        assert "stream_aapl" in inserted_df["stream_id"].values

    @pytest.mark.asyncio
    @pytest.mark.timeout(15, func_only=True)
    async def test_historical_flow_api_error(self, error_fmp_block, monkeypatch, mocker):
        """Test the flow's behavior when FMP API calls fail."""
        psd_block = FakePrimitiveSourcesDescriptorBlock()
        tn_block = FakeTNAccessBlock()

        # Mock the tasks with no retries
        mocker.patch(
            "tsn_adapters.flows.fmp.historical_flow.fetch_historical_data",
            side_effect=fetch_historical_data.with_options(retries=0),
        )
        mocker.patch(
            "tsn_adapters.flows.fmp.historical_flow.get_earliest_data_date",
            side_effect=get_earliest_data_date.with_options(retries=0),
        )

        # Flow should complete without raising an exception
        await historical_flow(
            fmp_block=error_fmp_block,
            psd_block=psd_block,
            tn_block=tn_block,
        )

        # Verify that no records were inserted for the failed ticker
        assert len(tn_block.inserted_records) == 0


class TestDataProcessing:
    """Tests for data processing functionality."""

    @pytest.mark.timeout(5, func_only=True)
    def test_get_earliest_data_date(self):
        """Test getting earliest data date for different tickers."""
        tn = FakeTNAccessBlock()
        res = get_earliest_data_date(tn_block=tn, stream_id="stream_aapl")
        assert isinstance(res, datetime.datetime)
        with pytest.raises(TNAccessBlock.StreamNotFoundError):
            get_earliest_data_date(tn_block=tn, stream_id="unknown")

    @pytest.mark.timeout(5, func_only=True)
    def test_fetch_historical_data_success(self, sample_eod_data):
        """Test successful historical data fetching."""
        fmp_block = FakeFMPBlock(api_key=SecretStr("fake"))
        start_date = "2024-01-01"
        end_date = "2024-01-02"

        result = fetch_historical_data(
            fmp_block=fmp_block,
            symbol="AAPL",
            start_date=start_date,
            end_date=end_date,
        )
        assert isinstance(result, DataFrame)
        assert len(result) > 0
        assert all(col in result.columns for col in ["date", "symbol", "price", "volume"])

    @pytest.mark.timeout(5, func_only=True)
    def test_fetch_historical_data_empty(self):
        """Test fetching historical data for a symbol with no data."""
        fmp_block = FakeFMPBlock(api_key=SecretStr("fake"))
        start_date = "2024-01-01"
        end_date = "2024-01-02"

        result = fetch_historical_data(
            fmp_block=fmp_block,
            symbol="UNKNOWN",
            start_date=start_date,
            end_date=end_date,
        )
        assert isinstance(result, DataFrame)
        assert len(result) == 0

    @pytest.mark.timeout(5, func_only=True)
    def test_convert_eod_to_tn_data(self, sample_eod_data):
        """Test conversion from EOD data to TN format."""
        result = convert_eod_to_tn_df(sample_eod_data, "stream_aapl")
        # Convert list of records to DataFrame
        result_df = pd.DataFrame(result)
        assert_tn_data_schema(result_df)
        assert len(result) == len(sample_eod_data)
        # Check stream_id and value directly from DataFrame
        assert all(result_df["stream_id"] == "stream_aapl")
        assert all(result_df["value"].astype(float) > 0)  # Values should be positive prices


class TestHistoricalFlowAdvanced:
    """Advanced tests for historical flow focusing on async behavior and batching."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(5, func_only=True)
    async def test_historical_flow_async(self):
        """Test that the historical flow works correctly in async context."""
        fmp_block = FakeFMPBlock(api_key=SecretStr("fake"))
        psd_block = FakePrimitiveSourcesDescriptorBlock()
        tn_block = FakeTNAccessBlock()

        await historical_flow(
            fmp_block=fmp_block,
            psd_block=psd_block,
            tn_block=tn_block,
            min_fetch_date=datetime.datetime(2023, 1, 1),
        )

        # Verify that data was processed and inserted
        assert len(tn_block.inserted_records) > 0
        inserted_df = tn_block.inserted_records[0]
        assert_tn_data_schema(inserted_df)
        assert "stream_aapl" in inserted_df["stream_id"].values

    @pytest.mark.asyncio
    @pytest.mark.timeout(15, func_only=True)
    async def test_historical_flow_batch_processing(self, monkeypatch):
        """Test that the flow correctly handles batch processing of records."""
        # Mock a smaller batch size for testing
        import tsn_adapters.flows.fmp.historical_flow as flow_module

        TEST_BATCH_SIZE = 2
        monkeypatch.setattr(flow_module, "BATCH_SIZE", TEST_BATCH_SIZE)

        # Create a mock FMP block that returns a small dataset
        class SmallBatchFMPBlock(FakeFMPBlock):
            def get_historical_eod_data(
                self, symbol: str, start_date: str | None = None, end_date: str | None = None
            ) -> DataFrame[EODData]:
                """Return a small dataset to test batching."""
                if symbol != "AAPL":
                    return DataFrame[EODData](
                        pd.DataFrame(
                            {
                                "date": [],
                                "symbol": [],
                                "price": [],
                                "volume": [],
                            }
                        )
                    )

                # Generate 5 records (2.5x TEST_BATCH_SIZE)
                dates = pd.date_range("2023-01-01", periods=5, freq="h")  # Use 'h' instead of 'H'
                data = {
                    "date": dates,
                    "symbol": ["AAPL"] * len(dates),
                    "price": [150.0] * len(dates),
                    "volume": [1000000] * len(dates),
                }
                return DataFrame[EODData](pd.DataFrame(data))

        fmp_block = SmallBatchFMPBlock(api_key=SecretStr("fake"))
        psd_block = FakePrimitiveSourcesDescriptorBlock()
        tn_block = FakeTNAccessBlock()

        await historical_flow(
            fmp_block=fmp_block,
            psd_block=psd_block,
            tn_block=tn_block,
            min_fetch_date=datetime.datetime(2023, 1, 1),
        )

        # Verify that data was processed in batches
        assert len(tn_block.inserted_records) == 1  # Should have 1 batch with all records
        total_records = sum(len(df) for df in tn_block.inserted_records)
        assert total_records == 5  # All records should be processed

        # Verify batch size
        assert len(tn_block.inserted_records[0]) == 5  # All records in one batch
        assert all(tn_block.inserted_records[0]["stream_id"] == "stream_aapl"), "All records should be from AAPL"

    @pytest.mark.asyncio
    @pytest.mark.timeout(15, func_only=True)
    async def test_historical_flow_sequential_processing(self, monkeypatch, mocker):
        """Test that ticker processing is sequential and waits for TN insertion."""
        import time

        import tsn_adapters.flows.fmp.historical_flow as flow_module

        # Mock a smaller batch size
        TEST_BATCH_SIZE = 2
        monkeypatch.setattr(flow_module, "BATCH_SIZE", TEST_BATCH_SIZE)

        # Mock the tasks with no retries
        mocker.patch(
            "tsn_adapters.flows.fmp.historical_flow.fetch_historical_data",
            side_effect=fetch_historical_data.with_options(retries=0),
        )
        mocker.patch(
            "tsn_adapters.flows.fmp.historical_flow.get_earliest_data_date",
            side_effect=get_earliest_data_date.with_options(retries=0),
        )

        class TimingFMPBlock(FakeFMPBlock):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.fetch_times = {}  # Track when each symbol's data is fetched

            def get_historical_eod_data(
                self, symbol: str, start_date: str | None = None, end_date: str | None = None
            ) -> DataFrame[EODData]:
                """Track when each symbol's data is fetched."""
                self.fetch_times[symbol] = time.time()
                # Add a small delay to make timing more obvious
                time.sleep(0.1)
                return super().get_historical_eod_data(symbol, start_date, end_date)

        class TimingTNAccessBlock(FakeTNAccessBlock):
            def __init__(self):
                super().__init__()
                self._insert_times = []

            def batch_insert_unix_tn_records(
                self, records: DataFrame[TnDataRowModel], data_provider: str | None = None
            ) -> Optional[str]:
                time.sleep(0.05)
                self._insert_times.append(time.time())
                return super().batch_insert_unix_tn_records(records, data_provider)

            def get_earliest_date(self, stream_id: str, data_provider: str | None = None) -> datetime.datetime:
                return datetime.datetime(2024, 1, 1)

            def wait_for_tx(self, tx_hash: str) -> None:
                time.sleep(0.02)

        # Create blocks with timing tracking
        fmp_block = TimingFMPBlock(api_key=SecretStr("fake"))
        tn_block = TimingTNAccessBlock()

        # Create a descriptor block with multiple tickers that will generate enough data for batching
        class MultiTickerDescriptorBlock(PrimitiveSourcesDescriptorBlock):
            def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
                data = {
                    "source_id": ["AAPL", "AAPL", "AAPL", "AAPL"],  # Multiple entries to generate more data
                    "stream_id": ["stream_aapl"] * 4,
                    "source_type": ["stock"] * 4,
                }
                return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(data))

        psd_block = MultiTickerDescriptorBlock()

        # Run the flow
        await historical_flow(
            fmp_block=fmp_block,
            psd_block=psd_block,
            tn_block=tn_block,
            min_fetch_date=datetime.datetime(2023, 1, 1),
        )

        # Verify sequential processing
        fetch_times = list(fmp_block.fetch_times.values())
        insert_times = tn_block.insert_times

        # We should have at least two batches
        assert len(insert_times) >= 2, "Expected at least two batch insertions"

        # For each batch insertion except the last one, verify that the next fetch happened after the insertion
        for i in range(len(insert_times) - 1):
            batch_insert_time = insert_times[i]
            next_fetch_index = (i + 1) * TEST_BATCH_SIZE
            if next_fetch_index < len(fetch_times):
                next_fetch_time = fetch_times[next_fetch_index]
                assert next_fetch_time > batch_insert_time, (
                    f"Fetch {next_fetch_index} (at {next_fetch_time}) should happen after "
                    f"batch {i} insertion (at {batch_insert_time})"
                )

    @pytest.mark.asyncio
    @pytest.mark.timeout(15, func_only=True)
    async def test_historical_flow_batch_accumulation(self, monkeypatch, mocker):
        """
        Test that the flow properly handles backpressure by controlling data accumulation and processing.

        This test verifies that:
        1. Records are accumulated until reaching the batch size
        2. Each batch is inserted only after it reaches the target size
        3. Records are processed sequentially
        4. Memory pressure is controlled by not accumulating too much data at once

        The test uses 7 tickers that each produce 1 record:
        - First 4 records form a complete batch
        - Remaining 3 records form a partial batch
        """
        import tsn_adapters.flows.fmp.historical_flow as flow_module

        # Mock a smaller batch size
        TEST_BATCH_SIZE = 4  # We'll make each ticker return 1 record, so need 4 tickers for a batch
        monkeypatch.setattr(flow_module, "BATCH_SIZE", TEST_BATCH_SIZE)

        # Mock the tasks with no retries and return past date
        mocker.patch(
            "tsn_adapters.flows.fmp.historical_flow.fetch_historical_data",
            side_effect=fetch_historical_data.with_options(retries=0),
        )
        mocker.patch(
            "tsn_adapters.flows.fmp.historical_flow.get_earliest_data_date",
            return_value=datetime.datetime(2020, 1, 1),
        )

        # Create a sequence tracker to monitor execution order
        class SequenceTracker:
            def __init__(self):
                self.sequence = []
                self._counter = 0
            
            def next(self, operation: str) -> int:
                self._counter += 1
                self.sequence.append((self._counter, operation))
                return self._counter

        sequence = SequenceTracker()

        class SequencedFMPBlock(FakeFMPBlock):
            def __init__(self, sequence_tracker: SequenceTracker, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.sequence = sequence_tracker
                self.fetch_order = {}

            def get_historical_eod_data(
                self, symbol: str, start_date: str | None = None, end_date: str | None = None
            ) -> DataFrame[EODData]:
                """Return single record per symbol and track fetch sequence."""
                seq_num = self.sequence.next(f"fetch_{symbol}")
                self.fetch_order[symbol] = seq_num
                # Return just one record per symbol
                data = {
                    "date": ["2024-01-01"],
                    "symbol": [symbol],
                    "price": [150.0],
                    "volume": [1000000],
                }
                return DataFrame[EODData](pd.DataFrame(data))

        class SequencedTNAccessBlock(FakeTNAccessBlock):
            def __init__(self, sequence_tracker: SequenceTracker):
                super().__init__()
                self.sequence = sequence_tracker
                self.insert_order = []
                self._batch_sizes: list[int] = []

            @property
            def batch_sizes(self) -> list[int]:
                return self._batch_sizes

            def batch_insert_unix_tn_records(
                self, records: DataFrame[TnDataRowModel], data_provider: str | None = None
            ) -> Optional[str]:
                """Track inserted records with sequence information."""
                seq_num = self.sequence.next(f"insert_batch_{len(records)}")
                self.insert_order.append(seq_num)
                self._batch_sizes.append(len(records))
                self._inserted_records.append(records)
                return "fake_tx_hash"

            def get_earliest_date(self, stream_id: str, data_provider: str | None = None) -> datetime.datetime:
                return datetime.datetime(2024, 1, 1)

        # Create blocks with sequence tracking
        fmp_block = SequencedFMPBlock(sequence_tracker=sequence, api_key=SecretStr("fake"))
        tn_block = SequencedTNAccessBlock(sequence_tracker=sequence)

        # Create a descriptor block with enough tickers to create multiple batches
        class SequencedDescriptorBlock(PrimitiveSourcesDescriptorBlock):
            def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
                # Create 7 tickers - should result in one full batch of 4 and partial batch of 3
                tickers = [f"TICK{i}" for i in range(7)]
                data = {
                    "source_id": tickers,
                    "stream_id": [f"stream_{t.lower()}" for t in tickers],
                    "source_type": ["stock"] * len(tickers),
                }
                return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(data))

        psd_block = SequencedDescriptorBlock()

        # Run the flow
        await historical_flow(
            fmp_block=fmp_block,
            psd_block=psd_block,
            tn_block=tn_block,
            min_fetch_date=datetime.datetime(2023, 1, 1),
        )

        # Verify we got the expected number of batches with correct sizes
        assert tn_block.batch_sizes == [TEST_BATCH_SIZE, 3], "Expected one full batch and one partial batch"

        # Get sequence information
        all_operations = sequence.sequence
        
        # Verify operations are sequential
        fetch_operations = [(seq, op) for seq, op in all_operations if op.startswith("fetch_")]
        insert_operations = [(seq, op) for seq, op in all_operations if op.startswith("insert_batch_")]
        
        # Verify we have the right number of operations
        assert len(fetch_operations) == 7, "Should have 7 fetch operations"
        assert len(insert_operations) == 2, "Should have 2 insert operations"
        
        # Verify fetches are sequential
        for i in range(len(fetch_operations) - 1):
            curr_seq = fetch_operations[i][0]
            next_seq = fetch_operations[i + 1][0]
            assert curr_seq < next_seq, f"Fetch operations should be sequential, but {curr_seq} came before {next_seq}"
        
        # Verify first batch insert happens after 4 fetches
        first_insert_seq = insert_operations[0][0]
        fetches_before_first_insert = len([seq for seq, _ in fetch_operations if seq < first_insert_seq])
        assert fetches_before_first_insert == TEST_BATCH_SIZE, "First batch should be inserted after 4 fetches"
        
        # Verify second batch insert happens after all fetches
        second_insert_seq = insert_operations[1][0]
        fetches_before_second_insert = len([seq for seq, _ in fetch_operations if seq < second_insert_seq])
        assert fetches_before_second_insert == 7, "Second batch should be inserted after all fetches"
        
        # Verify batch sizes are correct
        assert insert_operations[0][1] == f"insert_batch_{TEST_BATCH_SIZE}", "First batch should have TEST_BATCH_SIZE records"
        assert insert_operations[1][1] == "insert_batch_3", "Second batch should have 3 records"

    @pytest.mark.asyncio
    @pytest.mark.timeout(15, func_only=True)
    async def test_historical_flow_large_fetch(self, monkeypatch, mocker):
        """
        Test that the flow correctly handles fetches larger than batch size.

        The flow should:
        1. Accumulate records until exceeding BATCH_SIZE
        2. When exceeded, process ALL accumulated records at once via split_and_insert_records_unix
        3. Start fresh accumulation for next records
        """
        import tsn_adapters.flows.fmp.historical_flow as flow_module

        # Mock a smaller batch size
        TEST_BATCH_SIZE = 3
        monkeypatch.setattr(flow_module, "BATCH_SIZE", TEST_BATCH_SIZE)

        # Create a mock FMP block that returns a large dataset for one ticker
        class LargeFetchFMPBlock(FakeFMPBlock):
            def get_historical_eod_data(
                self, symbol: str, start_date: str | None = None, end_date: str | None = None
            ) -> DataFrame[EODData]:
                """Return a large dataset for TICK0, small for others."""
                if symbol == "TICK0":
                    # Generate 8 records (> 2x TEST_BATCH_SIZE)
                    dates = pd.date_range("2023-01-01", periods=8, freq="h")
                    data = {
                        "date": dates,
                        "symbol": ["TICK0"] * len(dates),
                        "price": [150.0] * len(dates),
                        "volume": [1000000] * len(dates),
                    }
                    return DataFrame[EODData](pd.DataFrame(data))
                elif symbol == "TICK1":
                    # Generate 2 records (< TEST_BATCH_SIZE)
                    dates = pd.date_range("2023-01-01", periods=2, freq="h")
                    data = {
                        "date": dates,
                        "symbol": ["TICK1"] * len(dates),
                        "price": [160.0] * len(dates),
                        "volume": [1100000] * len(dates),
                    }
                    return DataFrame[EODData](pd.DataFrame(data))
                return DataFrame[EODData](
                    pd.DataFrame({"date": [], "symbol": [], "price": [], "volume": []})
                )

        # Create a descriptor block with two tickers
        class LargeFetchDescriptorBlock(PrimitiveSourcesDescriptorBlock):
            def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
                data = {
                    "source_id": ["TICK0", "TICK1"],  # TICK0 will return large dataset
                    "stream_id": ["stream_tick0", "stream_tick1"],
                    "source_type": ["stock"] * 2,
                }
                return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(data))

        # Mock the tasks with no retries
        mocker.patch(
            "tsn_adapters.flows.fmp.historical_flow.fetch_historical_data",
            side_effect=fetch_historical_data.with_options(retries=0),
        )
        mocker.patch(
            "tsn_adapters.flows.fmp.historical_flow.get_earliest_data_date",
            return_value=datetime.datetime(2020, 1, 1),
        )

        # Create blocks
        fmp_block = LargeFetchFMPBlock(api_key=SecretStr("fake"))
        psd_block = LargeFetchDescriptorBlock()
        tn_block = FakeTNAccessBlock()

        # Run the flow
        await historical_flow(
            fmp_block=fmp_block,
            psd_block=psd_block,
            tn_block=tn_block,
            min_fetch_date=datetime.datetime(2023, 1, 1),
        )

        # Verify batch handling
        batch_sizes = tn_block.batch_sizes
        assert len(batch_sizes) == 2, "Expected 2 batches: one for TICK0's 8 records, one for TICK1's 2 records"
        
        # First batch should contain all TICK0 records (processed when exceeding BATCH_SIZE)
        assert batch_sizes[0] == 8, "First batch should contain all TICK0 records"
        assert all(tn_block.inserted_records[0]["stream_id"] == "stream_tick0"), "First batch should be all TICK0"
        
        # Second batch should contain TICK1 records (processed at end of pipeline)
        assert batch_sizes[1] == 2, "Second batch should contain TICK1 records"
        assert all(tn_block.inserted_records[1]["stream_id"] == "stream_tick1"), "Second batch should be all TICK1"

        # Verify total records
        total_records = sum(batch_sizes)
        assert total_records == 10, "Expected 10 total records (8 from TICK0 + 2 from TICK1)"


if __name__ == "__main__":
    pytest.main(["-v", __file__])
