"""
Tests for the historical market data sync flow.

This module contains both unit tests for individual components and
integration tests for the complete flow, using mock objects to avoid
actual network calls.
"""

import datetime
import time
from typing import Any, Callable, Optional

import pandas as pd
from pandera.typing import DataFrame
from pydantic import SecretStr
import pytest
from pytest import MonkeyPatch
from pytest_mock import MockerFixture
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

# --- Helper Functions and Common Assertions ---

EXPECTED_TN_DATA_COLUMNS = {"data_provider", "stream_id", "date", "value"}


def assert_tn_data_schema(df: pd.DataFrame):
    """Helper to assert that a DataFrame has the TN data schema."""
    assert set(df.columns) == EXPECTED_TN_DATA_COLUMNS, f"Got columns: {df.columns}"


def assert_all_records_for_stream(df: pd.DataFrame, stream_id: str):
    """Helper to assert that all records in a DataFrame belong to a given stream."""
    assert all(df["stream_id"] == stream_id), f"Not all records are for {stream_id}"


def assert_batch_sizes(batch_sizes: list[int], expected_sizes: list[int]):
    """Helper to assert that batch sizes match expected sizes."""
    assert batch_sizes == expected_sizes, f"Expected batch sizes {expected_sizes}, got {batch_sizes}"


def parse_iso_date(date_str: str) -> datetime.datetime:
    """Parse ISO format date string to datetime object in UTC."""
    return datetime.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)


class SequenceTracker:
    """Helper class to track execution sequence of operations."""

    def __init__(self):
        self.sequence: list[tuple[int, str]] = []
        self._counter = 0

    def next(self, operation: str) -> int:
        """Record next operation in sequence."""
        self._counter += 1
        self.sequence.append((self._counter, operation))
        return self._counter

    def get_operations_by_type(self, prefix: str) -> list[tuple[int, str]]:
        """Get all operations that start with the given prefix."""
        return [(seq, op) for seq, op in self.sequence if op.startswith(prefix)]

    def assert_sequential_operations(self, prefix: str):
        """Assert that operations of a given type occurred in sequence."""
        operations = self.get_operations_by_type(prefix)
        for i in range(len(operations) - 1):
            curr_seq = operations[i][0]
            next_seq = operations[i + 1][0]
            assert (
                curr_seq < next_seq
            ), f"{prefix} operations should be sequential, but {curr_seq} came before {next_seq}"


# --- Common Mock Blocks ---


class FakeFMPBlock(FMPBlock):
    """Mock FMPBlock that returns predefined data."""

    def get_historical_eod_data(
        self, symbol: str, start_date: str | None = None, end_date: str | None = None
    ) -> DataFrame[EODData]:
        """Return fake EOD data."""
        if symbol == "AAPL":
            # Use proper ISO dates
            data = {
                "date": ["2024-01-01", "2024-01-02"],  # ISO format YYYY-MM-DD
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
        self._inserted_records: list[DataFrame[TnDataRowModel]] = []
        self._insert_times: list[float] = []
        self._batch_sizes: list[int] = []

    @property
    def inserted_records(self) -> list[DataFrame[TnDataRowModel]]:
        return self._inserted_records

    @property
    def insert_times(self) -> list[float]:
        return self._insert_times

    @property
    def batch_sizes(self) -> list[int]:
        return self._batch_sizes

    def stream_exists(self, data_provider: str, stream_id: str) -> bool:
        """Mock stream existence check."""
        return stream_id.startswith("stream_")

    def batch_insert_tn_records(
        self,
        records: DataFrame[TnDataRowModel],
        is_unix: bool = False,
        has_external_created_at: bool = False,
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
    def get_historical_eod_data(
        self, symbol: str, start_date: str | None = None, end_date: str | None = None
    ) -> DataFrame[EODData]:
        """Simulate API error."""
        raise RuntimeError("API Error")


# --- Fixtures ---


@pytest.fixture(scope="session", autouse=True)
def include_prefect_in_all_tests(prefect_test_fixture: Any):
    """Include Prefect test harness in all tests."""
    yield prefect_test_fixture


@pytest.fixture
def fake_fmp_block() -> FMPBlock:
    """Fixture for basic FMP block."""
    return FakeFMPBlock(api_key=SecretStr("fake"))


@pytest.fixture
def fake_psd_block() -> PrimitiveSourcesDescriptorBlock:
    """Fixture for basic PSD block."""
    return FakePrimitiveSourcesDescriptorBlock()


@pytest.fixture
def fake_tn_block() -> FakeTNAccessBlock:
    """Fixture for basic TN block."""
    return FakeTNAccessBlock()


@pytest.fixture
def error_fmp_block():
    """Fixture for error-raising FMP block."""



    return ErrorFMPBlock(api_key=SecretStr("fake"))


@pytest.fixture
def set_test_batch_size(monkeypatch: MonkeyPatch):
    """Fixture to set test batch size."""

    def _set_batch_size(size: int):
        import tsn_adapters.flows.fmp.historical_flow as flow_module

        monkeypatch.setattr(flow_module, "BATCH_SIZE", size)

    return _set_batch_size


@pytest.fixture
def sample_eod_data() -> DataFrame[EODData]:
    """Create a sample EOD data DataFrame."""
    data = {
        "date": ["2024-01-01", "2024-01-02"],  # ISO format YYYY-MM-DD, comes from their API
        "symbol": ["AAPL", "AAPL"],
        "price": [151.0, 152.0],
        "volume": [1000000, 1100000],
    }
    df = DataFrame[EODData](pd.DataFrame(data))
    return df


# --- Test Classes ---


class TestHistoricalFlow:
    """Tests for the historical flow functionality."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(30, func_only=True)
    async def test_historical_flow_success(
        self,
        fake_fmp_block: FakeFMPBlock,
        fake_psd_block: FakePrimitiveSourcesDescriptorBlock,
        fake_tn_block: FakeTNAccessBlock,
    ):
        """Test the complete historical flow with mock blocks."""
        await historical_flow(fmp_block=fake_fmp_block, psd_block=fake_psd_block, tn_block=fake_tn_block)

        # Verify that data was processed and inserted
        assert len(fake_tn_block.inserted_records) > 0
        inserted_df = fake_tn_block.inserted_records[0]
        assert_tn_data_schema(inserted_df)
        assert "stream_aapl" in inserted_df["stream_id"].values

    @pytest.mark.asyncio
    @pytest.mark.timeout(30, func_only=True)
    async def test_historical_flow_api_error(
        self,
        error_fmp_block: ErrorFMPBlock,
        fake_psd_block: FakePrimitiveSourcesDescriptorBlock,
        fake_tn_block: FakeTNAccessBlock,
        mocker: MockerFixture,
    ):
        """Test the flow's behavior when FMP API calls fail."""
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
            psd_block=fake_psd_block,
            tn_block=fake_tn_block,
        )

        # Verify that no records were inserted for the failed ticker
        assert len(fake_tn_block.inserted_records) == 0


class TestDataProcessing:
    """Tests for data processing functionality."""

    @pytest.mark.timeout(5, func_only=True)
    @pytest.mark.parametrize(
        "stream_id, should_raise",
        [
            ("stream_aapl", False),
            ("unknown", True),
        ],
    )
    def test_get_earliest_data_date(
        self,
        fake_tn_block: FakeTNAccessBlock,
        stream_id: str,
        should_raise: bool,
    ):
        """Test getting earliest data date for different tickers."""
        if should_raise:
            with pytest.raises(TNAccessBlock.StreamNotFoundError):
                get_earliest_data_date(tn_block=fake_tn_block, stream_id=stream_id)
        else:
            res = get_earliest_data_date(tn_block=fake_tn_block, stream_id=stream_id)
            assert isinstance(res, datetime.datetime)

    @pytest.mark.timeout(5, func_only=True)
    @pytest.mark.parametrize(
        "symbol, expected_length",
        [
            ("AAPL", 2),  # returns two rows
            ("UNKNOWN", 0),  # returns an empty DataFrame
        ],
    )
    def test_fetch_historical_data(
        self,
        fake_fmp_block: FakeFMPBlock,
        symbol: str,
        expected_length: int,
    ):
        """Test historical data fetching for different scenarios."""
        start_date = "2024-01-01"
        end_date = "2024-01-02"

        result = fetch_historical_data(
            fmp_block=fake_fmp_block,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        assert isinstance(result, DataFrame)
        assert len(result) == expected_length
        if expected_length > 0:
            assert all(col in result.columns for col in ["date", "symbol", "price", "volume"])

    @pytest.mark.timeout(5, func_only=True)
    def test_convert_eod_to_tn_data(
        self,
        sample_eod_data: DataFrame[EODData],
    ):
        """Test conversion of EOD data to TN format."""
        stream_id = "test_stream"
        tn_df = convert_eod_to_tn_df(sample_eod_data, stream_id)

        # Verify schema
        assert_tn_data_schema(tn_df)
        assert_all_records_for_stream(tn_df, stream_id)

        # expect first date to be 1704067200, which is a valid unix timestamp (seconds)
        expected_date = "1704067200"
        assert tn_df["date"][0] == expected_date, f"Date should be {expected_date}, got {tn_df['date'][0]}"

        # Verify date conversion to UNIX timestamps
        for iso_date, unix_ts in zip(sample_eod_data["date"], tn_df["date"]):
            expected_ts = str(int(parse_iso_date(iso_date).timestamp()))
            assert unix_ts == expected_ts, f"Timestamp mismatch for {iso_date}: expected {expected_ts}, got {unix_ts}"


class TestHistoricalFlowAdvanced:
    """Advanced tests for historical flow focusing on async behavior and batching."""

    @pytest.mark.asyncio
    @pytest.mark.timeout(5, func_only=True)
    async def test_historical_flow_async(
        self,
        fake_fmp_block: FakeFMPBlock,
        fake_psd_block: FakePrimitiveSourcesDescriptorBlock,
        fake_tn_block: FakeTNAccessBlock,
    ):
        """Test that the historical flow works correctly in async context."""
        await historical_flow(
            fmp_block=fake_fmp_block,
            psd_block=fake_psd_block,
            tn_block=fake_tn_block,
            min_fetch_date=datetime.datetime(2023, 1, 1),
        )

        # Verify that data was processed and inserted
        assert len(fake_tn_block.inserted_records) > 0
        inserted_df = fake_tn_block.inserted_records[0]
        assert_tn_data_schema(inserted_df)
        assert_all_records_for_stream(inserted_df, "stream_aapl")

    @pytest.mark.asyncio
    @pytest.mark.timeout(30, func_only=True)
    async def test_historical_flow_batch_processing(
        self,
        set_test_batch_size: Callable[[int], None],
        monkeypatch: MonkeyPatch,
    ):
        """Test that the flow correctly handles batch processing of records."""
        # Set a smaller batch size for testing
        TEST_BATCH_SIZE = 2
        set_test_batch_size(TEST_BATCH_SIZE)

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
                dates = pd.date_range("2023-01-01", periods=5, freq="h")
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
        assert_all_records_for_stream(tn_block.inserted_records[0], "stream_aapl")

    @pytest.mark.asyncio
    @pytest.mark.timeout(30, func_only=True)
    async def test_historical_flow_sequential_processing(
        self,
        set_test_batch_size: Callable[[int], None],
        monkeypatch: MonkeyPatch,
        mocker: MockerFixture,
    ):
        """Test that ticker processing is sequential and waits for TN insertion."""
        # Set a smaller batch size
        TEST_BATCH_SIZE = 2
        set_test_batch_size(TEST_BATCH_SIZE)

        mocker.patch(
            "tsn_adapters.flows.fmp.historical_flow.get_earliest_data_date",
            return_value=datetime.datetime(2020, 1, 1),  # Return a fixed date to avoid complexity
        )

        # Create a sequence tracker
        sequence = SequenceTracker()

        class TimingFMPBlock(FakeFMPBlock):
            def __init__(self, sequence_tracker: SequenceTracker, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.sequence = sequence_tracker

            def get_historical_eod_data(
                self, symbol: str, start_date: str | None = None, end_date: str | None = None
            ) -> DataFrame[EODData]:
                """Track when each symbol's data is fetched and return a single record."""
                self.sequence.next(f"fetch_{symbol}")
                # Return a single record for each symbol to make batch counting predictable
                data = {
                    "date": ["2024-01-01"],
                    "symbol": [symbol],
                    "price": [150.0],
                    "volume": [1000000],
                }
                return DataFrame[EODData](pd.DataFrame(data))

        class TimingTNAccessBlock(FakeTNAccessBlock):
            def __init__(self, sequence_tracker: SequenceTracker):
                super().__init__()
                self.sequence = sequence_tracker

            def batch_insert_tn_records(
                self,
                records: DataFrame[TnDataRowModel],
                is_unix: bool = False,
                has_external_created_at: bool = False,
            ) -> Optional[str]:
                """Track batch insertions with sequence information."""
                self.sequence.next(f"insert_batch_{len(records)}")
                return super().batch_insert_tn_records(records, is_unix, has_external_created_at)

        # Create blocks with timing tracking
        fmp_block = TimingFMPBlock(sequence_tracker=sequence, api_key=SecretStr("fake"))
        tn_block = TimingTNAccessBlock(sequence_tracker=sequence)

        # Create a descriptor block with multiple tickers
        class MultiTickerDescriptorBlock(PrimitiveSourcesDescriptorBlock):
            def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
                # Create 4 tickers - should result in two batches (2 records each)
                tickers = [f"TICK{i}" for i in range(4)]
                data = {
                    "source_id": tickers,
                    "stream_id": [f"stream_{t.lower()}" for t in tickers],
                    "source_type": ["stock"] * len(tickers),
                }
                return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(data))

        psd_block = MultiTickerDescriptorBlock()

        # Run the flow
        result = await historical_flow(
            fmp_block=fmp_block,
            psd_block=psd_block,
            tn_block=tn_block,
            min_fetch_date=datetime.datetime(2023, 1, 1),
            return_state=True,
        )

        assert result is not None
        assert result.is_completed

        # Verify operations are sequential
        fetch_operations = sequence.get_operations_by_type("fetch_")
        insert_operations = sequence.get_operations_by_type("insert_batch_")

        # Verify we have the right number of operations
        assert len(fetch_operations) == 4, "Should have 4 fetch operations"
        assert len(insert_operations) == 2, "Should have 2 insert operations"

        # Verify fetches are sequential
        sequence.assert_sequential_operations("fetch_")

        # Verify batch insertions happen at the right time
        first_insert_seq = insert_operations[0][0]
        second_insert_seq = insert_operations[1][0]

        # Count fetches before each insert
        fetches_before_first = len([seq for seq, _ in fetch_operations if seq < first_insert_seq])
        fetches_before_second = len([seq for seq, _ in fetch_operations if seq < second_insert_seq])

        # First batch should be inserted after TEST_BATCH_SIZE fetches
        assert fetches_before_first == TEST_BATCH_SIZE, (
            f"First batch should be inserted after {TEST_BATCH_SIZE} fetches, "
            f"but was inserted after {fetches_before_first}"
        )

        # Second batch should be inserted after all fetches
        assert fetches_before_second == 4, (
            "Second batch should be inserted after all fetches, " f"but was inserted after {fetches_before_second}"
        )

        # Verify batch sizes
        assert_batch_sizes(tn_block.batch_sizes, [2, 2])

    @pytest.mark.asyncio
    @pytest.mark.timeout(30, func_only=True)
    async def test_historical_flow_large_fetch(
        self,
        set_test_batch_size: Callable[[int], None],
        mocker: MockerFixture,
    ):
        """Test that the flow correctly handles fetches larger than batch size."""
        # Set a smaller batch size
        TEST_BATCH_SIZE = 3
        set_test_batch_size(TEST_BATCH_SIZE)

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
                return DataFrame[EODData](pd.DataFrame({"date": [], "symbol": [], "price": [], "volume": []}))

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
        assert_batch_sizes(tn_block.batch_sizes, [8, 2])

        # First batch should contain all TICK0 records (processed when exceeding BATCH_SIZE)
        assert_all_records_for_stream(tn_block.inserted_records[0], "stream_tick0")

        # Second batch should contain TICK1 records (processed at end of pipeline)
        assert_all_records_for_stream(tn_block.inserted_records[1], "stream_tick1")

        # Verify total records
        total_records = sum(tn_block.batch_sizes)
        assert total_records == 10, "Expected 10 total records (8 from TICK0 + 2 from TICK1)"


if __name__ == "__main__":
    pytest.main(["-v", __file__])
