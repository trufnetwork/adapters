import os
import re
from datetime import datetime
import time
from typing import Any

from pydantic import SecretStr
import pytest

from tsn_adapters.blocks.fmp import FMPBlock, EODData
from pandera.typing import DataFrame

def is_iso_date(date_str: str) -> bool:
    """Check if a string is in ISO date format (YYYY-MM-DD)."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def iso_to_unix_timestamp(iso_date: str) -> int:
    """Convert ISO date string to UNIX timestamp in seconds."""
    return int(datetime.strptime(iso_date, "%Y-%m-%d").timestamp())

def verify_date_formats(df: DataFrame[EODData]):
    """Verify that dates in the DataFrame are in ISO format and match their UNIX timestamp equivalents."""
    # Check ISO format
    assert all(is_iso_date(date) for date in df['date']), "All dates should be in ISO format (YYYY-MM-DD)"
    
    # If we have timestamps, verify they match the ISO dates
    if 'timestamp' in df.columns:
        for iso_date, timestamp in zip(df['date'], df['timestamp']):
            expected_timestamp = iso_to_unix_timestamp(iso_date)
            assert timestamp == expected_timestamp, f"Timestamp mismatch for {iso_date}: expected {expected_timestamp}, got {timestamp}"

@pytest.fixture
def fmp_block(prefect_test_fixture: Any):
    """Instantiate FMPBlock with API key from environment variable. Skip test if not provided."""
    api_key = os.environ.get("FMP_API_KEY")
    if not api_key:
        pytest.skip("FMP API key not provided in environment variable FMP_API_KEY")
    assert isinstance(api_key, str), "API key must be a string"
    return FMPBlock(api_key=SecretStr(api_key))


@pytest.mark.integration
def test_get_active_tickers(fmp_block: FMPBlock):
    """Integration test for get_active_tickers method."""
    df = fmp_block.get_active_tickers()
    # Ensuring that we got a response that contains at least one active ticker
    assert df is not None, "Expected non-None result"
    assert len(df) > 0, "Expected non-empty active tickers list"
    
    # Validate schema requirements
    assert all(isinstance(symbol, str) for symbol in df['symbol']), "All symbols should be strings"
    assert all(isinstance(name, (str, type(None))) for name in df['name']), "All names should be strings or None"
    
    print(f"Number of active tickers: {len(df)}")


@pytest.mark.integration
def test_get_batch_quote(fmp_block: FMPBlock):
    """Integration test for get_batch_quote method."""
    symbols = ["AAPL", "GOOGL"]
    df = fmp_block.get_batch_quote(symbols)
    # Ensuring that we received batch quotes for the provided symbols
    assert df is not None, "Expected non-None result"
    assert len(df) > 0, "Expected non-empty batch quote result"
    
    # Validate schema requirements
    assert all(isinstance(symbol, str) for symbol in df['symbol']), "All symbols should be strings"
    assert all(isinstance(price, (int, float)) for price in df['price']), "All prices should be numeric"
    assert all(isinstance(volume, int) for volume in df['volume']), "All volumes should be integers"
    
    print(f"Batch quote for {symbols}:")
    print(df)


@pytest.mark.integration
def test_get_historical_eod_data(fmp_block: FMPBlock):
    """Integration test for get_historical_eod_data method."""
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    df = fmp_block.get_historical_eod_data("AAPL", start_date=start_date, end_date=end_date)
    assert df is not None, "Expected non-None result"
    assert len(df) == 21, "Expected 21 rows of historical EOD data"
    
    # Validate schema requirements
    assert all(isinstance(symbol, str) for symbol in df['symbol']), "All symbols should be strings"
    assert all(is_iso_date(date) for date in df['date']), "All dates should be in ISO format"
    assert all(isinstance(price, (int, float)) for price in df['price']), "All prices should be numeric"
    assert all(isinstance(volume, int) for volume in df['volume']), "All volumes should be integers"
    
    # Validate date range
    assert min(df['date']) >= start_date, "Data should not be before start_date"
    assert max(df['date']) <= end_date, "Data should not be after end_date"
    
    # Verify date formats and timestamp conversions
    verify_date_formats(df)
    
    print(f"Historical EOD data for AAPL from {start_date} to {end_date}:")
    print(df)


if __name__ == "__main__":
    # run pytest with verbose output and show prints by disabling output capturing
    pytest.main(["-s", __file__])
