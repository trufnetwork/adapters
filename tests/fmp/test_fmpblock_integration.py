import os

from pydantic import SecretStr
import pytest

from tsn_adapters.blocks.fmp import FMPBlock


@pytest.fixture
def fmp_block():
    """Instantiate FMPBlock with API key from environment variable. Skip test if not provided."""
    api_key = os.environ.get("FMP_API_KEY")
    if not api_key:
        pytest.skip("FMP API key not provided in environment variable FMP_API_KEY")
    return FMPBlock(api_key=SecretStr(api_key))


@pytest.mark.integration
def test_get_active_tickers(fmp_block):
    """Integration test for get_active_tickers method."""
    df = fmp_block.get_active_tickers()
    # Ensuring that we got a response that contains at least one active ticker
    assert df is not None, "Expected non-None result"
    assert len(df) > 0, "Expected non-empty active tickers list"
    print(f"Number of active tickers: {len(df)}")


@pytest.mark.integration
def test_get_batch_quote(fmp_block):
    """Integration test for get_batch_quote method."""
    symbols = ["AAPL", "GOOGL"]
    df = fmp_block.get_batch_quote(symbols)
    # Ensuring that we received batch quotes for the provided symbols
    assert df is not None, "Expected non-None result"
    assert len(df) > 0, "Expected non-empty batch quote result"
    print(f"Batch quote for {symbols}:")
    print(df)


@pytest.mark.integration
def test_get_historical_eod_data(fmp_block):
    """Integration test for get_historical_eod_data method."""
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    df = fmp_block.get_historical_eod_data("AAPL", start_date=start_date, end_date=end_date)
    assert df is not None, "Expected non-None result"
    assert len(df) == 21, "Expected 21 rows of historical EOD data"
    print(f"Historical EOD data for AAPL from {start_date} to {end_date}:")
    print(df)


if __name__ == "__main__":
    # run pytest with verbose output and show prints by disabling output capturing
    pytest.main(["-s", __file__])
