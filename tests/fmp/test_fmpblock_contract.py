from __future__ import annotations

from datetime import datetime
import os
from typing import Any

from pandera.typing import DataFrame
from pydantic import SecretStr
import pytest
from prefect.testing.utilities import prefect_test_harness

from tsn_adapters.blocks.fmp import (
    CommodityInfo,
    CommodityQuote,
    ExchangeQuote,
    FMPBlock,
    FMPExchange,
    IndexConstituent,
    IndexInfo,
)


def is_iso_date(date_str: str) -> bool:
    """Check if a string is in ISO date format (YYYY-MM-DD)."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


@pytest.fixture
def fmp_block(prefect_test_fixture: Any):
    """Instantiate FMPBlock with API key from environment variable. Skip if not provided."""
    api_key = os.environ.get("FMP_API_KEY")
    if not api_key:
        pytest.skip("FMP API key not provided in environment variable FMP_API_KEY. Skipping contract tests.")
    assert isinstance(api_key, str), "API key must be a string"
    return FMPBlock(api_key=SecretStr(api_key))


@pytest.mark.integration
def test_contract_get_equities_nasdaq_common_stock(fmp_block: FMPBlock):
    """Contract test for get_equities with NASDAQ common stock."""
    df: DataFrame[ExchangeQuote] = fmp_block.get_equities(
        exchange=FMPExchange.NASDAQ,
    )
    assert not df.empty, "Expected NASDAQ common stocks to be non-empty"
    # Structural checks for quote fields
    for col in ("symbol", "price", "volume", "change"):
        assert col in df.columns, f"Missing expected column {col}"
    # Expect at least one major NASDAQ ticker
    assert df["symbol"].str.contains("AAPL|MSFT", regex=True).any(), \
        "Expected a major NASDAQ stock like AAPL or MSFT"


@pytest.mark.integration
def test_contract_get_sp500_constituents(fmp_block: FMPBlock):
    """Contract test for get_sp500_constituents."""
    df: DataFrame[IndexConstituent] = fmp_block.get_sp500_constituents()
    assert df is not None, "Expected non-None result for S&P 500 constituents"
    assert not df.empty, "Expected S&P 500 constituents list to be non-empty"
    # Rough count around 500
    assert 490 < len(df) < 515, f"Expected around 500 S&P constituents, got {len(df)}"
    # Basic column presence
    assert "symbol" in df.columns
    assert "name" in df.columns
    # Expect at least one major company
    assert df["symbol"].str.contains("AAPL|MSFT", regex=True).any(), "Expected a major S&P company like AAPL or MSFT"
    print(f"Fetched {len(df)} S&P 500 constituents.")


@pytest.mark.integration
def test_contract_get_cme_commodity_quotes(fmp_block: FMPBlock):
    """Contract test for get_cme_commodity_quotes."""
    df: DataFrame[CommodityQuote] = fmp_block.get_cme_commodity_quotes()
    assert not df.empty, "Expected CME commodity quotes to be non-empty"
    # Structural checks for quote fields
    for col in ("symbol", "price", "change", "volume"):
        assert col in df.columns, f"Missing expected column {col}"
    # Expect at least one CME group commodity
    assert df["symbol"].str.contains("ZBUSD|LEUSX", regex=True).any(), \
        "Expected at least one CME group commodity like ZBUSD or LEUSX"


@pytest.mark.integration
def test_contract_get_commodity_list(fmp_block: FMPBlock):
    """Contract test for get_commodity_list."""
    df: DataFrame[CommodityInfo] = fmp_block.get_commodity_list()
    assert not df.empty, "Expected commodity list to be non-empty"
    # Structural checks for fields based on CommodityInfo schema
    expected_cols = ["symbol", "name", "exchange", "tradeMonth", "currency"]
    for col in expected_cols:
        assert col in df.columns, f"Missing expected column {col} in commodity list"
    # Expect at least one common commodity symbol (e.g., Gold or Crude Oil)
    # Adjust these symbols if needed based on actual typical FMP response
    assert df["symbol"].str.contains("GCUSD|CLUSD", regex=True).any(), \
        "Expected at least one common commodity like GCUSD (Gold) or CLUSD (Crude Oil)"
    print(f"Fetched {len(df)} commodities from the list.")


@pytest.mark.integration
def test_contract_get_index_list(fmp_block: FMPBlock):
    """Contract test for get_index_list."""
    df: DataFrame[IndexInfo] = fmp_block.get_index_list()
    assert not df.empty, "Expected stock market indexes list to be non-empty"
    # Structural checks for fields based on IndexInfo schema
    expected_cols = ["symbol", "name", "exchange", "currency"]
    for col in expected_cols:
        assert col in df.columns, f"Missing expected column {col} in index list"
    # Expect at least one major stock market index (S&P 500, NASDAQ, Dow Jones)
    # Common index symbols may vary, but these are typical patterns
    assert df["symbol"].str.contains("SPX|NDX|DJI|GSPC|IXIC", regex=True).any(), \
        "Expected at least one major index like SPX, NDX, DJI, GSPC, or IXIC"
    print(f"Fetched {len(df)} stock market indexes from the list.")


@pytest.mark.integration
def test_batch_counts_for_specified_filters(fmp_block: FMPBlock):
    """
    Validate counts for different exchange and index bulk endpoints:
    - NYSE equities
    - NASDAQ common stocks
    - S&P 500 constituents
    - CME commodity quotes
    - Stock market indexes
    """
    # Fetch data
    nyse_df: DataFrame[ExchangeQuote] = fmp_block.get_equities(
        exchange=FMPExchange.NYSE,
    )
    nasdaq_df: DataFrame[ExchangeQuote] = fmp_block.get_equities(
        exchange=FMPExchange.NASDAQ,
    )
    sp500_df: DataFrame[IndexConstituent] = fmp_block.get_sp500_constituents()
    cme_df: DataFrame[CommodityQuote] = fmp_block.get_cme_commodity_quotes()
    index_df: DataFrame[IndexInfo] = fmp_block.get_index_list()

    # Non-empty checks
    assert not nyse_df.empty, "Expected NYSE equities to be non-empty"
    assert not nasdaq_df.empty, "Expected NASDAQ common stocks to be non-empty"
    assert not sp500_df.empty, "Expected S&P 500 constituents to be non-empty"
    assert not cme_df.empty, "Expected CME commodity quotes to be non-empty"
    assert not index_df.empty, "Expected stock market indexes to be non-empty"

    # Approximate count assertions
    assert 490 < len(sp500_df) < 550, f"Expected ~500 SP500 constituents, got {len(sp500_df)}"
    assert len(nyse_df) > len(sp500_df), f"Expected NYSE equities ({len(nyse_df)}) > SP500 constituents ({len(sp500_df)})"
    assert len(nasdaq_df) > len(sp500_df), f"Expected NASDAQ common stocks ({len(nasdaq_df)}) > SP500 constituents ({len(sp500_df)})"
    assert len(cme_df) >= 20, f"Expected at least 20 CME commodities, got {len(cme_df)}"
    assert len(index_df) >= 10, f"Expected at least 10 stock market indexes, got {len(index_df)}"


def save_all_data(fmp_block: FMPBlock, relative_path: str):
    """Save all data to a file."""
    nyse_df: DataFrame[ExchangeQuote] = fmp_block.get_equities(exchange=FMPExchange.NYSE)
    nasdaq_df: DataFrame[ExchangeQuote] = fmp_block.get_equities(exchange=FMPExchange.NASDAQ)
    sp500_df: DataFrame[IndexConstituent] = fmp_block.get_sp500_constituents()
    cme_df: DataFrame[CommodityQuote] = fmp_block.get_cme_commodity_quotes()
    index_df: DataFrame[IndexInfo] = fmp_block.get_index_list()

    # Create directory if it doesn't exist
    os.makedirs(relative_path, exist_ok=True)

    nyse_df.to_csv(f"{relative_path}/nyse_equities.csv", index=False)
    nasdaq_df.to_csv(f"{relative_path}/nasdaq_common_stocks.csv", index=False)
    sp500_df.to_csv(f"{relative_path}/sp500_constituents.csv", index=False)
    cme_df.to_csv(f"{relative_path}/cme_commodity_quotes.csv", index=False)
    index_df.to_csv(f"{relative_path}/index_list.csv", index=False)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Save FMP data to CSV files')
    parser.add_argument('--path', type=str, default='gitignore/data', help='Relative path to save data (default: data)')
    args = parser.parse_args()

    api_key = os.environ.get("FMP_API_KEY")
    assert isinstance(api_key, str), "API key must be a string"
    with prefect_test_harness():
        fmp_block_test = FMPBlock(api_key=SecretStr(api_key))
    save_all_data(fmp_block_test, args.path)
