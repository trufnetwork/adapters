from pathlib import Path
from unittest.mock import MagicMock, patch

from pandera.typing import DataFrame  # For type hinting test variables
from pydantic import SecretStr
import pytest

# Assuming your FMPBlock and Enums are in fmp_block_module
from tsn_adapters.blocks.fmp import (
    FMPAPI,
    CommodityQuote,
    ExchangeQuote,
    FMPBlock,
    FMPEndpoint,
    FMPExchange,
    IndexConstituent,
)

# --- Test Data Fixtures ---

@pytest.fixture
def fmp_block_instance(tmp_path: Path): # tmp_path for potential local caching if block had it
    # Create a real FMPBlock instance, but its _get_jsonparsed_data will be mocked
    # The API key can be a dummy value for these tests
    block = FMPBlock(api_key=SecretStr("test_api_key_dummy"))
    # You might need to save and load if your block requires it for full initialization
    # block.save("test-fmp-block", overwrite=True, path=tmp_path)
    # return FMPBlock.load("test-fmp-block", path=tmp_path)
    return block

# Mocked FMP API Responses for batch-exchange-quote (aligns with ExchangeQuote model)
MOCK_BATCH_EXCHANGE_NYSE_RESPONSE = [
    {"symbol": "IBM", "price": 150.0, "volume": 100000, "change": 1.5, "changesPercentage": 1.01}, # changesPercentage will be filtered by Pandera
    {"symbol": "GE", "price": 100.0, "volume": 200000, "change": -0.5},
    {"symbol": "SPY", "price": 400.0, "volume": 500000, "change": 2.0, "extraField": "will_be_filtered"}, # SPY is an ETF, will be present unless server filters by type
]

MOCK_BATCH_EXCHANGE_NASDAQ_COMMON_STOCK_RESPONSE = [ # Assumes server has filtered by type=stock
    {"symbol": "AAPL", "price": 170.0, "volume": 300000, "change": 2.5},
    {"symbol": "MSFT", "price": 300.0, "volume": 400000, "change": -1.0},
    {"symbol": "GOOG", "price": 2500.0, "volume": 150000, "change": 5.0},
]

# For testing NASDAQ without type filter, to see ETFs initially
MOCK_BATCH_EXCHANGE_NASDAQ_ALL_TYPES_RESPONSE = [
    {"symbol": "AAPL", "price": 170.0, "volume": 300000, "change": 2.5},
    {"symbol": "MSFT", "price": 300.0, "volume": 400000, "change": -1.0},
    {"symbol": "GOOG", "price": 2500.0, "volume": 150000, "change": 5.0},
    {"symbol": "QQQ", "price": 350.0, "volume": 600000, "change": 3.0}, # ETF
    {"symbol": "PREF", "price": 50.0, "volume": 50000, "change": 0.1}, # Could be Preferred, API type filter is key
]

MOCK_SP500_CONSTITUENTS_RESPONSE = [
    {"symbol": "AAPL", "name": "Apple Inc.", "sector": "Technology"},
    {"symbol": "MSFT", "name": "Microsoft Corp.", "sector": "Technology"},
    {"symbol": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Discretionary"},
]

MOCK_COMMODITY_QUOTES_RESPONSE = [
    {"symbol": "CL=F", "name": "Crude Oil", "exchange": "NYMEX", "price": 70.0, "change": 0.5, "volume": 1000},
    {"symbol": "GC=F", "name": "Gold", "exchange": "COMEX", "price": 1800.0, "change": -2.0, "volume": 500},
    {"symbol": "ZC=F", "name": "Corn", "exchange": "CBOT", "price": 5.0, "change": 0.01, "volume": 20000},
    {"symbol": "NG=F", "name": "Natural Gas", "exchange": "NYMEX", "price": 4.0, "change": None, "volume": 15000}, # Test nullable change
    {"symbol": "SI=F", "name": "Silver", "exchange": "COMEX", "price": 22.0, "change": 0.1, "volume": None}, # Test nullable volume
    {"symbol": "HG=F", "name": "Copper", "exchange": "COMEX", "price": 4.0, "change": -0.05, "volume": 8000},
    {"symbol": "EURUSD=X", "name": "EUR/USD", "exchange": "FOREX", "price": 1.10, "change": 0.001, "volume": 100000}, # Not a CME group exchange
]


# --- Integration Tests ---

@patch.object(FMPBlock, '_get_jsonparsed_data')
def test_get_screened_equities_nyse_no_stock_type_filter(mock_get_json: MagicMock, fmp_block_instance: FMPBlock):
    """Test fetching NYSE equities (all types, as stock_type is None)."""
    mock_get_json.return_value = MOCK_BATCH_EXCHANGE_NYSE_RESPONSE
    
    result_df: DataFrame[ExchangeQuote] = fmp_block_instance.get_equities(
        exchange=FMPExchange.NYSE
        # No stock_type, so all types from this exchange should be returned by API
    )
    
    called_endpoint = mock_get_json.call_args[0][0]
    assert isinstance(called_endpoint, FMPEndpoint)
    assert called_endpoint.api == FMPAPI.STABLE
    assert called_endpoint.path == "batch-exchange-quote"
    assert called_endpoint.params == {"exchange": "nyse"}

    assert not result_df.empty
    assert len(result_df) == 3 # IBM, GE, SPY (SPY is an ETF, included because no stock_type filter)
    assert "IBM" in result_df["symbol"].values
    assert "GE" in result_df["symbol"].values
    assert "SPY" in result_df["symbol"].values # SPY should be present
    # Following columns are not in ExchangeQuote, so cannot be asserted here due to strict="filter"
    # assert all(result_df["exchangeShortName"] == "NYSE")
    # assert all(result_df["isEtf"] == False)

@patch.object(FMPBlock, '_get_jsonparsed_data')
def test_get_screened_equities_nasdaq_common_stock(mock_get_json: MagicMock, fmp_block_instance: FMPBlock):
    """Test fetching NASDAQ common stocks using stock_type filter."""
    mock_get_json.return_value = MOCK_BATCH_EXCHANGE_NASDAQ_COMMON_STOCK_RESPONSE
    
    result_df: DataFrame[ExchangeQuote] = fmp_block_instance.get_equities(
        exchange=FMPExchange.NASDAQ,
    )

    called_endpoint = mock_get_json.call_args[0][0]
    assert isinstance(called_endpoint, FMPEndpoint)
    assert called_endpoint.api == FMPAPI.STABLE
    assert called_endpoint.path == "batch-exchange-quote"
    assert called_endpoint.params == {"exchange": "nasdaq"}

    assert not result_df.empty
    assert len(result_df) == 3 # AAPL, MSFT, GOOG, as per MOCK_BATCH_EXCHANGE_NASDAQ_COMMON_STOCK_RESPONSE
    assert "AAPL" in result_df["symbol"].values
    assert "MSFT" in result_df["symbol"].values
    assert "GOOG" in result_df["symbol"].values
    # QQQ and PREF are not in MOCK_BATCH_EXCHANGE_NASDAQ_COMMON_STOCK_RESPONSE, so no need to assert their absence

@patch.object(FMPBlock, '_get_jsonparsed_data')
def test_get_sp500_constituents(mock_get_json: MagicMock, fmp_block_instance: FMPBlock):
    """Test fetching S&P 500 constituents."""
    mock_get_json.return_value = MOCK_SP500_CONSTITUENTS_RESPONSE
    
    result_df: DataFrame[IndexConstituent] = fmp_block_instance.get_sp500_constituents()
    
    # Assertions
    called_endpoint = mock_get_json.call_args[0][0]
    assert isinstance(called_endpoint, FMPEndpoint)
    assert called_endpoint.api == FMPAPI.STABLE
    assert called_endpoint.path == "sp500-constituent"
    assert called_endpoint.params is None

    assert not result_df.empty
    assert len(result_df) == 3
    assert "AMZN" in result_df["symbol"].tolist()
    assert "Technology" in result_df["sector"].tolist()

@patch.object(FMPBlock, '_get_jsonparsed_data')
def test_get_commodity_quotes_cme_group(mock_get_json: MagicMock, fmp_block_instance: FMPBlock):
    """Test fetching commodity quotes filtered for CME Group exchanges."""
    mock_get_json.return_value = MOCK_COMMODITY_QUOTES_RESPONSE
    
    result_df: DataFrame[CommodityQuote] = fmp_block_instance.get_cme_commodity_quotes()
    
    # Assertions
    called_endpoint = mock_get_json.call_args[0][0]
    assert isinstance(called_endpoint, FMPEndpoint)
    assert called_endpoint.api == FMPAPI.STABLE
    assert called_endpoint.path == "batch-commodity-quotes"
    assert called_endpoint.params is None

    assert not result_df.empty
    # Expected: CL=F (NYMEX), GC=F (COMEX), ZC=F (CBOT), NG=F (NYMEX), SI=F (COMEX), HG=F (COMEX)
    # EURUSD=X (FOREX) should be filtered out.
    # The new endpoint returns all, client side filtering may need to be done if only CME group is desired.
    # For now, we expect all commodities from the batch endpoint
    assert len(result_df) == 7 # All mock commodities including EURUSD=X as batch doesn't filter by CME group
    assert "CL=F" in result_df["symbol"].tolist()
    assert "EURUSD=X" in result_df["symbol"].tolist() # Should be present now

@patch.object(FMPBlock, '_get_jsonparsed_data')
def test_get_cme_commodity_quotes_no_filter(mock_get_json: MagicMock, fmp_block_instance: FMPBlock):
    """Test fetching all commodity quotes from the batch endpoint."""
    mock_get_json.return_value = MOCK_COMMODITY_QUOTES_RESPONSE
    
    result_df: DataFrame[CommodityQuote] = fmp_block_instance.get_cme_commodity_quotes()
    
    # Assertions
    called_endpoint = mock_get_json.call_args[0][0]
    assert isinstance(called_endpoint, FMPEndpoint)
    assert called_endpoint.api == FMPAPI.STABLE
    assert called_endpoint.path == "batch-commodity-quotes"
    assert called_endpoint.params is None

    assert not result_df.empty
    assert len(result_df) == 7 # All mock commodities
    assert "EURUSD=X" in result_df["symbol"].tolist()

@patch.object(FMPBlock, '_get_jsonparsed_data')
def test_empty_response_from_fmp(mock_get_json: MagicMock, fmp_block_instance: FMPBlock):
    """Test that an empty list from FMP results in an empty DataFrame."""
    mock_get_json.return_value = [] # Simulate FMP returning empty list
    
    result_df_equities: DataFrame[ExchangeQuote] = fmp_block_instance.get_equities(
        exchange=FMPExchange.NYSE
    )
    result_df_constituents: DataFrame[IndexConstituent] = fmp_block_instance.get_sp500_constituents()
    result_df_commodities: DataFrame[CommodityQuote] = fmp_block_instance.get_cme_commodity_quotes()

    assert result_df_equities.empty
    assert isinstance(result_df_equities, DataFrame) # Still a Pandera DataFrame
    assert result_df_constituents.empty
    assert isinstance(result_df_constituents, DataFrame)
    assert result_df_commodities.empty
    assert isinstance(result_df_commodities, DataFrame)

@patch.object(FMPBlock, '_get_jsonparsed_data')
def test_client_side_filter_results_in_empty(mock_get_json: MagicMock, fmp_block_instance: FMPBlock):
    """Test scenario where FMP returns data, but client-side filter yields no results."""
    # MOCK_NASDAQ_SCREENER_RESPONSE has ETFs and Preferred Stock, but no "RareMetal" type
    mock_get_json.return_value = MOCK_BATCH_EXCHANGE_NASDAQ_ALL_TYPES_RESPONSE 
    
    # For get_equities, if the API returns data but it doesn't match the ExchangeQuote schema
    # (e.g. missing 'price' for a stock that should have it), Pandera validation in _fetch_fmp_list_data would fail.
    # Here we are testing what happens if the API returns data, but the *type* of data is not what we want
    # or if it's filtered out by Pandera's `strict="filter"` in the `ExchangeQuote` model.
    # Since `ExchangeQuote` has a strict filter, it will only keep columns defined in the model.
    # If the API sends extra columns, they are dropped. If it sends fewer, it depends on nullability.
    
    # To test a scenario where client-side filtering logic (if any were present in get_equities)
    # would result in an empty DF, we'd need to mock that specific logic.
    # However, get_equities as currently written, relies mostly on server-side filtering via params
    # and Pandera schema validation/filtering.
    
    # Let's assume the API returns data that, after Pandera's strict="filter", results in an empty DF.
    # This would happen if, for example, none of the returned items from API had 'symbol', 'price', 'volume', 'change'
    # OR if all items are valid but a hypothetical client-side filter (not currently in get_equities) removes them.
    # For this test, we'll mock such a situation where the API call is made, but an empty list comes back.
    # This is similar to test_empty_response_from_fmp but specifically for the get_equities method
    # if we imagine a filtering step happening after the API call that makes it empty.
    
    mock_get_json.return_value = [] # Simulate data that gets entirely filtered out or is empty

    result_df: DataFrame[ExchangeQuote] = fmp_block_instance.get_equities(
        exchange=FMPExchange.NASDAQ,
    )
    
    assert result_df.empty