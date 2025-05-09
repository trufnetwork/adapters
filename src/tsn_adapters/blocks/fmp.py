import json
import logging
from typing import Any, Union, cast
from urllib.parse import urlencode
from urllib.request import urlopen

import certifi
import pandas as pd
from pandera import DataFrameModel, Field
from pandera.typing import DataFrame, Series
from prefect.blocks.core import Block
from prefect.concurrency.sync import rate_limit
from pydantic import Field as PydanticField, SecretStr

from tsn_adapters.utils.logging import get_logger_safe


class ActiveTicker(DataFrameModel):
    symbol: Series[str]
    name: Series[str] = Field(nullable=True)

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True

class TickerDetail(DataFrameModel):
    symbol: Series[str]
    marketCap: Series[str] = Field(nullable=True)
    beta: Series[float] = Field(nullable=True)
    lastDividend: Series[float] = Field(nullable=True)
    range: Series[str] = Field(nullable=True)
    change: Series[float] = Field(nullable=True)
    changePercentage: Series[float] = Field(nullable=True)
    volume: Series[int] = Field(nullable=True)
    averageVolume: Series[int] = Field(nullable=True)
    companyName: Series[str] = Field(nullable=True)
    currency: Series[str] = Field(nullable=True)
    cik: Series[str] = Field(nullable=True)
    isin: Series[str] = Field(nullable=True)
    cusip: Series[str] = Field(nullable=True)
    exchangeFullName: Series[str] = Field(nullable=True)
    exchange: Series[str] = Field(nullable=True)
    industry: Series[str] = Field(nullable=True)
    website: Series[str] = Field(nullable=True)
    description: Series[str] = Field(nullable=True)
    ceo: Series[str] = Field(nullable=True)
    sector: Series[str] = Field(nullable=True)
    country: Series[str] = Field(nullable=True)
    fullTimeEmployees: Series[str] = Field(nullable=True)
    phone: Series[str] = Field(nullable=True)
    address: Series[str] = Field(nullable=True)
    city: Series[str] = Field(nullable=True)
    state: Series[str] = Field(nullable=True)
    zip: Series[str] = Field(nullable=True)
    image: Series[str] = Field(nullable=True)
    ipoDate: Series[str] = Field(nullable=True)
    defaultImage: Series[bool] = Field(nullable=True)
    isEtf: Series[bool] = Field(nullable=True)
    isActivelyTrading: Series[bool] = Field(nullable=True)
    isAdr: Series[bool] = Field(nullable=True)
    isFund: Series[bool] = Field(nullable=True)

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True

class BatchQuoteShort(DataFrameModel):
    """Schema for batch quote data from FMP API.

    The schema allows null prices but requires non-negative values when present.
    """

    symbol: Series[str]
    price: Series[float] = Field(
        nullable=True, ge=0.0, coerce=True
    )  # Allow null prices, must be non-negative when present
    volume: Series[int]

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True
        dtype_backend = "numpy_nullable"  # Use nullable dtypes to handle None values correctly


class IntradayData(DataFrameModel):
    date: Series[str]  # ISO format date string
    open: Series[float]
    high: Series[float]
    low: Series[float]
    close: Series[float]
    volume: Series[int]

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True


class EODData(DataFrameModel):
    symbol: Series[str]
    date: Series[str]  # ISO format date string
    price: Series[float]
    volume: Series[int]

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True


class FMPBlock(Block):
    api_key: SecretStr

    base_url: str = PydanticField(default="https://financialmodelingprep.com/stable/")

    @property
    def logger(self) -> logging.Logger:
        if not hasattr(self, "_logger"):
            self._logger = get_logger_safe(__name__)
        return self._logger

    def _get_jsonparsed_data(self, path: str) -> Union[dict[str, Any], list[dict[str, Any]]]:
        separator = "&" if "?" in path else "?"
        url = self.base_url + path + separator + "apikey=" + self.api_key.get_secret_value()
        rate_limit("fmp_api")
        response = urlopen(url, cafile=certifi.where())
        data = response.read().decode("utf-8")
        return json.loads(data)

    def _filter_null_prices(self, df: DataFrame[BatchQuoteShort]) -> tuple[DataFrame[BatchQuoteShort], list[str]]:
        """
        Filter out rows with null prices from a BatchQuoteShort DataFrame.

        Args:
            df: DataFrame containing batch quote data

        Returns:
            A tuple containing:
            - DataFrame with null prices filtered out
            - List of symbols that were filtered out due to null prices
        """
        # Get symbols with null prices
        null_price_mask = df["price"].isna()
        filtered_symbols = cast(list[str], df.loc[null_price_mask, "symbol"].tolist())

        # Filter out rows with null prices
        filtered_df = df.loc[~null_price_mask].copy()

        # Log the filtering if any symbols were filtered
        if filtered_symbols:
            self.logger.info(f"Filtered out {len(filtered_symbols)} symbols with null prices: {filtered_symbols}")

        return filtered_df, filtered_symbols

    def get_active_tickers(self) -> DataFrame[ActiveTicker]:
        """
        List all actively trading companies and financial instruments with the FMP Actively Trading List API.
        This endpoint allows users to filter and display securities that are currently being traded on public exchanges,
        ensuring you access real-time market activity.
        """
        path = "actively-trading-list"
        data = self._get_jsonparsed_data(path)
        if isinstance(data, list):
            return DataFrame[ActiveTicker](data)
        return DataFrame[ActiveTicker](pd.DataFrame())
    
    def get_ticker_detail(self, symbol: str):
        """
        Access detailed company profile data with the FMP Company Profile Data API. 
        This API provides key financial and operational information for a specific stock symbol, 
        including the company's market capitalization, stock price, industry, and much more.
        """
        path = f"profile?symbol={symbol}"
        data = self._get_jsonparsed_data(path)
        if isinstance(data, list):
            return DataFrame[TickerDetail](data)
        return DataFrame[TickerDetail](pd.DataFrame())

    def get_batch_quote(self, symbols: list[str]) -> DataFrame[BatchQuoteShort]:
        """
        Retrieve batch quote short for the given list of stock symbols.
        This endpoint returns short quote data with symbol, price, and volume.
        Symbols with null prices will be filtered out and logged.

        Args:
            symbols: List of stock symbols to fetch quotes for

        Returns:
            DataFrame containing quote data with null prices filtered out

        *Symbol Limited to US, UK, and Canada Exchanges
        """
        symbol_str = ",".join(symbols)
        path = f"batch-quote-short?symbols={symbol_str}"
        data = self._get_jsonparsed_data(path)

        if isinstance(data, list):
            df = DataFrame[BatchQuoteShort](data)
            filtered_df, _ = self._filter_null_prices(df)
            return filtered_df

        return DataFrame[BatchQuoteShort](pd.DataFrame())

    def get_intraday_data(
        self, symbol: str, start_date: str | None = None, end_date: str | None = None
    ) -> DataFrame[IntradayData]:
        """
        Retrieve intraday 1-hour interval stock chart data for the given symbol using FMP's API.
        API Documentation: https://site.financialmodelingprep.com/developer/docs/stable/intraday-1-hour

        Args:
            symbol: The stock symbol to fetch data for
            start_date: Optional start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format

        Returns:
            DataFrame containing intraday data with columns: date, open, high, low, close, volume
        """
        path = f"historical-chart/1hour/{symbol}"
        if start_date:
            path += f"?from={start_date}"
            if end_date:
                path += f"&to={end_date}"
        elif end_date:
            path += f"?to={end_date}"

        data = self._get_jsonparsed_data(path)
        if isinstance(data, list):
            return DataFrame[IntradayData](data)
        elif "historical" in data:
            assert isinstance(data, dict)
            return DataFrame[IntradayData](data["historical"])
        return DataFrame[IntradayData](pd.DataFrame())  # Return empty DataFrame with correct schema

    def get_historical_eod_data(
        self, symbol: str, start_date: str | None = None, end_date: str | None = None
    ) -> DataFrame[EODData]:
        """
        Retrieve historical end-of-day stock data for the given symbol using FMP's API.
        API Documentation: https://site.financialmodelingprep.com/developer/docs/stable/historical-price-eod-light

        Args:
            symbol: The stock symbol to fetch data for
            start_date: Optional start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format

        Returns:
            DataFrame containing EOD data with columns: symbol, date, price, volume
        """
        path = "historical-price-eod/light/"
        params = {"symbol": symbol}
        if start_date:
            params["from"] = start_date
        if end_date:
            params["to"] = end_date

        path += f"?{urlencode(params)}"

        data = self._get_jsonparsed_data(path)
        if not data:
            return cast(DataFrame[EODData], pd.DataFrame())
        df = pd.DataFrame(data)
        df["symbol"] = symbol
        return DataFrame[EODData](df)
