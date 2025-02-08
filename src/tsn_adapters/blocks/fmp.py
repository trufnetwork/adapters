import json
from urllib.request import urlopen

import certifi
import pandas as pd
import pandera as pa
from pandera import DataFrameModel
from pandera.typing import DataFrame, Series
from prefect.blocks.core import Block
from prefect.concurrency.sync import rate_limit
from pydantic import Field, SecretStr


class ActiveTicker(DataFrameModel):
    symbol: Series[str]
    name: Series[str] = pa.Field(nullable=True)

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True


class BatchQuoteShort(DataFrameModel):
    symbol: Series[str]
    price: Series[float]
    volume: Series[int]

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True


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


class FMPBlock(Block):
    api_key: SecretStr

    base_url: str = Field(default="https://financialmodelingprep.com/stable/")

    def _get_jsonparsed_data(self, path: str) -> dict:
        separator = "&" if "?" in path else "?"
        url = self.base_url + path + separator + "apikey=" + self.api_key.get_secret_value()
        rate_limit('fmp_api')
        response = urlopen(url, cafile=certifi.where())
        data = response.read().decode("utf-8")
        return json.loads(data)

    def get_active_tickers(self) -> DataFrame[ActiveTicker]:
        """
        List all actively trading companies and financial instruments with the FMP Actively Trading List API.
        This endpoint allows users to filter and display securities that are currently being traded on public exchanges,
        ensuring you access real-time market activity.
        """
        path = "actively-trading-list"
        return DataFrame[ActiveTicker](self._get_jsonparsed_data(path))

    def get_batch_quote(self, symbols: list[str]) -> DataFrame[BatchQuoteShort]:
        """
        Retrieve batch quote short for the given list of stock symbols.
        This endpoint returns short quote data with symbol, price, and volume.

        *Symbol Limited to US, UK, and Canada Exchanges
        """
        symbol_str = ",".join(symbols)
        path = f"batch-quote-short?symbols={symbol_str}"
        return DataFrame[BatchQuoteShort](self._get_jsonparsed_data(path))

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
        elif isinstance(data, dict) and "historical" in data:
            return DataFrame[IntradayData](data["historical"])
        return DataFrame[IntradayData](pd.DataFrame())  # Return empty DataFrame with correct schema
