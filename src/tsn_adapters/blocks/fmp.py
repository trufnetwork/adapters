import json
from urllib.request import urlopen

import certifi
import pandera as pa
from pandera import DataFrameModel
from pandera.typing import DataFrame, Series
from prefect.blocks.core import Block
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


class FMPBlock(Block):
    api_key: SecretStr

    base_url: str = Field(default="https://financialmodelingprep.com/stable/")

    def _get_jsonparsed_data(self, path: str) -> dict:
        separator = "&" if "?" in path else "?"
        url = self.base_url + path + separator + "apikey=" + self.api_key.get_secret_value()
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
