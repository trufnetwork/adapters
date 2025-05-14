from enum import Enum
import json
import logging
from typing import Any, Callable, Optional, TypeVar, Union, cast
from urllib.parse import urlencode
from urllib.request import urlopen

import certifi
import pandas as pd
from pandera import DataFrameModel, Field
from pandera.typing import DataFrame, Series
from prefect.blocks.core import Block
from prefect.concurrency.sync import rate_limit
from pydantic import Field as PydanticField, SecretStr

from tsn_adapters.utils.create_empty_df import create_empty_df
from tsn_adapters.utils.logging import get_logger_safe

PanderaModelType = TypeVar("PanderaModelType", bound=DataFrameModel)







class FMPExchange(Enum):
    NYSE = "nyse"
    NASDAQ = "nasdaq"
    CME = "CME"
    CBOT = "CBOT"
    NYMEX = "NYMEX"
    COMEX = "COMEX"

class FMPAPI(Enum):
    LEGACY = "api/v3/"
    STABLE = "stable/"

CME_GROUP_EXCHANGES: set[str] = {
    FMPExchange.CME.value,
    FMPExchange.CBOT.value,
    FMPExchange.NYMEX.value,
    FMPExchange.COMEX.value,
}


class FMPEndpoint:
    def __init__(self, api: FMPAPI, path: str, params: Optional[dict[str, Any]] = None):
        self.api = api
        self.path = path
        self.params = params
    
    def get_url(self, base_url: str) -> str:
        return base_url + self.api.value + self.path + (f"?{urlencode(self.params)}" if self.params else "")



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
    price: Series[pd.Float64Dtype] = Field(
        nullable=True, ge=0.0, coerce=True
    )  # Allow null prices, must be non-negative when present
    volume: Series[pd.Int64Dtype]

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True


class IntradayData(DataFrameModel):
    date: Series[str]  # ISO format date string
    open: Series[pd.Float64Dtype]
    high: Series[pd.Float64Dtype]
    low: Series[pd.Float64Dtype]
    close: Series[pd.Float64Dtype]
    volume: Series[pd.Int64Dtype]

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True


class EODData(DataFrameModel):
    symbol: Series[str]
    date: Series[str]  # ISO format date string
    price: Series[pd.Float64Dtype]
    volume: Series[pd.Int64Dtype]

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True


class IndexConstituent(DataFrameModel):
    """
    Schema for S&P 500 constituents from FMP API.
    https://site.financialmodelingprep.com/developer/docs/stable/sp-500
    """
    symbol: Series[str]
    name: Series[str] = Field(nullable=True)
    sector: Series[str] = Field(nullable=True)
    subSector: Series[str] = Field(nullable=True)
    headQuarter: Series[str] = Field(nullable=True)
    dateFirstAdded: Series[str] = Field(nullable=True)
    cik: Series[str] = Field(nullable=True)
    founded: Series[str] = Field(nullable=True)

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True


class CommodityQuote(DataFrameModel):
    """
    Schema for commodity quotes from legacy FMP API endpoint /api/v3/quotes/commodity.
    https://site.financialmodelingprep.com/developer/docs/commodities-prices-api
    """
    symbol: Series[str]
    price: Series[pd.Float64Dtype]
    change: Series[pd.Float64Dtype]
    volume: Series[pd.Int64Dtype]

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True


class ExchangeQuote(DataFrameModel):
    """
    Schema for bulk exchange quotes from FMP API endpoint /stable/batch-exchange-quote.
    Only relevant fields are captured; extras are filtered out by strict="filter".
    """
    symbol: Series[str]
    price: Series[pd.Float64Dtype] = Field(nullable=True)
    volume: Series[pd.Int64Dtype] = Field(nullable=True)
    change: Series[pd.Float64Dtype] = Field(nullable=True)

    class Config(DataFrameModel.Config):
        strict = "filter"
        coerce = True


class FMPBlock(Block):
    api_key: SecretStr

    base_url: str = PydanticField(default="https://financialmodelingprep.com/")

    @property
    def logger(self) -> logging.Logger:
        if not hasattr(self, "_logger"):
            self._logger = get_logger_safe(__name__)
        return self._logger
    

    def _get_jsonparsed_data(self, endpoint: FMPEndpoint) -> Union[dict[str, Any], list[dict[str, Any]]]:
        base_endpoint_url = endpoint.get_url(self.base_url)
        separator = "&" if "?" in base_endpoint_url else "?"
        url = base_endpoint_url + separator + "apikey=" + self.api_key.get_secret_value()
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
        null_price_mask = df["price"].isna()
        filtered_symbols = cast(list[str], df.loc[null_price_mask, "symbol"].tolist())
        filtered_df = df.loc[~null_price_mask].copy()
        if filtered_symbols:
            self.logger.info(f"Filtered out {len(filtered_symbols)} symbols with null prices: {filtered_symbols}")
        return filtered_df, filtered_symbols

    def get_active_tickers(self) -> DataFrame[ActiveTicker]:
        """
        List all actively trading companies and financial instruments with the FMP Actively Trading List API.
        This endpoint allows users to filter and display securities that are currently being traded on public exchanges,
        ensuring you access real-time market activity.
        """
        data = self._get_jsonparsed_data(FMPEndpoint(FMPAPI.STABLE, "actively-trading-list"))
        if isinstance(data, list):
            return DataFrame[ActiveTicker](data)
        return DataFrame[ActiveTicker](pd.DataFrame())

    def get_ticker_detail(self, symbol: str):
        """
        Access detailed company profile data with the FMP Company Profile Data API.
        This API provides key financial and operational information for a specific stock symbol,
        including the company's market capitalization, stock price, industry, and much more.
        """
        data = self._get_jsonparsed_data(FMPEndpoint(FMPAPI.STABLE, "profile", {"symbol": symbol}))
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
        data = self._get_jsonparsed_data(FMPEndpoint(FMPAPI.STABLE, f"batch-quote-short?symbols={symbol_str}"))
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
        data = self._get_jsonparsed_data(FMPEndpoint(FMPAPI.STABLE, path))
        if isinstance(data, list):
            return DataFrame[IntradayData](data)
        elif isinstance(data, dict) and "historical" in data:  # type: ignore
            return DataFrame[IntradayData](data["historical"])
        return DataFrame[IntradayData](pd.DataFrame())

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
        data = self._get_jsonparsed_data(FMPEndpoint(FMPAPI.STABLE, path, params))
        if not data:
            return cast(DataFrame[EODData], pd.DataFrame())
        df = pd.DataFrame(data)
        df["symbol"] = symbol
        return DataFrame[EODData](df)

    def _fetch_fmp_list_data(
        self,
        endpoint: FMPEndpoint,
        model_type: type[PanderaModelType],
        log_entity_name: str,
        client_side_filter_fn: Optional[Callable[[list[dict[str, Any]]], list[dict[str, Any]]]] = None,
    ) -> DataFrame[PanderaModelType]:
        """
        Private helper to fetch list-based data from FMP, apply an optional client-side filter,
        and convert to a Pandera DataFrame.

        Args:
            path: The FMP API path (without base_url or apikey).
            model_type: The Pandera DataFrameModel class for validation.
            log_entity_name: A descriptive name for the data being fetched (for logging).
            client_side_filter_fn: An optional function to filter the raw list of dictionaries.

        Returns:
            A Pandera DataFrame of the specified model_type. Returns an empty DataFrame on error
            or if no data is found/passes filters.
        """
        self.logger.info(f"Fetching {log_entity_name} from FMP: {endpoint.get_url(self.base_url)}")
        try:
            raw_data: Union[dict[str, Any], list[dict[str, Any]]] = self._get_jsonparsed_data(endpoint)

            if not isinstance(raw_data, list):
                # This is an unexpected API response format, treat as an error.
                raise ValueError(
                    f"Unexpected data format received for {log_entity_name}: {type(raw_data)}. Expected list."
                )

            if not raw_data:
                self.logger.info(f"FMP returned an empty list for {log_entity_name}.")
                return create_empty_df(model_type)

            self.logger.debug(f"Fetched {len(raw_data)} raw records for {log_entity_name}.")
            processed_data: list[dict[str, Any]] = raw_data
            if client_side_filter_fn:
                processed_data = client_side_filter_fn(raw_data)
                self.logger.info(
                    f"Applied client-side filter to {log_entity_name}. "
                    f"Original count: {len(raw_data)}, Filtered count: {len(processed_data)}."
                )
                if not processed_data:
                    self.logger.info(f"All records for {log_entity_name} were filtered out client-side.")
                    return create_empty_df(model_type)
            
            # Convert list of dicts directly to a typed Pandera DataFrame
            self.logger.info(f"Successfully fetched {len(processed_data)} {log_entity_name} records.")
            try:
                df_raw = pd.DataFrame(processed_data)
                df_raw = df_raw.convert_dtypes(dtype_backend="numpy_nullable")
                schema = model_type.to_schema()
                all_schema_columns = list(schema.columns.keys())
                df_for_validation = df_raw.reindex(columns=all_schema_columns)
                df_validated = model_type.validate(df_for_validation, lazy=True)
                self.logger.info(f"Successfully fetched & validated {len(df_validated)} {log_entity_name} records.")
                return df_validated
            except Exception as e: # Catch PanderaError or other validation issues
                self.logger.error(f"Schema validation failed for {log_entity_name} after processing: {e}", exc_info=True)
                raise # Re-raise validation errors

        except Exception as e: # Catch issues from _get_jsonparsed_data or other initial errors
            self.logger.error(f"Error fetching or processing {log_entity_name}: {e}", exc_info=True)
            raise # Re-raise fetching/processing errors

    def get_equities(self, exchange: FMPExchange) -> DataFrame[ExchangeQuote]:
        """
        All equities on a given exchange in one go.
        """
        params = {"exchange": exchange.value}

        return self._fetch_fmp_list_data(
            endpoint=FMPEndpoint(FMPAPI.STABLE, "batch-exchange-quote", params),
            model_type=ExchangeQuote,
            log_entity_name=f"{exchange.name} equities",
        )

    def get_sp500_constituents(self) -> DataFrame[IndexConstituent]:
        """
        The full S&P 500 constituent list.
        """
        return self._fetch_fmp_list_data(
            endpoint=FMPEndpoint(FMPAPI.STABLE, "sp500-constituent"),
            model_type=IndexConstituent,
            log_entity_name="S&P 500 constituents",
        )

    def get_cme_commodity_quotes(self) -> DataFrame[CommodityQuote]:
        """
        All commodity quotes (CME Group).
        """
        return self._fetch_fmp_list_data(
            endpoint=FMPEndpoint(FMPAPI.STABLE, "batch-commodity-quotes"),
            model_type=CommodityQuote,
            log_entity_name="CME commodity quotes",
        )
