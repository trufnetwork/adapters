"""Yahoo Finance Prefect block for EPS data."""
from __future__ import annotations

import logging

import pandas as pd
from pandera.typing import DataFrame
from prefect.blocks.core import Block

from tsn_adapters.blocks.fmp import EarningsData
from tsn_adapters.utils.create_empty_df import create_empty_df
from tsn_adapters.utils.logging import get_logger_safe


class YahooBlock(Block):
    """Prefect block wrapping yfinance for EPS data.

    No API key required — yfinance uses Yahoo Finance's public endpoints.
    """

    @property
    def logger(self) -> logging.Logger:
        if not hasattr(self, "_logger"):
            self._logger = get_logger_safe(__name__)
        return self._logger

    def get_historical_earnings(self, symbol: str, limit: int = 40) -> DataFrame[EarningsData]:
        """Fetch earnings history for `symbol` from Yahoo Finance.

        Uses yfinance.Ticker.get_earnings_dates(limit=N).
        limit=40 covers ~10 years of quarterly data.
        Returns an EarningsData-compatible DataFrame; epsActual is NaN
        for future (not-yet-reported) quarters.
        """
        try:
            import yfinance as yf

            df = yf.Ticker(symbol).get_earnings_dates(limit=limit)
        except Exception as exc:
            self.logger.warning(f"yfinance fetch failed for {symbol}: {exc}")
            return create_empty_df(EarningsData)

        if df is None or df.empty:
            return create_empty_df(EarningsData)

        df = df.reset_index()
        date_col = df.columns[0]  # "Earnings Date" — timezone-aware DatetimeTZDtype
        df["date"] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")
        df["symbol"] = symbol
        df = df.rename(columns={
            "Reported EPS": "epsActual",
            "EPS Estimate": "epsEstimated",
        })
        df["lastUpdated"] = None

        return DataFrame[EarningsData](
            df[["symbol", "date", "epsEstimated", "epsActual", "lastUpdated"]]
        )
