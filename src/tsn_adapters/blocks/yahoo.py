"""Yahoo Finance Prefect block for EPS data."""
from __future__ import annotations

import logging
import random
import time

import pandas as pd
from pandera.typing import DataFrame
from prefect.blocks.core import Block

from tsn_adapters.blocks.fmp import EarningsData
from tsn_adapters.utils.logging import get_logger_safe

# Yahoo aggressively rate-limits non-residential IPs (notably AWS).
# On HTTP 429, yfinance.Ticker.get_earnings_dates returns None without
# raising — so empty/None results must be treated as failures to retry.
# We retry inside the block; the caller's @task retry stays a backstop.
_PRE_CALL_JITTER_S: tuple[float, float] = (0.5, 2.0)
_BACKOFF_BASE_S: float = 5.0
_BACKOFF_JITTER_S: tuple[float, float] = (0.0, 3.0)
_MAX_ATTEMPTS: int = 3


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

        Raises on persistent yfinance failures so the calling @task retry
        decorator can take over. An empty result after _MAX_ATTEMPTS is
        treated as a failure and raised — yfinance returns None on HTTP 429
        without surfacing an exception.
        """
        import yfinance as yf

        time.sleep(random.uniform(*_PRE_CALL_JITTER_S))

        last_exc: Exception | None = None
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                df = yf.Ticker(symbol).get_earnings_dates(limit=limit)
            except Exception as exc:
                last_exc = exc
                self.logger.error(
                    f"yfinance fetch raised for {symbol} (attempt {attempt}/{_MAX_ATTEMPTS}): {exc}"
                )
                df = None

            if df is not None and not df.empty:
                return self._normalize(df, symbol)

            if attempt < _MAX_ATTEMPTS:
                delay = _BACKOFF_BASE_S * (2 ** (attempt - 1)) + random.uniform(*_BACKOFF_JITTER_S)
                self.logger.warning(
                    f"yfinance returned no data for {symbol} (attempt {attempt}/{_MAX_ATTEMPTS}) — "
                    f"likely HTTP 429; sleeping {delay:.1f}s before retry"
                )
                time.sleep(delay)

        msg = (
            f"yfinance returned no earnings for {symbol} after {_MAX_ATTEMPTS} attempts — "
            f"Yahoo Finance is likely rate-limiting this worker (HTTP 429)"
        )
        self.logger.error(msg)
        if last_exc is not None:
            raise RuntimeError(msg) from last_exc
        raise RuntimeError(msg)

    @staticmethod
    def _normalize(df: pd.DataFrame, symbol: str) -> DataFrame[EarningsData]:
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
