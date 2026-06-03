"""Yahoo Finance Prefect block for EPS data."""

from __future__ import annotations

from datetime import date
import logging
import os
import random
import time

import pandas as pd
from pandera.typing import DataFrame
import platformdirs
from prefect.blocks.core import Block
from pydantic import Field

from tsn_adapters.blocks.fmp import EarningsData
from tsn_adapters.utils.logging import get_logger_safe

_PRE_CALL_JITTER_S: tuple[float, float] = (0.5, 2.0)
_BACKOFF_BASE_S: float = 5.0
_BACKOFF_JITTER_S: tuple[float, float] = (0.0, 3.0)
_MAX_ATTEMPTS: int = 3

_MODERN_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

_COOKIE_CACHE_PATH = os.path.join(platformdirs.user_cache_dir(), "py-yfinance", "cookies.db")

_yfinance_patched = False


def _patch_yfinance(logger: logging.Logger | None = None) -> None:
    """Apply one-time monkey-patches to yfinance.

    1. Replaces the ancient Chrome/39 UA with a modern one (Yahoo blocks the
       2014-era string even from non-AWS IPs).
    2. Guards ``get_earnings_dates`` against a missing ``Surprise(%)`` column
       that yfinance 0.2.49 expects but Yahoo's HTML no longer includes.

    Safe to call multiple times — only patches once per process.
    """
    global _yfinance_patched
    if _yfinance_patched:
        return

    import yfinance.data as yfdata

    yfdata.YfData.user_agent_headers = property(lambda self: {"User-Agent": _MODERN_UA})

    import yfinance as yf

    _orig_get_earnings_dates = yf.Ticker.get_earnings_dates

    def _safe_get_earnings_dates(self: yf.Ticker, limit: int = 12, **kwargs):  # type: ignore[no-untyped-def]
        try:
            return _orig_get_earnings_dates(self, limit=limit, **kwargs)
        except KeyError as exc:
            if "Surprise" in str(exc):
                if logger:
                    logger.warning("yfinance Surprise(%%) column missing — falling back to JSON")
                return None
            raise

    yf.Ticker.get_earnings_dates = _safe_get_earnings_dates  # type: ignore[assignment]

    _yfinance_patched = True


def _clear_stale_cookie_cache() -> None:
    """Delete yfinance's persistent cookie cache.

    yfinance caches cookies in SQLite. If those were created without a proxy,
    reusing them through a proxy still gets 429'd. Clearing once on startup
    forces a fresh cookie fetch through the proxy.
    """
    try:
        os.remove(_COOKIE_CACHE_PATH)
    except (FileNotFoundError, PermissionError, OSError):
        pass


def _announcement_to_period_end(announcement: date) -> str:
    """Map a Yahoo "Earnings Date" (announcement date) to its fiscal-period-end.

    The contract for ``get_historical_earnings`` is that the ``date`` column is
    the **fiscal-period-end** — that is what the JSON ``earnings_history``
    accessor returns, what FMP is reduced to via its income-statement join, and
    what the reconciler's ``canonical_key`` expects. But the HTML-scrape
    accessor (``get_earnings_dates``) dates each row by the *announcement*
    (earnings-call) date instead — ~3-5 weeks after the quarter closed. Left
    unconverted, those rows land in the wrong ``(year, calendar_quarter)``
    bucket and silently fail to reconcile against FMP.

    The announcement always sits close to a calendar-quarter boundary, so we
    snap to the nearest quarter-end. That lands in the correct
    ``(year, calendar_quarter)`` for both calendar-quarter reporters
    (META/AAPL/GOOGL/AMZN/MSFT/TSLA — period-ends 03-31/06-30/09-30/12-31) and
    NVDA's off-cycle Jan/Apr/Jul/Oct fiscal — verified across all four quarters
    of each.

    This is the **fallback** for ``_resolve_period_end``: it keeps the canonical
    quarter correct, but the literal date can differ from the JSON accessor for
    off-cycle fiscals (NVDA → 06-30 here vs the JSON's actual 04-30). Prefer the
    JSON-derived period-end so both accessors publish an identical date.
    """
    candidates = [
        date(y, m, 31 if m in (3, 12) else 30)
        for y in (announcement.year - 1, announcement.year, announcement.year + 1)
        for m in (3, 6, 9, 12)
    ]
    return min(candidates, key=lambda q: abs((q - announcement).days)).isoformat()


def _resolve_period_end(announcement: date, period_ends: list[date]) -> str:
    """Resolve a scraped announcement date to its fiscal-period-end.

    Prefers the actual period-end reported by the JSON ``earnings_history``
    accessor — the most recent quarter close on or before the announcement —
    so the scrape and JSON paths publish an **identical** ``date`` for a given
    quarter. This is what keeps off-cycle fiscals consistent: NVDA's period-end
    is the month-end ``2026-04-30``, not the calendar quarter-end ``2026-06-30``.

    Falls back to ``_announcement_to_period_end`` (nearest calendar quarter-end)
    only when no known period-end precedes the announcement — e.g. quarters
    older than ``earnings_history``'s short window, or when that accessor is
    unavailable.
    """
    prior = [pe for pe in period_ends if pe <= announcement]
    if prior:
        return max(prior).isoformat()
    return _announcement_to_period_end(announcement)


class YahooBlock(Block):
    """Prefect block wrapping yfinance for EPS data.

    No API key required — yfinance uses Yahoo Finance's public endpoints.

    When ``proxy_url`` is set, all yfinance traffic is routed through the
    proxy via ``HTTP_PROXY``/``HTTPS_PROXY`` env vars, the User-Agent is
    patched to a modern browser string, and any stale cookie cache is cleared.
    """

    proxy_url: str | None = Field(
        default=None,
        description=(
            "HTTP proxy URL for Yahoo Finance requests. "
            "Required from AWS/cloud IPs where Yahoo aggressively rate-limits. "
            "Example: http://user:pass@proxy.example.com:823"
        ),
    )

    _proxy_initialized: bool = False

    @property
    def logger(self) -> logging.Logger:
        if not hasattr(self, "_logger"):
            self._logger = get_logger_safe(__name__)
        return self._logger

    def _ensure_proxy(self) -> None:
        if self._proxy_initialized:
            return

        proxy = self.proxy_url
        if proxy:
            os.environ["HTTP_PROXY"] = proxy
            os.environ["HTTPS_PROXY"] = proxy
            os.environ.setdefault("NO_PROXY", "localhost,127.0.0.1,172.17.0.1,169.254.169.254")
            _clear_stale_cookie_cache()
            self.logger.info("Yahoo proxy configured — routing yfinance through proxy")

        _patch_yfinance(self.logger)
        self._proxy_initialized = True

    def get_historical_earnings(self, symbol: str, limit: int = 40) -> DataFrame[EarningsData]:
        """Fetch earnings history for `symbol` from Yahoo Finance.

        Tries ``yfinance.Ticker.get_earnings_dates(limit=N)`` first (HTML scrape,
        up to 10 years of quarterly data). Falls back to the JSON-based
        ``earnings_history`` accessor (last 4 quarters) when the scraper
        returns empty — e.g. when yfinance 0.2.49 encounters a column
        Yahoo no longer serves.

        Both paths return ``date`` as the **fiscal-period-end** — the scrape's
        announcement date is resolved to the period-end the JSON accessor would
        report for the same quarter — so callers and the reconciler can treat
        the two accessors interchangeably, and both publish a row for a given
        quarter under an identical date. Note the two Yahoo endpoints can report
        different `epsActual` for the same quarter; the scrape's "Reported EPS"
        is generally the more accurate of the two.

        Raises on persistent failures so the calling @task retry decorator can
        take over.
        """
        import yfinance as yf

        self._ensure_proxy()

        time.sleep(random.uniform(*_PRE_CALL_JITTER_S))

        last_exc: Exception | None = None
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                ticker = yf.Ticker(symbol)
                df = ticker.get_earnings_dates(limit=limit)
            except Exception as exc:
                last_exc = exc
                self.logger.error(f"yfinance scrape raised for {symbol} (attempt {attempt}/{_MAX_ATTEMPTS}): {exc}")
                df = None

            if df is not None and not df.empty:
                return self._normalize_scrape(df, symbol, self._period_ends_from_history(symbol))

            # Fallback: JSON-based earnings_history (last 4 quarters only)
            try:
                ticker = yf.Ticker(symbol)
                eh = ticker.earnings_history
                if eh is not None and hasattr(eh, "empty") and not eh.empty:
                    self.logger.info(f"Scrape empty for {symbol} — using JSON earnings_history fallback")
                    return self._normalize_json(eh, symbol)
            except Exception as fallback_exc:
                self.logger.warning(f"JSON fallback also failed for {symbol}: {fallback_exc}")
                if last_exc is None:
                    last_exc = fallback_exc

            if attempt < _MAX_ATTEMPTS:
                delay = _BACKOFF_BASE_S * (2 ** (attempt - 1)) + random.uniform(*_BACKOFF_JITTER_S)
                self.logger.warning(
                    f"No data for {symbol} (attempt {attempt}/{_MAX_ATTEMPTS}) — " f"sleeping {delay:.1f}s before retry"
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

    def _period_ends_from_history(self, symbol: str) -> list[date]:
        """Fiscal-period-end dates from the JSON ``earnings_history`` accessor.

        Used to date scraped rows identically to the JSON path (see
        ``_resolve_period_end``). Returns ``[]`` on any failure, so the scrape
        path degrades to the calendar-quarter approximation rather than raising.
        """
        import yfinance as yf

        try:
            eh = yf.Ticker(symbol).earnings_history
            if hasattr(eh, "empty") and not eh.empty:
                return [pd.Timestamp(d).date() for d in eh.index]
        except Exception as exc:
            self.logger.warning(f"period-end lookup for {symbol} unavailable: {exc}")
        return []

    @staticmethod
    def _normalize_scrape(
        df: pd.DataFrame, symbol: str, period_ends: list[date] | None = None
    ) -> DataFrame[EarningsData]:
        """Normalize output from ``get_earnings_dates`` (HTML scrape).

        ``get_earnings_dates`` keys each row by the *announcement* date, so we
        resolve it to the fiscal-period-end (see ``_resolve_period_end``) to
        honor the ``date == period-end`` contract shared with the JSON accessor
        and the reconciler. ``period_ends`` are the JSON accessor's actual
        period-ends; without them each announcement falls back to the nearest
        calendar quarter-end. Without this step scraped rows reconcile in the
        wrong calendar quarter.
        """
        pes = period_ends or []
        df = df.reset_index()
        date_col = df.columns[0]  # "Earnings Date" — announcement date, timezone-aware
        announced = pd.to_datetime(df[date_col])
        df["date"] = [_resolve_period_end(ts.date(), pes) for ts in announced]
        df["symbol"] = symbol
        df = df.rename(
            columns={
                "Reported EPS": "epsActual",
                "EPS Estimate": "epsEstimated",
            }
        )
        df["lastUpdated"] = None

        return DataFrame[EarningsData](df[["symbol", "date", "epsEstimated", "epsActual", "lastUpdated"]])

    @staticmethod
    def _normalize_json(df: pd.DataFrame, symbol: str) -> DataFrame[EarningsData]:
        """Normalize output from ``Ticker.earnings_history`` (JSON quoteSummary).

        The DataFrame has a DatetimeIndex (quarter-end dates) with columns
        ``epsActual`` and ``epsEstimate``.
        """
        df = df.copy()
        df["date"] = pd.to_datetime(df.index).strftime("%Y-%m-%d")
        df = df.reset_index(drop=True)
        df["symbol"] = symbol
        df = df.rename(columns={"epsEstimate": "epsEstimated"})
        df["lastUpdated"] = None

        return DataFrame[EarningsData](df[["symbol", "date", "epsEstimated", "epsActual", "lastUpdated"]])
