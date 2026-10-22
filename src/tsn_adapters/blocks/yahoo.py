"""Yahoo Finance Prefect block for EPS data."""

from __future__ import annotations

import calendar
from datetime import date
from io import StringIO
import logging
import os
import random
import re
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

# Yahoo's earnings page, newest rows first. 25 is the page's smallest size and
# covers about six years of quarters plus the next report.
_EARNINGS_PAGE_URL = "https://finance.yahoo.com/calendar/earnings"
_EARNINGS_PAGE_ROWS = 25

# English month abbreviations; calendar.month_abbr follows the process locale.
_MONTHS = {
    name: number
    for number, name in enumerate(
        ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), start=1
    )
}
_PAGE_DAY = re.compile(r"([A-Za-z]{3})[A-Za-z]*\.? (\d{1,2}), (\d{4})")

_yfinance_patched = False


def _patch_yfinance() -> None:
    """Apply a one-time monkey-patch to yfinance.

    Replaces the ancient Chrome/39 UA with a modern one (Yahoo blocks the
    2014-era string even from non-AWS IPs).

    Safe to call multiple times — only patches once per process.
    """
    global _yfinance_patched
    if _yfinance_patched:
        return

    import yfinance.data as yfdata

    yfdata.YfData.user_agent_headers = property(lambda self: {"User-Agent": _MODERN_UA})

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

    The contract for the block's readers is that the ``date`` column is the
    **fiscal-period-end** — that is what the JSON ``earnings_history`` accessor
    returns, what FMP is reduced to via its income-statement join, and what the
    reconciler's ``canonical_key`` expects. But the earnings page dates each row
    by the *announcement* (earnings-call) date instead — ~3-5 weeks after the
    quarter closed. Left unconverted, those rows land in the wrong
    ``(year, calendar_quarter)`` bucket and silently fail to reconcile against
    FMP.

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


def _quarter_after(period_end: date) -> date:
    """The month end three months after ``period_end`` (06-30 → 09-30, 07-31 → 10-31)."""
    month = period_end.month + 3
    year = period_end.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    return date(year, month, calendar.monthrange(year, month)[1])


def _resolve_period_end(announcement: date, period_ends: list[date]) -> str:
    """Resolve an earnings-page announcement date to its fiscal-period-end.

    Prefers the period-ends reported by the JSON ``earnings_history`` accessor,
    so the page and JSON paths publish an **identical** ``date`` for a given
    quarter. This is what keeps off-cycle fiscals consistent: NVDA's period-end
    is the month-end ``2026-04-30``, not the calendar quarter-end ``2026-06-30``.

    The JSON feed can add a quarter hours after the page shows its figure, so
    the newest period-end on or before the announcement can be the previous
    quarter's. Stepping forward by whole quarters to the latest period-end on
    or before the announcement dates a new report correctly (TSLA's 2026-10-21
    report → 2026-09-30, not 2026-06-30; NVDA's 2026-11-17 → 2026-10-31).

    Falls back to ``_announcement_to_period_end`` (nearest calendar quarter-end)
    only when no known period-end precedes the announcement — e.g. quarters
    older than ``earnings_history``'s short window, or when that accessor is
    unavailable.
    """
    prior = [pe for pe in period_ends if pe <= announcement]
    if not prior:
        return _announcement_to_period_end(announcement)
    period_end = max(prior)
    while _quarter_after(period_end) <= announcement:
        period_end = _quarter_after(period_end)
    return period_end.isoformat()


def _page_announcement_day(text: str) -> date:
    """The day in an earnings-page "Earnings Date" cell.

    Reads the page's current "October 21, 2026 at 4 PM EDT" and its earlier
    "Oct 21, 2026, 4 PM EDT". The day is the one the page shows, US Eastern,
    the same day FMP dates the announcement by.
    """
    match = _PAGE_DAY.search(text)
    if match is None or match.group(1).title() not in _MONTHS:
        raise ValueError(f"unrecognized Earnings Date on Yahoo's earnings page: {text!r}")
    month, day, year = match.groups()
    return date(int(year), _MONTHS[month.title()], int(day))


def _parse_earnings_page(html: str) -> pd.DataFrame:
    """Rows of Yahoo's earnings page, indexed by announcement day.

    Returns columns ``EPS Estimate`` and ``Reported EPS``; a report still to
    come shows "-", which reads as NaN. Raises ``ValueError`` when the page has
    no earnings table (a consent or error page, or a changed layout).
    """
    for table in pd.read_html(StringIO(html), na_values=["-"]):
        if {"Earnings Date", "EPS Estimate", "Reported EPS"} <= set(table.columns):
            break
    else:
        raise ValueError("no earnings table on Yahoo's earnings page")
    table = table.dropna(subset=["Earnings Date"])
    announced = [pd.Timestamp(_page_announcement_day(str(text))) for text in table["Earnings Date"]]
    return pd.DataFrame(
        {
            "EPS Estimate": pd.to_numeric(table["EPS Estimate"], errors="coerce").to_numpy(),
            "Reported EPS": pd.to_numeric(table["Reported EPS"], errors="coerce").to_numpy(),
        },
        index=pd.DatetimeIndex(announced, name="Earnings Date"),
    )


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

        _patch_yfinance()
        self._proxy_initialized = True

    def get_historical_earnings(self, symbol: str, limit: int = 40) -> DataFrame[EarningsData]:
        """Fetch the last four quarters of EPS for `symbol` from Yahoo's JSON feed.

        Reads ``yfinance.Ticker.earnings_history`` (quoteSummary
        ``earningsHistory``), dated by fiscal-period-end. ``limit`` stays for
        the callers' signature; the feed returns four quarters.

        Yahoo's earnings page is read on its own, by ``get_page_earnings``: it
        is a separate dataset, sometimes on a different basis, so it never
        stands in for the feed here (the historical backfill writes whatever
        this returns to the write-once Yahoo stream).

        Raises on persistent failures so the calling @task retry decorator can
        take over.
        """
        import yfinance as yf

        self._ensure_proxy()

        time.sleep(random.uniform(*_PRE_CALL_JITTER_S))

        last_exc: Exception | None = None
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                eh = yf.Ticker(symbol).earnings_history
                if eh is not None and hasattr(eh, "empty") and not eh.empty:
                    return self._normalize_json(eh, symbol)
            except Exception as exc:
                last_exc = exc
                self.logger.error(
                    f"yfinance earnings_history raised for {symbol} (attempt {attempt}/{_MAX_ATTEMPTS}): {exc}"
                )

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

    def get_page_earnings(self, symbol: str, period_ends: list[date]) -> DataFrame[EarningsData]:
        """Reported EPS from Yahoo's earnings page, dated by fiscal-period-end.

        The page can show a new quarter's figure hours before the JSON feed
        (Meta's 2026-10-21 report: on the page by 01:07 UTC the next day, still
        missing from the feed at 03:35 UTC). It is a separate dataset, and its
        basis can differ from the feed's (Meta Q3 2025: page 7.25 adjusted,
        feed 1.05 GAAP), so callers use a page reading only where it matches
        another source.

        ``period_ends`` are the JSON feed's quarter ends, which date each
        announcement (see ``_resolve_period_end``). One request, no retries:
        callers treat the page as an extra source and carry on without it.
        """
        from yfinance.data import YfData

        self._ensure_proxy()

        time.sleep(random.uniform(*_PRE_CALL_JITTER_S))

        # The shared yfinance session (cookie, crumb, proxy, User-Agent). get(),
        # not cache_get(): cache_get keeps a response for the life of the process.
        response = YfData(session=None).get(
            _EARNINGS_PAGE_URL, params={"symbol": symbol, "offset": 0, "size": _EARNINGS_PAGE_ROWS}
        )
        response.raise_for_status()
        return self._normalize_scrape(_parse_earnings_page(response.text), symbol, period_ends)

    @staticmethod
    def _normalize_scrape(
        df: pd.DataFrame, symbol: str, period_ends: list[date] | None = None
    ) -> DataFrame[EarningsData]:
        """Normalize earnings-page rows (``_parse_earnings_page`` output).

        The page keys each row by the *announcement* date, so we resolve it to
        the fiscal-period-end (see ``_resolve_period_end``) to honor the
        ``date == period-end`` contract shared with the JSON accessor and the
        reconciler. ``period_ends`` are the JSON accessor's actual period-ends;
        without them each announcement falls back to the nearest calendar
        quarter-end. Without this step page rows reconcile in the wrong
        calendar quarter.
        """
        pes = period_ends or []
        df = df.reset_index()
        date_col = df.columns[0]  # "Earnings Date" — announcement date
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
