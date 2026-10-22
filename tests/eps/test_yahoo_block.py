"""Unit tests for YahooBlock EPS normalization and its two readers.

Covers the period-end normalization that keeps Yahoo's two datasets
interchangeable. The earnings page dates rows by the announcement date while
``earnings_history`` (JSON) dates them by the fiscal-period-end; the block must
reduce both to period-end so they land in the same ``canonical_key`` bucket.
Regression guard for the silent mis-bucketing where page rows failed to
reconcile against FMP. Also covers the page parser against Yahoo's markup and
the split between the JSON reader and the page reader.
"""

from datetime import date
from typing import ClassVar

import pandas as pd
import pytest

import tsn_adapters.blocks.yahoo as yahoo_module
from tsn_adapters.blocks.yahoo import (
    YahooBlock,
    _announcement_to_period_end,
    _page_announcement_day,
    _parse_earnings_page,
    _resolve_period_end,
)
from tsn_adapters.tasks.eps.reconciler import canonical_key


def _scrape_df(rows: list[dict]) -> pd.DataFrame:
    """Mimic ``yfinance.Ticker.get_earnings_dates`` output (announcement-indexed)."""
    idx = pd.DatetimeIndex(
        [pd.Timestamp(r["announce"], tz="America/New_York") for r in rows],
        name="Earnings Date",
    )
    return pd.DataFrame(
        {
            "EPS Estimate": [r.get("est") for r in rows],
            "Reported EPS": [r["eps"] for r in rows],
            "Surprise(%)": [None] * len(rows),
        },
        index=idx,
    )


def _json_df(rows: list[dict]) -> pd.DataFrame:
    """Mimic ``yfinance.Ticker.earnings_history`` output (period-end-indexed)."""
    idx = pd.DatetimeIndex([pd.Timestamp(r["period_end"]) for r in rows], name="quarter")
    return pd.DataFrame(
        {"epsActual": [r["eps"] for r in rows], "epsEstimate": [r.get("est") for r in rows]},
        index=idx,
    )


# (announcement date, expected period-end, expected (year, quarter)) — verified
# against real Mag-7 release dates. NVDA exercises the off-cycle fiscal calendar.
@pytest.mark.parametrize(
    "announce, period_end, year, quarter",
    [
        # META / calendar-quarter reporters: announce ~4 weeks after quarter-end
        ("2026-04-29", "2026-03-31", 2026, 1),
        ("2026-01-28", "2025-12-31", 2025, 4),
        ("2025-10-29", "2025-09-30", 2025, 3),
        ("2025-07-30", "2025-06-30", 2025, 2),
        # NVDA: Jan/Apr/Jul/Oct fiscal — snapped quarter-end differs from the
        # literal period-end but resolves to the same canonical calendar quarter.
        ("2026-05-20", "2026-06-30", 2026, 2),
        ("2026-02-25", "2026-03-31", 2026, 1),
        ("2025-11-19", "2025-12-31", 2025, 4),
        ("2025-08-27", "2025-09-30", 2025, 3),
    ],
)
def test_announcement_to_period_end(announce: str, period_end: str, year: int, quarter: int) -> None:
    got = _announcement_to_period_end(pd.Timestamp(announce).date())
    assert got == period_end
    # The whole point: it lands in the correct calendar quarter.
    assert canonical_key("X", got) == ("X", year, quarter)


def test_normalize_scrape_emits_period_end_dates() -> None:
    """Scraped rows must come out period-end dated, not announcement dated."""
    df = _scrape_df(
        [
            {"announce": "2026-04-29", "eps": 7.31, "est": 6.82},  # META Q1'26
            {"announce": "2026-01-28", "eps": 8.88, "est": 8.18},  # META Q4'25
        ]
    )
    out = YahooBlock._normalize_scrape(df, "META")
    assert list(out["date"]) == ["2026-03-31", "2025-12-31"]
    assert list(out["epsActual"]) == [7.31, 8.88]
    assert list(out["symbol"]) == ["META", "META"]


def test_scrape_and_json_reconcile_to_same_key() -> None:
    """A scrape row and a JSON row for the same quarter share a canonical key.

    This is the invariant the reconciler depends on: regardless of which Yahoo
    accessor produced the row, the same fiscal quarter maps to one bucket.
    """
    scrape = YahooBlock._normalize_scrape(_scrape_df([{"announce": "2026-04-29", "eps": 7.31}]), "META")
    json_ = YahooBlock._normalize_json(_json_df([{"period_end": "2026-03-31", "eps": 7.31}]), "META")
    assert canonical_key("META", scrape["date"].iloc[0]) == canonical_key("META", json_["date"].iloc[0])


def test_normalize_json_preserves_period_end() -> None:
    """Regression guard: the JSON path stays period-end dated."""
    out = YahooBlock._normalize_json(
        _json_df([{"period_end": "2026-03-31", "eps": 10.44}, {"period_end": "2025-12-31", "eps": 8.88}]),
        "META",
    )
    assert list(out["date"]) == ["2026-03-31", "2025-12-31"]


# (announcement, JSON period-ends, expected date) — the JSON period-end wins;
# fall back to the calendar quarter-end only when none precede the announcement.
@pytest.mark.parametrize(
    "announce, period_ends, expected",
    [
        # NVDA off-cycle: the JSON month-end (04-30) wins over the calendar snap (06-30)
        ("2026-05-20", ["2026-01-31", "2026-04-30"], "2026-04-30"),
        ("2025-11-19", ["2025-07-31", "2025-10-31"], "2025-10-31"),
        # META: JSON period-end and calendar snap already agree (03-31)
        ("2026-04-29", ["2025-12-31", "2026-03-31"], "2026-03-31"),
        # No known period-end precedes the announcement -> calendar-quarter fallback
        ("2026-05-20", [], "2026-06-30"),
        ("2026-05-20", ["2026-09-30"], "2026-06-30"),
        # A new report before the JSON feed has its quarter: step forward by whole
        # quarters, never back to the previous quarter's period-end
        ("2026-10-21", ["2025-12-31", "2026-03-31", "2026-06-30"], "2026-09-30"),  # TSLA, META Q3
        ("2026-11-02", ["2025-12-31", "2026-03-31", "2026-06-30"], "2026-09-30"),  # AAPL Q4 FY26
        ("2026-11-17", ["2026-01-31", "2026-04-30", "2026-07-31"], "2026-10-31"),  # NVDA Q3 FY27
        ("2026-10-21", ["2026-03-31"], "2026-09-30"),  # two quarters behind
        # An older report keeps its own quarter
        ("2026-07-22", ["2026-03-31", "2026-06-30", "2026-09-30"], "2026-06-30"),
    ],
)
def test_resolve_period_end(announce: str, period_ends: list[str], expected: str) -> None:
    pes = [date.fromisoformat(d) for d in period_ends]
    assert _resolve_period_end(date.fromisoformat(announce), pes) == expected


def test_scrape_matches_json_date_for_offcycle_ticker() -> None:
    """Accessor equality for an off-cycle fiscal (NVDA).

    The scrape publishes a row under the SAME literal date as the JSON path —
    ``2026-04-30``, not the calendar quarter-end ``2026-06-30`` — so the two
    accessors never split one quarter into two primitive rows, and the
    read-before-write idempotency check (keyed on the exact date) holds across
    a scrape/JSON switch. (`detect_and_prepare_eps` publishes the primitive
    under this same `date`; its passthrough is covered in test_real_time_flow.)
    """
    json_out = YahooBlock._normalize_json(_json_df([{"period_end": "2026-04-30", "eps": 1.87}]), "NVDA")
    period_ends = [date.fromisoformat(d) for d in json_out["date"]]
    scrape_out = YahooBlock._normalize_scrape(
        _scrape_df([{"announce": "2026-05-20", "eps": 1.87}]), "NVDA", period_ends
    )
    assert scrape_out["date"].iloc[0] == "2026-04-30"  # the JSON period-end, not 06-30
    assert scrape_out["date"].iloc[0] == json_out["date"].iloc[0]  # identical across accessors


def test_scrape_falls_back_to_calendar_quarter_without_period_ends() -> None:
    """With no JSON period-ends, the scrape degrades to the nearest calendar
    quarter-end — canonical-correct even if the literal date differs."""
    out = YahooBlock._normalize_scrape(_scrape_df([{"announce": "2026-05-20", "eps": 1.87}]), "NVDA")
    assert out["date"].iloc[0] == "2026-06-30"
    assert canonical_key("NVDA", out["date"].iloc[0]) == ("NVDA", 2026, 2)


# Yahoo's earnings page as served on 2026-10-22, cut to a few rows, with the
# styling attributes removed. Meta's 2025-10-29 row shows the adjusted 7.25;
# GAAP EPS was 1.05.
_PAGE_HEADER = (
    '<tr> <th data-testid-header="ticker"><div> Symbol </div> </th> '
    '<th data-testid-header="companyshortname"><div> Company </div> </th> '
    '<th data-testid-header="startdatetime"><div> Earnings Date </div> </th> '
    '<th data-testid-header="epsestimate"><div> EPS Estimate </div> </th> '
    '<th data-testid-header="epsactual"><div> Reported EPS </div> </th> '
    '<th data-testid-header="epssurprisepct"><div> Surprise (%) </div> </th> </tr>'
)


def _page_row(symbol: str, company: str, when: str, estimate: str, reported: str, surprise: str) -> str:
    return (
        '<tr data-testid="data-table-v2-row"> '
        f'<td data-testid-cell="ticker"> <div data-testid=""><a href="/quote/{symbol}/" title="{symbol}">{symbol} </a>'
        " </div> </td> "
        f'<td data-testid-cell="companyshortname"> {company} </td> '
        f'<td data-testid-cell="startdatetime"> {when} </td> '
        f'<td data-testid-cell="epsestimate"> {estimate} </td> '
        f'<td data-testid-cell="epsactual"> {reported} </td> '
        f'<td data-testid-cell="epssurprisepct"> <p data-testid="typography">{surprise} </p> </td></tr>'
    )


def _page(*rows: str) -> str:
    return f"<html><body><table>{_PAGE_HEADER}{''.join(rows)}</table></body></html>"


META_PAGE = _page(
    _page_row("META", "Meta Platforms, Inc.", "October 21, 2026 at 4 PM EDT", "5.46", "3.83", "-29.91"),
    _page_row("META", "Meta Platforms, Inc.", "October 29, 2025 at 4 PM EDT", "6.67", "7.25", "+8.66"),
)
NVDA_PAGE = _page(
    _page_row("NVDA", "NVIDIA Corporation", "November 17, 2026 at 3 PM EST", "2.47", "-", "-"),
    _page_row("NVDA", "NVIDIA Corporation", "August 26, 2026 at 4 PM EDT", "2.09", "2.22", "+6.16"),
)


@pytest.mark.parametrize(
    "text, expected",
    [
        ("October 21, 2026 at 4 PM EDT", "2026-10-21"),
        ("January 28, 2026 at 4 PM EST", "2026-01-28"),
        ("May 20, 2026 at 4 PM EDT", "2026-05-20"),
        ("Sept 30, 2026 at 8 AM EDT", "2026-09-30"),
        ("Oct 21, 2026, 4 PM EDT", "2026-10-21"),  # the page's earlier format
    ],
)
def test_page_announcement_day(text: str, expected: str) -> None:
    assert _page_announcement_day(text).isoformat() == expected


def test_page_announcement_day_rejects_an_unknown_format() -> None:
    with pytest.raises(ValueError):
        _page_announcement_day("21/10/2026 16:00")


def test_parse_earnings_page_reads_yahoos_markup() -> None:
    df = _parse_earnings_page(META_PAGE)
    assert [ts.date().isoformat() for ts in df.index] == ["2026-10-21", "2025-10-29"]
    assert df["Reported EPS"].tolist() == [3.83, 7.25]
    assert df["EPS Estimate"].tolist() == [5.46, 6.67]


def test_parse_earnings_page_reads_a_pending_report_as_missing() -> None:
    df = _parse_earnings_page(NVDA_PAGE)
    assert pd.isna(df["Reported EPS"].iloc[0])
    assert df["Reported EPS"].iloc[1] == 2.22


def test_parse_earnings_page_rejects_a_page_without_the_table() -> None:
    with pytest.raises(ValueError):
        _parse_earnings_page("<html><body><table><tr><th>Symbol</th></tr><tr><td>META</td></tr></table></body></html>")


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class _FakeYfData:
    """Stands in for yfinance's shared session; records each request."""

    calls: ClassVar[list[tuple[str, dict[str, object]]]] = []

    def __init__(self, session: object = None) -> None:
        pass

    def get(self, url: str, params: dict[str, object] | None = None, **_: object) -> _FakeResponse:
        _FakeYfData.calls.append((url, dict(params or {})))
        return _FakeResponse(META_PAGE)

    def cache_get(self, *_: object, **__: object) -> _FakeResponse:
        raise AssertionError("cache_get keeps a response for the life of the process")


@pytest.fixture
def offline_block(monkeypatch: pytest.MonkeyPatch) -> YahooBlock:
    monkeypatch.setattr(YahooBlock, "_ensure_proxy", lambda self: None)
    monkeypatch.setattr(yahoo_module.time, "sleep", lambda _s: None)
    return YahooBlock()


def test_get_page_earnings_dates_a_new_report_by_its_own_quarter(
    offline_block: YahooBlock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Meta's 2026-10-21 row lands on 2026-09-30 although the JSON feed's
    newest quarter is 2026-06-30, and each request reads the page afresh."""
    import yfinance.data

    _FakeYfData.calls = []
    monkeypatch.setattr(yfinance.data, "YfData", _FakeYfData)
    feed_period_ends = [date(2025, 9, 30), date(2025, 12, 31), date(2026, 3, 31), date(2026, 6, 30)]

    out = offline_block.get_page_earnings("META", feed_period_ends)

    assert list(out["date"]) == ["2026-09-30", "2025-09-30"]
    assert list(out["epsActual"]) == [3.83, 7.25]
    assert list(out["symbol"]) == ["META", "META"]
    assert _FakeYfData.calls == [
        ("https://finance.yahoo.com/calendar/earnings", {"symbol": "META", "offset": 0, "size": 25})
    ]


class _FeedOnlyTicker:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol

    @property
    def earnings_history(self) -> pd.DataFrame:
        return _json_df([{"period_end": "2026-06-30", "eps": 6.18}, {"period_end": "2026-03-31", "eps": 7.31}])

    def get_earnings_dates(self, *_: object, **__: object) -> None:
        raise AssertionError("the page is read by get_page_earnings, never in place of the feed")


def test_get_historical_earnings_reads_the_json_feed_only(
    offline_block: YahooBlock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The historical backfill writes whatever this returns to the write-once
    Yahoo stream, so a page figure on another basis must never reach it."""
    import yfinance

    monkeypatch.setattr(yfinance, "Ticker", _FeedOnlyTicker)

    out = offline_block.get_historical_earnings("META", limit=5)

    assert list(out["date"]) == ["2026-06-30", "2026-03-31"]
    assert list(out["epsActual"]) == [6.18, 7.31]
