"""Unit tests for YahooBlock EPS normalization.

Covers the period-end normalization that keeps the two yfinance accessors
interchangeable. ``get_earnings_dates`` (HTML scrape) dates rows by the
announcement date while ``earnings_history`` (JSON) dates them by the
fiscal-period-end; the block must reduce both to period-end so they land in
the same ``canonical_key`` bucket. Regression guard for the silent
mis-bucketing where scraped Yahoo rows failed to reconcile against FMP.
"""

from datetime import date

import pandas as pd
import pytest

from tsn_adapters.blocks.yahoo import YahooBlock, _announcement_to_period_end, _resolve_period_end
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
