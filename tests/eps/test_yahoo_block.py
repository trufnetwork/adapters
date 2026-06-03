"""Unit tests for YahooBlock EPS normalization.

Covers the period-end normalization that keeps the two yfinance accessors
interchangeable. ``get_earnings_dates`` (HTML scrape) dates rows by the
announcement date while ``earnings_history`` (JSON) dates them by the
fiscal-period-end; the block must reduce both to period-end so they land in
the same ``canonical_key`` bucket. Regression guard for the silent
mis-bucketing where scraped Yahoo rows failed to reconcile against FMP.
"""

import pandas as pd
import pytest

from tsn_adapters.blocks.yahoo import YahooBlock, _announcement_to_period_end
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
