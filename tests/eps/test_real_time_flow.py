"""Tests for the EPS real-time reconciliation flow.

Covers the canonical-key reconciliation introduced in
trufnetwork/website#3936: FMP and Yahoo use different date semantics for
the same earnings event (announcement date vs fiscal-period-end). The
reconciler must derive a canonical event identity from each source so
naïve dict-key joins on raw dates don't treat the same event as two
separate single-source entries.
"""

from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
from pandera.typing import DataFrame
from pydantic import PrivateAttr, SecretStr
import pytest
from tests.utils.constants import FAKE_PRIVATE_KEY
from tests.utils.fake_tn_access import FakeTNAccessBlock

from tsn_adapters.blocks.fmp import EarningsData, FMPBlock, QuarterlyIncomeStatementData
from tsn_adapters.blocks.yahoo import YahooBlock
import tsn_adapters.flows.eps.real_time_flow as real_time_flow_module
from tsn_adapters.flows.eps.real_time_flow import detect_and_prepare_eps, eps_real_time_flow
from tsn_adapters.utils.create_empty_df import create_empty_df
from tsn_adapters.utils.time_utils import date_string_to_unix

# --- DataFrame builders matching the schemas the blocks return ---


def _earnings_df(rows: list[dict]) -> DataFrame[EarningsData]:
    if not rows:
        return create_empty_df(EarningsData)
    return DataFrame[EarningsData](pd.DataFrame(rows))


def _income_df(rows: list[dict]) -> DataFrame[QuarterlyIncomeStatementData]:
    if not rows:
        return create_empty_df(QuarterlyIncomeStatementData)
    return DataFrame[QuarterlyIncomeStatementData](pd.DataFrame(rows))


# --- Fake blocks (mirror the pattern in test_historical_flow.py) ---


class RealTimeFakeFMPBlock(FMPBlock):
    _earnings_data: dict = PrivateAttr(default_factory=dict)
    _income_data: dict = PrivateAttr(default_factory=dict)

    def get_historical_earnings(self, symbol: str, limit: int = 40) -> DataFrame[EarningsData]:
        return _earnings_df(self._earnings_data.get(symbol, []))

    def get_quarterly_income_statements(self, symbol: str, limit: int = 8) -> DataFrame[QuarterlyIncomeStatementData]:
        return _income_df(self._income_data.get(symbol, []))


class RealTimeFakeYahooBlock(YahooBlock):
    """JSON feed rows in `_data`; earnings-page rows, already period-end dated, in `_page`."""

    _data: dict = PrivateAttr(default_factory=dict)
    _page: dict = PrivateAttr(default_factory=dict)
    _page_error: Exception | None = PrivateAttr(default=None)

    def get_historical_earnings(self, symbol: str, limit: int = 40) -> DataFrame[EarningsData]:
        return _earnings_df(self._data.get(symbol, []))

    def get_page_earnings(self, symbol: str, period_ends: list[date]) -> DataFrame[EarningsData]:
        if self._page_error is not None:
            raise self._page_error
        return _earnings_df(self._page.get(symbol, []))


def _make_fmp(earnings: dict[str, list[dict]], income_statements: dict[str, list[dict]]) -> RealTimeFakeFMPBlock:
    block = RealTimeFakeFMPBlock(api_key=SecretStr(FAKE_PRIVATE_KEY.get_secret_value()))
    block._earnings_data = earnings
    block._income_data = income_statements
    return block


def _make_yahoo(
    data: dict[str, list[dict]],
    page: dict[str, list[dict]] | None = None,
    page_error: Exception | None = None,
) -> RealTimeFakeYahooBlock:
    block = RealTimeFakeYahooBlock()
    block._data = data
    block._page = page or {}
    block._page_error = page_error
    return block


# A fixed clock away from every report day below, so no fixture falls in the
# overdue window by accident. The overdue tests set their own.
QUIET_NOW = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _quiet_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(real_time_flow_module, "_utc_now", lambda: QUIET_NOW)


# --- Test fixtures — the events from issue #3936 ---

# NVDA Q1 FY27 — Michael's primary example.
# FMP date = announcement date (2026-05-20)
# Yahoo date = fiscal-period-end (2026-04-30, calendar-month-end rounding)
# Income-statement date = exact fiscal-period-end (2026-04-26)
# Income-statement filingDate = matches FMP announcement date exactly
NVDA_Q1_FY27_FMP_EARNINGS = {
    "symbol": "NVDA",
    "date": "2026-05-20",
    "epsActual": 1.87,
    "epsEstimated": 1.76,
    "lastUpdated": None,
}
NVDA_Q1_FY27_YAHOO_EARNINGS = {
    "symbol": "NVDA",
    "date": "2026-04-30",
    "epsActual": 1.87,
    "epsEstimated": 1.76,
    "lastUpdated": None,
}
NVDA_Q1_FY27_INCOME_STMT = {
    "symbol": "NVDA",
    "period": "Q1",
    "fiscalYear": "2027",
    "date": "2026-04-26",
    "filingDate": "2026-05-20",
    "acceptedDate": None,
}


# --- The bug from #3936: same event, mismatched source dates ---


def test_reconciles_despite_source_date_mismatch():
    """The exact bug condition from #3936.

    FMP and Yahoo report the same NVDA Q1 FY27 event under different dates
    (announcement vs period-end). Under the previous dict-key-union join,
    these became two single-source entries and the consensus stream never
    settled. Under canonical-key matching, both sources reduce to
    (NVDA, 2026, 2) and reconcile correctly.
    """
    fmp = _make_fmp(
        earnings={"NVDA": [NVDA_Q1_FY27_FMP_EARNINGS]},
        income_statements={"NVDA": [NVDA_Q1_FY27_INCOME_STMT]},
    )
    yahoo = _make_yahoo({"NVDA": [NVDA_Q1_FY27_YAHOO_EARNINGS]})
    fmp_tn = FakeTNAccessBlock()
    yahoo_tn = FakeTNAccessBlock()
    truf_tn = FakeTNAccessBlock()

    fmp_rows, yahoo_rows, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=fmp_tn,
        yahoo_tn_block=yahoo_tn,
        truf_tn_block=truf_tn,
        symbol="NVDA",
    )

    # Each primitive stream gets its own row, keyed by its own source date
    assert len(fmp_rows) == 1
    assert fmp_rows[0]["value"] == "1.87"
    assert fmp_rows[0]["date"] == date_string_to_unix("2026-05-20")

    assert len(yahoo_rows) == 1
    assert yahoo_rows[0]["value"] == "1.87"
    assert yahoo_rows[0]["date"] == date_string_to_unix("2026-04-30")

    # And — the fix — the consensus stream gets the settled row.
    # Consensus uses FMP's announcement date (preserves the "consensus at
    # announcement time" semantic that downstream markets rely on).
    assert len(truf_rows) == 1
    assert truf_rows[0]["value"] == "1.87"
    assert truf_rows[0]["date"] == date_string_to_unix("2026-05-20")


def test_aapl_fiscal_quarter_mismatch_resolves():
    """AAPL Q2 FY26 (a non-calendar fiscal): FMP announces in April (calendar
    Q2), Yahoo period-end is March 31 (calendar Q1). Income-statement period-
    end is March 28 (calendar Q1). Both FMP and Yahoo reduce to (AAPL, 2026, 1)
    via canonical-key — proves the fix handles non-calendar fiscals too.
    """
    aapl_fmp = {"symbol": "AAPL", "date": "2026-04-30", "epsActual": 2.01, "epsEstimated": 1.99, "lastUpdated": None}
    aapl_yahoo = {"symbol": "AAPL", "date": "2026-03-31", "epsActual": 2.01, "epsEstimated": 1.99, "lastUpdated": None}
    aapl_income = {
        "symbol": "AAPL",
        "period": "Q2",
        "fiscalYear": "2026",
        "date": "2026-03-28",
        "filingDate": "2026-04-30",
        "acceptedDate": None,
    }

    fmp = _make_fmp(earnings={"AAPL": [aapl_fmp]}, income_statements={"AAPL": [aapl_income]})
    yahoo = _make_yahoo({"AAPL": [aapl_yahoo]})

    _, _, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="AAPL",
    )

    assert len(truf_rows) == 1
    assert truf_rows[0]["value"] == "2.01"


def test_aapl_filing_date_off_by_one():
    """Real production data: AAPL's income-statement filingDate is one day
    after FMP earnings date (2026-05-01 vs 2026-04-30). The ±1 day fuzzy
    match on filing dates resolves this.
    """
    aapl_fmp = {"symbol": "AAPL", "date": "2026-04-30", "epsActual": 2.01, "epsEstimated": 1.99, "lastUpdated": None}
    aapl_yahoo = {"symbol": "AAPL", "date": "2026-03-31", "epsActual": 2.01, "epsEstimated": 1.99, "lastUpdated": None}
    aapl_income = {
        "symbol": "AAPL",
        "period": "Q2",
        "fiscalYear": "2026",
        "date": "2026-03-28",
        "filingDate": "2026-05-01",  # +1 day vs earnings date
        "acceptedDate": None,
    }

    fmp = _make_fmp(earnings={"AAPL": [aapl_fmp]}, income_statements={"AAPL": [aapl_income]})
    yahoo = _make_yahoo({"AAPL": [aapl_yahoo]})

    fmp_rows, yahoo_rows, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="AAPL",
    )

    assert len(fmp_rows) == 1
    assert len(yahoo_rows) == 1
    assert len(truf_rows) == 1
    assert truf_rows[0]["value"] == "2.01"
    assert truf_rows[0]["date"] == date_string_to_unix("2026-04-30")


# --- Disagreement / pending paths ---


def test_disputed_when_same_event_different_values():
    """FMP and Yahoo reduce to the same canonical key but disagree on the
    EPS value (beyond $0.01 tolerance). Consensus is NOT written; primitive
    streams still get their per-source rows."""
    fmp_row = dict(NVDA_Q1_FY27_FMP_EARNINGS, epsActual=1.87)
    yahoo_row = dict(NVDA_Q1_FY27_YAHOO_EARNINGS, epsActual=1.50)  # 0.37 mismatch

    fmp = _make_fmp(
        earnings={"NVDA": [fmp_row]},
        income_statements={"NVDA": [NVDA_Q1_FY27_INCOME_STMT]},
    )
    yahoo = _make_yahoo({"NVDA": [yahoo_row]})

    fmp_rows, yahoo_rows, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="NVDA",
    )

    # Each primitive is written with its raw source value
    assert len(fmp_rows) == 1
    assert fmp_rows[0]["value"] == "1.87"
    assert len(yahoo_rows) == 1
    assert yahoo_rows[0]["value"] == "1.5"

    # Consensus deliberately NOT written — disputed
    assert len(truf_rows) == 0


def test_pending_when_only_fmp_has_event():
    """Only FMP has data for this event. Result: pending (no consensus)."""
    fmp = _make_fmp(
        earnings={"NVDA": [NVDA_Q1_FY27_FMP_EARNINGS]},
        income_statements={"NVDA": [NVDA_Q1_FY27_INCOME_STMT]},
    )
    yahoo = _make_yahoo({"NVDA": []})

    fmp_rows, yahoo_rows, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="NVDA",
    )

    assert len(fmp_rows) == 1
    assert len(yahoo_rows) == 0
    assert len(truf_rows) == 0


def test_pending_when_only_yahoo_has_event():
    """Only Yahoo has data for this event. Result: pending (no consensus)."""
    fmp = _make_fmp(earnings={"NVDA": []}, income_statements={"NVDA": []})
    yahoo = _make_yahoo({"NVDA": [NVDA_Q1_FY27_YAHOO_EARNINGS]})

    fmp_rows, yahoo_rows, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="NVDA",
    )

    assert len(fmp_rows) == 0
    assert len(yahoo_rows) == 1
    assert len(truf_rows) == 0


# --- Edge cases ---


def test_settles_on_yahoo_quarter_when_income_statement_not_yet_published():
    """FMP /stable/earnings has the new actual but /stable/income-statement has
    no row for that filing yet (for Tesla it arrives with the 10-Q, the day
    after the release, which held the Q2 2026 print for about 16 hours).
    FMP's reading pairs with the Yahoo quarter that ended shortly before the
    announcement, so the consensus settles without waiting for the statement.
    """
    fmp = _make_fmp(
        earnings={"NVDA": [NVDA_Q1_FY27_FMP_EARNINGS]},
        income_statements={"NVDA": []},  # income-statement hasn't published
    )
    yahoo = _make_yahoo({"NVDA": [NVDA_Q1_FY27_YAHOO_EARNINGS]})

    fmp_rows, yahoo_rows, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="NVDA",
    )

    assert len(fmp_rows) == 1
    assert fmp_rows[0]["date"] == date_string_to_unix("2026-05-20")
    assert len(yahoo_rows) == 1
    # Yahoo's quarter ended 2026-04-30, 20 days before FMP's 2026-05-20
    # announcement, so both readings describe the same quarter
    assert len(truf_rows) == 1
    assert truf_rows[0]["value"] == "1.87"
    assert truf_rows[0]["date"] == date_string_to_unix("2026-05-20")


def test_pending_when_income_statement_and_yahoo_quarter_both_missing():
    """The new quarter is only on FMP: no income-statement row and no Yahoo row
    for it yet. Yahoo's newest quarter is the previous one, with the same
    figure, so pairing FMP's new print with it would settle Q3 on Q2's data.
    The consensus must stay pending instead.
    """
    tsla_q3_fmp = {"symbol": "TSLA", "date": "2026-10-21", "epsActual": 0.33, "epsEstimated": 0.45, "lastUpdated": None}
    tsla_q2_income = {
        "symbol": "TSLA",
        "period": "Q2",
        "fiscalYear": "2026",
        "date": "2026-06-30",
        "filingDate": "2026-07-23",
        "acceptedDate": None,
    }
    tsla_q2_yahoo = {
        "symbol": "TSLA",
        "date": "2026-06-30",
        "epsActual": 0.33,
        "epsEstimated": 0.54,
        "lastUpdated": None,
    }

    fmp = _make_fmp(earnings={"TSLA": [tsla_q3_fmp]}, income_statements={"TSLA": [tsla_q2_income]})
    yahoo = _make_yahoo({"TSLA": [tsla_q2_yahoo]})

    fmp_rows, _, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="TSLA",
    )

    assert [r["date"] for r in fmp_rows] == [date_string_to_unix("2026-10-21")]
    assert truf_rows == []


def test_settles_when_the_announcement_falls_in_the_next_calendar_quarter():
    """Tesla's Q2 2026 shape: the quarter ended 2026-06-30 (calendar Q2) and was
    announced on 2026-07-22 (calendar Q3), before FMP had an income-statement
    row for it. Keyed by its announcement date, FMP's reading would sit in a
    different quarter from Yahoo's, so this only settles through the Yahoo
    quarter.
    """
    tsla_q2_fmp = {"symbol": "TSLA", "date": "2026-07-22", "epsActual": 0.33, "epsEstimated": 0.54, "lastUpdated": None}
    tsla_q2_yahoo = {
        "symbol": "TSLA",
        "date": "2026-06-30",
        "epsActual": 0.33,
        "epsEstimated": 0.54,
        "lastUpdated": None,
    }
    fmp = _make_fmp(earnings={"TSLA": [tsla_q2_fmp]}, income_statements={"TSLA": []})
    yahoo = _make_yahoo({"TSLA": [tsla_q2_yahoo]})

    _, _, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="TSLA",
    )

    assert [(r["date"], r["value"]) for r in truf_rows] == [(date_string_to_unix("2026-07-22"), "0.33")]


def test_an_older_unmatched_fmp_row_pairs_with_its_own_quarter():
    """FMP lists newest first. The Q3 row matches its income statement; the Q2
    statement has no filingDate, so Q2 pairs through Yahoo. Q2 must pair with
    the quarter that ended before its announcement (06-30), never with Q3's
    (09-30, after it): that would take Q3's place, and Q3 would never print.
    """
    q3_fmp = {"symbol": "TSLA", "date": "2026-10-21", "epsActual": 0.5, "epsEstimated": 0.45, "lastUpdated": None}
    q2_fmp = {"symbol": "TSLA", "date": "2026-07-22", "epsActual": 0.33, "epsEstimated": 0.54, "lastUpdated": None}
    q3_income = {
        "symbol": "TSLA",
        "period": "Q3",
        "fiscalYear": "2026",
        "date": "2026-09-30",
        "filingDate": "2026-10-22",
        "acceptedDate": None,
    }
    q2_income_without_filing = {
        "symbol": "TSLA",
        "period": "Q2",
        "fiscalYear": "2026",
        "date": "2026-06-30",
        "filingDate": pd.NA,
        "acceptedDate": None,
    }
    q3_yahoo = {"symbol": "TSLA", "date": "2026-09-30", "epsActual": 0.5, "epsEstimated": 0.45, "lastUpdated": None}
    q2_yahoo = {"symbol": "TSLA", "date": "2026-06-30", "epsActual": 0.33, "epsEstimated": 0.54, "lastUpdated": None}
    fmp = _make_fmp(
        earnings={"TSLA": [q3_fmp, q2_fmp]},
        income_statements={"TSLA": [q3_income, q2_income_without_filing]},
    )
    yahoo = _make_yahoo({"TSLA": [q3_yahoo, q2_yahoo]})

    _, _, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="TSLA",
    )

    assert sorted((r["date"], r["value"]) for r in truf_rows) == [
        (date_string_to_unix("2026-07-22"), "0.33"),
        (date_string_to_unix("2026-10-21"), "0.5"),
    ]


def test_null_filing_date_on_an_old_income_statement_row_does_not_crash():
    """FMP can return an income-statement row with no filingDate. In a column
    where other rows carry a date it is pd.NA, and `bool(pd.NA)` raises, which
    used to fail the whole run for every ticker. The row is skipped instead.
    """
    old_row = {
        "symbol": "NVDA",
        "period": "Q4",
        "fiscalYear": "2026",
        "date": "2026-01-25",
        "filingDate": pd.NA,
        "acceptedDate": None,
    }
    fmp = _make_fmp(
        earnings={"NVDA": [NVDA_Q1_FY27_FMP_EARNINGS]},
        income_statements={"NVDA": [NVDA_Q1_FY27_INCOME_STMT, old_row]},
    )
    yahoo = _make_yahoo({"NVDA": [NVDA_Q1_FY27_YAHOO_EARNINGS]})

    _, _, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="NVDA",
    )

    assert [r["value"] for r in truf_rows] == ["1.87"]


def test_null_last_updated_on_an_fmp_row_does_not_crash():
    """`lastUpdated` is pd.NA on a row when another row carries a timestamp, and
    `pd.NA or now()` raises. The reading falls back to the time of the run.
    """
    with_timestamp = {**NVDA_Q1_FY27_FMP_EARNINGS, "date": "2026-02-25", "lastUpdated": "2026-02-26T00:00:00Z"}
    without_timestamp = {**NVDA_Q1_FY27_FMP_EARNINGS, "lastUpdated": pd.NA}
    fmp = _make_fmp(
        earnings={"NVDA": [with_timestamp, without_timestamp]},
        income_statements={"NVDA": [NVDA_Q1_FY27_INCOME_STMT]},
    )
    yahoo = _make_yahoo({"NVDA": [NVDA_Q1_FY27_YAHOO_EARNINGS]})

    _, _, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="NVDA",
    )

    assert [r["value"] for r in truf_rows] == ["1.87"]


def test_idempotent_skips_already_published_consensus():
    """Per-stream idempotency: when only consensus is already on-chain
    but the primitive streams are not, the primitives still emit. The
    _is_published guard is independent per stream — consensus state
    doesn't gate primitive emission. This is the property required by
    CodeRabbit's review on #229.
    """
    fmp = _make_fmp(
        earnings={"NVDA": [NVDA_Q1_FY27_FMP_EARNINGS]},
        income_statements={"NVDA": [NVDA_Q1_FY27_INCOME_STMT]},
    )
    yahoo = _make_yahoo({"NVDA": [NVDA_Q1_FY27_YAHOO_EARNINGS]})

    truf_tn = FakeTNAccessBlock()
    # Seed ONLY the consensus stream with the FMP announcement date
    from tsn_adapters.tasks.eps.config import EPS_STREAM_IDS

    truf_tn.seed_records(
        EPS_STREAM_IDS["NVDA"],
        [date_string_to_unix("2026-05-20")],
    )

    fmp_rows, yahoo_rows, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=truf_tn,
        symbol="NVDA",
    )

    # Primitive streams aren't seeded → they emit
    assert len(fmp_rows) == 1
    assert len(yahoo_rows) == 1
    # Consensus stream is seeded → it's skipped
    assert len(truf_rows) == 0


def test_publishes_fresh_print_despite_older_stream_history():
    """A fresh print must publish even when the stream already holds older
    quarters. TN's gap-filled read answers a [date, date] query with the
    PREVIOUS record (the LOCF anchor), so an is-it-published check that only
    tests non-emptiness sees every new announcement as a duplicate once the
    stream has any history at all. That silently no-op'd every scheduled run
    (website#4362: TSLA's Q2-2026 print never reached TN while the streams
    held Q1 and older). Only an exact date match may count as published.
    """
    from tsn_adapters.tasks.eps.config import EPS_STREAM_IDS, FMP_EPS_STREAM_IDS, YAHOO_EPS_STREAM_IDS

    fmp = _make_fmp(
        earnings={"NVDA": [NVDA_Q1_FY27_FMP_EARNINGS]},
        income_statements={"NVDA": [NVDA_Q1_FY27_INCOME_STMT]},
    )
    yahoo = _make_yahoo({"NVDA": [NVDA_Q1_FY27_YAHOO_EARNINGS]})

    # Every stream already carries the PREVIOUS quarter — the production
    # steady state. The gap-filled read returns these as anchors for the
    # fresh dates.
    fmp_tn = FakeTNAccessBlock()
    yahoo_tn = FakeTNAccessBlock()
    truf_tn = FakeTNAccessBlock()
    fmp_tn.seed_records(FMP_EPS_STREAM_IDS["NVDA"], [date_string_to_unix("2026-02-25")])
    yahoo_tn.seed_records(YAHOO_EPS_STREAM_IDS["NVDA"], [date_string_to_unix("2026-01-31")])
    truf_tn.seed_records(EPS_STREAM_IDS["NVDA"], [date_string_to_unix("2026-02-25")])

    fmp_rows, yahoo_rows, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=fmp_tn,
        yahoo_tn_block=yahoo_tn,
        truf_tn_block=truf_tn,
        symbol="NVDA",
    )

    # The fresh quarter publishes everywhere — the older history must not
    # shadow it.
    assert [r["date"] for r in fmp_rows] == [date_string_to_unix("2026-05-20")]
    assert [r["date"] for r in yahoo_rows] == [date_string_to_unix("2026-04-30")]
    assert [r["date"] for r in truf_rows] == [date_string_to_unix("2026-05-20")]


def test_idempotent_when_all_streams_already_published():
    """Full idempotency: when every stream's date is already on-chain,
    a re-run produces no writes anywhere. Mirrors the steady-state
    behaviour of the scheduled flow re-encountering the same recent
    earnings rows on subsequent ticks.
    """
    from tsn_adapters.tasks.eps.config import EPS_STREAM_IDS, FMP_EPS_STREAM_IDS, YAHOO_EPS_STREAM_IDS

    fmp = _make_fmp(
        earnings={"NVDA": [NVDA_Q1_FY27_FMP_EARNINGS]},
        income_statements={"NVDA": [NVDA_Q1_FY27_INCOME_STMT]},
    )
    yahoo = _make_yahoo({"NVDA": [NVDA_Q1_FY27_YAHOO_EARNINGS]})

    fmp_tn = FakeTNAccessBlock()
    yahoo_tn = FakeTNAccessBlock()
    truf_tn = FakeTNAccessBlock()
    fmp_tn.seed_records(FMP_EPS_STREAM_IDS["NVDA"], [date_string_to_unix("2026-05-20")])
    yahoo_tn.seed_records(YAHOO_EPS_STREAM_IDS["NVDA"], [date_string_to_unix("2026-04-30")])
    truf_tn.seed_records(EPS_STREAM_IDS["NVDA"], [date_string_to_unix("2026-05-20")])

    fmp_rows, yahoo_rows, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=fmp_tn,
        yahoo_tn_block=yahoo_tn,
        truf_tn_block=truf_tn,
        symbol="NVDA",
    )

    assert len(fmp_rows) == 0
    assert len(yahoo_rows) == 0
    assert len(truf_rows) == 0


# --- Flow-level: the 2026-07-22 duplicate-row incident (website#4362) ---

GOOGL_Q2_2026_FMP_EARNINGS = {
    "symbol": "GOOGL",
    "date": "2026-07-22",
    "epsActual": 9.11,
    "epsEstimated": None,
    "lastUpdated": None,
}
GOOGL_Q2_2026_YAHOO_EARNINGS = {
    "symbol": "GOOGL",
    "date": "2026-06-30",
    "epsActual": 9.11,
    "epsEstimated": None,
    "lastUpdated": None,
}
GOOGL_Q2_2026_INCOME_STMT = {
    "symbol": "GOOGL",
    "period": "Q2",
    "fiscalYear": "2026",
    "date": "2026-06-30",
    "filingDate": "2026-07-22",
    "acceptedDate": None,
}


@pytest.fixture(scope="module")
def prefect_harness(prefect_test_fixture: Any):
    yield prefect_test_fixture


@pytest.mark.timeout(120, func_only=True)
def test_duplicate_fmp_announcement_rows_publish_once(prefect_harness: Any):
    """The 2026-07-22 incident shape (website#4362): FMP returned GOOGL's
    fresh Q2 print twice; both copies passed the not-yet-published check
    and rode one insert_records tx, whose primitive PK rejected the whole
    batch on-chain — TSLA's print rolled back with it. Duplicates must
    collapse before the write so one repeated upstream row can't sink
    every record in the batch."""
    from tsn_adapters.tasks.eps.config import EPS_STREAM_IDS, FMP_EPS_STREAM_IDS, YAHOO_EPS_STREAM_IDS

    fmp = _make_fmp(
        earnings={"GOOGL": [GOOGL_Q2_2026_FMP_EARNINGS, GOOGL_Q2_2026_FMP_EARNINGS]},
        income_statements={"GOOGL": [GOOGL_Q2_2026_INCOME_STMT]},
    )
    yahoo = _make_yahoo({"GOOGL": [GOOGL_Q2_2026_YAHOO_EARNINGS]})
    fmp_tn = FakeTNAccessBlock(existing_streams={FMP_EPS_STREAM_IDS["GOOGL"]})
    yahoo_tn = FakeTNAccessBlock(existing_streams={YAHOO_EPS_STREAM_IDS["GOOGL"]})
    truf_tn = FakeTNAccessBlock(existing_streams={EPS_STREAM_IDS["GOOGL"]})

    eps_real_time_flow(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=fmp_tn,
        yahoo_tn_block=yahoo_tn,
        truf_tn_block=truf_tn,
        symbols=["GOOGL"],
    )

    fmp_inserted = pd.concat(fmp_tn.inserted_records, ignore_index=True)
    assert len(fmp_inserted) == 1
    assert fmp_inserted.iloc[0]["date"] == date_string_to_unix("2026-07-22")

    yahoo_inserted = pd.concat(yahoo_tn.inserted_records, ignore_index=True)
    assert len(yahoo_inserted) == 1

    truf_inserted = pd.concat(truf_tn.inserted_records, ignore_index=True)
    assert len(truf_inserted) == 1
    assert truf_inserted.iloc[0]["value"] == "9.11"


class _FailingFMPBlock(RealTimeFakeFMPBlock):
    """Raises a given error for given symbols, as a schema error or an outage would."""

    _failures: dict = PrivateAttr(default_factory=dict)

    def get_historical_earnings(self, symbol: str, limit: int = 40) -> DataFrame[EarningsData]:
        if symbol in self._failures:
            raise self._failures[symbol]
        return super().get_historical_earnings(symbol, limit)


def _googl_blocks(failures: dict) -> tuple:
    from tsn_adapters.tasks.eps.config import EPS_STREAM_IDS, FMP_EPS_STREAM_IDS, YAHOO_EPS_STREAM_IDS

    fmp = _FailingFMPBlock(api_key=SecretStr(FAKE_PRIVATE_KEY.get_secret_value()))
    fmp._earnings_data = {"GOOGL": [GOOGL_Q2_2026_FMP_EARNINGS]}
    fmp._income_data = {"GOOGL": [GOOGL_Q2_2026_INCOME_STMT]}
    fmp._failures = failures
    yahoo = _make_yahoo({"GOOGL": [GOOGL_Q2_2026_YAHOO_EARNINGS]})
    fmp_tn = FakeTNAccessBlock(existing_streams={FMP_EPS_STREAM_IDS["GOOGL"]})
    yahoo_tn = FakeTNAccessBlock(existing_streams={YAHOO_EPS_STREAM_IDS["GOOGL"]})
    truf_tn = FakeTNAccessBlock(existing_streams={EPS_STREAM_IDS["GOOGL"]})
    return fmp, yahoo, fmp_tn, yahoo_tn, truf_tn


def test_failing_symbols_do_not_block_the_others(prefect_harness: Any):
    """AAPL runs first. One failing symbol used to end the run before any other
    symbol was read or published. Now the healthy symbols publish, and the run
    still fails, naming every failed symbol, whatever the error type and even
    when the error text does not mention the symbol.
    """
    fmp, yahoo, fmp_tn, yahoo_tn, truf_tn = _googl_blocks(
        {"AAPL": ValueError("schema mismatch"), "MSFT": KeyError("lastUpdated")}
    )

    with pytest.raises(RuntimeError) as raised:
        eps_real_time_flow(
            fmp_block=fmp,
            yahoo_block=yahoo,
            fmp_tn_block=fmp_tn,
            yahoo_tn_block=yahoo_tn,
            truf_tn_block=truf_tn,
            symbols=["AAPL", "GOOGL", "MSFT"],
        )

    assert "AAPL" in str(raised.value) and "MSFT" in str(raised.value)
    truf_inserted = pd.concat(truf_tn.inserted_records, ignore_index=True)
    assert truf_inserted["value"].tolist() == ["9.11"]


def test_a_failing_symbol_fails_the_run_even_when_nothing_is_new(prefect_harness: Any):
    """Most scheduled runs have nothing new to publish. A symbol failing in such
    a run must still fail it, or the failure would end as a green run.
    """
    from tsn_adapters.tasks.eps.config import EPS_STREAM_IDS, FMP_EPS_STREAM_IDS, YAHOO_EPS_STREAM_IDS

    fmp, yahoo, fmp_tn, yahoo_tn, truf_tn = _googl_blocks({"AAPL": ValueError("schema mismatch")})
    fmp_tn.seed_records(FMP_EPS_STREAM_IDS["GOOGL"], [date_string_to_unix("2026-07-22")])
    yahoo_tn.seed_records(YAHOO_EPS_STREAM_IDS["GOOGL"], [date_string_to_unix(GOOGL_Q2_2026_YAHOO_EARNINGS["date"])])
    truf_tn.seed_records(EPS_STREAM_IDS["GOOGL"], [date_string_to_unix("2026-07-22")])

    with pytest.raises(RuntimeError, match="AAPL"):
        eps_real_time_flow(
            fmp_block=fmp,
            yahoo_block=yahoo,
            fmp_tn_block=fmp_tn,
            yahoo_tn_block=yahoo_tn,
            truf_tn_block=truf_tn,
            symbols=["AAPL", "GOOGL"],
        )

    assert truf_tn.inserted_records == []


# --- Yahoo's earnings page (website#4987: Meta's 2026-10-21 report) ---

META_Q3_2026_FMP_EARNINGS = {
    "symbol": "META",
    "date": "2026-10-21",
    "epsActual": 3.83,
    "epsEstimated": 5.46,
    "lastUpdated": None,
}
META_Q3_2026_INCOME_STMT = {
    "symbol": "META",
    "period": "Q3",
    "fiscalYear": "2026",
    "date": "2026-09-30",
    "filingDate": "2026-10-21",
    "acceptedDate": None,
}
META_Q2_2026_YAHOO_FEED = {
    "symbol": "META",
    "date": "2026-06-30",
    "epsActual": 6.18,
    "epsEstimated": 7.22,
    "lastUpdated": None,
}
META_Q3_2026_YAHOO_PAGE = {
    "symbol": "META",
    "date": "2026-09-30",
    "epsActual": 3.83,
    "epsEstimated": 5.46,
    "lastUpdated": None,
}


def _meta_q3_fmp() -> RealTimeFakeFMPBlock:
    return _make_fmp(
        earnings={"META": [META_Q3_2026_FMP_EARNINGS]},
        income_statements={"META": [META_Q3_2026_INCOME_STMT]},
    )


def _detect_meta(
    fmp: RealTimeFakeFMPBlock,
    yahoo: RealTimeFakeYahooBlock,
    truf_tn: FakeTNAccessBlock | None = None,
) -> tuple[list[dict], list[dict], list[dict], list[str]]:
    return detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=truf_tn or FakeTNAccessBlock(),
        symbol="META",
    )


def test_settles_on_the_page_while_the_feed_has_no_row():
    """Meta's 2026-10-21 report: FMP had 3.83 from the first run after the
    release, Yahoo's earnings page showed 3.83 by 01:07 UTC, and Yahoo's JSON
    feed still had no Q3 row hours later. The page matches FMP, so the
    consensus settles without waiting for the feed, and the Yahoo stream gets
    the page's figure.
    """
    fmp = _meta_q3_fmp()
    yahoo = _make_yahoo({"META": [META_Q2_2026_YAHOO_FEED]}, page={"META": [META_Q3_2026_YAHOO_PAGE]})

    _, yahoo_rows, truf_rows, overdue = _detect_meta(fmp, yahoo)

    assert [(r["date"], r["value"]) for r in truf_rows] == [(date_string_to_unix("2026-10-21"), "3.83")]
    assert (date_string_to_unix("2026-09-30"), "3.83") in [(r["date"], r["value"]) for r in yahoo_rows]
    assert overdue == []


def test_settles_on_the_page_without_an_income_statement_or_a_feed_row():
    """FMP's income statement has no row for the filing yet and the feed has no
    row for the quarter. The page's quarter dates FMP's reading, as a feed row
    would, so the print still settles.
    """
    fmp = _make_fmp(earnings={"META": [META_Q3_2026_FMP_EARNINGS]}, income_statements={"META": []})
    yahoo = _make_yahoo({"META": [META_Q2_2026_YAHOO_FEED]}, page={"META": [META_Q3_2026_YAHOO_PAGE]})

    _, _, truf_rows, _ = _detect_meta(fmp, yahoo)

    assert [(r["date"], r["value"]) for r in truf_rows] == [(date_string_to_unix("2026-10-21"), "3.83")]


def test_a_page_figure_on_another_basis_is_left_out():
    """Meta Q3 2025: GAAP EPS was 1.05 with a one-time tax charge and 7.25
    without it; Yahoo's page listed 7.25 while its feed recorded 1.05. A page
    figure that matches nothing may be on the wrong basis, and the Yahoo stream
    keeps the first value written for a date, so it is neither written nor
    reconciled.
    """
    q3_2025_gaap = {
        "symbol": "META",
        "date": "2025-10-29",
        "epsActual": 1.05,
        "epsEstimated": 6.67,
        "lastUpdated": None,
    }
    fmp = _make_fmp(
        earnings={"META": [q3_2025_gaap]},
        income_statements={
            "META": [
                {
                    "symbol": "META",
                    "period": "Q3",
                    "fiscalYear": "2025",
                    "date": "2025-09-30",
                    "filingDate": "2025-10-29",
                    "acceptedDate": None,
                }
            ]
        },
    )
    yahoo = _make_yahoo(
        {"META": []},
        page={"META": [dict(q3_2025_gaap, date="2025-09-30", epsActual=7.25)]},
    )

    _, yahoo_rows, truf_rows, _ = _detect_meta(fmp, yahoo)

    assert yahoo_rows == []
    assert truf_rows == []


def test_the_page_settles_when_the_feed_is_on_another_basis():
    """The reverse: the feed carries a figure on another basis and the page
    matches FMP. The page's reading is the one kept, for the consensus and for
    the Yahoo stream.
    """
    fmp = _meta_q3_fmp()
    feed_q3 = dict(META_Q3_2026_YAHOO_PAGE, epsActual=7.40)
    yahoo = _make_yahoo({"META": [feed_q3]}, page={"META": [META_Q3_2026_YAHOO_PAGE]})

    _, yahoo_rows, truf_rows, _ = _detect_meta(fmp, yahoo)

    assert [r["value"] for r in truf_rows] == ["3.83"]
    assert [(r["date"], r["value"]) for r in yahoo_rows] == [(date_string_to_unix("2026-09-30"), "3.83")]


def test_the_feed_is_kept_when_both_match():
    """When the feed and the page both match FMP, the feed's figure is the one
    written, as before the page was read."""
    fmp = _meta_q3_fmp()
    page_q3 = dict(META_Q3_2026_YAHOO_PAGE, epsActual=3.84)
    yahoo = _make_yahoo({"META": [META_Q3_2026_YAHOO_PAGE]}, page={"META": [page_q3]})

    _, yahoo_rows, truf_rows, _ = _detect_meta(fmp, yahoo)

    assert [r["value"] for r in truf_rows] == ["3.83"]
    assert [r["value"] for r in yahoo_rows] == ["3.83"]


def test_a_page_reading_waits_until_another_source_matches_it():
    """Only the page has the quarter: FMP and the feed have nothing for it yet.
    Nothing is written for it; the reading waits for FMP or the feed."""
    fmp = _make_fmp(earnings={"META": []}, income_statements={"META": []})
    yahoo = _make_yahoo({"META": [META_Q2_2026_YAHOO_FEED]}, page={"META": [META_Q3_2026_YAHOO_PAGE]})

    _, yahoo_rows, truf_rows, _ = _detect_meta(fmp, yahoo)

    assert [r["date"] for r in yahoo_rows] == [date_string_to_unix("2026-06-30")]
    assert truf_rows == []


def test_a_page_that_cannot_be_read_leaves_the_feed_path_as_before():
    """A consent page, a changed layout or an HTTP error on the page must not
    cost the run anything the feed provides."""
    fmp = _make_fmp(
        earnings={"NVDA": [NVDA_Q1_FY27_FMP_EARNINGS]},
        income_statements={"NVDA": [NVDA_Q1_FY27_INCOME_STMT]},
    )
    yahoo = _make_yahoo({"NVDA": [NVDA_Q1_FY27_YAHOO_EARNINGS]}, page_error=ValueError("no earnings table"))

    fmp_rows, yahoo_rows, truf_rows, _ = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="NVDA",
    )

    assert [r["value"] for r in fmp_rows] == ["1.87"]
    assert [r["value"] for r in yahoo_rows] == ["1.87"]
    assert [r["value"] for r in truf_rows] == ["1.87"]


# --- A report still missing the morning after (OVERDUE_AFTER) ---


def _clock_at(monkeypatch: pytest.MonkeyPatch, when: str) -> None:
    monkeypatch.setattr(real_time_flow_module, "_utc_now", lambda: datetime.fromisoformat(when))


def test_a_report_still_pending_the_next_morning_is_overdue(monkeypatch: pytest.MonkeyPatch):
    """Meta's report as the adapter saw it before reading the page: FMP had the
    figure and Yahoo had nothing. At 06:00 UTC the next day, about 14 hours
    before its market settles, the report is overdue."""
    _clock_at(monkeypatch, "2026-10-22T06:00:00+00:00")
    fmp = _meta_q3_fmp()
    yahoo = _make_yahoo({"META": [META_Q2_2026_YAHOO_FEED]})

    _, _, truf_rows, overdue = _detect_meta(fmp, yahoo)

    assert truf_rows == []
    assert overdue == ["META 2026-10-21"]


@pytest.mark.parametrize("when", ["2026-10-22T05:59:00+00:00", "2026-10-24T00:00:00+00:00"])
def test_a_report_is_not_overdue_outside_its_window(monkeypatch: pytest.MonkeyPatch, when: str):
    """Before 06:00 UTC the next day the runs still have time; three days on,
    the market has settled."""
    _clock_at(monkeypatch, when)
    fmp = _meta_q3_fmp()
    yahoo = _make_yahoo({"META": [META_Q2_2026_YAHOO_FEED]})

    _, _, _, overdue = _detect_meta(fmp, yahoo)

    assert overdue == []


def test_a_report_already_on_tn_is_not_overdue(monkeypatch: pytest.MonkeyPatch):
    """Meta's 2026-10-21 record, written outside the scheduled runs, ends the
    alarm for that report."""
    from tsn_adapters.tasks.eps.config import EPS_STREAM_IDS

    _clock_at(monkeypatch, "2026-10-22T06:00:00+00:00")
    fmp = _meta_q3_fmp()
    yahoo = _make_yahoo({"META": [META_Q2_2026_YAHOO_FEED]})
    truf_tn = FakeTNAccessBlock()
    truf_tn.seed_records(EPS_STREAM_IDS["META"], [date_string_to_unix("2026-10-21")])

    _, _, truf_rows, overdue = _detect_meta(fmp, yahoo, truf_tn)

    assert truf_rows == []
    assert overdue == []


def test_a_report_this_run_settles_is_not_overdue(monkeypatch: pytest.MonkeyPatch):
    _clock_at(monkeypatch, "2026-10-22T06:00:00+00:00")
    fmp = _meta_q3_fmp()
    yahoo = _make_yahoo({"META": [META_Q2_2026_YAHOO_FEED]}, page={"META": [META_Q3_2026_YAHOO_PAGE]})

    _, _, truf_rows, overdue = _detect_meta(fmp, yahoo)

    assert [r["value"] for r in truf_rows] == ["3.83"]
    assert overdue == []


def test_a_report_with_no_figure_anywhere_is_overdue(monkeypatch: pytest.MonkeyPatch):
    """FMP lists a report's day before it has the figure. With no figure from
    any source the next morning, the report is overdue all the same."""
    _clock_at(monkeypatch, "2026-10-22T06:00:00+00:00")
    no_figure_yet = dict(META_Q3_2026_FMP_EARNINGS, epsActual=None)
    fmp = _make_fmp(earnings={"META": [no_figure_yet]}, income_statements={"META": []})
    yahoo = _make_yahoo({"META": [META_Q2_2026_YAHOO_FEED]})

    fmp_rows, _, truf_rows, overdue = _detect_meta(fmp, yahoo)

    assert fmp_rows == []
    assert truf_rows == []
    assert overdue == ["META 2026-10-21"]


@pytest.mark.timeout(120, func_only=True)
def test_an_overdue_report_fails_the_run_after_the_others_publish(
    prefect_harness: Any, monkeypatch: pytest.MonkeyPatch
):
    """GOOGL's print is new and META's report is overdue. The run writes
    GOOGL's print first, then fails, naming META's report, so the failed run
    shows the miss hours before the market settles."""
    from tsn_adapters.tasks.eps.config import EPS_STREAM_IDS, FMP_EPS_STREAM_IDS, YAHOO_EPS_STREAM_IDS

    _clock_at(monkeypatch, "2026-10-22T06:00:00+00:00")
    fmp = _make_fmp(
        earnings={"GOOGL": [GOOGL_Q2_2026_FMP_EARNINGS], "META": [META_Q3_2026_FMP_EARNINGS]},
        income_statements={"GOOGL": [GOOGL_Q2_2026_INCOME_STMT], "META": [META_Q3_2026_INCOME_STMT]},
    )
    yahoo = _make_yahoo({"GOOGL": [GOOGL_Q2_2026_YAHOO_EARNINGS], "META": [META_Q2_2026_YAHOO_FEED]})
    fmp_tn = FakeTNAccessBlock(existing_streams={FMP_EPS_STREAM_IDS["GOOGL"], FMP_EPS_STREAM_IDS["META"]})
    yahoo_tn = FakeTNAccessBlock(existing_streams={YAHOO_EPS_STREAM_IDS["GOOGL"], YAHOO_EPS_STREAM_IDS["META"]})
    truf_tn = FakeTNAccessBlock(existing_streams={EPS_STREAM_IDS["GOOGL"], EPS_STREAM_IDS["META"]})

    with pytest.raises(RuntimeError, match="META 2026-10-21"):
        eps_real_time_flow(
            fmp_block=fmp,
            yahoo_block=yahoo,
            fmp_tn_block=fmp_tn,
            yahoo_tn_block=yahoo_tn,
            truf_tn_block=truf_tn,
            symbols=["GOOGL", "META"],
        )

    truf_inserted = pd.concat(truf_tn.inserted_records, ignore_index=True)
    assert truf_inserted["value"].tolist() == ["9.11"]
