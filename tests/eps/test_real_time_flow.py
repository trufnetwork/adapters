"""Tests for the EPS real-time reconciliation flow.

Covers the canonical-key reconciliation introduced in
trufnetwork/website#3936: FMP and Yahoo use different date semantics for
the same earnings event (announcement date vs fiscal-period-end). The
reconciler must derive a canonical event identity from each source so
naïve dict-key joins on raw dates don't treat the same event as two
separate single-source entries.
"""

import pandas as pd
from pandera.typing import DataFrame
from pydantic import PrivateAttr, SecretStr
from tests.utils.constants import FAKE_PRIVATE_KEY
from tests.utils.fake_tn_access import FakeTNAccessBlock

from tsn_adapters.blocks.fmp import EarningsData, FMPBlock, QuarterlyIncomeStatementData
from tsn_adapters.blocks.yahoo import YahooBlock
from tsn_adapters.flows.eps.real_time_flow import detect_and_prepare_eps
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
    _data: dict = PrivateAttr(default_factory=dict)

    def get_historical_earnings(self, symbol: str, limit: int = 40) -> DataFrame[EarningsData]:
        return _earnings_df(self._data.get(symbol, []))


def _make_fmp(earnings: dict[str, list[dict]], income_statements: dict[str, list[dict]]) -> RealTimeFakeFMPBlock:
    block = RealTimeFakeFMPBlock(api_key=SecretStr(FAKE_PRIVATE_KEY.get_secret_value()))
    block._earnings_data = earnings
    block._income_data = income_statements
    return block


def _make_yahoo(data: dict[str, list[dict]]) -> RealTimeFakeYahooBlock:
    block = RealTimeFakeYahooBlock()
    block._data = data
    return block


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

    fmp_rows, yahoo_rows, truf_rows = detect_and_prepare_eps.fn(
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

    _, _, truf_rows = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="AAPL",
    )

    assert len(truf_rows) == 1
    assert truf_rows[0]["value"] == "2.01"


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

    fmp_rows, yahoo_rows, truf_rows = detect_and_prepare_eps.fn(
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

    fmp_rows, yahoo_rows, truf_rows = detect_and_prepare_eps.fn(
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

    fmp_rows, yahoo_rows, truf_rows = detect_and_prepare_eps.fn(
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


def test_skips_fmp_event_when_income_statement_not_yet_published():
    """Rare race at the moment of an earnings announcement: FMP /stable/earnings
    has the new actual but /stable/income-statement hasn't yet been populated
    for that filing. The raw FMP primitive still emits under the announcement
    date — the income-statement is only needed to derive the canonical key
    for reconciliation. Yahoo still writes its primitive. Consensus is
    pending and resolves on the next cycle once income-statement catches up.
    """
    fmp = _make_fmp(
        earnings={"NVDA": [NVDA_Q1_FY27_FMP_EARNINGS]},
        income_statements={"NVDA": []},  # income-statement hasn't published
    )
    yahoo = _make_yahoo({"NVDA": [NVDA_Q1_FY27_YAHOO_EARNINGS]})

    fmp_rows, yahoo_rows, truf_rows = detect_and_prepare_eps.fn(
        fmp_block=fmp,
        yahoo_block=yahoo,
        fmp_tn_block=FakeTNAccessBlock(),
        yahoo_tn_block=FakeTNAccessBlock(),
        truf_tn_block=FakeTNAccessBlock(),
        symbol="NVDA",
    )

    # FMP primitive still emits under the announcement date — primitive
    # emission is decoupled from reconciliation
    assert len(fmp_rows) == 1
    assert fmp_rows[0]["value"] == "1.87"
    assert fmp_rows[0]["date"] == date_string_to_unix("2026-05-20")
    # Yahoo still publishes its primitive
    assert len(yahoo_rows) == 1
    # No consensus possible — FMP entry isn't in the canonical-key dict
    # without a period-end lookup, so reconciliation defers to next cycle
    assert len(truf_rows) == 0


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

    fmp_rows, yahoo_rows, truf_rows = detect_and_prepare_eps.fn(
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


def test_idempotent_when_all_streams_already_published():
    """Full idempotency: when every stream's date is already on-chain,
    a re-run produces no writes anywhere. Mirrors the steady-state
    behaviour of the scheduled flow re-encountering the same recent
    earnings rows on subsequent ticks.
    """
    from tsn_adapters.tasks.eps.config import (
        EPS_STREAM_IDS,
        FMP_EPS_STREAM_IDS,
        YAHOO_EPS_STREAM_IDS,
    )

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

    fmp_rows, yahoo_rows, truf_rows = detect_and_prepare_eps.fn(
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
