"""Tests for the EPS historical backfill flow."""

from typing import Any

import pandas as pd
from pandera.typing import DataFrame
from pydantic import PrivateAttr, SecretStr
import pytest
from tests.utils.constants import FAKE_PRIVATE_KEY
from tests.utils.fake_tn_access import FakeTNAccessBlock

from tsn_adapters.blocks.fmp import EarningsData, FMPBlock
from tsn_adapters.blocks.yahoo import YahooBlock
from tsn_adapters.flows.eps.historical_flow import build_new_records, eps_historical_flow
from tsn_adapters.tasks.eps.config import FMP_EPS_STREAM_IDS, YAHOO_EPS_STREAM_IDS
from tsn_adapters.utils.create_empty_df import create_empty_df
from tsn_adapters.utils.time_utils import date_string_to_unix


def _earnings_df(rows: list[dict]) -> DataFrame[EarningsData]:
    if not rows:
        return create_empty_df(EarningsData)
    return DataFrame[EarningsData](pd.DataFrame(rows))


class FakeFMPBlock(FMPBlock):
    _data: dict = PrivateAttr(default_factory=dict)

    def get_historical_earnings(self, symbol: str, limit: int = 40) -> DataFrame[EarningsData]:
        return _earnings_df(self._data.get(symbol, []))


class FakeYahooBlock(YahooBlock):
    _data: dict = PrivateAttr(default_factory=dict)

    def get_historical_earnings(self, symbol: str, limit: int = 40) -> DataFrame[EarningsData]:
        return _earnings_df(self._data.get(symbol, []))


def make_fmp_block(data: dict) -> FakeFMPBlock:
    block = FakeFMPBlock(api_key=SecretStr(FAKE_PRIVATE_KEY.get_secret_value()))
    block._data = data
    return block


def make_yahoo_block(data: dict) -> FakeYahooBlock:
    block = FakeYahooBlock()
    block._data = data
    return block


# --- build_new_records unit tests (call .fn to skip Prefect overhead) ---

AAPL_ROW = {"symbol": "AAPL", "date": "2024-03-31", "epsActual": 1.53, "epsEstimated": 1.50, "lastUpdated": None}


def test_build_new_records_basic():
    df = _earnings_df([AAPL_ROW])
    result = build_new_records.fn(df, stream_id="st_test", published_dates=set())
    assert len(result) == 1
    assert result.iloc[0]["stream_id"] == "st_test"
    assert result.iloc[0]["value"] == "1.53"


def test_build_new_records_skips_null_eps():
    row = {**AAPL_ROW, "epsActual": None}
    df = _earnings_df([row])
    result = build_new_records.fn(df, stream_id="st_test", published_dates=set())
    assert result.empty


def test_build_new_records_skips_already_published():
    df = _earnings_df([AAPL_ROW])
    already = {date_string_to_unix("2024-03-31")}
    result = build_new_records.fn(df, stream_id="st_test", published_dates=already)
    assert result.empty


def test_build_new_records_empty_input():
    df = create_empty_df(EarningsData)
    result = build_new_records.fn(df, stream_id="st_test", published_dates=set())
    assert result.empty


# --- flow-level tests ---


@pytest.fixture(scope="module", autouse=True)
def prefect_harness(prefect_test_fixture: Any):
    yield prefect_test_fixture


AAPL_FMP_ROW = AAPL_ROW
AAPL_YAHOO_ROW = {"symbol": "AAPL", "date": "2024-03-31", "epsActual": 1.53, "epsEstimated": None, "lastUpdated": None}


@pytest.mark.timeout(30, func_only=True)
def test_backfill_collapses_duplicate_source_rows():
    """FMP can repeat an announcement row in its response (website#4362);
    both copies pass the published-dates filter and would ride one insert
    tx, which the primitive PK rejects wholesale. The publish path must
    collapse them so the backfill still lands."""
    fmp_block = make_fmp_block({"AAPL": [AAPL_FMP_ROW, AAPL_FMP_ROW]})
    yahoo_block = make_yahoo_block({})
    fmp_tn_block = FakeTNAccessBlock(existing_streams={FMP_EPS_STREAM_IDS["AAPL"]})
    yahoo_tn_block = FakeTNAccessBlock()

    eps_historical_flow(
        fmp_block=fmp_block,
        yahoo_block=yahoo_block,
        fmp_tn_block=fmp_tn_block,
        yahoo_tn_block=yahoo_tn_block,
        symbols=["AAPL"],
    )

    inserted = pd.concat(fmp_tn_block.inserted_records, ignore_index=True)
    assert len(inserted) == 1
    assert inserted.iloc[0]["date"] == date_string_to_unix("2024-03-31")


@pytest.mark.timeout(30, func_only=True)
def test_historical_flow_inserts_fmp_and_yahoo_records():
    fmp_block = make_fmp_block({"AAPL": [AAPL_FMP_ROW]})
    yahoo_block = make_yahoo_block({"AAPL": [AAPL_YAHOO_ROW]})
    fmp_tn_block = FakeTNAccessBlock(existing_streams={FMP_EPS_STREAM_IDS["AAPL"]})
    yahoo_tn_block = FakeTNAccessBlock(existing_streams={YAHOO_EPS_STREAM_IDS["AAPL"]})

    eps_historical_flow(
        fmp_block=fmp_block,
        yahoo_block=yahoo_block,
        fmp_tn_block=fmp_tn_block,
        yahoo_tn_block=yahoo_tn_block,
        symbols=["AAPL"],
    )

    fmp_inserted = pd.concat(fmp_tn_block.inserted_records, ignore_index=True)
    yahoo_inserted = pd.concat(yahoo_tn_block.inserted_records, ignore_index=True)

    assert FMP_EPS_STREAM_IDS["AAPL"] in set(fmp_inserted["stream_id"].tolist())
    assert YAHOO_EPS_STREAM_IDS["AAPL"] in set(yahoo_inserted["stream_id"].tolist())
    # Each wallet only receives its own source's records — no cross-wallet leak
    assert YAHOO_EPS_STREAM_IDS["AAPL"] not in set(fmp_inserted["stream_id"].tolist())
    assert FMP_EPS_STREAM_IDS["AAPL"] not in set(yahoo_inserted["stream_id"].tolist())


@pytest.mark.timeout(30, func_only=True)
def test_historical_flow_no_insert_when_all_published():
    already_unix = date_string_to_unix("2024-03-31")

    fmp_block = make_fmp_block({"AAPL": [AAPL_FMP_ROW]})
    yahoo_block = make_yahoo_block({"AAPL": [AAPL_YAHOO_ROW]})
    fmp_tn_block = FakeTNAccessBlock()
    yahoo_tn_block = FakeTNAccessBlock()
    fmp_tn_block.seed_records(FMP_EPS_STREAM_IDS["AAPL"], [already_unix])
    yahoo_tn_block.seed_records(YAHOO_EPS_STREAM_IDS["AAPL"], [already_unix])

    eps_historical_flow(
        fmp_block=fmp_block,
        yahoo_block=yahoo_block,
        fmp_tn_block=fmp_tn_block,
        yahoo_tn_block=yahoo_tn_block,
        symbols=["AAPL"],
    )

    assert fmp_tn_block.inserted_records == []
    assert yahoo_tn_block.inserted_records == []
