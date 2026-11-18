"""Tests for the EPS report watch (flows/eps/report_watch_flow.py).

MSFT and GOOGL both report on 2026-10-28, in US summer time (EDT), so their
window runs from 20:00 UTC that day to 02:00 UTC the next. Prefect Variables
and the Prefect client are in-memory fakes.
"""

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pandas as pd
from pandera.typing import DataFrame
from prefect.client.schemas.objects import FlowRun, StateType
from pydantic import PrivateAttr, SecretStr
import pytest
from tests.utils.constants import FAKE_PRIVATE_KEY
from tests.utils.fake_prefect_variables import FakeVariables
from tests.utils.fake_tn_access import FakeTNAccessBlock

from tsn_adapters.blocks.fmp import EarningsData, FMPBlock, QuarterlyIncomeStatementData
from tsn_adapters.blocks.yahoo import YahooBlock
import tsn_adapters.flows.eps.real_time_flow as real_time_flow_module
from tsn_adapters.flows.eps.real_time_flow import EpsDetection
import tsn_adapters.flows.eps.report_watch_flow as watch_module
from tsn_adapters.flows.eps.report_watch_flow import due_symbols, eps_report_watch_flow, window_start
from tsn_adapters.tasks.eps.config import EPS_STREAM_IDS, FMP_EPS_STREAM_IDS, YAHOO_EPS_STREAM_IDS
from tsn_adapters.utils.create_empty_df import create_empty_df
from tsn_adapters.utils.time_utils import date_string_to_unix

REPORT_DAY = "2026-10-28"
IN_WINDOW = datetime(2026, 10, 28, 22, 0, tzinfo=timezone.utc)
BUDGET = "eps_watch_fmp_calls"
REPORT_DAYS = "eps_report_days"

MSFT_Q3_FMP = {"symbol": "MSFT", "date": REPORT_DAY, "epsActual": 4.13, "epsEstimated": 3.9, "lastUpdated": None}
MSFT_Q3_YAHOO = {"symbol": "MSFT", "date": "2026-09-30", "epsActual": 4.13, "epsEstimated": 3.9, "lastUpdated": None}
GOOGL_Q3_FMP = {"symbol": "GOOGL", "date": REPORT_DAY, "epsActual": 2.87, "epsEstimated": 2.5, "lastUpdated": None}
GOOGL_Q3_YAHOO = {"symbol": "GOOGL", "date": "2026-09-30", "epsActual": 2.87, "epsEstimated": 2.5, "lastUpdated": None}


def _earnings_df(rows: list[dict]) -> DataFrame[EarningsData]:
    return DataFrame[EarningsData](pd.DataFrame(rows)) if rows else create_empty_df(EarningsData)


class PollCountingFMPBlock(FMPBlock):
    """Records every FMP call, and the budget Variable as it stood at each call."""

    _earnings: dict = PrivateAttr(default_factory=dict)
    _errors: dict = PrivateAttr(default_factory=dict)
    _calls: list = PrivateAttr(default_factory=list)
    _budget_at_call: list = PrivateAttr(default_factory=list)
    _variables: Any = PrivateAttr(default=None)

    def get_historical_earnings(self, symbol: str, limit: int = 40) -> DataFrame[EarningsData]:
        self._calls.append(("earnings", symbol))
        if self._variables is not None:
            self._budget_at_call.append(self._variables.values.get(BUDGET))
        if symbol in self._errors:
            raise self._errors[symbol]
        return _earnings_df(self._earnings.get(symbol, []))

    def get_quarterly_income_statements(self, symbol: str, limit: int = 8) -> DataFrame[QuarterlyIncomeStatementData]:
        self._calls.append(("income-statement", symbol))
        return create_empty_df(QuarterlyIncomeStatementData)


class FeedYahooBlock(YahooBlock):
    _feed: dict = PrivateAttr(default_factory=dict)

    def get_historical_earnings(self, symbol: str, limit: int = 40) -> DataFrame[EarningsData]:
        return _earnings_df(self._feed.get(symbol, []))

    def get_page_earnings(self, symbol: str, period_ends: list[date]) -> DataFrame[EarningsData]:
        return create_empty_df(EarningsData)


class Untouchable:
    """A block that fails the test on any attribute access, so on any call."""

    def __getattribute__(self, name: str) -> Any:
        raise AssertionError(f"a block was touched: .{name}")


class FakeClient:
    """The sync Prefect client: returns `runs` from read_flow_runs, or raises `error`."""

    def __init__(self, runs: list[FlowRun] | None = None, error: Exception | None = None) -> None:
        self.runs = runs or []
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def read_flow_runs(self, **kwargs: Any) -> list[FlowRun]:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.runs


def _running(started: datetime, run_id: Any = None) -> FlowRun:
    return FlowRun(
        id=run_id or uuid4(),
        name="eps-real-time-run",
        flow_id=uuid4(),
        start_time=started,
        state_type=StateType.RUNNING,
    )


# --- Fixtures ---


@pytest.fixture(scope="module")
def prefect_harness(prefect_test_fixture: Any):
    yield prefect_test_fixture


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch):
    def set_now(now: datetime) -> None:
        monkeypatch.setattr(watch_module, "_utc_now", lambda: now)
        monkeypatch.setattr(real_time_flow_module, "_utc_now", lambda: now)

    set_now(IN_WINDOW)
    return set_now


@pytest.fixture
def variables(monkeypatch: pytest.MonkeyPatch) -> FakeVariables:
    store = FakeVariables({REPORT_DAYS: {"updated_at": "2026-10-28T18:00:00+00:00", "days": {}}})
    monkeypatch.setattr(watch_module, "Variable", store)
    monkeypatch.setattr(real_time_flow_module, "Variable", store)
    return store


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> FakeClient:
    fake = FakeClient()

    def get_client(*args: Any, sync_client: bool = False, **kwargs: Any) -> FakeClient:
        assert sync_client, "the watch reads runs with the sync client"
        return fake

    monkeypatch.setattr(watch_module, "get_client", get_client)
    return fake


@pytest.fixture
def consensus_reads(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int]]:
    """The watch's own consensus-stream checks, as (stream_id, date)."""
    reads: list[tuple[str, int]] = []
    real = watch_module.is_published

    def recording(tn_block: Any, stream_id: str, date_unix: int) -> bool:
        reads.append((stream_id, date_unix))
        return real(tn_block, stream_id, date_unix)

    monkeypatch.setattr(watch_module, "is_published", recording)
    return reads


def _report_days(variables: FakeVariables, days: dict[str, list[str]]) -> None:
    variables.values[REPORT_DAYS] = {"updated_at": "2026-10-28T18:00:00+00:00", "days": days}


def _untouched_watch(**kwargs: Any) -> None:
    """Run one tick through the flow's function with blocks that must not be touched."""
    eps_report_watch_flow.fn(
        fmp_block=Untouchable(),
        yahoo_block=Untouchable(),
        fmp_tn_block=Untouchable(),
        yahoo_tn_block=Untouchable(),
        truf_tn_block=kwargs.pop("truf_tn_block", Untouchable()),
        **kwargs,
    )


def _published(*symbols: str) -> FakeTNAccessBlock:
    truf_tn = FakeTNAccessBlock()
    for symbol in symbols:
        truf_tn.seed_records(EPS_STREAM_IDS[symbol], [date_string_to_unix(REPORT_DAY)])
    return truf_tn


# --- The report window: 16:00 New York time for 6 hours ---


@pytest.mark.parametrize(
    "day,opens",
    [
        (date(2026, 10, 21), datetime(2026, 10, 21, 20, 0, tzinfo=timezone.utc)),  # EDT
        (date(2026, 11, 2), datetime(2026, 11, 2, 21, 0, tzinfo=timezone.utc)),  # EST, the day after the change
        (date(2026, 11, 17), datetime(2026, 11, 17, 21, 0, tzinfo=timezone.utc)),  # EST
    ],
)
def test_the_window_opens_at_four_pm_new_york_time(day: date, opens: datetime):
    assert window_start(day) == opens


@pytest.mark.parametrize(
    "day,now,due",
    [
        ("2026-10-21", "2026-10-21T19:59:59+00:00", False),
        ("2026-10-21", "2026-10-21T20:00:00+00:00", True),
        ("2026-10-21", "2026-10-22T01:59:59+00:00", True),
        ("2026-10-21", "2026-10-22T02:00:00+00:00", False),
        ("2026-11-02", "2026-11-02T20:30:00+00:00", False),
        ("2026-11-02", "2026-11-02T21:00:00+00:00", True),
        ("2026-11-17", "2026-11-17T20:59:59+00:00", False),
        ("2026-11-17", "2026-11-17T21:00:00+00:00", True),
        ("2026-11-17", "2026-11-18T02:59:59+00:00", True),
        ("2026-11-17", "2026-11-18T03:00:00+00:00", False),
    ],
)
def test_window_edges_in_summer_and_winter_time(day: str, now: str, due: bool):
    assert due_symbols({"NVDA": [day]}, ["NVDA"], datetime.fromisoformat(now)) == ([("NVDA", day)] if due else [])


def test_due_symbols_follow_the_symbol_order():
    """Only the symbols asked for, in their order, each with its day in the
    window. Other days, unknown symbols and values that are not dates are left
    out."""
    report_days = {
        "GOOGL": [REPORT_DAY],
        "MSFT": ["2026-07-29", REPORT_DAY],
        "AMZN": ["2026-10-29"],
        "TSLA": ["not a date"],
        "XYZ": [REPORT_DAY],
    }

    due = due_symbols(report_days, ["MSFT", "GOOGL", "AMZN", "TSLA"], IN_WINDOW)

    assert due == [("MSFT", REPORT_DAY), ("GOOGL", REPORT_DAY)]


# --- A tick with nothing to do touches nothing ---


def test_no_due_symbol_touches_no_block_and_no_other_api(clock: Any, variables: FakeVariables, client: FakeClient):
    """Most ticks: a report day is recorded but its window is not open yet."""
    clock(datetime(2026, 10, 28, 19, 50, tzinfo=timezone.utc))
    _report_days(variables, {"MSFT": [REPORT_DAY], "GOOGL": [REPORT_DAY]})

    _untouched_watch(busy_deployments=["eps-real-time-flow"])

    assert variables.calls == [("get", REPORT_DAYS)]
    assert client.calls == []


@pytest.mark.parametrize(
    "stored",
    [
        pytest.param(None, id="missing"),
        pytest.param("not report days", id="not-a-dict"),
        pytest.param({"days": ["MSFT"]}, id="days-not-a-dict"),
    ],
)
def test_report_days_that_are_missing_or_broken_end_the_tick(
    clock: Any, variables: FakeVariables, client: FakeClient, stored: Any
):
    if stored is None:
        del variables.values[REPORT_DAYS]
    else:
        variables.values[REPORT_DAYS] = stored

    _untouched_watch(busy_deployments=["eps-real-time-flow"])

    assert variables.calls == [("get", REPORT_DAYS)]
    assert client.calls == []


def test_report_days_that_cannot_be_read_end_the_tick(clock: Any, variables: FakeVariables, client: FakeClient):
    variables.get_errors[REPORT_DAYS] = ConnectionError("Prefect API unreachable")

    _untouched_watch(busy_deployments=["eps-real-time-flow"])

    assert client.calls == []


# --- The busy guard ---


def test_a_recent_eps_run_skips_the_tick(clock: Any, variables: FakeVariables, client: FakeClient):
    """The 00:00 real-time run is still RUNNING: the tick stays out of its way."""
    _report_days(variables, {"MSFT": [REPORT_DAY]})
    client.runs = [_running(IN_WINDOW - timedelta(minutes=30))]

    _untouched_watch(busy_deployments=["eps-real-time-flow", "eps-historical-flow"])

    [call] = client.calls
    assert call["deployment_filter"].name.any_ == ["eps-real-time-flow", "eps-historical-flow"]
    assert call["flow_run_filter"].state.type.any_ == [StateType.RUNNING]
    assert variables.names_touched() == {REPORT_DAYS}


@pytest.mark.parametrize("age", [timedelta(minutes=60), timedelta(hours=5)])
def test_a_run_stuck_in_running_for_an_hour_no_longer_blocks(
    clock: Any, variables: FakeVariables, client: FakeClient, consensus_reads: list, age: timedelta
):
    """A run that crashed without reporting it stays RUNNING for good; after an
    hour it stops holding the watch back. MSFT is already on the consensus
    stream, so the tick ends at that check."""
    _report_days(variables, {"MSFT": [REPORT_DAY]})
    client.runs = [_running(IN_WINDOW - age)]

    _untouched_watch(busy_deployments=["eps-real-time-flow"], truf_tn_block=_published("MSFT"))

    assert consensus_reads == [(EPS_STREAM_IDS["MSFT"], date_string_to_unix(REPORT_DAY))]


def test_this_run_does_not_count_as_busy(
    clock: Any, variables: FakeVariables, client: FakeClient, consensus_reads: list, monkeypatch: pytest.MonkeyPatch
):
    """The watch's own deployment can be listed to keep two ticks apart."""
    own_id = uuid4()
    monkeypatch.setenv("PREFECT__FLOW_RUN_ID", str(own_id))
    _report_days(variables, {"MSFT": [REPORT_DAY]})
    client.runs = [_running(IN_WINDOW - timedelta(minutes=1), run_id=own_id)]

    _untouched_watch(busy_deployments=["eps-report-watch"], truf_tn_block=_published("MSFT"))

    assert len(consensus_reads) == 1


def test_runs_that_cannot_be_read_skip_the_tick(clock: Any, variables: FakeVariables, client: FakeClient):
    _report_days(variables, {"MSFT": [REPORT_DAY]})
    client.error = ConnectionError("Prefect API unreachable")

    _untouched_watch(busy_deployments=["eps-real-time-flow"])

    assert len(client.calls) == 1


def test_without_busy_deployments_no_runs_are_read(
    clock: Any, variables: FakeVariables, client: FakeClient, consensus_reads: list
):
    _report_days(variables, {"MSFT": [REPORT_DAY]})

    _untouched_watch(truf_tn_block=_published("MSFT"))

    assert client.calls == []
    assert len(consensus_reads) == 1


# --- Symbols already published, and the FMP budget ---


def test_a_symbol_already_on_the_consensus_stream_is_not_polled(clock: Any, variables: FakeVariables):
    """Once a print has settled, the rest of its window costs no FMP call."""
    _report_days(variables, {"MSFT": [REPORT_DAY], "GOOGL": [REPORT_DAY]})

    _untouched_watch(truf_tn_block=_published("MSFT", "GOOGL"))

    assert BUDGET not in variables.names_touched()


def test_a_spent_budget_polls_nothing(clock: Any, variables: FakeVariables):
    _report_days(variables, {"MSFT": [REPORT_DAY]})
    variables.values[BUDGET] = {"report_day": REPORT_DAY, "fmp_calls": 100}

    _untouched_watch(truf_tn_block=_published())

    assert variables.values[BUDGET] == {"report_day": REPORT_DAY, "fmp_calls": 100}
    assert ("set", BUDGET) not in variables.calls


def test_a_broken_count_for_the_day_polls_nothing(clock: Any, variables: FakeVariables):
    """A count that is not a number must not reopen the day's budget."""
    _report_days(variables, {"MSFT": [REPORT_DAY]})
    variables.values[BUDGET] = {"report_day": REPORT_DAY, "fmp_calls": "many"}

    _untouched_watch(truf_tn_block=_published())

    assert ("set", BUDGET) not in variables.calls


@pytest.mark.parametrize("failing", ["get", "set"])
def test_a_count_that_cannot_be_kept_polls_nothing_and_fails_the_run(
    clock: Any, variables: FakeVariables, failing: str
):
    """Polling without the count written could overspend the shared FMP key."""
    _report_days(variables, {"MSFT": [REPORT_DAY]})
    errors = variables.get_errors if failing == "get" else variables.set_errors
    errors[BUDGET] = ConnectionError("Prefect API unreachable")

    with pytest.raises(RuntimeError, match="MSFT"):
        _untouched_watch(truf_tn_block=_published())


# --- Polling: retries off, one FMP call, published as the real-time flow does ---


def test_a_poll_runs_with_task_retries_off_and_no_income_statement(
    clock: Any, variables: FakeVariables, monkeypatch: pytest.MonkeyPatch
):
    """conftest.py strips retries from every task in tests, so this checks
    what the watch asks of Prefect. The overdue list is not the watch's to act
    on: it does not fail the tick."""
    _report_days(variables, {"MSFT": [REPORT_DAY]})
    options: list[dict[str, Any]] = []
    polls: list[dict[str, Any]] = []

    def poll(**kwargs: Any) -> EpsDetection:
        polls.append(kwargs)
        return EpsDetection([], [], [], [f"MSFT {REPORT_DAY}"], [])

    def with_options(**kwargs: Any) -> Any:
        options.append(kwargs)
        return poll

    monkeypatch.setattr(watch_module, "detect_and_prepare_eps", SimpleNamespace(with_options=with_options))

    eps_report_watch_flow.fn(
        fmp_block=Untouchable(),
        yahoo_block=Untouchable(),
        fmp_tn_block=Untouchable(),
        yahoo_tn_block=Untouchable(),
        truf_tn_block=_published(),
    )

    assert options == [{"retries": 0}]
    assert [(p["symbol"], p["fetch_income_statement"]) for p in polls] == [("MSFT", False)]


def _blocks(variables: FakeVariables) -> dict[str, Any]:
    fmp = PollCountingFMPBlock(api_key=SecretStr(FAKE_PRIVATE_KEY.get_secret_value()))
    fmp._earnings = {"MSFT": [MSFT_Q3_FMP], "GOOGL": [GOOGL_Q3_FMP]}
    fmp._variables = variables
    yahoo = FeedYahooBlock()
    yahoo._feed = {"MSFT": [MSFT_Q3_YAHOO], "GOOGL": [GOOGL_Q3_YAHOO]}
    return {
        "fmp_block": fmp,
        "yahoo_block": yahoo,
        "fmp_tn_block": FakeTNAccessBlock(existing_streams={FMP_EPS_STREAM_IDS["MSFT"], FMP_EPS_STREAM_IDS["GOOGL"]}),
        "yahoo_tn_block": FakeTNAccessBlock(
            existing_streams={YAHOO_EPS_STREAM_IDS["MSFT"], YAHOO_EPS_STREAM_IDS["GOOGL"]}
        ),
        "truf_tn_block": FakeTNAccessBlock(existing_streams={EPS_STREAM_IDS["MSFT"], EPS_STREAM_IDS["GOOGL"]}),
    }


def _consensus(blocks: dict[str, Any]) -> list[tuple[str, str]]:
    inserted = blocks["truf_tn_block"].inserted_records
    if not inserted:
        return []
    frame = pd.concat(inserted, ignore_index=True)
    symbol_of = {stream: symbol for symbol, stream in EPS_STREAM_IDS.items()}
    return sorted((symbol_of[row["stream_id"]], row["value"]) for _, row in frame.iterrows())


@pytest.mark.timeout(120, func_only=True)
def test_each_due_symbol_costs_one_fmp_call_and_publishes(prefect_harness: Any, clock: Any, variables: FakeVariables):
    """Both prints settle from FMP's earnings and Yahoo's quarter, without the
    income statement, and the count is written before the first call."""
    _report_days(variables, {"MSFT": [REPORT_DAY], "GOOGL": [REPORT_DAY]})
    variables.values[BUDGET] = {"report_day": "2026-10-21", "fmp_calls": 100}  # an earlier report day's
    blocks = _blocks(variables)

    eps_report_watch_flow(**blocks)

    fmp = blocks["fmp_block"]
    assert fmp._calls == [("earnings", "MSFT"), ("earnings", "GOOGL")]
    assert fmp._budget_at_call == [{"report_day": REPORT_DAY, "fmp_calls": 2}] * 2
    assert _consensus(blocks) == [("GOOGL", "2.87"), ("MSFT", "4.13")]
    assert len(pd.concat(blocks["fmp_tn_block"].inserted_records)) == 2
    assert len(pd.concat(blocks["yahoo_tn_block"].inserted_records)) == 2


@pytest.mark.timeout(120, func_only=True)
def test_the_budget_caps_the_polls_in_symbol_order(prefect_harness: Any, clock: Any, variables: FakeVariables):
    """One call left of 100: MSFT (first in the Mag-7 order) is polled, GOOGL
    waits, and the run does not fail for it."""
    _report_days(variables, {"MSFT": [REPORT_DAY], "GOOGL": [REPORT_DAY]})
    variables.values[BUDGET] = {"report_day": REPORT_DAY, "fmp_calls": 99}
    blocks = _blocks(variables)

    eps_report_watch_flow(**blocks)

    assert blocks["fmp_block"]._calls == [("earnings", "MSFT")]
    assert variables.values[BUDGET] == {"report_day": REPORT_DAY, "fmp_calls": 100}
    assert _consensus(blocks) == [("MSFT", "4.13")]


@pytest.mark.timeout(120, func_only=True)
def test_a_failed_poll_still_counts_and_fails_the_run_after_the_others_publish(
    prefect_harness: Any, clock: Any, variables: FakeVariables
):
    """The count was reserved before the call, so the failed poll is counted;
    GOOGL's print is published before the run fails, naming MSFT."""
    _report_days(variables, {"MSFT": [REPORT_DAY], "GOOGL": [REPORT_DAY]})
    variables.values[BUDGET] = {"report_day": REPORT_DAY, "fmp_calls": 10}
    blocks = _blocks(variables)
    blocks["fmp_block"]._errors = {"MSFT": ConnectionError("FMP timed out")}

    with pytest.raises(RuntimeError, match="MSFT"):
        eps_report_watch_flow(**blocks)

    assert variables.values[BUDGET] == {"report_day": REPORT_DAY, "fmp_calls": 12}
    assert _consensus(blocks) == [("GOOGL", "2.87")]


@pytest.mark.timeout(120, func_only=True)
def test_only_the_symbols_not_yet_published_are_polled(prefect_harness: Any, clock: Any, variables: FakeVariables):
    _report_days(variables, {"MSFT": [REPORT_DAY], "GOOGL": [REPORT_DAY]})
    blocks = _blocks(variables)
    blocks["truf_tn_block"].seed_records(EPS_STREAM_IDS["MSFT"], [date_string_to_unix(REPORT_DAY)])

    eps_report_watch_flow(**blocks)

    assert blocks["fmp_block"]._calls == [("earnings", "GOOGL")]
    assert variables.values[BUDGET] == {"report_day": REPORT_DAY, "fmp_calls": 1}
    assert _consensus(blocks) == [("GOOGL", "2.87")]
