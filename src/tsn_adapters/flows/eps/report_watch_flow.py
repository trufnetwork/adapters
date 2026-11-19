"""EPS Report Watch Flow.

The real-time flow runs every 6 hours, so a print can wait about 4 hours after
its release before it reaches TN. This flow runs every 10 minutes and polls a
symbol on the evening it reports, from the report days the real-time flow
records in a Prefect Variable (REPORT_DAYS_VARIABLE). Nothing is set by hand
per quarter.

Per tick:
  1. Read the report days. Missing or unreadable: log and stop.
  2. A symbol is due from 16:00 New York time on its report day, for
     REPORT_WINDOW. All Mag-7 report after the US close (16:00 to 16:30 New
     York time). No symbol due: stop before any FMP, Yahoo or TN call.
  3. Skip the tick while another EPS run is busy or about to start. Two EPS
     processes at once break each other's Yahoo session: YahooBlock clears
     the container-wide yfinance cookie cache. Busy means a run of
     `busy_deployments`, or another tick of this deployment, RUNNING for less
     than BUSY_RUN_MAX_AGE (a run stuck in RUNNING longer stops counting).
     About to start means a run of `busy_deployments` SCHEDULED or PENDING
     to start within `busy_lookahead_minutes`. The other EPS flows never
     check for the watch, and the 00:00 UTC real-time run falls inside every
     report window, so the watch keeps out of their way.
  4. Leave out a symbol whose consensus stream already holds the day.
  5. Count FMP calls per report day in a Prefect Variable (`budget_variable`),
     capped at `fmp_budget_per_report_day`. The FMP key is shared with other
     flows and allows 250 calls a day. The other flows made about 80 to 95
     calls a day in the 30 days to 18 Nov 2026 and 150 on their busiest day,
     so the default cap of 50 keeps even such a day at about 200. One poll
     is one FMP call: no income statement and no task retries. The count is
     written before polling, so a crash cannot under-count. A tick skips
     while another tick of its deployment is RUNNING (step 3), so two ticks
     do not read and write the count at once.
  6. Publish as the real-time flow does. The overdue check stays with the
     real-time flow. A symbol that fails fails the run after the others are
     published.

Usage:
    eps_report_watch_flow(
        fmp_block=FMPBlock.load("default"),
        yahoo_block=YahooBlock.load("default"),
        fmp_tn_block=TNAccessBlock.load("fmp-eps"),
        yahoo_tn_block=TNAccessBlock.load("yahoo-eps"),
        truf_tn_block=TNAccessBlock.load("truf-eps"),
        busy_deployments=["eps-real-time-flow", "eps-historical-flow", "testnet_eps_historical"],
    )
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
import os
from typing import Any, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    DeploymentFilter,
    DeploymentFilterId,
    DeploymentFilterName,
    FlowRunFilter,
    FlowRunFilterExpectedStartTime,
    FlowRunFilterState,
    FlowRunFilterStateType,
    Operator,
)
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.client.schemas.sorting import FlowRunSort
from prefect.runtime import deployment as current_deployment
from prefect.runtime.flow_run import get_id as current_flow_run_id
from prefect.variables import Variable

from tsn_adapters.blocks.fmp import FMPBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.blocks.yahoo import YahooBlock
from tsn_adapters.flows.eps.publish import publish_detected_rows
from tsn_adapters.flows.eps.real_time_flow import (
    REPORT_DAYS_VARIABLE,
    detect_and_prepare_eps,
    detect_symbols,
    is_published,
)
from tsn_adapters.tasks.eps.config import EPS_STREAM_IDS, MAG7
from tsn_adapters.utils.logging import get_logger_safe
from tsn_adapters.utils.time_utils import date_string_to_unix

NEW_YORK = ZoneInfo("America/New_York")
REPORT_WINDOW_OPENS = time(16, 0)  # New York time, the US close
REPORT_WINDOW = timedelta(hours=6)
BUSY_RUN_MAX_AGE = timedelta(hours=1)
BUSY_LOOKAHEAD_MINUTES = 15

BUDGET_VARIABLE = "eps_watch_fmp_calls"
FMP_BUDGET_PER_REPORT_DAY = 50


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def window_start(day: date) -> datetime:
    """When a report day's window opens: 16:00 New York time, in UTC.

    20:00 UTC in summer time (EDT), 21:00 UTC in winter time (EST).
    """
    return datetime.combine(day, REPORT_WINDOW_OPENS, tzinfo=NEW_YORK).astimezone(timezone.utc)


def due_symbols(report_days: dict[str, list[str]], symbols: Sequence[str], now: datetime) -> list[tuple[str, str]]:
    """`(symbol, report day)` for each of `symbols`, in that order, whose window holds `now`.

    The windows of two days never overlap, so a symbol has one due day at most.
    """
    logger = get_logger_safe(__name__)
    due: list[tuple[str, str]] = []
    for symbol in symbols:
        for recorded in report_days.get(symbol, []):
            try:
                day = datetime.fromisoformat(recorded).date()
            except ValueError:
                logger.warning(f"{symbol}: report day {recorded!r} is not a date, ignored")
                continue
            opens = window_start(day)
            if opens <= now < opens + REPORT_WINDOW:
                due.append((symbol, day.isoformat()))
                break
    return due


def _read_report_days(variable: str) -> dict[str, list[str]] | None:
    """Each symbol's recorded report days, or None when there are none to read."""
    logger = get_logger_safe(__name__)
    try:
        value: Any = Variable.get(variable)
    except Exception as err:
        logger.warning(f"Report days not read from Prefect Variable {variable!r}: {err!r}")
        return None
    days: Any = cast(dict[str, Any], value).get("days") if isinstance(value, dict) else None
    if not isinstance(days, dict):
        if value is None:
            logger.info(f"No report days recorded in Prefect Variable {variable!r} yet")
        else:
            logger.warning(f"Prefect Variable {variable!r} holds no report days: {value!r}")
        return None
    recorded = cast(dict[Any, Any], days)
    return {
        str(symbol): [str(d) for d in cast(list[Any], symbol_days)]
        for symbol, symbol_days in recorded.items()
        if isinstance(symbol_days, list)
    }


def _own_deployment_id() -> str | None:
    """The deployment this tick runs under, or None outside a deployment.

    When a wrapper flow calls `eps_report_watch_flow.fn`, it is the wrapper's.
    """
    deployment_id: Any = current_deployment.id
    return str(deployment_id) if deployment_id else None


def _busy_runs(deployments: Sequence[str], now: datetime, lookahead: timedelta) -> list[FlowRun]:
    """The runs this tick must keep out of the way of.

    - A run of `deployments`, or another tick of this tick's own deployment,
      RUNNING and started less than BUSY_RUN_MAX_AGE ago. A run stuck in
      RUNNING longer is logged.
    - A run of `deployments` SCHEDULED or PENDING to start before
      `now + lookahead`, and not more than BUSY_RUN_MAX_AGE ago. This
      deployment's own coming ticks do not count.

    This run itself never counts. Raises when the runs cannot be read.
    """
    own_deployment = _own_deployment_id()
    if not deployments and own_deployment is None:
        return []
    # This run when it is deployed directly, or the deployed run that called it.
    own_ids = {current_flow_run_id(), os.environ.get("PREFECT__FLOW_RUN_ID")}
    since, until = now - BUSY_RUN_MAX_AGE, now + lookahead
    # One read per rule. Prefect returns one page (200 runs by default), by id
    # unless sorted, and ids are random, so old runs that cannot count (stuck in
    # RUNNING, or SCHEDULED and missed) could fill it and hide one that counts.
    # RUNNING runs come newest first: stuck ones come after every run that
    # counts, and are still read so they can be logged. SCHEDULED and PENDING
    # runs are bounded in time by the server, so missed ones never come back.
    # model_validate: `before_` and `after_` take a datetime and `operator` is a
    # field, though the type stubs say otherwise.
    running = FlowRunFilter(
        state=FlowRunFilterState(type=FlowRunFilterStateType(any_=[StateType.RUNNING])),
        expected_start_time=FlowRunFilterExpectedStartTime.model_validate({"before_": until}),
    )
    waiting = FlowRunFilter(
        state=FlowRunFilterState(type=FlowRunFilterStateType(any_=[StateType.PENDING, StateType.SCHEDULED])),
        expected_start_time=FlowRunFilterExpectedStartTime.model_validate({"after_": since, "before_": until}),
    )
    deployment_filter = DeploymentFilter.model_validate(
        {
            "operator": Operator.or_,  # a run of `deployments` or of this deployment
            "name": DeploymentFilterName(any_=list(deployments)) if deployments else None,
            "id": DeploymentFilterId(any_=[UUID(own_deployment)]) if own_deployment else None,
        }
    )
    with get_client(sync_client=True) as client:
        runs = client.read_flow_runs(
            flow_run_filter=running, deployment_filter=deployment_filter, sort=FlowRunSort.START_TIME_DESC
        )
        if deployments:  # this deployment's own coming ticks never count
            runs = runs + client.read_flow_runs(
                flow_run_filter=waiting,
                deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=list(deployments))),
            )
    logger = get_logger_safe(__name__)
    busy: list[FlowRun] = []
    for run in runs:
        if str(run.id) in own_ids:
            continue
        if run.state_type == StateType.RUNNING:
            if run.start_time is None or now - run.start_time >= BUSY_RUN_MAX_AGE:
                logger.warning(f"Flow run {run.name} has been RUNNING since {run.start_time}; not waiting for it")
                continue
            busy.append(run)
        elif own_deployment is not None and str(run.deployment_id) == own_deployment:
            continue  # this deployment's own coming ticks
        elif run.expected_start_time and now - BUSY_RUN_MAX_AGE < run.expected_start_time < now + lookahead:
            busy.append(run)
    return busy


def _describe(run: FlowRun) -> str:
    """A busy run for the log, such as "eps-real-time-run (SCHEDULED for 2026-10-29 00:00 UTC)"."""
    running = run.state_type == StateType.RUNNING
    when = run.start_time if running else run.expected_start_time
    state = run.state_type.value if run.state_type else "no state"
    return f"{run.name} ({state} {'since' if running else 'for'} {when:%Y-%m-%d %H:%M} UTC)"


def _reserve_fmp_calls(variable: str, report_day: str, wanted: int, budget: int) -> tuple[int, int]:
    """Reserve up to `wanted` FMP calls for `report_day`; return (reserved, used before).

    The count is written before any call is made. It starts at 0 for a new
    report day. A count for this day that is not a number allows nothing, so
    a broken value cannot reopen the budget. Raises when the Variable cannot
    be read or written.
    """
    logger = get_logger_safe(__name__)
    stored: Any = Variable.get(variable)
    used = 0
    if isinstance(stored, dict) and cast(dict[str, Any], stored).get("report_day") == report_day:
        count: Any = cast(dict[str, Any], stored).get("fmp_calls")
        if isinstance(count, int) and not isinstance(count, bool) and count >= 0:
            used = count
        else:
            logger.warning(f"Prefect Variable {variable!r} holds no FMP call count for {report_day}: {stored!r}")
            used = budget
    reserved = max(0, min(wanted, budget - used))
    if reserved:
        Variable.set(variable, {"report_day": report_day, "fmp_calls": used + reserved}, overwrite=True)
    return reserved, used


@flow(name="EPS Report Watch Flow")
def eps_report_watch_flow(
    fmp_block: FMPBlock,
    yahoo_block: YahooBlock,
    fmp_tn_block: TNAccessBlock,
    yahoo_tn_block: TNAccessBlock,
    truf_tn_block: TNAccessBlock,
    symbols: list[str] = MAG7,
    report_days_variable: str = REPORT_DAYS_VARIABLE,
    budget_variable: str = BUDGET_VARIABLE,
    fmp_budget_per_report_day: int = FMP_BUDGET_PER_REPORT_DAY,
    busy_deployments: Sequence[str] = (),
    busy_lookahead_minutes: int = BUSY_LOOKAHEAD_MINUTES,
) -> None:
    """Poll each symbol on its report evening and publish its print to TN.

    See the module docstring for the steps of a tick. `busy_deployments` are
    the names (without the flow name) of the deployments that share the
    watch's Yahoo cookie cache. The watch keeps two of its own ticks apart
    without being listed; this run never counts as busy.
    """
    logger = get_logger_safe(__name__)
    now = _utc_now()

    report_days = _read_report_days(report_days_variable)
    if report_days is None:
        return
    due = due_symbols(report_days, symbols, now)
    if not due:
        logger.info(f"No EPS report in its window at {now:%Y-%m-%d %H:%M} UTC")
        return
    logger.info(f"In their report window: {', '.join(f'{symbol} {day}' for symbol, day in due)}")

    try:
        busy = _busy_runs(busy_deployments, now, timedelta(minutes=busy_lookahead_minutes))
    except Exception as err:
        logger.warning(f"Tick skipped: the EPS runs could not be read: {err!r}")
        return
    if busy:
        logger.info(f"Tick skipped while another EPS run is busy or about to start: {', '.join(map(_describe, busy))}")
        return

    failed: dict[str, str] = {}
    pending: list[tuple[str, str]] = []
    for symbol, day in due:
        try:
            published = is_published(truf_tn_block, EPS_STREAM_IDS[symbol], date_string_to_unix(day))
        except Exception as err:
            logger.error(f"{symbol} {day}: consensus stream not read, not polled: {err!r}")
            failed[symbol] = repr(err)
            continue
        if published:
            logger.info(f"{symbol} {day}: already on the consensus stream")
        else:
            pending.append((symbol, day))

    to_poll: list[str] = []
    for report_day in dict.fromkeys(day for _, day in pending):  # one day: windows never overlap
        wanted = [symbol for symbol, day in pending if day == report_day]
        try:
            reserved, used = _reserve_fmp_calls(budget_variable, report_day, len(wanted), fmp_budget_per_report_day)
        except Exception as err:
            logger.error(f"{report_day}: FMP call count not kept in {budget_variable!r}, nothing polled: {err!r}")
            failed.update({symbol: repr(err) for symbol in wanted})
            continue
        logger.info(
            f"{report_day}: {used + reserved} of {fmp_budget_per_report_day} FMP calls used, "
            f"{reserved} of them reserved for this tick"
        )
        if reserved < len(wanted):
            logger.warning(f"{report_day}: FMP budget spent, not polling {', '.join(wanted[reserved:])}")
        to_poll.extend(wanted[:reserved])

    if to_poll:
        found = detect_symbols(
            detect_and_prepare_eps.with_options(retries=0),
            to_poll,
            fmp_block=fmp_block,
            yahoo_block=yahoo_block,
            fmp_tn_block=fmp_tn_block,
            yahoo_tn_block=yahoo_tn_block,
            truf_tn_block=truf_tn_block,
            fetch_income_statement=False,
        )
        failed.update(found.failed)
        publish_detected_rows(
            fmp_tn_block=fmp_tn_block,
            yahoo_tn_block=yahoo_tn_block,
            truf_tn_block=truf_tn_block,
            fmp_rows=found.fmp_rows,
            yahoo_rows=found.yahoo_rows,
            truf_rows=found.truf_rows,
        )

    if failed:
        raise RuntimeError(f"EPS report watch failed for {', '.join(failed)}: {failed}")
