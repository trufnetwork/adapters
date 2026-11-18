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
  3. Skip the tick while a run of `busy_deployments` is RUNNING and started
     less than BUSY_RUN_MAX_AGE ago. Two EPS processes at once break each
     other's Yahoo session: YahooBlock clears the process-wide yfinance cookie
     cache. A run stuck in RUNNING longer stops counting.
  4. Leave out a symbol whose consensus stream already holds the day.
  5. Count FMP calls per report day in a Prefect Variable (`budget_variable`),
     capped at `fmp_budget_per_report_day`; the FMP key is shared with other
     flows and allows 250 calls a day. Each poll is one FMP call: no income
     statement and no task retries. The count is written before polling, so
     a crash cannot under-count.
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
        busy_deployments=["eps-real-time-flow", "eps-report-watch"],
    )
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, time, timedelta, timezone
import os
from typing import Any, cast
from zoneinfo import ZoneInfo

from prefect import flow
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    DeploymentFilter,
    DeploymentFilterName,
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateType,
)
from prefect.client.schemas.objects import StateType
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

BUDGET_VARIABLE = "eps_watch_fmp_calls"
FMP_BUDGET_PER_REPORT_DAY = 100


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


def _busy_runs(deployments: Sequence[str], now: datetime) -> list[str]:
    """Runs of `deployments`, other than this one, RUNNING for less than BUSY_RUN_MAX_AGE."""
    logger = get_logger_safe(__name__)
    # This run when it is deployed directly, or the deployed run that called it.
    own_ids = {current_flow_run_id(), os.environ.get("PREFECT__FLOW_RUN_ID")}
    with get_client(sync_client=True) as client:
        runs = client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(type=FlowRunFilterStateType(any_=[StateType.RUNNING])),
            ),
            deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=list(deployments))),
        )
    busy: list[str] = []
    for run in runs:
        if str(run.id) in own_ids:
            continue
        if run.start_time is None or now - run.start_time >= BUSY_RUN_MAX_AGE:
            logger.warning(f"Flow run {run.name} has been RUNNING since {run.start_time}; not waiting for it")
            continue
        busy.append(f"{run.name} (started {run.start_time:%Y-%m-%d %H:%M} UTC)")
    return busy


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
) -> None:
    """Poll each symbol on its report evening and publish its print to TN.

    See the module docstring for the steps of a tick. `busy_deployments` are
    deployment names (without the flow name); this run never counts as busy,
    so the watch's own deployment can be listed to keep two ticks apart.
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

    if busy_deployments:
        try:
            busy = _busy_runs(busy_deployments, now)
        except Exception as err:
            logger.warning(f"Tick skipped: the runs of {list(busy_deployments)} could not be read: {err!r}")
            return
        if busy:
            logger.info(f"Tick skipped while another EPS run is busy: {', '.join(busy)}")
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
