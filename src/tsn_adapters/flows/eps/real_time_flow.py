"""EPS Real-Time Detection Flow.

Single-shot: polls FMP + Yahoo for the most recent quarters per Mag-7 ticker,
writes each source's raw value to its own primitive TN stream, runs the
two-source reconciler, and writes the settled value to the consensus stream.

Stream layout per ticker (spec §3):
  fmp_eps_<sym>_<conv>   — raw FMP ingestion (always written when new)
  yahoo_eps_<sym>_<conv> — raw Yahoo ingestion (written when Yahoo has the data)
  truf_eps_<sym>_<conv>  — consensus (written only when ≥2 sources agree)

Settlement logic (spec §3-4):
  SETTLED  — FMP and Yahoo agree within $0.01 → write consensus; exact figure,
             no averaging (primary source = FMP)
  PENDING  — only one source has reported → skip consensus; re-run later
  DISPUTED — sources disagree beyond tolerance → log warning; skip consensus;
             manual review required (spec §4)

Yahoo has two datasets: its JSON feed and its earnings page. The page can carry
a new figure hours before the feed, and the two can differ in basis, so the
Yahoo reading for a quarter is whichever of them matches FMP (`_pick_yahoo`).

A report still missing from the consensus stream at 06:00 UTC the day after
it (OVERDUE_AFTER) fails the run, after everything else is published, so it
shows hours before the market settles.

Idempotency: each stream is checked before writing (TN read-before-write).
Scheduling: external (Prefect, cron); this flow is single-shot.

Usage:
    from tsn_adapters.flows.eps.real_time_flow import eps_real_time_flow
    from tsn_adapters.blocks.fmp import FMPBlock
    from tsn_adapters.blocks.yahoo import YahooBlock
    from tsn_adapters.blocks.tn_access import TNAccessBlock

    eps_real_time_flow(
        fmp_block=FMPBlock.load("default"),
        yahoo_block=YahooBlock.load("default"),
        fmp_tn_block=TNAccessBlock.load("fmp-eps"),
        yahoo_tn_block=TNAccessBlock.load("yahoo-eps"),
        truf_tn_block=TNAccessBlock.load("truf-eps"),
    )

Each source identity owns a distinct TN wallet so the on-chain
`data_provider` field reflects who produced the data. Pass the same
block for all three when source identities share a wallet.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from prefect import flow, task

from tsn_adapters.blocks.fmp import EarningsData, FMPBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.blocks.yahoo import YahooBlock
from tsn_adapters.flows.eps.publish import publish_eps_records
from tsn_adapters.tasks.eps.config import EPS_STREAM_IDS, FMP_EPS_STREAM_IDS, MAG7, YAHOO_EPS_STREAM_IDS
from tsn_adapters.tasks.eps.reconciler import TOLERANCE, SourceReading, canonical_key, reconcile
from tsn_adapters.utils.create_empty_df import create_empty_df
from tsn_adapters.utils.logging import get_logger_safe
from tsn_adapters.utils.time_utils import date_string_to_unix

RECENT_QUARTERS = 4

# A company reports a quarter within this many days of its end (17 to 37 days
# for the Mag-7 in 2026), while the previous quarter ended about 90 days before
# that. Pairing FMP's announcement with a Yahoo quarter in this window cannot
# reach the wrong quarter.
MAX_REPORT_LAG_DAYS = 75

# Each market settles about 24 hours after its report, around 20:00 UTC the
# day after an after-close release. A report still missing from the consensus
# stream 30 hours after its day began (06:00 UTC that next day) fails the run,
# which leaves about 14 hours to step in. Past OVERDUE_UNTIL the market has
# settled and the report stops failing runs.
OVERDUE_AFTER = timedelta(hours=30)
OVERDUE_UNTIL = timedelta(days=3)

YahooReading = tuple[float, str]  # (eps, fiscal-period-end date)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _present(value: Any) -> bool:
    """True for a real value. None, NaN, pd.NA and '' are absent; bool(pd.NA) raises."""
    return value is not None and not pd.isna(value) and value != ""


def _readings_by_key(symbol: str, df: pd.DataFrame) -> dict[tuple[str, int, int], YahooReading]:
    """A Yahoo dataset's actuals, keyed by canonical quarter."""
    readings: dict[tuple[str, int, int], YahooReading] = {}
    for _, row in df.iterrows():
        raw = row.get("epsActual")
        if raw is None or pd.isna(raw):
            continue
        period_end = str(row["date"])
        readings[canonical_key(symbol, period_end)] = (float(raw), period_end)
    return readings


def _eps_of(readings: dict[tuple[str, int, int], YahooReading], key: tuple[str, int, int]) -> float | None:
    return readings[key][0] if key in readings else None


def _pick_yahoo(
    fmp_eps: float | None, feed: YahooReading | None, page: YahooReading | None
) -> tuple[YahooReading | None, str | None]:
    """Yahoo's reading for one quarter, and which dataset it came from.

    The reading that matches FMP wins, the JSON feed first. Without a match the
    feed's reading stands, as before the page was read. A page figure that
    matches nothing is left out: it may be on another basis than the stream
    (Meta Q3 2025: page 7.25 adjusted, feed 1.05 GAAP), and the Yahoo stream
    keeps the first value written for a date.
    """
    if fmp_eps is not None:
        for dataset, reading in (("feed", feed), ("page", page)):
            if reading is not None and round(abs(reading[0] - fmp_eps), 10) <= TOLERANCE:
                return reading, dataset
    if feed is not None:
        return feed, "feed"
    return None, None


def _recent_period_end(announcement_date: str, period_ends: list[date]) -> str | None:
    """The latest quarter end within MAX_REPORT_LAG_DAYS before an announcement."""
    announced = date.fromisoformat(announcement_date)
    candidates = [p for p in period_ends if 0 < (announced - p).days <= MAX_REPORT_LAG_DAYS]
    return max(candidates).isoformat() if candidates else None


def _is_published(tn_block: TNAccessBlock, stream_id: str, date_unix: int) -> bool:
    """Return True if a record for this exact date already exists in TN.

    TN's `get_records` gap-fills: a query for [date, date] also returns the
    latest record at-or-before `date` (the LOCF anchor), so a non-empty
    result only proves the stream has *some* earlier history — not that this
    date is published. Only an exact event-time match counts; treating the
    anchor as "published" made every date after a stream's first record read
    as a duplicate and silently no-op'd all publishes (website#4362).

    Raises on TN errors (fail-closed) — callers are retried by Prefect so a
    transient failure never silently breaks the read-before-write guarantee.
    """
    df = tn_block.read_records(
        stream_id=stream_id,
        date_from=date_unix,
        date_to=date_unix,
    )
    if df.empty:
        return False
    return bool((df["date"].astype("int64") == int(date_unix)).any())


@task(retries=3, retry_delay_seconds=30)
def detect_and_prepare_eps(
    fmp_block: FMPBlock,
    yahoo_block: YahooBlock,
    fmp_tn_block: TNAccessBlock,
    yahoo_tn_block: TNAccessBlock,
    truf_tn_block: TNAccessBlock,
    symbol: str,
) -> tuple[list[dict], list[dict], list[dict], list[str]]:
    """Detect new EPS prints for one symbol and return per-wallet TN rows.

    Returns a tuple `(fmp_rows, yahoo_rows, truf_rows, overdue)`. The first
    three lists are destined for the matching wallet's TN block:
    - fmp_rows  → FMP primitive stream (raw value, when FMP has the data)
    - yahoo_rows → Yahoo primitive stream (raw value, when Yahoo has the data)
    - truf_rows → consensus stream (exact primary-source value, when ≥2 agree)
    `overdue` names each report ("SYMBOL YYYY-MM-DD") that is still missing
    from the consensus stream past OVERDUE_AFTER and will not be written by
    this run.

    Read scoping: each stream's "already-published?" check uses the block
    that owns it, so the underlying `read_records` query resolves to the
    correct on-chain `data_provider`.

    Workset is the union of canonical event keys from both sources so a
    quarter reported by only one source is still processed. The canonical
    key (symbol, calendar_year, calendar_quarter) is derived from each
    source's fiscal-period-end so FMP rows (keyed by announcement date)
    and Yahoo rows (keyed by fiscal-period-end) reconcile on event identity
    rather than on a release-date artifact. See trufnetwork/website#3936 for
    the diagnosis.

    Per spec §3: readings are passed to the reconciler in source-priority
    order (fmp first) so the committed consensus value is FMP's exact
    figure.

    On-chain date semantics:
      - fmp_eps_* primitive stream: written under FMP's announcement date
      - yahoo_eps_* primitive stream: written under Yahoo's period-end date
      - truf_eps_* consensus stream: written under FMP's announcement date
        (preserves "consensus appears at announcement time" semantics)

    Primitive emission is decoupled from reconciliation: each ingestion
    loop emits its source's raw value as soon as fresh data is available,
    even when reconciliation can't yet run (e.g. FMP's income-statement
    hasn't been published for the current announcement). Only the
    consensus write is gated on the reconciler verdict.
    """
    logger = get_logger_safe(__name__)
    consensus_stream = EPS_STREAM_IDS[symbol]
    fmp_stream = FMP_EPS_STREAM_IDS[symbol]
    yahoo_stream = YAHOO_EPS_STREAM_IDS[symbol]

    earnings_df = fmp_block.get_historical_earnings(symbol, limit=RECENT_QUARTERS)
    yahoo_df = yahoo_block.get_historical_earnings(symbol, limit=RECENT_QUARTERS)
    # +2 keeps the lookup tolerant of fetch-window drift between earnings and
    # income-statement (income-statement may lag earnings by one filing).
    # Capped at 5: the current FMP plan rejects `limit` > 5 with HTTP 402.
    income_stmt_df = fmp_block.get_quarterly_income_statements(symbol, limit=min(RECENT_QUARTERS + 2, 5))
    # Yahoo's earnings page is an extra source: when it cannot be read, the run
    # goes on with the JSON feed alone, as it did before the page was read.
    feed_period_ends = [date.fromisoformat(str(d)[:10]) for d in yahoo_df["date"]]
    try:
        page_df = yahoo_block.get_page_earnings(symbol, feed_period_ends)
    except Exception as err:
        logger.warning(f"{symbol}: Yahoo's earnings page not read, using its JSON feed alone: {err!r}")
        page_df = create_empty_df(EarningsData)

    # Build filing_date → fiscal-period-end mapping from quarterly income
    # statements. For most tickers, `filingDate` matches FMP earnings'
    # `date` exactly. Some tickers (AAPL, GOOGL, AMZN, META, TSLA) are
    # off by ±1 day, so we index each filing under its date and both
    # neighbors.
    filing_to_period_end: dict[str, str] = {}
    for _, row in income_stmt_df.iterrows():
        fd = row.get("filingDate")
        pe = row.get("date")
        if _present(fd) and _present(pe):
            fd_date = date.fromisoformat(str(fd))
            for offset in (-1, 0, 1):
                key = (fd_date + timedelta(days=offset)).isoformat()
                filing_to_period_end.setdefault(key, str(pe))

    # Quarters Yahoo already has an actual for, in its feed or on its page. When
    # the income statement has no row for an announcement yet (for TSLA it comes
    # with the 10-Q, the day after the release), FMP's reading pairs with the
    # Yahoo quarter that ended shortly before the announcement.
    yahoo_period_ends = sorted(
        {
            date.fromisoformat(str(r["date"])[:10])
            for df in (yahoo_df, page_df)
            for _, r in df.iterrows()
            if _present(r.get("epsActual"))
        }
    )

    fmp_rows: list[dict] = []
    yahoo_rows: list[dict] = []
    truf_rows: list[dict] = []

    # FMP ingestion. Always emit the raw primitive when fresh; populate the
    # canonical-key dict only when income-statement provides the period-end.
    # Decoupling lets the fmp_eps_* stream stay faithful to FMP's announcement
    # even during the rare race window where the income-statement filing
    # hasn't landed yet — reconciliation simply defers to the next cycle.
    fmp_by_key: dict[tuple[str, int, int], tuple[float, str, str]] = {}
    for _, row in earnings_df.iterrows():
        if pd.isna(row.get("epsActual")):
            continue
        announcement_date = str(row["date"])
        eps = float(row["epsActual"])
        last_updated = row.get("lastUpdated")
        retrieved_at = str(last_updated) if _present(last_updated) else datetime.now(timezone.utc).isoformat()

        # Emit primitive (always, when FMP has data and the date isn't published)
        fmp_date_unix = date_string_to_unix(announcement_date)
        if not _is_published(fmp_tn_block, fmp_stream, fmp_date_unix):
            fmp_rows.append(
                {
                    "stream_id": fmp_stream,
                    "date": fmp_date_unix,
                    "value": str(eps),
                    "data_provider": None,
                }
            )

        # Populate canonical-key dict for reconciliation. The income statement
        # gives the period end; without it, a Yahoo quarter that ended shortly
        # before the announcement does. With neither, reconciliation retries
        # next cycle.
        period_end = filing_to_period_end.get(announcement_date)
        if period_end is None:
            period_end = _recent_period_end(announcement_date, yahoo_period_ends)
            if period_end is None:
                logger.info(
                    f"{symbol} {announcement_date}: income-statement not yet "
                    f"published for this filing and no Yahoo quarter ended in the "
                    f"{MAX_REPORT_LAG_DAYS} days before it; primitive emitted, "
                    f"reconciliation deferred to next cycle"
                )
                continue
            logger.info(
                f"{symbol} {announcement_date}: income-statement not yet published "
                f"for this filing; paired with Yahoo's quarter ending {period_end}"
            )
        fmp_by_key[canonical_key(symbol, period_end)] = (eps, announcement_date, retrieved_at)

    # Yahoo ingestion. The block dates both of its datasets by fiscal-period-end
    # (the page's announcement date is resolved to the feed's period-end), so
    # the feed and the page meet on the canonical key. Per quarter, the reading
    # `_pick_yahoo` keeps is the one emitted and reconciled.
    feed_by_key = _readings_by_key(symbol, yahoo_df)
    # The page lists about six years of quarters. Only those FMP or the feed
    # also has can ever be matched, so the rest are left out.
    page_by_key = {
        key: reading
        for key, reading in _readings_by_key(symbol, page_df).items()
        if key in fmp_by_key or key in feed_by_key
    }
    yahoo_by_key: dict[tuple[str, int, int], YahooReading] = {}
    yahoo_dataset: dict[tuple[str, int, int], str] = {}
    for key in sorted(set(feed_by_key) | set(page_by_key)):
        fmp_eps = fmp_by_key[key][0] if key in fmp_by_key else None
        reading, dataset = _pick_yahoo(fmp_eps, feed_by_key.get(key), page_by_key.get(key))
        if reading is None or dataset is None:
            continue  # a page figure FMP does not match; the pending line below shows both
        eps, yahoo_date = reading

        # Emit primitive
        yahoo_date_unix = date_string_to_unix(yahoo_date)
        if not _is_published(yahoo_tn_block, yahoo_stream, yahoo_date_unix):
            yahoo_rows.append(
                {
                    "stream_id": yahoo_stream,
                    "date": yahoo_date_unix,
                    "value": str(eps),
                    "data_provider": None,
                }
            )
        yahoo_by_key[key] = reading
        yahoo_dataset[key] = dataset

    # Reconciliation loop. Primitive writes were emitted above during
    # ingestion; this loop only decides each event's consensus verdict.
    # Iterate the union of canonical keys so single-source quarters are
    # still surfaced (they resolve as `pending`).
    for key in sorted(set(fmp_by_key) | set(yahoo_by_key)):
        fmp_data = fmp_by_key.get(key)
        yahoo_data = yahoo_by_key.get(key)

        # Consensus stream is keyed by FMP announcement date when available
        # (preserves the "consensus appears at announcement time" semantic
        # that downstream markets rely on). Fall back to Yahoo's date when
        # only Yahoo has reported — that branch resolves as `pending` from
        # the reconciler anyway.
        consensus_date_str = fmp_data[1] if fmp_data else yahoo_data[1]
        consensus_date_unix = date_string_to_unix(consensus_date_str)

        # Skip if consensus is already committed
        if _is_published(truf_tn_block, consensus_stream, consensus_date_unix):
            continue

        # Build readings in priority order (spec §3: fmp first)
        readings: list[SourceReading] = []
        fmp_eps: float | None = None
        yahoo_eps: float | None = None

        if fmp_data is not None:
            fmp_eps, _, fmp_retrieved_at = fmp_data
            readings.append(
                SourceReading(
                    source="fmp",
                    eps_actual=fmp_eps,
                    retrieved_at=fmp_retrieved_at,
                )
            )

        if yahoo_data is not None:
            yahoo_eps, _ = yahoo_data
            readings.append(
                SourceReading(
                    source="yahoo",
                    eps_actual=yahoo_eps,
                    retrieved_at=datetime.now(timezone.utc).isoformat(),
                )
            )

        result = reconcile(readings)

        if result.status == "settled":
            truf_rows.append(
                {
                    "stream_id": consensus_stream,
                    "date": consensus_date_unix,
                    "value": str(result.value),  # exact primary-source figure
                    "data_provider": None,
                }
            )
            logger.info(
                f"{symbol} {key}: settled at {result.value} "
                f"(agreed: {result.sources_agree}, yahoo from its {yahoo_dataset.get(key)}, "
                f"consensus_date={consensus_date_str})"
            )
        elif result.status == "disputed":
            logger.warning(
                f"{symbol} {key}: DISPUTED — fmp={fmp_eps}, yahoo feed={_eps_of(feed_by_key, key)}, "
                f"yahoo page={_eps_of(page_by_key, key)}; manual review required"
            )
        else:
            logger.info(
                f"{symbol} {key}: pending — {len(readings)} source(s) available, need ≥2 to settle "
                f"(fmp={fmp_eps}, yahoo feed={_eps_of(feed_by_key, key)}, yahoo page={_eps_of(page_by_key, key)})"
            )

    # A report still missing past OVERDUE_AFTER fails the run (see
    # eps_real_time_flow), whatever holds it: one source, a dispute, or none
    # yet. FMP lists a report's day before it has the figure, so its rows
    # without an actual count too.
    now = _utc_now()
    settling = {row["date"] for row in truf_rows}
    overdue: list[str] = []
    for report_day in sorted({str(d) for d in earnings_df["date"]}):
        day_start = datetime.fromisoformat(report_day).replace(tzinfo=timezone.utc)
        if not day_start + OVERDUE_AFTER <= now < day_start + OVERDUE_UNTIL:
            continue
        report_unix = date_string_to_unix(report_day)
        if report_unix in settling or _is_published(truf_tn_block, consensus_stream, report_unix):
            continue
        logger.error(
            f"{symbol} {report_day}: not on the consensus stream at {now:%Y-%m-%d %H:%M} UTC; "
            f"its market settles on the previous quarter's value unless the figure prints first"
        )
        overdue.append(f"{symbol} {report_day}")

    return fmp_rows, yahoo_rows, truf_rows, overdue


@flow(name="EPS Real-Time Detection Flow")
def eps_real_time_flow(
    fmp_block: FMPBlock,
    yahoo_block: YahooBlock,
    fmp_tn_block: TNAccessBlock,
    yahoo_tn_block: TNAccessBlock,
    truf_tn_block: TNAccessBlock,
    symbols: list[str] = MAG7,
) -> None:
    """Detect and publish Mag-7 EPS prints to TN.

    Writes raw values to per-source primitive streams and, when ≥2 sources
    agree within $0.01, commits the consensus value to the truf_eps stream.
    Each destination uses its own wallet block so the on-chain `data_provider`
    reflects the identity that produced the data. Single-shot — driven by
    external scheduler.

    Fails the run, after publishing, when a symbol fails or a report is still
    missing from the consensus stream past OVERDUE_AFTER.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"EPS real-time detection starting for {symbols}")

    fmp_rows: list[dict] = []
    yahoo_rows: list[dict] = []
    truf_rows: list[dict] = []
    failed: dict[str, str] = {}
    overdue: list[str] = []

    for symbol in symbols:
        # One symbol's failure must not drop the others' prints: the run used
        # to end at the first failing symbol, before anything was published.
        try:
            fmp_new, yahoo_new, truf_new, overdue_new = detect_and_prepare_eps(
                fmp_block=fmp_block,
                yahoo_block=yahoo_block,
                fmp_tn_block=fmp_tn_block,
                yahoo_tn_block=yahoo_tn_block,
                truf_tn_block=truf_tn_block,
                symbol=symbol,
            )
        except Exception as err:
            logger.error(f"{symbol}: EPS detection failed, continuing with the other symbols: {err!r}")
            failed[symbol] = repr(err)
            continue
        fmp_rows.extend(fmp_new)
        yahoo_rows.extend(yahoo_new)
        truf_rows.extend(truf_new)
        overdue.extend(overdue_new)

    total = len(fmp_rows) + len(yahoo_rows) + len(truf_rows)
    if total == 0:
        logger.info("No new EPS records to publish")

    if fmp_rows:
        publish_eps_records(fmp_tn_block, pd.DataFrame(fmp_rows), "FMP")

    if yahoo_rows:
        publish_eps_records(yahoo_tn_block, pd.DataFrame(yahoo_rows), "Yahoo")

    if truf_rows:
        publish_eps_records(truf_tn_block, pd.DataFrame(truf_rows), "Truf consensus")

    # Fail the run after publishing, so the other symbols' prints still land
    # and the failure stays visible in the run state.
    problems: list[str] = []
    if failed:
        problems.append(f"EPS detection failed for {', '.join(failed)}: {failed}")
    if overdue:
        problems.append(f"EPS print not on the consensus stream yet for {', '.join(overdue)}")
    if problems:
        raise RuntimeError("; ".join(problems))


if __name__ == "__main__":
    import asyncio

    from tsn_adapters.utils import deroutine

    async def main() -> None:
        fmp_block = deroutine(FMPBlock.load("default"))
        yahoo_block = deroutine(YahooBlock.load("default"))
        fmp_tn_block = deroutine(TNAccessBlock.load("fmp-eps"))
        yahoo_tn_block = deroutine(TNAccessBlock.load("yahoo-eps"))
        truf_tn_block = deroutine(TNAccessBlock.load("truf-eps"))
        eps_real_time_flow(
            fmp_block=fmp_block,
            yahoo_block=yahoo_block,
            fmp_tn_block=fmp_tn_block,
            yahoo_tn_block=yahoo_tn_block,
            truf_tn_block=truf_tn_block,
        )

    asyncio.run(main())
