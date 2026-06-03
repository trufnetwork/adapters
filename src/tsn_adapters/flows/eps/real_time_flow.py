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

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow, task

from tsn_adapters.blocks.fmp import FMPBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.blocks.yahoo import YahooBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.common.trufnetwork.tasks.insert import task_split_and_insert_records
from tsn_adapters.tasks.eps.config import (
    EPS_STREAM_IDS,
    FMP_EPS_STREAM_IDS,
    MAG7,
    YAHOO_EPS_STREAM_IDS,
)
from tsn_adapters.tasks.eps.reconciler import SourceReading, canonical_key, reconcile
from tsn_adapters.utils.logging import get_logger_safe
from tsn_adapters.utils.time_utils import date_string_to_unix

RECENT_QUARTERS = 4


def _is_published(tn_block: TNAccessBlock, stream_id: str, date_unix: int) -> bool:
    """Return True if a record for this exact date already exists in TN.

    Raises on TN errors (fail-closed) — callers are retried by Prefect so a
    transient failure never silently breaks the read-before-write guarantee.
    """
    df = tn_block.read_records(
        stream_id=stream_id,
        date_from=date_unix,
        date_to=date_unix,
    )
    return not df.empty


@task(retries=3, retry_delay_seconds=30)
def detect_and_prepare_eps(
    fmp_block: FMPBlock,
    yahoo_block: YahooBlock,
    fmp_tn_block: TNAccessBlock,
    yahoo_tn_block: TNAccessBlock,
    truf_tn_block: TNAccessBlock,
    symbol: str,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Detect new EPS prints for one symbol and return per-wallet TN rows.

    Returns a tuple `(fmp_rows, yahoo_rows, truf_rows)` where each list is
    destined for the matching wallet's TN block:
    - fmp_rows  → FMP primitive stream (raw value, when FMP has the data)
    - yahoo_rows → Yahoo primitive stream (raw value, when Yahoo has the data)
    - truf_rows → consensus stream (exact primary-source value, when ≥2 agree)

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
    income_stmt_df = fmp_block.get_quarterly_income_statements(symbol, limit=RECENT_QUARTERS + 2)

    # Build filing_date → fiscal-period-end mapping from quarterly income
    # statements. For most tickers, `filingDate` matches FMP earnings'
    # `date` exactly. Some tickers (AAPL, GOOGL, AMZN, META, TSLA) are
    # off by ±1 day, so we index each filing under its date and both
    # neighbors.
    filing_to_period_end: dict[str, str] = {}
    for _, row in income_stmt_df.iterrows():
        fd = row.get("filingDate")
        pe = row.get("date")
        if fd and pe and not pd.isna(fd) and not pd.isna(pe):
            fd_date = date.fromisoformat(str(fd))
            for offset in (-1, 0, 1):
                key = (fd_date + timedelta(days=offset)).isoformat()
                filing_to_period_end.setdefault(key, str(pe))

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
        retrieved_at = str(row.get("lastUpdated") or datetime.now(timezone.utc).isoformat())

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

        # Populate canonical-key dict for reconciliation, only when income-
        # statement lookup resolves. Missing income-statement is the rare
        # race-at-announcement window; reconciliation retries next cycle.
        period_end = filing_to_period_end.get(announcement_date)
        if period_end is None:
            logger.info(
                f"{symbol} {announcement_date}: income-statement not yet "
                f"published for this filing; primitive emitted, "
                f"reconciliation deferred to next cycle"
            )
            continue
        fmp_by_key[canonical_key(symbol, period_end)] = (eps, announcement_date, retrieved_at)

    # Yahoo ingestion. The block normalizes both of its accessors to a
    # fiscal-period-end `date` (the scrape's announcement date is resolved to
    # the JSON accessor's period-end), so no auxiliary lookup is needed here —
    # primitive emission and canonical-key population happen together.
    yahoo_by_key: dict[tuple[str, int, int], tuple[float, str]] = {}
    for _, row in yahoo_df.iterrows():
        raw = row.get("epsActual")
        if raw is None or pd.isna(raw):
            continue
        yahoo_date = str(row["date"])
        eps = float(raw)

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
        yahoo_by_key[canonical_key(symbol, yahoo_date)] = (eps, yahoo_date)

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
                f"(agreed: {result.sources_agree}, "
                f"consensus_date={consensus_date_str})"
            )
        elif result.status == "disputed":
            logger.warning(f"{symbol} {key}: DISPUTED — " f"fmp={fmp_eps}, yahoo={yahoo_eps}; manual review required")
        else:
            logger.info(f"{symbol} {key}: pending — " f"{len(readings)} source(s) available, need ≥2 to settle")

    return fmp_rows, yahoo_rows, truf_rows


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
    """
    logger = get_logger_safe(__name__)
    logger.info(f"EPS real-time detection starting for {symbols}")

    fmp_rows: list[dict] = []
    yahoo_rows: list[dict] = []
    truf_rows: list[dict] = []

    for symbol in symbols:
        fmp_new, yahoo_new, truf_new = detect_and_prepare_eps(
            fmp_block=fmp_block,
            yahoo_block=yahoo_block,
            fmp_tn_block=fmp_tn_block,
            yahoo_tn_block=yahoo_tn_block,
            truf_tn_block=truf_tn_block,
            symbol=symbol,
        )
        fmp_rows.extend(fmp_new)
        yahoo_rows.extend(yahoo_new)
        truf_rows.extend(truf_new)

    total = len(fmp_rows) + len(yahoo_rows) + len(truf_rows)
    if total == 0:
        logger.info("No new EPS records to publish")
        return

    if fmp_rows:
        task_split_and_insert_records(
            block=fmp_tn_block,
            records=DataFrame[TnDataRowModel](pd.DataFrame(fmp_rows)),
        )
        logger.info(f"Published {len(fmp_rows)} record(s) to FMP streams")

    if yahoo_rows:
        task_split_and_insert_records(
            block=yahoo_tn_block,
            records=DataFrame[TnDataRowModel](pd.DataFrame(yahoo_rows)),
        )
        logger.info(f"Published {len(yahoo_rows)} record(s) to Yahoo streams")

    if truf_rows:
        task_split_and_insert_records(
            block=truf_tn_block,
            records=DataFrame[TnDataRowModel](pd.DataFrame(truf_rows)),
        )
        logger.info(f"Published {len(truf_rows)} record(s) to Truf consensus streams")


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
