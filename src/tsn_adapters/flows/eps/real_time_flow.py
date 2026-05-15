"""EPS Real-Time Detection Flow.

Single-shot: polls FMP + Yahoo for the most recent quarters per Mag-7 ticker,
writes each source's raw value to its own primitive TN stream, runs the
two-source reconciler, and writes the settled value to the consensus stream.

Stream layout per ticker (spec §3):
  fmp_eps_<sym>_<conv>   — raw FMP ingestion (always written when new)
  yahoo_eps_<sym>_<conv> — raw Yahoo ingestion (written when Yahoo has the data)
  truf_eps_<sym>_<conv>  — consensus (written only when ≥2 sources agree)

Settlement logic (spec §3–4):
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
        tn_block=TNAccessBlock.load("default"),
    )
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow, task

from tsn_adapters.blocks.fmp import FMPBlock
from tsn_adapters.blocks.yahoo import YahooBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.common.trufnetwork.tasks.insert import task_split_and_insert_records
from tsn_adapters.tasks.eps.config import (
    EPS_STREAM_IDS,
    FMP_EPS_STREAM_IDS,
    MAG7,
    YAHOO_EPS_STREAM_IDS,
)
from tsn_adapters.tasks.eps.reconciler import SourceReading, reconcile
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
    tn_block: TNAccessBlock,
    symbol: str,
) -> list[dict]:
    """Detect new EPS prints for one symbol and return all TN rows to insert.

    Returns records for:
    - the FMP primitive stream (raw value, when FMP has the data)
    - the Yahoo primitive stream (raw value, when Yahoo has the data)
    - the consensus stream (exact primary-source value, when ≥2 agree)

    Workset is the union of quarter dates from both sources so a quarter
    reported by only one source is still processed. Per spec §3: readings
    are passed to the reconciler in source-priority order (fmp first) so
    the committed consensus value is FMP's exact figure.
    """
    logger = get_logger_safe(__name__)
    consensus_stream = EPS_STREAM_IDS[symbol]
    fmp_stream = FMP_EPS_STREAM_IDS[symbol]
    yahoo_stream = YAHOO_EPS_STREAM_IDS[symbol]

    earnings_df = fmp_block.get_historical_earnings(symbol, limit=RECENT_QUARTERS)
    yahoo_df = yahoo_block.get_historical_earnings(symbol, limit=RECENT_QUARTERS)

    # Build per-date lookup dicts for O(1) access inside the loop
    fmp_by_date: dict[str, float] = {}
    fmp_retrieved: dict[str, str] = {}
    for _, row in earnings_df.iterrows():
        if not pd.isna(row.get("epsActual")):
            d = str(row["date"])
            fmp_by_date[d] = float(row["epsActual"])
            fmp_retrieved[d] = str(row.get("lastUpdated") or datetime.now(timezone.utc).isoformat())

    yahoo_by_date: dict[str, float] = {}
    for _, row in yahoo_df.iterrows():
        raw = row.get("epsActual")
        if raw is not None and not pd.isna(raw):
            yahoo_by_date[str(row["date"])] = float(raw)

    # Union of all quarter dates with at least one source reporting
    all_dates = sorted(set(fmp_by_date) | set(yahoo_by_date))

    records: list[dict] = []

    for date_str in all_dates:
        date_unix = date_string_to_unix(date_str)

        # Skip entirely if consensus is already committed
        if _is_published(tn_block, consensus_stream, date_unix):
            continue

        # Build readings in priority order (spec §3: fmp first)
        readings: list[SourceReading] = []

        fmp_eps = fmp_by_date.get(date_str)
        if fmp_eps is not None:
            if not _is_published(tn_block, fmp_stream, date_unix):
                records.append({
                    "stream_id": fmp_stream,
                    "date": date_unix,
                    "value": str(fmp_eps),
                    "data_provider": None,
                })
            readings.append(
                SourceReading(
                    source="fmp",
                    eps_actual=fmp_eps,
                    retrieved_at=fmp_retrieved[date_str],
                )
            )

        yahoo_eps = yahoo_by_date.get(date_str)
        if yahoo_eps is not None:
            if not _is_published(tn_block, yahoo_stream, date_unix):
                records.append({
                    "stream_id": yahoo_stream,
                    "date": date_unix,
                    "value": str(yahoo_eps),
                    "data_provider": None,
                })
            readings.append(
                SourceReading(
                    source="yahoo",
                    eps_actual=yahoo_eps,
                    retrieved_at=datetime.now(timezone.utc).isoformat(),
                )
            )

        # --- Reconcile & write consensus ---
        result = reconcile(readings)

        if result.status == "settled":
            records.append({
                "stream_id": consensus_stream,
                "date": date_unix,
                "value": str(result.value),  # exact primary-source figure
                "data_provider": None,
            })
            logger.info(
                f"{symbol} {date_str}: settled at {result.value} "
                f"(agreed: {result.sources_agree})"
            )
        elif result.status == "disputed":
            logger.warning(
                f"{symbol} {date_str}: DISPUTED — "
                f"fmp={fmp_eps}, yahoo={yahoo_eps}; manual review required"
            )
        else:
            logger.info(
                f"{symbol} {date_str}: pending — "
                f"{len(readings)} source(s) available, need ≥2 to settle"
            )

    return records


@flow(name="EPS Real-Time Detection Flow")
def eps_real_time_flow(
    fmp_block: FMPBlock,
    yahoo_block: YahooBlock,
    tn_block: TNAccessBlock,
    symbols: list[str] = MAG7,
) -> None:
    """Detect and publish Mag-7 EPS prints to TN.

    Writes raw values to per-source primitive streams and, when ≥2 sources
    agree within $0.01, commits the consensus value to the truf_eps stream.
    Single-shot — driven by external scheduler.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"EPS real-time detection starting for {symbols}")

    all_rows: list[dict] = []
    for symbol in symbols:
        new_rows = detect_and_prepare_eps(
            fmp_block=fmp_block,
            yahoo_block=yahoo_block,
            tn_block=tn_block,
            symbol=symbol,
        )
        all_rows.extend(new_rows)

    if not all_rows:
        logger.info("No new EPS records to publish")
        return

    records_df = DataFrame[TnDataRowModel](pd.DataFrame(all_rows))
    task_split_and_insert_records(block=tn_block, records=records_df)
    logger.info(f"Published {len(all_rows)} EPS record(s) across source + consensus streams")


if __name__ == "__main__":
    import asyncio
    from tsn_adapters.utils import deroutine

    async def main() -> None:
        fmp_block = deroutine(FMPBlock.load("default"))
        yahoo_block = deroutine(YahooBlock.load("default"))
        tn_block = deroutine(TNAccessBlock.load("default"))
        eps_real_time_flow(fmp_block=fmp_block, yahoo_block=yahoo_block, tn_block=tn_block)

    asyncio.run(main())
