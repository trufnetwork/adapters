"""EPS Historical Backfill Flow.

Fetches all available quarterly EPS prints per Mag-7 ticker from both FMP
(/stable/earnings) and Yahoo Finance (yfinance.Ticker.get_earnings_dates) and
inserts them into the per-source primitive streams on TN.

Per spec §3: each source feeds its own dedicated primitive stream.
The consensus (truf_eps) stream is populated by the real-time flow once
≥2 independent sources agree.

Usage (single-shot, idempotent — already-published dates are skipped):

    from tsn_adapters.flows.eps.historical_flow import eps_historical_flow
    from tsn_adapters.blocks.fmp import FMPBlock
    from tsn_adapters.blocks.yahoo import YahooBlock
    from tsn_adapters.blocks.tn_access import TNAccessBlock

    eps_historical_flow(
        fmp_block=FMPBlock.load("default"),
        yahoo_block=YahooBlock.load("default"),
        fmp_tn_block=TNAccessBlock.load("fmp-eps"),
        yahoo_tn_block=TNAccessBlock.load("yahoo-eps"),
    )

`fmp_tn_block` and `yahoo_tn_block` may point at the same TN wallet
when source identities are not split.
"""
from __future__ import annotations

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow, task

from tsn_adapters.blocks.fmp import EarningsData, FMPBlock
from tsn_adapters.blocks.yahoo import YahooBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.common.trufnetwork.tasks.insert import task_split_and_insert_records
from tsn_adapters.tasks.eps.config import FMP_EPS_STREAM_IDS, MAG7, YAHOO_EPS_STREAM_IDS
from tsn_adapters.utils.logging import get_logger_safe
from tsn_adapters.utils.time_utils import date_string_to_unix

# FMP earnings endpoint returns up to `limit` quarters. 50 covers ~12.5 years.
EARNINGS_LIMIT = 50


@task(retries=3, retry_delay_seconds=30)
def fetch_fmp_earnings(fmp_block: FMPBlock, symbol: str) -> DataFrame[EarningsData]:
    return fmp_block.get_historical_earnings(symbol, limit=EARNINGS_LIMIT)


@task(retries=3, retry_delay_seconds=30)
def fetch_yahoo_earnings(yahoo_block: YahooBlock, symbol: str) -> DataFrame[EarningsData]:
    return yahoo_block.get_historical_earnings(symbol, limit=EARNINGS_LIMIT)


@task(retries=3, retry_delay_seconds=30)
def read_published_dates(tn_block: TNAccessBlock, stream_id: str) -> set[int]:
    """Return unix timestamps already in TN for the given stream.

    No date filter — covers the full history returned by EARNINGS_LIMIT so
    quarters fetched before BACKFILL_START are not re-inserted on reruns.
    Raises on TN errors (fail-closed); Prefect retries handle transient faults.
    """
    df = tn_block.read_records(stream_id=stream_id)
    return set(df["date"].tolist())


@task
def build_new_records(
    earnings_df: DataFrame[EarningsData],
    stream_id: str,
    published_dates: set[int],
) -> DataFrame[TnDataRowModel]:
    """Filter to unpublished quarters and convert to TN row format."""
    _empty = DataFrame[TnDataRowModel](
        pd.DataFrame(columns=["stream_id", "date", "value", "data_provider"])
    )
    if earnings_df.empty:
        return _empty

    rows = []
    for _, row in earnings_df.iterrows():
        if pd.isna(row.get("epsActual")):
            continue
        date_unix = date_string_to_unix(str(row["date"]))
        if date_unix in published_dates:
            continue
        rows.append({
            "stream_id": stream_id,
            "date": date_unix,
            "value": str(float(row["epsActual"])),
            "data_provider": None,
        })

    if not rows:
        return _empty
    return DataFrame[TnDataRowModel](pd.DataFrame(rows))


@flow(name="EPS Historical Backfill Flow")
def eps_historical_flow(
    fmp_block: FMPBlock,
    yahoo_block: YahooBlock,
    fmp_tn_block: TNAccessBlock,
    yahoo_tn_block: TNAccessBlock,
    symbols: list[str] = MAG7,
) -> None:
    """Backfill Mag-7 historical EPS into FMP and Yahoo primitive streams on TN.

    Each source writes via its own TN block: `fmp_tn_block` owns the
    `fmp_eps_*` streams, `yahoo_tn_block` owns the `yahoo_eps_*` streams.
    Pass the same block for both when source identities share a wallet.

    Safe to re-run: already-published dates are skipped. Both the FMP and Yahoo
    primitive streams must be deployed before running.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"Starting EPS historical backfill for {symbols}")

    fmp_records = pd.DataFrame(columns=["stream_id", "date", "value", "data_provider"])
    yahoo_records = pd.DataFrame(columns=["stream_id", "date", "value", "data_provider"])

    for symbol in symbols:
        fmp_stream_id = FMP_EPS_STREAM_IDS[symbol]
        yahoo_stream_id = YAHOO_EPS_STREAM_IDS[symbol]

        # --- FMP source ---
        fmp_earnings = fetch_fmp_earnings(fmp_block=fmp_block, symbol=symbol)
        fmp_published = read_published_dates(tn_block=fmp_tn_block, stream_id=fmp_stream_id)
        fmp_new = build_new_records(
            earnings_df=fmp_earnings,
            stream_id=fmp_stream_id,
            published_dates=fmp_published,
        )
        logger.info(f"{symbol}: {len(fmp_new)} new FMP record(s)")

        # --- Yahoo source ---
        yahoo_earnings = fetch_yahoo_earnings(yahoo_block=yahoo_block, symbol=symbol)
        yahoo_published = read_published_dates(tn_block=yahoo_tn_block, stream_id=yahoo_stream_id)
        yahoo_new = build_new_records(
            earnings_df=yahoo_earnings,
            stream_id=yahoo_stream_id,
            published_dates=yahoo_published,
        )
        logger.info(f"{symbol}: {len(yahoo_new)} new Yahoo record(s)")

        fmp_records = pd.concat([fmp_records, fmp_new], ignore_index=True)
        yahoo_records = pd.concat([yahoo_records, yahoo_new], ignore_index=True)

    if fmp_records.empty and yahoo_records.empty:
        logger.info("No new EPS records to insert — backfill already up-to-date")
        return

    if not fmp_records.empty:
        task_split_and_insert_records(
            block=fmp_tn_block,
            records=DataFrame[TnDataRowModel](fmp_records),
        )
        logger.info(f"Inserted {len(fmp_records)} EPS records into FMP streams")

    if not yahoo_records.empty:
        task_split_and_insert_records(
            block=yahoo_tn_block,
            records=DataFrame[TnDataRowModel](yahoo_records),
        )
        logger.info(f"Inserted {len(yahoo_records)} EPS records into Yahoo streams")


if __name__ == "__main__":
    import asyncio
    from tsn_adapters.utils import deroutine

    async def main() -> None:
        fmp_block = deroutine(FMPBlock.load("default"))
        yahoo_block = deroutine(YahooBlock.load("default"))
        fmp_tn_block = deroutine(TNAccessBlock.load("fmp-eps"))
        yahoo_tn_block = deroutine(TNAccessBlock.load("yahoo-eps"))
        eps_historical_flow(
            fmp_block=fmp_block,
            yahoo_block=yahoo_block,
            fmp_tn_block=fmp_tn_block,
            yahoo_tn_block=yahoo_tn_block,
        )

    asyncio.run(main())
