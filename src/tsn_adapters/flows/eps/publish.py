"""Shared EPS publish path — dedup + fail-loud TN insert.

The EPS flows funnel their outgoing rows through here so that

1. duplicate (stream_id, date) rows are collapsed before the write. FMP
   occasionally repeats a fresh announcement row in its response (observed
   for GOOGL on 2026-07-22), and two records with the same primitive key
   inside one insert_records tx violate the stream's primary key and roll
   back the WHOLE batch — including every non-duplicate record riding in it.
2. insert failures fail the flow run. The SplitInsertResults returned by
   task_split_and_insert_records was previously discarded, so a rolled-back
   batch still logged "Published" and the loss was invisible (website#4362).
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from pandera.typing import DataFrame

from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.common.trufnetwork.tasks.insert import task_split_and_insert_records
from tsn_adapters.utils.logging import get_logger_safe

# One TN row as the EPS flows build it: stream_id, date (unix), value (str), data_provider.
EpsRow = dict[str, Any]


def publish_eps_records(block: TNAccessBlock, records: pd.DataFrame, label: str) -> int:
    """Insert EPS rows for one wallet, deduped and fail-loud.

    Returns the number of records submitted after deduplication. Raises
    RuntimeError when any record fails to insert, so the flow run reports
    the loss instead of logging success over a rollback.
    """
    logger = get_logger_safe(__name__)

    frame = records.drop_duplicates(subset=["stream_id", "date"], keep="first")
    dropped = len(records) - len(frame)
    if dropped:
        logger.warning(f"{label}: dropped {dropped} duplicate (stream_id, date) row(s) before insert")

    results = task_split_and_insert_records(
        block=block,
        records=DataFrame[TnDataRowModel](frame),
    )
    failed = results["failed_records"]
    if not failed.empty:
        raise RuntimeError(f"{label}: {len(failed)} record(s) failed to insert: {results['failed_reasons']}")

    logger.info(f"Published {len(frame)} record(s) to {label} streams")
    return len(frame)


def publish_detected_rows(
    *,
    fmp_tn_block: TNAccessBlock,
    yahoo_tn_block: TNAccessBlock,
    truf_tn_block: TNAccessBlock,
    fmp_rows: list[EpsRow],
    yahoo_rows: list[EpsRow],
    truf_rows: list[EpsRow],
) -> None:
    """Publish a real-time run's rows, each list with the wallet block that owns its streams.

    Shared by the real-time flow and the report watch. Raises like
    `publish_eps_records` when a record fails to insert.
    """
    logger = get_logger_safe(__name__)

    total = len(fmp_rows) + len(yahoo_rows) + len(truf_rows)
    if total == 0:
        logger.info("No new EPS records to publish")

    if fmp_rows:
        publish_eps_records(fmp_tn_block, pd.DataFrame(fmp_rows), "FMP")

    if yahoo_rows:
        publish_eps_records(yahoo_tn_block, pd.DataFrame(yahoo_rows), "Yahoo")

    if truf_rows:
        publish_eps_records(truf_tn_block, pd.DataFrame(truf_rows), "Truf consensus")
