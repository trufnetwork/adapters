"""Verifies the bulk_insert_tn_records resume loop: lock scope (the
tn-write slot must NOT be held across the inter-attempt sleep), correct
slicing on failed_chunk_index, cumulative tx_hash propagation, and the
drain_failure short-circuit.
"""
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest
from pandera.typing import DataFrame
from trufnetwork_sdk_py import BulkInsertError

from tests.utils.fake_tn_access import FakeTNAccessBlock
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel


class _ResumeBlock(FakeTNAccessBlock):
    """FakeTNAccessBlock re-binds bulk_insert_tn_records to the production
    implementation (the parent override bypasses the resume loop entirely
    by calling batch_insert per chunk)."""

    bulk_insert_tn_records = TNAccessBlock.bulk_insert_tn_records


def _make_df(n: int) -> DataFrame[TnDataRowModel]:
    """n rows, unix-timestamp dates, numeric string values."""
    df = pd.DataFrame({
        "stream_id": [f"st_{i}" for i in range(n)],
        "data_provider": ["fake_provider"] * n,
        "date": [1700000000 + i for i in range(n)],
        "value": [str(i + 1) for i in range(n)],  # all non-zero, won't be filtered
    })
    return DataFrame[TnDataRowModel](df)


@contextmanager
def _record_lock(events: list[str], name: str = "lock"):
    """Stand-in for prefect.concurrency that records enter/exit ordering."""
    events.append(f"{name}:enter")
    try:
        yield
    finally:
        events.append(f"{name}:exit")


def test_lock_is_released_across_resume_sleep():
    """Primary fix verification: the tn-write slot must be released before
    time.sleep() so other writers aren't starved during backoff. Required
    event order is enter -> insert -> exit -> sleep -> enter -> insert -> exit,
    NEVER ...insert -> sleep -> insert... inside one lock span."""
    block = _ResumeBlock()
    events: list[str] = []

    # First insert_all fails after committing chunk 0 (failed_chunk_index=1),
    # second succeeds for the remaining slice.
    call_count = {"n": 0}

    class _FakeBulkInserter:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def insert_all(self, batches: list[dict[str, Any]]) -> list[str]:
            call_count["n"] += 1
            events.append(f"insert_all(n={len(batches)})")
            if call_count["n"] == 1:
                raise BulkInsertError(
                    "simulated transient backend error",
                    tx_hashes=["tx_chunk0"],
                    drain_failure=False,
                    failed_chunk_index=1,
                )
            return ["tx_resume_a", "tx_resume_b"]

    def _fake_concurrency(*args: Any, **kwargs: Any):
        return _record_lock(events, name="tn-write")

    def _fake_sleep(_: float) -> None:
        events.append("sleep")

    with (
        patch("tsn_adapters.blocks.tn_access.BulkInserter", _FakeBulkInserter),
        patch("tsn_adapters.blocks.tn_access.concurrency", _fake_concurrency),
        patch("tsn_adapters.blocks.tn_access.time.sleep", _fake_sleep),
    ):
        hashes = block.bulk_insert_tn_records(_make_df(30), batch_size=10)

    # Cumulative tx hashes carried across both attempts.
    assert hashes == ["tx_chunk0", "tx_resume_a", "tx_resume_b"]

    # Two attempts, first failing then succeeding.
    assert call_count["n"] == 2

    # Lock-scope assertion: sleep happens BETWEEN two lock spans, never
    # inside a single one. insert_all is handed the flat list of per-row
    # batches; BulkInserter chunks internally at batch_size.
    assert events == [
        "tn-write:enter",
        "insert_all(n=30)",
        "tn-write:exit",
        "sleep",
        "tn-write:enter",
        "insert_all(n=20)",   # 30 - failed_chunk_index(1)*batch_size(10) = 20
        "tn-write:exit",
    ]


def test_drain_failure_short_circuits_with_cumulative_hashes():
    """A drain_failure means every chunk WAS broadcast; only WaitTx polling
    failed. Must not retry (would re-broadcast already-admitted txs), must
    re-raise BulkInsertError carrying the cumulative tx_hashes."""
    block = _ResumeBlock()
    events: list[str] = []
    call_count = {"n": 0}

    class _FakeBulkInserter:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def insert_all(self, batches: list[dict[str, Any]]) -> list[str]:
            call_count["n"] += 1
            events.append(f"insert_all(n={len(batches)})")
            raise BulkInsertError(
                "drain timeout",
                tx_hashes=["tx_a", "tx_b"],
                drain_failure=True,
                failed_chunk_index=2,
            )

    with (
        patch("tsn_adapters.blocks.tn_access.BulkInserter", _FakeBulkInserter),
        patch("tsn_adapters.blocks.tn_access.concurrency", lambda *a, **kw: _record_lock(events)),
        patch("tsn_adapters.blocks.tn_access.time.sleep", lambda _: events.append("sleep")),
    ):
        with pytest.raises(BulkInsertError) as excinfo:
            block.bulk_insert_tn_records(_make_df(20), batch_size=10)

    assert call_count["n"] == 1, "drain_failure must not trigger a resume attempt"
    assert excinfo.value.drain_failure is True
    assert excinfo.value.tx_hashes == ["tx_a", "tx_b"]
    assert "sleep" not in events
