"""Tests for the shared skip-unchanged filter on the TN write path.

The filter's whole claim is that dropping a record changes no answer TN gives,
because TN carries the last observation forward. Every test here is written
against that claim rather than against the implementation: seed what the stream
already holds, hand the filter a batch, and assert on which rows survive.
"""

import pandas as pd
from pandera.typing import DataFrame
import pytest

from tests.utils.fake_tn_access import FakeTNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
import tsn_adapters.common.trufnetwork.tasks.insert as insert_module
from tsn_adapters.common.trufnetwork.tasks.insert import (
    MAX_STREAMS_FOR_UNCHANGED_CHECK,
    SplitInsertResults,
    _filter_unchanged_records,  # pyright: ignore[reportPrivateUsage]
    task_split_and_insert_records,
)

PROVIDER = "0x1234567890123456789012345678901234567890"


def make_records(rows: list[tuple[str, int, str]]) -> DataFrame[TnDataRowModel]:
    """rows are (stream_id, event_time, value)."""
    return DataFrame[TnDataRowModel](
        pd.DataFrame(
            [{"data_provider": PROVIDER, "stream_id": s, "date": d, "value": v} for s, d, v in rows],
            columns=["data_provider", "stream_id", "date", "value"],
        )
    )


def surviving(block: FakeTNAccessBlock, records: DataFrame[TnDataRowModel]) -> list[tuple[int, str]]:
    kept = _filter_unchanged_records(block=block, records=records, max_streams_for_unchanged_check=500)
    return [(int(r["date"]), str(r["value"])) for _, r in kept.iterrows()]


# ---------------------------------------------------------------------------
# The case the filter exists for
# ---------------------------------------------------------------------------


def test_drops_a_value_that_matches_what_the_stream_already_holds():
    block = FakeTNAccessBlock()
    block.seed_records("st_flat", dates=[100], values=["1.5"])

    assert surviving(block, make_records([("st_flat", 200, "1.5")])) == []


def test_keeps_a_value_that_moved():
    block = FakeTNAccessBlock()
    block.seed_records("st_moves", dates=[100], values=["1.5"])

    assert surviving(block, make_records([("st_moves", 200, "1.6")])) == [(200, "1.6")]


def test_keeps_the_first_record_of_an_empty_stream():
    block = FakeTNAccessBlock()

    assert surviving(block, make_records([("st_new", 100, "1.5")])) == [(100, "1.5")]


# ---------------------------------------------------------------------------
# Runs inside a single batch
# ---------------------------------------------------------------------------


def test_collapses_a_run_within_one_batch_to_its_first_row():
    """A batch carrying its own repeats must not write them just because TN has
    not seen them yet. Comparing only against the chain would let the whole run
    through on the first send."""
    block = FakeTNAccessBlock()

    kept = surviving(
        block,
        make_records(
            [
                ("st_run", 100, "1.5"),
                ("st_run", 200, "1.5"),
                ("st_run", 300, "1.5"),
                ("st_run", 400, "2.0"),
                ("st_run", 500, "2.0"),
            ]
        ),
    )

    assert kept == [(100, "1.5"), (400, "2.0")]


def test_out_of_order_rows_are_judged_in_event_time_order():
    """Row order in the frame says nothing about time order. A backfill can hand
    us 300 before 100, and the comparison must still walk the timeline."""
    block = FakeTNAccessBlock()

    kept = surviving(
        block,
        make_records(
            [
                ("st_unsorted", 300, "1.5"),
                ("st_unsorted", 100, "1.5"),
                ("st_unsorted", 200, "2.0"),
            ]
        ),
    )

    assert sorted(kept) == [(100, "1.5"), (200, "2.0"), (300, "1.5")]


def test_a_record_already_on_chain_between_two_candidates_breaks_the_run():
    """The reason the read covers the batch's whole span rather than just its
    start. Backfills write history around records that already exist, and the
    value standing before a candidate may be one this batch is not sending."""
    block = FakeTNAccessBlock()
    block.seed_records("st_interleaved", dates=[150], values=["9.9"])

    kept = surviving(
        block,
        make_records(
            [
                ("st_interleaved", 100, "1.5"),
                ("st_interleaved", 200, "1.5"),
            ]
        ),
    )

    assert kept == [(100, "1.5"), (200, "1.5")], "9.9 stands between them, so 200 is a change"


# ---------------------------------------------------------------------------
# Comparison details that silently break the feature
# ---------------------------------------------------------------------------


def test_compares_numerically_not_as_strings():
    """TN renders NUMERIC(36,18) as '1.500000000000000000' while adapters emit
    '1.5'. String equality would mark every row as changed, and the feature
    would look shipped while doing nothing."""
    block = FakeTNAccessBlock()
    block.seed_records("st_precision", dates=[100], values=["1.500000000000000000"])

    assert surviving(block, make_records([("st_precision", 200, "1.5")])) == []


def test_streams_are_judged_independently():
    block = FakeTNAccessBlock()
    block.seed_records("st_a", dates=[100], values=["1.5"])
    block.seed_records("st_b", dates=[100], values=["7.0"])

    kept = surviving(
        block,
        make_records(
            [
                ("st_a", 200, "1.5"),
                ("st_b", 200, "1.5"),
            ]
        ),
    )

    assert kept == [(200, "1.5")], "only st_b moved"


def test_a_value_that_cannot_be_read_as_a_number_is_kept():
    """Diagnosing junk is not this filter's job. Let it through and fail where it
    would have failed anyway."""
    block = FakeTNAccessBlock()
    block.seed_records("st_junk", dates=[100], values=["1.5"])

    assert surviving(block, make_records([("st_junk", 200, "not-a-number")])) == [(200, "not-a-number")]


# ---------------------------------------------------------------------------
# The ceiling
# ---------------------------------------------------------------------------


def test_refuses_a_batch_wider_than_the_stream_ceiling():
    """One serialised read per stream is affordable for hundreds of streams and
    hopeless for tens of thousands. Failing loudly beats stalling a flow."""
    block = FakeTNAccessBlock()
    records = make_records([(f"st_{i}", 100, "1.0") for i in range(4)])

    with pytest.raises(ValueError, match="above the 3 ceiling"):
        _filter_unchanged_records(block=block, records=records, max_streams_for_unchanged_check=3)


# ---------------------------------------------------------------------------
# Wiring into the shared write path
# ---------------------------------------------------------------------------


def test_default_off_does_not_consult_the_chain(monkeypatch: pytest.MonkeyPatch):
    """The flag defaults off, so an existing caller must not gain a read per
    stream — nor any behaviour change — by upgrading."""
    filtered: list[int] = []
    inserted: list[object] = []
    monkeypatch.setattr(insert_module, "_filter_unchanged_records", lambda **kwargs: filtered.append(1))
    monkeypatch.setattr(
        insert_module,
        "_perform_batch_insertions",
        lambda **kwargs: inserted.append(kwargs["records_to_insert"])
        or SplitInsertResults(success_tx_hashes=["tx"], failed_records=make_records([]), failed_reasons=[]),
    )

    block = FakeTNAccessBlock()
    records = make_records([("st_default", 200, "1.5")])

    task_split_and_insert_records.fn(block=block, records=records, filter_deployed_streams=False)

    assert filtered == [], "skip_unchanged defaults to off, so the filter must not run"
    assert len(inserted) == 1 and len(inserted[0]) == 1, "the record reaches the insert path untouched"


def test_opting_in_runs_the_filter_before_inserting(monkeypatch: pytest.MonkeyPatch):
    seen: dict[str, object] = {}

    def spy(**kwargs: object) -> object:
        seen.update(kwargs)
        return make_records([])

    monkeypatch.setattr(insert_module, "_filter_unchanged_records", spy)

    block = FakeTNAccessBlock()
    block.seed_records("st_optin", dates=[100], values=["1.5"])
    records = make_records([("st_optin", 200, "1.5")])

    result = task_split_and_insert_records.fn(
        block=block, records=records, filter_deployed_streams=False, skip_unchanged=True
    )

    assert seen, "skip_unchanged=True must route the batch through the filter"
    assert seen["max_streams_for_unchanged_check"] == MAX_STREAMS_FOR_UNCHANGED_CHECK
    assert result["success_tx_hashes"] == [], "a fully-filtered batch writes nothing"
