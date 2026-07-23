"""Tests for the shared EPS publish path — dedup + fail-loud insert.

Covers website#4362: FMP repeated GOOGL's Q2-2026 announcement row in its
response, both copies rode one insert_records tx, and the primitive PK
rejected the whole batch — while the flow logged "Published" because the
insert result was discarded.
"""

from typing import Any

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow
import pytest
from tests.utils.fake_tn_access import FakeTNAccessBlock

from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.flows.eps import publish as publish_module
from tsn_adapters.flows.eps.publish import publish_eps_records
from tsn_adapters.tasks.eps.config import FMP_EPS_STREAM_IDS

STREAM = FMP_EPS_STREAM_IDS["GOOGL"]


def _frame(dates: list[int]) -> pd.DataFrame:
    return pd.DataFrame([{"stream_id": STREAM, "date": d, "value": "9.11", "data_provider": None} for d in dates])


@pytest.fixture(scope="module")
def prefect_harness(prefect_test_fixture: Any):
    yield prefect_test_fixture


def test_collapses_duplicate_stream_date_rows(prefect_harness: Any):
    block = FakeTNAccessBlock(existing_streams={STREAM})

    @flow
    def run_publish() -> int:
        return publish_eps_records(block, _frame([100, 100, 200]), "FMP")

    submitted = run_publish()
    assert submitted == 2
    inserted = pd.concat(block.inserted_records, ignore_index=True)
    assert sorted(inserted["date"].tolist()) == [100, 200]


def test_raises_when_records_fail_to_insert(monkeypatch: pytest.MonkeyPatch):
    failed = DataFrame[TnDataRowModel](_frame([100]).assign(data_provider="0x" + "0" * 40))

    def fake_insert(block, records):
        return {"success_tx_hashes": [], "failed_records": failed, "failed_reasons": ["duplicate key"]}

    monkeypatch.setattr(publish_module, "task_split_and_insert_records", fake_insert)
    with pytest.raises(RuntimeError, match="duplicate key"):
        publish_eps_records(FakeTNAccessBlock(existing_streams={STREAM}), _frame([100]), "FMP")
