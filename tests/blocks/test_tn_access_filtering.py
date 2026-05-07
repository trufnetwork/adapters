import pandas as pd
from pandera.typing import DataFrame
import pytest

from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel, TnRecordModel

from ..utils.fake_tn_access import FakeTNAccessBlock


@pytest.fixture
def block() -> FakeTNAccessBlock:
    # Instantiate block with dummy provider and key
    return FakeTNAccessBlock()


def test_insert_tn_records_filters_various_zeros(block: FakeTNAccessBlock):
    # `date` must be unix-second ints — TnRecordModel schema is `Series[int]`
    # and runtime code in tn_access does `int(row.date)`. ISO date strings
    # don't satisfy either constraint.
    data = {
        "date": [
            1672531200,
            1672617600,
            1672704000,
            1672790400,
            1672876800,
            1672963200,
            1673049600,
            1673136000,
            1673222400,
        ],
        "value": [
            "1.0",  # Keep
            "0",  # Skip
            "0.0",  # Skip
            "0.000000000000000000",  # Skip
            "0.000",  # Skip
            "-0.0",  # Skip
            "0.000000000000000001",  # Keep, exactly 18 decimals
            "123.456",  # Keep
            "0.000000000000000000000000000000001",  # Skip, more than 18 decimals
        ],
    }
    df = pd.DataFrame(data)
    records_df = DataFrame[TnRecordModel](df)

    result = block.insert_tn_records(stream_id="test_stream", records=records_df)
    assert result is not None  # FakeInternalTNClient returns fake_client_tx_hash_<n>

    # Verify only non-zero values reached the client (compare as floats —
    # the value formatter may emit "1e-18" for 0.000000000000000001)
    assert len(block.inserted_records) == 1
    sent_df = block.inserted_records[0]
    sent_values = sorted(float(v) for v in sent_df["value"].tolist())
    assert sent_values == sorted([1.0, float("0.000000000000000001"), 123.456])


def test_insert_tn_records_skips_all_zero(block: FakeTNAccessBlock):
    data = {"date": [1672531200, 1672617600], "value": ["0", "0.00000"]}
    df = pd.DataFrame(data)
    records_df = DataFrame[TnRecordModel](df)

    result = block.insert_tn_records(stream_id="test_stream", records=records_df)
    assert result is None
    assert len(block.inserted_records) == 0


def test_insert_tn_records_empty_input(block: FakeTNAccessBlock):
    df = pd.DataFrame(columns=["date", "value"])
    records_df = DataFrame[TnRecordModel](df)

    result = block.insert_tn_records(stream_id="test_stream", records=records_df)
    assert result is None
    assert len(block.inserted_records) == 0


def test_batch_insert_tn_records_filters_various_zeros(block: FakeTNAccessBlock):
    data = {
        "stream_id": ["s1", "s1", "s2", "s2", "s3", "s3", "s4", "s4", "s4"],
        "data_provider": ["dp1"] * 9,
        "date": [
            1672531200,
            1672617600,
            1672704000,
            1672790400,
            1672876800,
            1672963200,
            1673049600,
            1673136000,
            1673222400,
        ],
        "value": [
            "1.0",  # Keep s1
            "0",  # Skip s1
            "0.0",  # Skip s2
            "0.000000000000000000",  # Skip s2
            "-0.0",  # Skip s3
            "0.000000000000000001",  # Keep s3
            "123.456",  # Keep s4
            "0.000",  # Skip s4
            "0.000000000000000000000000000000001",  # Skip s4 (>18 decimals quantizes to 0)
        ],
    }
    df = pd.DataFrame(data)
    records_df = DataFrame[TnDataRowModel](df)

    result = block.batch_insert_tn_records(records=records_df)
    assert result is not None

    # FakeInternalTNClient records the reconstructed batch in inserted_dataframes_history
    assert len(block.inserted_records) == 1
    sent_df = block.inserted_records[0]
    by_stream = sent_df.groupby("stream_id")["value"].apply(list).to_dict()
    assert set(by_stream.keys()) == {"s1", "s3", "s4"}
    assert len(by_stream["s1"]) == 1 and float(by_stream["s1"][0]) == 1.0
    assert len(by_stream["s3"]) == 1 and float(by_stream["s3"][0]) == float("0.000000000000000001")
    assert len(by_stream["s4"]) == 1 and float(by_stream["s4"][0]) == 123.456


def test_batch_insert_tn_records_skips_all_zero(block: FakeTNAccessBlock):
    data = {
        "stream_id": ["s1", "s2"],
        "data_provider": ["dp1", "dp1"],
        "date": [1672531200, 1672617600],
        "value": ["0", "0.00"],
    }
    df = pd.DataFrame(data)
    records_df = DataFrame[TnDataRowModel](df)

    result = block.batch_insert_tn_records(records=records_df)
    assert result is None
    assert len(block.inserted_records) == 0


def test_batch_insert_tn_records_empty_input(block: FakeTNAccessBlock):
    df = pd.DataFrame(columns=["stream_id", "data_provider", "date", "value"])
    records_df = DataFrame[TnDataRowModel](df)

    result = block.batch_insert_tn_records(records=records_df)
    assert result is None
    assert len(block.inserted_records) == 0
