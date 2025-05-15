from unittest.mock import MagicMock

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
    # Prepare mixed zero and non-zero string values
    data = {
        "date": [
            "2023-01-01",
            "2023-01-02",
            "2023-01-03",
            "2023-01-04",
            "2023-01-05",
            "2023-01-06",
            "2023-01-07",
            "2023-01-08",
            "2023-01-09",
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

    # Mock client
    mock_client = MagicMock()
    mock_client.execute_procedure.return_value = "tx_hash"
    # Patch block's client
    block.set_client(mock_client)

    result = block.insert_tn_records(stream_id="test_stream", records=records_df)
    assert result == "tx_hash"
    mock_client.execute_procedure.assert_called_once()

    # Inspect args passed to execute_procedure
    call_kwargs = mock_client.execute_procedure.call_args.kwargs
    args_list = call_kwargs.get("args", [])
    # Expect only non-zero values
    expected_values = {
        "1.0",
        "0.000000000000000001",
        "123.456",
    }
    # Extract the value strings from args: index 1 of each
    sent_values = {arg[1] for arg in args_list}
    assert sent_values == expected_values
    assert len(args_list) == len(expected_values)


def test_insert_tn_records_skips_all_zero(block: FakeTNAccessBlock):
    # All values are zero
    data = {"date": ["2023-01-01", "2023-01-02"], "value": ["0", "0.00000"]}
    df = pd.DataFrame(data)
    records_df = DataFrame[TnRecordModel](df)

    mock_client = MagicMock()
    block.set_client(mock_client)

    result = block.insert_tn_records(stream_id="test_stream", records=records_df)
    assert result is None
    mock_client.execute_procedure.assert_not_called()


def test_insert_tn_records_empty_input(block: FakeTNAccessBlock):
    # Empty DataFrame
    df = pd.DataFrame(columns=["date", "value"])
    records_df = DataFrame[TnRecordModel](df)

    mock_client = MagicMock()
    block.set_client(mock_client)

    result = block.insert_tn_records(stream_id="test_stream", records=records_df)
    assert result is None
    mock_client.execute_procedure.assert_not_called()


def test_batch_insert_tn_records_filters_various_zeros(block: FakeTNAccessBlock):
    # Prepare records with mixed zero and non-zero values across streams
    data = {
        "stream_id": ["s1", "s1", "s2", "s2", "s3", "s3", "s4", "s4", "s4"],
        "data_provider": ["dp1"] * 9,
        "date": [
            "2023-01-01",
            "2023-01-02",
            "2023-01-03",
            "2023-01-04",
            "2023-01-05",
            "2023-01-06",
            "2023-01-07",
            "2023-01-08",
            "2023-01-09",
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
            "0.000000000000000000000000000000001",  # Keep s4
        ],
    }
    df = pd.DataFrame(data)
    records_df = DataFrame[TnDataRowModel](df)

    mock_client = MagicMock()
    mock_client.batch_insert_records.return_value = {"tx_hash": "batch_hash"}
    block.set_client(mock_client)

    result = block.batch_insert_tn_records(records=records_df, is_unix=False, has_external_created_at=False)
    assert result == "batch_hash"
    mock_client.batch_insert_records.assert_called_once()

    # Inspect batches passed to client
    batches = mock_client.batch_insert_records.call_args.kwargs.get("batches", [])
    # Expect three streams: s1 (1), s3 (1), s4 (2)
    assert len(batches) == 3
    # Map stream_id to inputs list
    batch_map = {b["stream_id"]: b["inputs"] for b in batches}
    assert set(batch_map.keys()) == {"s1", "s3", "s4"}
    assert len(batch_map["s1"]) == 1 and float(batch_map["s1"][0]["value"]) == 1.0
    assert len(batch_map["s3"]) == 1 and float(batch_map["s3"][0]["value"]) == float("0.000000000000000001")
    assert len(batch_map["s4"]) == 1
    # Only the non-zero quantized value should remain for s4
    value_s4 = batch_map["s4"][0]["value"]
    assert float(value_s4) == 123.456


def test_batch_insert_tn_records_skips_all_zero(block: FakeTNAccessBlock):
    data = {
        "stream_id": ["s1", "s2"],
        "data_provider": ["dp1", "dp1"],
        "date": ["2023-01-01", "2023-01-02"],
        "value": ["0", "0.00"],
    }
    df = pd.DataFrame(data)
    records_df = DataFrame[TnDataRowModel](df)

    mock_client = MagicMock()
    block.set_client(mock_client)

    result = block.batch_insert_tn_records(records=records_df)
    assert result is None
    mock_client.batch_insert_records.assert_not_called()


def test_batch_insert_tn_records_empty_input(block: FakeTNAccessBlock):
    df = pd.DataFrame(columns=["stream_id", "data_provider", "date", "value"])
    records_df = DataFrame[TnDataRowModel](df)

    mock_client = MagicMock()
    block.set_client(mock_client)

    result = block.batch_insert_tn_records(records=records_df)
    assert result is None
    mock_client.batch_insert_records.assert_not_called()
