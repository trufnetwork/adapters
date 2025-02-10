"""
Integration tests for TNAccessBlock that require a live TN connection.

These tests verify the actual interaction with TN, including stream creation,
data insertion, and retrieval. They should only be run when TN access is available
and configured.
"""

from datetime import datetime

import pandas as pd
from pandera.typing import DataFrame
from prefect import flow, task
import pytest
from trufnetwork_sdk_py.utils import generate_stream_id

from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnRecordModel


@pytest.fixture
def test_stream_id() -> str:
    """Generate a unique test stream ID using the official TN utility.

    Uses a timestamp to ensure uniqueness across test runs.
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return generate_stream_id(f"test_{timestamp}")


@pytest.fixture
def deployed_test_stream_id(tn_block: TNAccessBlock, test_stream_id: str):
    """Deploys and initializes a test stream."""
    client = tn_block.get_client()
    client.deploy_stream(test_stream_id, wait=True)
    client.init_stream(test_stream_id, wait=True)
    yield test_stream_id
    tn_block.destroy_stream(test_stream_id)


@pytest.fixture
def sample_records() -> DataFrame[TnRecordModel]:
    """Create sample records for testing."""
    data = {
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "value": [100.0, 200.0, 300.0],
    }
    return DataFrame[TnRecordModel](pd.DataFrame(data))


@task
def insert_records(tn_block: TNAccessBlock, stream_id: str, records: DataFrame[TnRecordModel]) -> None:
    """Task to insert records into a stream."""
    tx_hash = tn_block.insert_tn_records(
        stream_id,
        records,
        include_current_date=False,
    )
    assert tx_hash is not None
    tn_block.wait_for_tx(tx_hash)


@flow
def insert_on_test_flow(block: TNAccessBlock, stream_id: str, records: DataFrame[TnRecordModel]) -> None:
    """Flow to test record insertion."""
    insert_records(block, stream_id, records)


class TestTNAccessBlockIntegration:
    """Integration tests for TNAccessBlock requiring live TN connection."""

    def test_get_first_record_success(
        self, tn_block: TNAccessBlock, deployed_test_stream_id: str, sample_records: DataFrame[TnRecordModel]
    ):
        """Test successful retrieval of first record after insertion."""
        # Insert test records (using Prefect flow)
        insert_on_test_flow(tn_block, deployed_test_stream_id, sample_records)

        # Get and verify first record
        first_record = tn_block.get_first_record(deployed_test_stream_id)
        assert first_record is not None
        assert first_record.date == "2024-01-01"
        assert float(first_record.value) == 100.0

    def test_get_first_record_invalid_stream_id(self, tn_block: TNAccessBlock):
        """Test get_first_record with an invalid stream ID format."""
        with pytest.raises(Exception, match="invalid stream id"):
            tn_block.get_first_record("non_existent_stream")

    def test_get_first_record_inexistent_stream(self, tn_block: TNAccessBlock):
        """Test get_first_record with a well-formatted but non-existent stream ID."""
        with pytest.raises(Exception, match="stream not found"):
            tn_block.get_first_record(generate_stream_id("inexistent_stream"))

    def test_get_first_record_empty_stream(self, tn_block: TNAccessBlock, deployed_test_stream_id: str):
        """Test get_first_record on an empty stream."""
        result = tn_block.get_first_record(deployed_test_stream_id)
        assert result is None


if __name__ == "__main__":
    pytest.main(["-v", __file__])
