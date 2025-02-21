import pandas as pd
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock
import io

from prefect_aws import S3Bucket  # type: ignore

from src.tsn_adapters.blocks.deployment_state import S3DeploymentStateBlock


@pytest.fixture
def mock_s3_bucket() -> S3Bucket:
    mock_bucket = MagicMock(spec=S3Bucket)
    stored_content: dict[str, bytes] = {}

    def sync_write_path(path: str, content: bytes) -> str:
        stored_content[path] = content
        return path

    def sync_read_path(path: str) -> bytes:
        if path not in stored_content:
            raise ValueError("No content stored")
        return stored_content[path]

    mock_bucket.write_path = MagicMock(side_effect=sync_write_path)
    mock_bucket.read_path = MagicMock(side_effect=sync_read_path)
    return mock_bucket


@pytest.fixture
def s3_block(mock_s3_bucket: S3Bucket) -> S3DeploymentStateBlock:
    return S3DeploymentStateBlock(s3_bucket=mock_s3_bucket, file_path="deployment_states/all_streams.parquet")


def test_mark_as_deployed_valid_timestamp(s3_block: S3DeploymentStateBlock) -> None:
    valid_timestamp = datetime(2023, 10, 10, 12, 0, tzinfo=timezone.utc)
    s3_block.mark_as_deployed("test_stream", valid_timestamp)

    # Verify that write_path was called with the correct path
    s3_block.s3_bucket.write_path.assert_called_once()  # type: ignore
    assert s3_block.s3_bucket.write_path.call_args[0][0] == "deployment_states/all_streams.parquet"  # type: ignore

    # Read back the data to verify it was written correctly
    content: bytes = s3_block.s3_bucket.read_path("deployment_states/all_streams.parquet")  # type: ignore
    buffer = io.BytesIO(content)
    df = pd.read_parquet(buffer, engine='pyarrow')
    
    assert len(df) == 1
    assert df.iloc[0]['stream_id'] == "test_stream"
    assert df.iloc[0]['deployment_timestamp'] == valid_timestamp


def test_mark_as_deployed_invalid_timestamp(s3_block: S3DeploymentStateBlock) -> None:
    # Create a timestamp without timezone info
    invalid_timestamp = datetime(2023, 10, 10, 12, 0)
    with pytest.raises(ValueError, match="Timestamp must be timezone-aware and in UTC."):
        s3_block.mark_as_deployed("test_stream", invalid_timestamp)


def test_has_been_deployed_true(s3_block: S3DeploymentStateBlock) -> None:
    # First mark a stream as deployed
    timestamp = datetime(2023, 10, 10, 12, 0, tzinfo=timezone.utc)
    s3_block.mark_as_deployed("test_stream", timestamp)
    
    # Then check if it's deployed
    result = s3_block.has_been_deployed("test_stream")
    assert result is True


def test_has_been_deployed_false(s3_block: S3DeploymentStateBlock) -> None:
    # Check a stream that hasn't been deployed
    result = s3_block.has_been_deployed("nonexistent_stream")
    assert result is False


def test_mark_multiple_as_deployed_valid_timestamp(s3_block: S3DeploymentStateBlock) -> None:
    valid_timestamp = datetime(2023, 10, 10, 12, 0, tzinfo=timezone.utc)
    stream_ids = ["test_stream_1", "test_stream_2", "test_stream_3"]
    s3_block.mark_multiple_as_deployed(stream_ids, valid_timestamp)

    # Verify that write_path was called with the correct path
    s3_block.s3_bucket.write_path.assert_called_once()  # type: ignore
    assert s3_block.s3_bucket.write_path.call_args[0][0] == "deployment_states/all_streams.parquet"  # type: ignore

    # Read back the data to verify it was written correctly
    content: bytes = s3_block.s3_bucket.read_path("deployment_states/all_streams.parquet")  # type: ignore
    buffer = io.BytesIO(content)
    df = pd.read_parquet(buffer, engine='pyarrow')
    
    assert len(df) == 3
    assert set(df['stream_id'].values) == set(stream_ids)
    assert all(df['deployment_timestamp'] == valid_timestamp)


def test_mark_multiple_as_deployed_invalid_timestamp(s3_block: S3DeploymentStateBlock) -> None:
    # Create a timestamp without timezone info
    invalid_timestamp = datetime(2023, 10, 10, 12, 0)
    stream_ids = ["test_stream_1", "test_stream_2"]
    with pytest.raises(ValueError, match="Timestamp must be timezone-aware and in UTC."):
        s3_block.mark_multiple_as_deployed(stream_ids, invalid_timestamp)


def test_mark_multiple_as_deployed_empty_list(s3_block: S3DeploymentStateBlock) -> None:
    valid_timestamp = datetime(2023, 10, 10, 12, 0, tzinfo=timezone.utc)
    s3_block.mark_multiple_as_deployed([], valid_timestamp)
    
    # Verify that write_path was not called
    s3_block.s3_bucket.write_path.assert_not_called()  # type: ignore


def test_check_multiple_streams_mixed_status(s3_block: S3DeploymentStateBlock) -> None:
    # First mark some streams as deployed
    timestamp = datetime(2023, 10, 10, 12, 0, tzinfo=timezone.utc)
    deployed_streams = ["test_stream_1", "test_stream_2"]
    s3_block.mark_multiple_as_deployed(deployed_streams, timestamp)
    
    # Check a mix of deployed and non-deployed streams
    check_streams = ["test_stream_1", "test_stream_3", "test_stream_2"]
    result = s3_block.check_multiple_streams(check_streams)
    
    assert result == {
        "test_stream_1": True,
        "test_stream_2": True,
        "test_stream_3": False
    }


def test_check_multiple_streams_empty_list(s3_block: S3DeploymentStateBlock) -> None:
    result = s3_block.check_multiple_streams([])
    assert result == {}


def test_check_multiple_streams_none_deployed(s3_block: S3DeploymentStateBlock) -> None:
    stream_ids = ["test_stream_1", "test_stream_2"]
    result = s3_block.check_multiple_streams(stream_ids)
    assert result == {
        "test_stream_1": False,
        "test_stream_2": False
    }


def test_get_deployment_states_empty(s3_block: S3DeploymentStateBlock) -> None:
    # Get states when no file exists
    df = s3_block.get_deployment_states()
    assert len(df) == 0
    assert list(df.columns) == ['stream_id', 'deployment_timestamp']


def test_get_deployment_states_with_data(s3_block: S3DeploymentStateBlock) -> None:
    # First add some deployment states
    timestamp = datetime(2023, 10, 10, 12, 0, tzinfo=timezone.utc)
    stream_ids = ["test_stream_1", "test_stream_2"]
    s3_block.mark_multiple_as_deployed(stream_ids, timestamp)
    
    # Get the states
    df = s3_block.get_deployment_states()
    assert len(df) == 2
    assert set(df['stream_id'].values) == set(stream_ids)
    # Compare timestamps using pandas Series equality
    expected_ts = pd.Series([timestamp] * len(df))
    expected_ts = expected_ts.dt.tz_localize(None)  # Convert to naive timestamps for comparison
    actual_ts = df['deployment_timestamp'].dt.tz_localize(None)
    pd.testing.assert_series_equal(actual_ts, expected_ts, check_names=False)


def test_update_deployment_states_valid(s3_block: S3DeploymentStateBlock) -> None:
    # Create a valid DataFrame with deployment states
    timestamp = datetime(2023, 10, 10, 12, 0, tzinfo=timezone.utc)
    data = {
        'stream_id': ['test_stream_1', 'test_stream_2'],
        'deployment_timestamp': [timestamp, timestamp]
    }
    df = pd.DataFrame(data)
    from src.tsn_adapters.blocks.deployment_state import DataFrame, DeploymentStateModel
    states = DataFrame[DeploymentStateModel](df)
    
    # Update the states
    s3_block.update_deployment_states(states)
    
    # Verify the states were written correctly
    content: bytes = s3_block.s3_bucket.read_path("deployment_states/all_streams.parquet")  # type: ignore
    buffer = io.BytesIO(content)
    read_df = pd.read_parquet(buffer, engine='pyarrow')
    
    assert len(read_df) == 2
    assert set(read_df['stream_id'].values) == {'test_stream_1', 'test_stream_2'}
    assert all(read_df['deployment_timestamp'] == timestamp)


def test_update_deployment_states_empty(s3_block: S3DeploymentStateBlock) -> None:
    # Create an empty DataFrame with the correct schema
    df = pd.DataFrame(columns=['stream_id', 'deployment_timestamp'])
    from src.tsn_adapters.blocks.deployment_state import DataFrame, DeploymentStateModel
    states = DataFrame[DeploymentStateModel](df)
    
    # Update with empty states
    s3_block.update_deployment_states(states)
    
    # Verify the states were written correctly
    content: bytes = s3_block.s3_bucket.read_path("deployment_states/all_streams.parquet")  # type: ignore
    buffer = io.BytesIO(content)
    read_df = pd.read_parquet(buffer, engine='pyarrow')
    
    assert len(read_df) == 0
    assert list(read_df.columns) == ['stream_id', 'deployment_timestamp']


def test_mark_as_deployed_replaces_existing(s3_block: S3DeploymentStateBlock) -> None:
    """Test that marking a stream as deployed replaces any existing deployment record."""
    # First deployment
    first_timestamp = datetime(2023, 10, 10, 12, 0, tzinfo=timezone.utc)
    s3_block.mark_as_deployed("test_stream", first_timestamp)
    
    # Second deployment with different timestamp
    second_timestamp = datetime(2023, 10, 10, 13, 0, tzinfo=timezone.utc)
    s3_block.mark_as_deployed("test_stream", second_timestamp)
    
    # Verify that only the second deployment exists
    content: bytes = s3_block.s3_bucket.read_path("deployment_states/all_streams.parquet")  # type: ignore
    buffer = io.BytesIO(content)
    df = pd.read_parquet(buffer, engine='pyarrow')
    
    assert len(df) == 1, "Should only have one record for the stream"
    assert df.iloc[0]['stream_id'] == "test_stream"
    assert df.iloc[0]['deployment_timestamp'] == second_timestamp


def test_mark_multiple_as_deployed_replaces_existing(s3_block: S3DeploymentStateBlock) -> None:
    """Test that marking multiple streams as deployed replaces any existing deployment records."""
    # First deployment
    first_timestamp = datetime(2023, 10, 10, 12, 0, tzinfo=timezone.utc)
    stream_ids = ["stream1", "stream2", "stream3"]
    s3_block.mark_multiple_as_deployed(stream_ids, first_timestamp)
    
    # Second deployment for subset of streams with different timestamp
    second_timestamp = datetime(2023, 10, 10, 13, 0, tzinfo=timezone.utc)
    updated_streams = ["stream1", "stream3"]
    s3_block.mark_multiple_as_deployed(updated_streams, second_timestamp)
    
    # Verify the state
    content: bytes = s3_block.s3_bucket.read_path("deployment_states/all_streams.parquet")  # type: ignore
    buffer = io.BytesIO(content)
    df = pd.read_parquet(buffer, engine='pyarrow')
    
    # Convert DataFrame to dictionary for easier assertion
    stream_to_timestamp: dict[str, datetime] = dict(zip(df['stream_id'], df['deployment_timestamp']))
    
    assert len(df) == 3, "Should have three records total"
    assert stream_to_timestamp["stream1"] == second_timestamp, "stream1 should have second timestamp"
    assert stream_to_timestamp["stream2"] == first_timestamp, "stream2 should retain first timestamp"
    assert stream_to_timestamp["stream3"] == second_timestamp, "stream3 should have second timestamp"


def test_mark_as_deployed_maintains_other_records(s3_block: S3DeploymentStateBlock) -> None:
    """Test that marking a stream as deployed doesn't affect other stream records."""
    # Deploy multiple streams
    first_timestamp = datetime(2023, 10, 10, 12, 0, tzinfo=timezone.utc)
    stream_ids = ["stream1", "stream2", "stream3"]
    s3_block.mark_multiple_as_deployed(stream_ids, first_timestamp)
    
    # Update one stream
    second_timestamp = datetime(2023, 10, 10, 13, 0, tzinfo=timezone.utc)
    s3_block.mark_as_deployed("stream2", second_timestamp)
    
    # Verify the state
    content: bytes = s3_block.s3_bucket.read_path("deployment_states/all_streams.parquet")  # type: ignore
    buffer = io.BytesIO(content)
    df = pd.read_parquet(buffer, engine='pyarrow')
    
    # Convert DataFrame to dictionary for easier assertion
    stream_to_timestamp: dict[str, datetime] = dict(zip(df['stream_id'], df['deployment_timestamp']))
    
    assert len(df) == 3, "Should still have three records"
    assert stream_to_timestamp["stream1"] == first_timestamp, "stream1 should retain first timestamp"
    assert stream_to_timestamp["stream2"] == second_timestamp, "stream2 should have second timestamp"
    assert stream_to_timestamp["stream3"] == first_timestamp, "stream3 should retain first timestamp" 