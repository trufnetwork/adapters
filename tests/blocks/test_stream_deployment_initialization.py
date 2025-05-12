from datetime import datetime
import logging
from unittest.mock import MagicMock

import pytest
from trufnetwork_sdk_py import StreamDefinitionInput
from trufnetwork_sdk_py.utils import generate_stream_id

from tsn_adapters.blocks.tn_access import StreamAlreadyExistsError, TNAccessBlock
from tsn_adapters.common.trufnetwork.tn import task_batch_deploy_streams, task_deploy_primitive


@pytest.fixture
def test_stream_id() -> str:
    """Generate a unique stream ID for deployment tests."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return generate_stream_id(f"deploy_test_{timestamp}")


def test_deploy_stream(tn_block: TNAccessBlock, test_stream_id: str):
    """Test deploying a stream successfully."""
    # Deploy the stream
    tx_hash = tn_block.deploy_stream(test_stream_id, wait=True)
    assert tx_hash is not None, "Deploy transaction hash should be returned"

    # Clean up: destroy the deployed stream
    destroy_tx = tn_block.destroy_stream(test_stream_id, wait=True)
    assert destroy_tx is not None, "Stream destruction should return a transaction hash"

def test_get_stream_type(tn_block: TNAccessBlock, test_stream_id: str):
    """Test checking stream type status."""
    # Initially, for a non-existent stream, it should error out
    with pytest.raises(Exception, match="no type found"):
        tn_block.get_stream_type("", test_stream_id)
    
    # Deploy the stream
    deploy_tx = tn_block.deploy_stream(test_stream_id, wait=True)
    assert deploy_tx is not None, "Deploy transaction hash should be returned"
    
    # It should return a valid stream type
    stream_type = tn_block.get_stream_type("", test_stream_id)
    assert stream_type is not None, "Initialized stream should return a valid stream type"
    
    # Clean up: destroy the stream
    destroy_tx = tn_block.destroy_stream(test_stream_id, wait=True)
    assert destroy_tx is not None, "Stream destruction should return a transaction hash"


# --- Tests for task_deploy_primitive --- #

def test_task_deploy_primitive_success():
    """Test task_deploy_primitive successfully deploys."""
    mock_block = MagicMock(spec=TNAccessBlock)
    mock_block.deploy_stream.return_value = "dummy_tx_hash"

    result = task_deploy_primitive.fn(block=mock_block, stream_id="test_stream_success")

    mock_block.deploy_stream.assert_called_once_with(
        stream_id="test_stream_success",
        wait=True, # Default value
        stream_type="primitive"
    )
    assert result == "dummy_tx_hash"


def test_task_deploy_primitive_already_exists(caplog: pytest.LogCaptureFixture):
    """Test task_deploy_primitive handles 'already exists' error by raising StreamAlreadyExistsError."""
    mock_block = MagicMock(spec=TNAccessBlock)
    # Simulate the specific error message
    mock_block.deploy_stream.side_effect = StreamAlreadyExistsError("test_stream_exists")

    # Expect the custom exception
    with pytest.raises(StreamAlreadyExistsError) as excinfo:
        with caplog.at_level(logging.INFO):
            task_deploy_primitive.fn(block=mock_block, stream_id="test_stream_exists")

    # Assert the exception has the correct stream ID
    assert excinfo.value.stream_id == "test_stream_exists"
    mock_block.deploy_stream.assert_called_once()
    assert "Stream test_stream_exists already exists. Skipping deployment." in caplog.text


def test_task_deploy_primitive_other_error(caplog: pytest.LogCaptureFixture):
    """Test task_deploy_primitive re-raises other errors."""
    mock_block = MagicMock(spec=TNAccessBlock)
    mock_block.deploy_stream.side_effect = ValueError("Some other deployment error")

    with pytest.raises(ValueError, match="Some other deployment error"):
        with caplog.at_level(logging.ERROR):
            task_deploy_primitive.fn(block=mock_block, stream_id="test_stream_other_error")

    mock_block.deploy_stream.assert_called_once()
    assert "Error deploying stream test_stream_other_error: Some other deployment error" in caplog.text

def test_task_batch_deploy_streams_block_success():
    """Test the Prefect task wrapper around TNAccessBlock.batch_deploy_streams."""
    mock_block = MagicMock(spec=TNAccessBlock)
    definitions = [StreamDefinitionInput(stream_id="s", stream_type="primitive")]
    mock_block.batch_deploy_streams.return_value = "block_batch_tx"

    result = task_batch_deploy_streams.fn(
        block=mock_block, definitions=definitions, wait=True
    )

    _, kwargs = mock_block.batch_deploy_streams.call_args
    assert kwargs["wait"] == True
    assert kwargs["definitions"] == definitions
    assert result == "block_batch_tx"

def test_task_batch_deploy_streams_block_default_wait():
    """
    When no `wait` argument is provided, it should default to True.
    """
    mock_block = MagicMock(spec=TNAccessBlock)
    definitions = [StreamDefinitionInput(stream_id="y", stream_type="composed")]

    # call without explicit wait
    task_batch_deploy_streams.fn(block=mock_block, definitions=definitions)

    _, kwargs = mock_block.batch_deploy_streams.call_args
    assert kwargs["wait"] == True
    assert kwargs["definitions"] == definitions

def test_task_batch_deploy_streams_block_propagates_error():
    """
    If TNAccessBlock.batch_deploy_streams raises, the task should not swallow it.
    """
    mock_block = MagicMock(spec=TNAccessBlock)
    definitions = [StreamDefinitionInput(stream_id="err", stream_type="primitive")]
    mock_block.batch_deploy_streams.side_effect = StreamAlreadyExistsError("err")

    with pytest.raises(StreamAlreadyExistsError) as exc:
        task_batch_deploy_streams.fn(
            block=mock_block, definitions=definitions, wait=True
        )

    # ensure it bubbled up
    assert exc.value.stream_id == "err"
    mock_block.batch_deploy_streams.assert_called_once()
