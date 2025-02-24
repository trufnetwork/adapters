import pytest
from datetime import datetime

from tsn_adapters.blocks.tn_access import TNAccessBlock
from trufnetwork_sdk_py.utils import generate_stream_id


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

    # Verify that the stream exists
    # data_provider is set to empty string if not otherwise specified
    exists = tn_block.stream_exists(data_provider="", stream_id=test_stream_id)
    assert exists, "The stream should exist after deployment"

    # Clean up: destroy the deployed stream
    destroy_tx = tn_block.destroy_stream(test_stream_id, wait=True)
    assert destroy_tx is not None, "Stream destruction should return a transaction hash"


def test_init_stream(tn_block: TNAccessBlock, test_stream_id: str):
    """Test initializing a deployed stream."""
    # First deploy the stream
    deploy_tx = tn_block.deploy_stream(test_stream_id, wait=True)
    assert deploy_tx is not None, "Deploy transaction hash should be returned"
    
    # Initialize the stream
    init_tx = tn_block.init_stream(test_stream_id, wait=True)
    assert init_tx is not None, "Init transaction hash should be returned"
    
    # After initialization, the stream should be empty
    record = tn_block.get_first_record(test_stream_id)
    assert record is None, "The stream should be empty after initialization"
    
    # Clean up: destroy the stream
    destroy_tx = tn_block.destroy_stream(test_stream_id, wait=True)
    assert destroy_tx is not None, "Stream destruction should return a transaction hash"


def test_get_stream_type(tn_block: TNAccessBlock, test_stream_id: str):
    """Test checking stream type status."""
    # Initially, for a non-existent stream, it should error out
    with pytest.raises(Exception, match="stream not found"):
        tn_block.get_stream_type("", test_stream_id)
    
    # Deploy the stream
    deploy_tx = tn_block.deploy_stream(test_stream_id, wait=True)
    assert deploy_tx is not None, "Deploy transaction hash should be returned"
    
    # After deployment but before initialization, should error out
    with pytest.raises(Exception, match="no type found"):
        tn_block.get_stream_type("", test_stream_id)
    
    # Initialize the stream
    init_tx = tn_block.init_stream(test_stream_id, wait=True)
    assert init_tx is not None, "Init transaction hash should be returned"
    
    # Now, after initialization, it should return a valid stream type
    stream_type = tn_block.get_stream_type("", test_stream_id)
    assert stream_type is not None, "Initialized stream should return a valid stream type"
    
    # Clean up: destroy the stream
    destroy_tx = tn_block.destroy_stream(test_stream_id, wait=True)
    assert destroy_tx is not None, "Stream destruction should return a transaction hash" 