from unittest.mock import MagicMock

import pandas as pd
from pandera.typing import DataFrame
import pytest
from pytest import LogCaptureFixture

from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    S3SourceDescriptor,
)
from tsn_adapters.tasks.argentina.tasks.descriptor_tasks import DescriptorError, load_product_descriptor

# Sample valid BASE descriptor data (columns from PrimitiveSourceDataModel)
SAMPLE_DESCRIPTOR_DATA = {
    "stream_id": ["s1", "s2"],
    "source_id": ["p1", "p2"],
    "source_type": ["argentina_sepa_product"] * 2,
}


@pytest.fixture
def mock_s3_descriptor_block() -> MagicMock:
    """Fixture for a mocked S3SourceDescriptor block."""
    mock_block = MagicMock(spec=S3SourceDescriptor)
    mock_block.file_path = "mock/path/descriptor.csv.zip"
    # Mock the get_descriptor method directly on the instance
    mock_block.get_descriptor = MagicMock()
    return mock_block


@pytest.mark.asyncio
async def test_load_product_descriptor_success(mock_s3_descriptor_block: MagicMock):
    """Test successful loading of a valid product descriptor."""
    # Arrange
    # Create DataFrame conforming to the BASE model
    valid_df = DataFrame[PrimitiveSourceDataModel](pd.DataFrame(SAMPLE_DESCRIPTOR_DATA))
    mock_s3_descriptor_block.get_descriptor.return_value = valid_df

    # Act
    result_df = await load_product_descriptor.fn(descriptor_block=mock_s3_descriptor_block)

    # Assert
    mock_s3_descriptor_block.get_descriptor.assert_called_once()
    # Ensure the returned DataFrame matches the expected BASE model structure
    pd.testing.assert_frame_equal(result_df, valid_df)


@pytest.mark.asyncio
async def test_load_product_descriptor_empty_dataframe(mock_s3_descriptor_block: MagicMock, caplog: LogCaptureFixture):
    """Test that an empty descriptor DataFrame raises DescriptorError."""
    # Arrange
    # Create empty DataFrame conforming to the BASE model
    empty_df = DataFrame[PrimitiveSourceDataModel](pd.DataFrame(columns=list(SAMPLE_DESCRIPTOR_DATA.keys())))
    mock_s3_descriptor_block.get_descriptor.return_value = empty_df

    # Act & Assert
    with pytest.raises(DescriptorError, match="descriptor loaded from .* is empty"):
        await load_product_descriptor.fn(descriptor_block=mock_s3_descriptor_block)

    mock_s3_descriptor_block.get_descriptor.assert_called_once()
    assert "Product descriptor loaded from mock/path/descriptor.csv.zip is empty" in caplog.text
    assert "ERROR" in caplog.text


@pytest.mark.asyncio
async def test_load_product_descriptor_block_exception(mock_s3_descriptor_block: MagicMock, caplog: LogCaptureFixture):
    """Test that an exception from the block's get_descriptor raises DescriptorError."""
    # Arrange
    test_exception = ValueError("Simulated block error")
    mock_s3_descriptor_block.get_descriptor.side_effect = test_exception

    # Act & Assert
    with pytest.raises(DescriptorError, match="Failed to load or validate product descriptor") as exc_info:
        await load_product_descriptor.fn(descriptor_block=mock_s3_descriptor_block)

    mock_s3_descriptor_block.get_descriptor.assert_called_once()
    assert "Failed to load or validate product descriptor" in caplog.text
    assert "Simulated block error" in caplog.text
    assert "ERROR" in caplog.text
    # Check that the original exception is chained
    assert exc_info.value.__cause__ is test_exception
