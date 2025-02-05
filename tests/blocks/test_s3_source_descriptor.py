import io
import gzip
import logging
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from prefect_aws import S3Bucket
from pandera.typing import DataFrame
from tsn_adapters.blocks.primitive_source_descriptor import S3SourceDescriptor, PrimitiveSourceDataModel
from tsn_adapters.utils.deroutine import deroutine

@pytest.fixture
def mock_logger():
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.INFO)
    return logger


@pytest.fixture
def fake_s3_bucket():
    mock_bucket = MagicMock(spec=S3Bucket)
    mock_bucket.bucket_name = "fake-bucket"
    stored_content = {}

    async def mock_write_path(path, content):
        stored_content[path] = content
        return path

    async def mock_read_path(path):
        if path not in stored_content:
            raise ValueError("No content stored")
        return stored_content[path]

    # Use sync wrappers that handle the async operations
    def sync_write_path(path, content):
        return deroutine(mock_write_path(path, content))

    def sync_read_path(path):
        return deroutine(mock_read_path(path))

    mock_bucket.write_path = sync_write_path
    mock_bucket.read_path = sync_read_path
    return mock_bucket


@pytest.fixture
def sample_dataframe():
    # Create a sample DataFrame matching PrimitiveSourceDataModel schema
    data = {
        'stream_id': ['stream1', 'stream2', 'stream3'],
        # we add na, because we want to ensure it is not treated as NaN
        # as we had in a previous bug
        'source_id': ['src1', 'src2', 'NA'],
        'source_type': ['type1', 'type2', 'type3'],
    }
    return DataFrame[PrimitiveSourceDataModel](data)


def test_set_and_get_descriptor(fake_s3_bucket, sample_dataframe, mock_logger):
    # Mock the get_run_logger to avoid Prefect context issues
    with patch('tsn_adapters.blocks.primitive_source_descriptor.get_run_logger', return_value=mock_logger):
        # Initialize S3SourceDescriptor with the fake S3 bucket and a dummy file path
        descriptor = S3SourceDescriptor(s3_bucket=fake_s3_bucket, file_path='dummy_path.csv.gz')

        # Write the sample DataFrame using set_sources
        descriptor.set_sources(sample_dataframe)

        # Retrieve the DataFrame using get_descriptor
        df_result = descriptor.get_descriptor()

        # assert the type of both is correct
        PrimitiveSourceDataModel.validate(df_result)
        PrimitiveSourceDataModel.validate(sample_dataframe)

        # The retrieved DataFrame should match the original sample dataframe
        pd.testing.assert_frame_equal(sample_dataframe.reset_index(drop=True), df_result.reset_index(drop=True))


if __name__ == '__main__':
    pytest.main([__file__]) 