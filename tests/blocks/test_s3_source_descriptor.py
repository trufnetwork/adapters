import logging
import os
from collections.abc import Generator
from unittest.mock import patch

from mypy_boto3_s3 import S3Client

import pandas as pd
from moto import mock_aws
from pandera.typing import DataFrame
from prefect_aws import S3Bucket # type: ignore
import pytest

from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel, S3SourceDescriptor


# Define a constant for the bucket name
TEST_BUCKET_NAME = "test-s3-descriptor-bucket"


@pytest.fixture
def mock_logger():
    logger = logging.getLogger("test_logger")
    logger.setLevel(logging.INFO)
    return logger


# Add aws_credentials fixture
@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def mock_s3_bucket_block(aws_credentials: None) -> Generator[S3Bucket, None, None]:
    """Creates a mock S3 bucket using moto and yields a real S3Bucket block."""
    with mock_aws():
        _ = aws_credentials  # Ensure credentials are set
        s3_block = S3Bucket(bucket_name=TEST_BUCKET_NAME)
        s3_client: S3Client = s3_block._get_s3_client() # type: ignore

        s3_client.create_bucket(Bucket=TEST_BUCKET_NAME)
        yield s3_block


@pytest.fixture
def sample_dataframe():
    # Create a sample DataFrame matching PrimitiveSourceDataModel schema
    data = {
        "stream_id": ["stream1", "stream2", "stream3"],
        # we add na, because we want to ensure it is not treated as NaN
        # as we had in a previous bug
        "source_id": ["src1", "src2", "NA"],
        "source_type": ["type1", "type2", "type3"],
    }
    return DataFrame[PrimitiveSourceDataModel](data)


def test_set_and_get_descriptor(mock_s3_bucket_block: S3Bucket, sample_dataframe: DataFrame[PrimitiveSourceDataModel], mock_logger: logging.Logger):
    # Initialize S3SourceDescriptor with the moto-backed S3 bucket and a dummy file path
    descriptor = S3SourceDescriptor(s3_bucket=mock_s3_bucket_block, file_path="dummy_path.csv.zip")

    # Write the sample DataFrame using set_sources
    descriptor.set_sources(sample_dataframe)

    # Retrieve the DataFrame using get_descriptor
    df_result = descriptor.get_descriptor()

    # assert the type of both is correct
    PrimitiveSourceDataModel.validate(df_result)
    PrimitiveSourceDataModel.validate(sample_dataframe)

    # The retrieved DataFrame should match the original sample dataframe
    pd.testing.assert_frame_equal(sample_dataframe.reset_index(drop=True), df_result.reset_index(drop=True))
