"""
Shared fixtures for Argentina tests.
"""
import os
import pytest
from collections.abc import Generator
from typing import Any

import boto3 # type: ignore
from moto import mock_aws
from prefect_aws import S3Bucket # type: ignore
from prefect_sqlalchemy import SqlAlchemyConnector # type: ignore
from tsn_adapters.blocks.sql_source_descriptor import SqlAlchemySourceDescriptor
from tsn_adapters.blocks.sql_deployment_state import SqlAlchemyDeploymentState

# Constant moved from test_aggregate_products_flow.py
TEST_E2E_BUCKET_NAME = "test-e2e-aggregation-bucket"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3_bucket_block(aws_credentials: None, prefect_test_fixture: Any) -> Generator[S3Bucket, None, None]:
    """Creates a mock S3 bucket using moto and returns a real S3Bucket block."""
    _ = aws_credentials  # Ensure credentials are set
    _ = prefect_test_fixture  # Ensure Prefect context if needed
    with mock_aws():
        s3_client = boto3.client("s3", region_name="us-east-1") # type: ignore
        s3_client.create_bucket(Bucket=TEST_E2E_BUCKET_NAME) # type: ignore
        # Instantiate the Prefect block targeting the moto bucket
        s3_block = S3Bucket(bucket_name=TEST_E2E_BUCKET_NAME)
        # Provide access to the underlying boto3 client for direct manipulation if needed
        s3_block._boto_client = s3_client # type: ignore
        yield s3_block


@pytest.fixture(scope="function")
def sql_connector(db_engine_url: str) -> SqlAlchemyConnector:
    """Creates a SqlAlchemyConnector block configured for the test DB engine."""
    connector = SqlAlchemyConnector(
        connection_info=db_engine_url,
    )
    return connector


@pytest.fixture(scope="function")
def sql_descriptor_block(sql_connector: SqlAlchemyConnector) -> SqlAlchemySourceDescriptor:
    """Creates a SqlAlchemySourceDescriptor block configured for the test."""
    descriptor = SqlAlchemySourceDescriptor(
        sql_connector=sql_connector,
        table_name="primitive_sources",
        source_type="argentina_sepa_product",
    )
    return descriptor


@pytest.fixture(scope="function")
def sql_deployment_state(sql_connector: SqlAlchemyConnector) -> SqlAlchemyDeploymentState:
    """Creates a SqlAlchemyDeploymentState block configured for the test."""
    deployment_state_block = SqlAlchemyDeploymentState(
        sql_connector=sql_connector,
        table_name="primitive_sources",
        source_type="argentina_sepa_product",
    )
    return deployment_state_block