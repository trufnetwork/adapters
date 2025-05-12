"""
Tests for shared fixtures in tests/argentina/conftest.py.
"""

from mypy_boto3_s3 import S3Client
import pandas as pd
from prefect_aws import S3Bucket  # type: ignore
from prefect_sqlalchemy import SqlAlchemyConnector  # type: ignore
import pytest
from sqlalchemy import text

from tsn_adapters.blocks.deployment_state import DeploymentStateModel
from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel
from tsn_adapters.blocks.sql_deployment_state import SqlAlchemyDeploymentState
from tsn_adapters.blocks.sql_source_descriptor import SqlAlchemySourceDescriptor

from .conftest import TEST_E2E_BUCKET_NAME  # Import the constant


@pytest.mark.usefixtures("prefect_test_fixture")  # Add prefect context if needed by fixture
def test_s3_bucket_block_fixture(s3_bucket_block: S3Bucket):
    """Verify the s3_bucket_block fixture creates a bucket and client."""
    assert isinstance(s3_bucket_block, S3Bucket)
    assert s3_bucket_block.bucket_name == TEST_E2E_BUCKET_NAME
    assert hasattr(s3_bucket_block, "_get_s3_client"), "Fixture should provide _get_s3_client"

    # Optional: Basic interaction test
    s3_client: S3Client = s3_bucket_block._get_s3_client()  # type: ignore
    test_key = "test-object.txt"
    test_content = b"Hello Moto"
    try:
        s3_client.put_object(Bucket=s3_bucket_block.bucket_name, Key=test_key, Body=test_content)
        response = s3_client.get_object(Bucket=s3_bucket_block.bucket_name, Key=test_key)
        read_content = response["Body"].read()
        assert read_content == test_content
    except Exception as e:
        pytest.fail(f"S3 interaction failed: {e}")


def test_sql_connector_fixture(sql_connector: SqlAlchemyConnector):
    """Verify the sql_connector fixture initializes correctly."""
    assert isinstance(sql_connector, SqlAlchemyConnector)
    assert isinstance(sql_connector.connection_info, str)
    # Basic check for URL format (adapt if using SecretStr)
    assert sql_connector.connection_info.startswith("postgresql+psycopg2://")

    # Optional: Test connection (might be redundant with db_engine check)
    try:
        engine = sql_connector.get_engine()
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except Exception as e:
        pytest.fail(f"sql_connector failed to connect: {e}")


# Add db_session fixture to ensure table creation
def test_sql_descriptor_block_fixture(sql_descriptor_block: SqlAlchemySourceDescriptor, db_session: None):
    """Verify the sql_descriptor_block fixture initializes correctly."""
    assert isinstance(sql_descriptor_block, SqlAlchemySourceDescriptor)
    assert sql_descriptor_block.table_name == "primitive_sources"
    assert sql_descriptor_block.source_type == "argentina_sepa_product"
    # Check if it has the connector attribute
    assert hasattr(sql_descriptor_block, "sql_connector")
    assert isinstance(sql_descriptor_block.sql_connector, SqlAlchemyConnector)

    # Optional: Test get_descriptor returns empty DataFrame on clean DB
    try:
        descriptor_df = sql_descriptor_block.get_descriptor()
        assert isinstance(descriptor_df, pd.DataFrame)
        assert descriptor_df.empty  # Should be empty initially
        # Check columns match the model
        expected_cols = list(PrimitiveSourceDataModel.to_schema().columns.keys())
        assert list(descriptor_df.columns) == expected_cols
    except Exception as e:
        pytest.fail(f"sql_descriptor_block.get_descriptor() failed: {e}")


# Add db_session fixture to ensure table creation
def test_sql_deployment_state_fixture(sql_deployment_state: SqlAlchemyDeploymentState, db_session: None):
    """Verify the sql_deployment_state fixture initializes correctly."""
    assert isinstance(sql_deployment_state, SqlAlchemyDeploymentState)
    assert sql_deployment_state.table_name == "primitive_sources"
    assert sql_deployment_state.source_type == "argentina_sepa_product"
    # Check if it has the connector attribute
    assert hasattr(sql_deployment_state, "sql_connector")
    assert isinstance(sql_deployment_state.sql_connector, SqlAlchemyConnector)

    # Optional: Test a read method returns empty/default on clean DB
    try:
        assert not sql_deployment_state.has_been_deployed("non_existent_stream_id")
        states_df = sql_deployment_state.get_deployment_states()
        assert isinstance(states_df, pd.DataFrame)
        assert states_df.empty
        # Check columns match the model
        expected_cols = list(DeploymentStateModel.to_schema().columns.keys())
        assert list(states_df.columns) == expected_cols
    except Exception as e:
        pytest.fail(f"sql_deployment_state method failed: {e}")
