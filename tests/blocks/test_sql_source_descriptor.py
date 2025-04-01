"""
Tests for the SqlAlchemySourceDescriptor block.
"""


import pandas as pd
from pandera.typing import DataFrame
from prefect_sqlalchemy import SqlAlchemyConnector  # type: ignore
import pytest
from sqlalchemy.orm import Session
from tests.fixtures.test_sql import DB_CONFIG

from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel
from tsn_adapters.blocks.sql_source_descriptor import SqlAlchemySourceDescriptor

# Assuming db_session fixture is available from conftest.py
# Assuming db_engine fixture is available from conftest.py

@pytest.fixture
def sample_data_1() -> DataFrame[PrimitiveSourceDataModel]:
    """Provides a sample DataFrame for testing."""
    data = {
        "stream_id": ["stream1", "stream2"],
        "source_id": ["srcA", "srcB"],
        "source_type": ["typeX", "typeY"],
    }
    return DataFrame[PrimitiveSourceDataModel](data)


@pytest.fixture
def sample_data_2() -> DataFrame[PrimitiveSourceDataModel]:
    """Provides another sample DataFrame for testing overwrites."""
    data = {
        "stream_id": ["stream3", "stream4"],
        "source_id": ["srcC", "srcD"],
        "source_type": ["typeZ", "typeW"],
    }
    return DataFrame[PrimitiveSourceDataModel](data)


@pytest.fixture
def sources_data() -> pd.DataFrame:
    """Provides sample data for the sources table."""
    return pd.DataFrame(
        {
            "id": ["ar_indec_product_catalog", "ar_indec_product_prices"],
            "source": ["AR_INDEC", "AR_INDEC"],
            "description": [
                "Catalog of products used in Argentina's CPI calculation.",
                "Monthly prices for products in Argentina's CPI.",
            ],
            "last_updated": pd.to_datetime(["2023-10-01", "2023-10-26"]),
            "metadata": [
                '{"frequency": "monthly", "region": "national"}',
                '{"frequency": "monthly", "unit": "ARS"}',
            ],
        }
    ).set_index("id")


@pytest.fixture
def sql_connector() -> SqlAlchemyConnector:
    """Creates a SqlAlchemyConnector block instance for testing.
    
    Uses the DB_CONFIG from test_sql.py to ensure consistent credentials.
    """
    # Create connection_info directly from DB_CONFIG
    conn_info = {
        "driver": "postgresql+psycopg2", 
        "username": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "host": DB_CONFIG["host"],
        "port": DB_CONFIG["port"],
        "database": DB_CONFIG["dbname"]
    }

    # Initialize connector with the connection_info dictionary
    connector = SqlAlchemyConnector(connection_info=conn_info)
    return connector


@pytest.fixture
def sql_descriptor(sql_connector: SqlAlchemyConnector) -> SqlAlchemySourceDescriptor:
    """Creates a SqlAlchemySourceDescriptor instance using the test connector."""
    return SqlAlchemySourceDescriptor(sql_connector=sql_connector)


# --- Test Cases ---

def test_get_descriptor_empty(db_session: Session, sql_descriptor: SqlAlchemySourceDescriptor):
    """Test get_descriptor on an empty table."""
    result = sql_descriptor.get_descriptor()
    assert result.empty
    # Validate schema of empty dataframe
    PrimitiveSourceDataModel.validate(result)


def test_set_sources_and_get_descriptor(db_session: Session, sql_descriptor: SqlAlchemySourceDescriptor, sample_data_1: DataFrame[PrimitiveSourceDataModel]):
    """Test setting sources and then getting them back."""
    # Set initial data
    sql_descriptor.set_sources(sample_data_1)
    
    # Get data back
    result = sql_descriptor.get_descriptor()
    
    # Validate results
    PrimitiveSourceDataModel.validate(result)
    pd.testing.assert_frame_equal(sample_data_1.reset_index(drop=True), result.reset_index(drop=True), check_dtype=False)


def test_set_sources_overwrite(db_session: Session, sql_descriptor: SqlAlchemySourceDescriptor, sample_data_1: DataFrame[PrimitiveSourceDataModel], sample_data_2: DataFrame[PrimitiveSourceDataModel]):
    """Test that set_sources overwrites existing data."""
    # Set initial data
    sql_descriptor.set_sources(sample_data_1)
    
    # Overwrite with new data
    sql_descriptor.set_sources(sample_data_2)
    
    # Get data back
    result = sql_descriptor.get_descriptor()
    
    # Validate results - should match sample_data_2
    PrimitiveSourceDataModel.validate(result)
    pd.testing.assert_frame_equal(sample_data_2.reset_index(drop=True), result.reset_index(drop=True), check_dtype=False)
    # Ensure data1 is gone (optional check, length comparison is a good proxy)
    assert len(result) == len(sample_data_2)


def test_upsert_sources_not_implemented_yet(db_session: Session, sql_descriptor: SqlAlchemySourceDescriptor, sample_data_1: DataFrame[PrimitiveSourceDataModel]):
    """Test that the placeholder upsert_sources raises NotImplementedError."""
    with pytest.raises(NotImplementedError):
        sql_descriptor.upsert_sources(sample_data_1)
