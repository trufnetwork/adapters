"""
Tests for the SqlAlchemySourceDescriptor block.
"""

import pandas as pd
from pandera.typing import DataFrame
from prefect_sqlalchemy import SqlAlchemyConnector, ConnectionComponents  # type: ignore
import pytest
from sqlalchemy import Table, select  # Add select
from sqlalchemy.orm import Session
from tests.fixtures.test_sql import DB_CONFIG

from tsn_adapters.blocks.models import primitive_sources_table  # Import the table
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
def sample_data_3_update(sample_data_1: DataFrame[PrimitiveSourceDataModel]) -> DataFrame[PrimitiveSourceDataModel]:
    """Provides data to update sample_data_1 (changes source_id for stream1)."""
    df = sample_data_1.copy()
    # Change source_id for stream1
    df.loc[df["stream_id"] == "stream1", "source_id"] = "srcA_updated"
    # Add a new stream
    new_row = pd.DataFrame({"stream_id": ["stream_new"], "source_id": ["srcNew"], "source_type": ["typeNew"]})
    df = pd.concat([df, new_row], ignore_index=True)
    return DataFrame[PrimitiveSourceDataModel](df)


@pytest.fixture
def sample_data_4_same(sample_data_1: DataFrame[PrimitiveSourceDataModel]) -> DataFrame[PrimitiveSourceDataModel]:
    """Provides identical data to sample_data_1 for no-op test."""
    return sample_data_1.copy()


@pytest.fixture
def sql_connector() -> SqlAlchemyConnector:
    """Creates a SqlAlchemyConnector block instance for testing.

    Uses the DB_CONFIG from test_sql.py to ensure consistent credentials.
    """
    # Create connection_info directly from DB_CONFIG
    conn_info = ConnectionComponents(
        driver="postgresql+psycopg2",
        username=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["dbname"],
    )

    # Initialize connector with the connection_info dictionary
    connector = SqlAlchemyConnector(connection_info=conn_info)
    return connector


@pytest.fixture
def sql_descriptor(sql_connector: SqlAlchemyConnector) -> SqlAlchemySourceDescriptor:
    """Creates a SqlAlchemySourceDescriptor instance using the test connector."""
    return SqlAlchemySourceDescriptor(sql_connector=sql_connector, source_type="typeX")


# Helper function to fetch data directly using SQLAlchemy for verification
def get_data_from_db(session: Session, table: Table) -> pd.DataFrame:
    stmt = select(table.c.stream_id, table.c.source_id, table.c.source_type)
    with session.connection() as connection:
        df = pd.read_sql(stmt, connection)
    return df


# --- Test Cases ---


def test_get_descriptor_empty(db_session: Session, sql_descriptor: SqlAlchemySourceDescriptor):
    """Test get_descriptor on an empty table."""
    result = sql_descriptor.get_descriptor()
    assert result.empty
    # Validate schema of empty dataframe
    PrimitiveSourceDataModel.validate(result)


def test_set_sources_and_get_descriptor(
    db_session: Session, sql_descriptor: SqlAlchemySourceDescriptor, sample_data_1: DataFrame[PrimitiveSourceDataModel]
):
    """Test setting sources and then getting them back."""
    # Set initial data
    sql_descriptor.set_sources(sample_data_1)

    # Get data back
    result = sql_descriptor.get_descriptor()

    # Validate results
    PrimitiveSourceDataModel.validate(result)
    pd.testing.assert_frame_equal(
        sample_data_1.reset_index(drop=True), result.reset_index(drop=True), check_dtype=False
    )


def test_set_sources_overwrite(
    db_session: Session,
    sql_descriptor: SqlAlchemySourceDescriptor,
    sample_data_1: DataFrame[PrimitiveSourceDataModel],
    sample_data_2: DataFrame[PrimitiveSourceDataModel],
):
    """Test that set_sources overwrites existing data."""
    # Set initial data
    sql_descriptor.set_sources(sample_data_1)

    # Overwrite with new data
    sql_descriptor.set_sources(sample_data_2)

    # Get data back
    result = sql_descriptor.get_descriptor()

    # Validate results - should match sample_data_2
    PrimitiveSourceDataModel.validate(result)
    pd.testing.assert_frame_equal(
        sample_data_2.reset_index(drop=True), result.reset_index(drop=True), check_dtype=False
    )
    # Ensure data1 is gone (optional check, length comparison is a good proxy)
    assert len(result) == len(sample_data_2)


# --- New Upsert Tests ---


def test_upsert_sources_insert(
    db_session: Session, sql_descriptor: SqlAlchemySourceDescriptor, sample_data_1: DataFrame[PrimitiveSourceDataModel]
):
    """Test upsert_sources inserting data into an empty table."""
    # Initial state: empty table
    initial_data = get_data_from_db(db_session, primitive_sources_table)
    assert initial_data.empty

    # Act: Upsert data
    sql_descriptor.upsert_sources(sample_data_1)

    # Assert: Data should be inserted
    result_df = get_data_from_db(db_session, primitive_sources_table)
    PrimitiveSourceDataModel.validate(result_df)
    pd.testing.assert_frame_equal(
        sample_data_1.sort_values("stream_id").reset_index(drop=True),
        result_df.sort_values("stream_id").reset_index(drop=True),
        check_dtype=False,
    )


def test_upsert_sources_update_and_insert(
    db_session: Session,
    sql_descriptor: SqlAlchemySourceDescriptor,
    sample_data_1: DataFrame[PrimitiveSourceDataModel],
    sample_data_3_update: DataFrame[PrimitiveSourceDataModel],
):
    """Test upsert_sources updating existing rows and inserting new ones."""
    # Setup: Insert initial data (sample_data_1)
    sql_descriptor.set_sources(sample_data_1)
    initial_data = get_data_from_db(db_session, primitive_sources_table)
    assert len(initial_data) == 2

    # Act: Upsert with updated/new data (sample_data_3_update)
    sql_descriptor.upsert_sources(sample_data_3_update)

    # Assert: stream1 should be updated, stream2 unchanged, stream_new inserted
    result_df = get_data_from_db(db_session, primitive_sources_table)
    PrimitiveSourceDataModel.validate(result_df)
    # Expected data should match sample_data_3_update
    pd.testing.assert_frame_equal(
        sample_data_3_update.sort_values("stream_id").reset_index(drop=True),
        result_df.sort_values("stream_id").reset_index(drop=True),
        check_dtype=False,
    )
    assert len(result_df) == 3  # One updated, one same, one new


def test_upsert_sources_no_change(
    db_session: Session,
    sql_descriptor: SqlAlchemySourceDescriptor,
    sample_data_1: DataFrame[PrimitiveSourceDataModel],
    sample_data_4_same: DataFrame[PrimitiveSourceDataModel],
):
    """Test upsert_sources when input data matches existing data (no update)."""
    # Setup: Insert initial data
    sql_descriptor.set_sources(sample_data_1)
    initial_data = get_data_from_db(db_session, primitive_sources_table)
    assert len(initial_data) == 2

    # Act: Upsert with identical data
    sql_descriptor.upsert_sources(sample_data_4_same)

    # Assert: Data should remain unchanged
    result_df = get_data_from_db(db_session, primitive_sources_table)
    PrimitiveSourceDataModel.validate(result_df)
    # Data should still match sample_data_1 (or sample_data_4_same)
    pd.testing.assert_frame_equal(
        sample_data_1.sort_values("stream_id").reset_index(drop=True),
        result_df.sort_values("stream_id").reset_index(drop=True),
        check_dtype=False,
    )
    assert len(result_df) == 2
    # Ideally, check updated_at timestamp didn't change, but that requires fetching timestamps


def test_upsert_sources_empty_input(
    db_session: Session, sql_descriptor: SqlAlchemySourceDescriptor, sample_data_1: DataFrame[PrimitiveSourceDataModel]
):
    """Test upsert_sources with an empty DataFrame input."""
    # Setup: Insert initial data
    sql_descriptor.set_sources(sample_data_1)
    initial_data = get_data_from_db(db_session, primitive_sources_table)
    assert len(initial_data) == 2

    # Act: Upsert with empty data
    empty_df = DataFrame[PrimitiveSourceDataModel](
        {col: [] for col in PrimitiveSourceDataModel.to_schema().columns.keys()}
    )
    sql_descriptor.upsert_sources(empty_df)

    # Assert: Data should remain unchanged
    result_df = get_data_from_db(db_session, primitive_sources_table)
    PrimitiveSourceDataModel.validate(result_df)
    pd.testing.assert_frame_equal(
        sample_data_1.sort_values("stream_id").reset_index(drop=True),
        result_df.sort_values("stream_id").reset_index(drop=True),
        check_dtype=False,
    )
    assert len(result_df) == 2
