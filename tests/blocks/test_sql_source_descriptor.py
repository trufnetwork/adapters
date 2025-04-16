"""
Tests for the SqlAlchemySourceDescriptor block, focusing on source_type isolation.
"""

from collections.abc import Generator

import pandas as pd
from pandera.typing import DataFrame
from prefect_sqlalchemy import SqlAlchemyConnector  # type: ignore
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from tsn_adapters.blocks.models import primitive_sources_table  # Corrected import path
from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel
from tsn_adapters.blocks.sql_source_descriptor import SqlAlchemySourceDescriptor
from tests.fixtures.test_sql import DB_CONFIG

# --- Fixtures ---


@pytest.fixture
def test_source_type() -> str:
    return "typeX"


@pytest.fixture
def other_source_type() -> str:
    return "typeY"


@pytest.fixture
def db_url() -> str:
    """Constructs the database URL from DB_CONFIG."""
    # Assuming DB_CONFIG is imported or defined appropriately
    return (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )


@pytest.fixture(scope="function")
def sql_connector(db_url: str) -> Generator[SqlAlchemyConnector, None, None]:
    """Provides a configured SqlAlchemyConnector instance using a DB URL."""
    connector = SqlAlchemyConnector(connection_info=db_url)
    yield connector


@pytest.fixture
def sql_descriptor(sql_connector: SqlAlchemyConnector, test_source_type: str) -> SqlAlchemySourceDescriptor:
    """Creates a SqlAlchemySourceDescriptor instance configured for test_source_type."""
    return SqlAlchemySourceDescriptor(sql_connector=sql_connector, source_type=test_source_type)


@pytest.fixture
def initial_mixed_data(test_source_type: str, other_source_type: str) -> pd.DataFrame:
    """Provides initial data with mixed source_types."""
    data = {
        "stream_id": ["s1", "s2", "s3"],
        "source_id": ["srcA", "srcB", "srcC"],
        "source_type": [test_source_type, other_source_type, test_source_type],
        # Add other columns needed by table schema with defaults if necessary
        "is_deployed": [False, False, False],
        "deployed_at": [pd.NaT, pd.NaT, pd.NaT],
    }
    df = pd.DataFrame(data)
    # Ensure correct dtypes matching the table/model if needed, especially for timestamps
    df["deployed_at"] = pd.to_datetime(df["deployed_at"], utc=True)
    return df


@pytest.fixture
def valid_input_data(test_source_type: str) -> DataFrame[PrimitiveSourceDataModel]:
    """Provides valid data matching the test_source_type for set/upsert."""
    data = {
        "stream_id": ["s1", "s_new"],  # Update s1, add s_new
        "source_id": ["srcA_updated", "srcNew"],
        "source_type": [test_source_type, test_source_type],
    }
    return DataFrame[PrimitiveSourceDataModel](data)


@pytest.fixture
def mismatched_input_data(test_source_type: str, other_source_type: str) -> DataFrame[PrimitiveSourceDataModel]:
    """Provides data with mixed source_types for testing error conditions."""
    data = {
        "stream_id": ["s1", "s_other"],
        "source_id": ["srcA_mixed", "srcOther"],
        "source_type": [test_source_type, other_source_type],
    }
    return DataFrame[PrimitiveSourceDataModel](data)


@pytest.fixture
def empty_input_data() -> DataFrame[PrimitiveSourceDataModel]:
    """Provides an empty DataFrame conforming to the model."""
    return DataFrame[PrimitiveSourceDataModel]({col: [] for col in PrimitiveSourceDataModel.to_schema().columns.keys()})


# --- Helper ---


def get_all_data_from_db(session: Session) -> pd.DataFrame:
    """Fetches all rows and relevant columns directly from the database."""
    query = "SELECT stream_id, source_id, source_type, is_deployed, deployed_at FROM primitive_sources"
    # Use the connection directly from the session without a 'with' block managing its lifecycle here.
    # The db_session fixture is responsible for opening and closing the connection.
    connection = session.connection()
    df = pd.read_sql(text(query), connection)
    # Standardize types for comparison, especially timestamps
    if "deployed_at" in df.columns:
        df["deployed_at"] = pd.to_datetime(df["deployed_at"], utc=True)
    if "is_deployed" in df.columns:
        df["is_deployed"] = df["is_deployed"].astype(bool)
    return df


def setup_table_with_data(session: Session, engine: Engine, data: pd.DataFrame):
    """Helper to clear and insert data into the test table."""
    table = primitive_sources_table
    with engine.connect() as connection:
        with connection.begin():  # Use transaction
            # Clear existing data
            connection.execute(table.delete())
            # Insert new data if not empty
            if not data.empty:
                data.to_sql(table.name, connection, if_exists="append", index=False)


# --- Tests ---


def test_get_descriptor_empty(db_session: Session, sql_descriptor: SqlAlchemySourceDescriptor):
    """Test get_descriptor returns an empty, correctly typed DataFrame when table is empty."""
    result = sql_descriptor.get_descriptor()
    assert result.empty
    PrimitiveSourceDataModel.validate(result)  # Validate schema


def test_get_descriptor_returns_only_own_type(
    db_session: Session,
    db_engine: Engine,  # Assuming db_engine fixture provides the engine
    sql_descriptor: SqlAlchemySourceDescriptor,
    initial_mixed_data: pd.DataFrame,
    test_source_type: str,
):
    """Test get_descriptor filters and returns only rows matching its source_type."""
    # Setup: Insert mixed data
    setup_table_with_data(db_session, db_engine, initial_mixed_data)

    # Act
    result_df = sql_descriptor.get_descriptor()

    # Assert: Only typeX rows should be returned
    expected_df = initial_mixed_data[initial_mixed_data["source_type"] == test_source_type][
        PrimitiveSourceDataModel.to_schema().columns.keys()  # Select only model columns
    ].reset_index(drop=True)

    PrimitiveSourceDataModel.validate(result_df)
    pd.testing.assert_frame_equal(
        result_df.sort_values("stream_id").reset_index(drop=True),
        expected_df.sort_values("stream_id").reset_index(drop=True),
        check_dtype=False,  # Dtype checks can be strict, focus on content for isolation
    )


def test_set_sources_isolation_and_correctness(
    db_session: Session,
    db_engine: Engine,
    sql_descriptor: SqlAlchemySourceDescriptor,
    initial_mixed_data: pd.DataFrame,
    valid_input_data: DataFrame[PrimitiveSourceDataModel],
    test_source_type: str,
    other_source_type: str,
):
    """
    Test set_sources:
    1. Correctly overwrites ONLY its own source_type.
    2. Leaves other source types untouched.
    """
    # --- Test correct overwrite while ignoring other types ---
    setup_table_with_data(db_session, db_engine, initial_mixed_data)  # Contains typeY
    initial_other_type_data = initial_mixed_data[initial_mixed_data["source_type"] == other_source_type].copy()

    # Act: Set sources with new typeX data - should NOT raise error anymore
    sql_descriptor.set_sources(valid_input_data)

    # Assert: Database should contain valid_input_data (typeX) AND the original typeY data
    db_state = get_all_data_from_db(db_session)

    # 1. Check other type data is untouched
    final_other_type_data = db_state[db_state["source_type"] == other_source_type]
    pd.testing.assert_frame_equal(
        initial_other_type_data[["stream_id", "source_id", "source_type"]]
        .sort_values("stream_id")
        .reset_index(drop=True),
        final_other_type_data[["stream_id", "source_id", "source_type"]]
        .sort_values("stream_id")
        .reset_index(drop=True),
        check_dtype=False,
    )

    # 2. Check own type data reflects the new input data
    final_own_type_data = db_state[db_state["source_type"] == test_source_type]
    expected_final_own_data = valid_input_data[["stream_id", "source_id", "source_type"]].copy()

    pd.testing.assert_frame_equal(
        final_own_type_data[["stream_id", "source_id", "source_type"]].sort_values("stream_id").reset_index(drop=True),
        expected_final_own_data.sort_values("stream_id").reset_index(drop=True),
        check_dtype=False,
    )

    # 3. Check total row count
    assert len(db_state) == len(valid_input_data) + len(initial_other_type_data)


def test_set_sources_empty_clears_own_type(
    db_session: Session,
    db_engine: Engine,
    sql_descriptor: SqlAlchemySourceDescriptor,
    initial_mixed_data: pd.DataFrame,
    empty_input_data: DataFrame[PrimitiveSourceDataModel],
    test_source_type: str,
    other_source_type: str,
):
    """Test set_sources with empty DF clears own type, leaves others."""
    # Setup: Insert mixed data
    setup_table_with_data(db_session, db_engine, initial_mixed_data)

    # Act: Set with empty data
    sql_descriptor.set_sources(empty_input_data)  # Should only delete typeX

    # Assert: typeX rows gone, typeY remains
    db_state = get_all_data_from_db(db_session)
    assert not any(db_state["source_type"] == test_source_type)
    assert any(db_state["source_type"] == other_source_type)
    assert len(db_state) == len(initial_mixed_data[initial_mixed_data["source_type"] == other_source_type])


def test_set_sources_raises_on_mismatched_input(
    sql_descriptor: SqlAlchemySourceDescriptor, mismatched_input_data: DataFrame[PrimitiveSourceDataModel]
):
    """Test set_sources raises ValueError if input contains wrong source_type."""
    with pytest.raises(ValueError, match="Input descriptor contains rows with source_type"):
        sql_descriptor.set_sources(mismatched_input_data)


def test_upsert_sources_isolation_and_correctness(
    db_session: Session,
    db_engine: Engine,
    sql_descriptor: SqlAlchemySourceDescriptor,
    initial_mixed_data: pd.DataFrame,
    valid_input_data: DataFrame[PrimitiveSourceDataModel],
    test_source_type: str,
    other_source_type: str,
):
    """
    Test upsert_sources:
    1. Inserts new rows of its source_type.
    2. Updates existing rows of its source_type.
    3. Leaves rows of other source_types untouched.
    """
    # Setup: Insert mixed data
    setup_table_with_data(db_session, db_engine, initial_mixed_data)
    initial_other_type_data = get_all_data_from_db(db_session)[lambda df: df["source_type"] == other_source_type].copy()

    # Act: Upsert valid data (updates s1, inserts s_new, all typeX)
    sql_descriptor.upsert_sources(valid_input_data)

    # Assert: Check final state
    db_state = get_all_data_from_db(db_session)

    # 1. Check other type data is untouched
    final_other_type_data = db_state[db_state["source_type"] == other_source_type]
    pd.testing.assert_frame_equal(
        initial_other_type_data.sort_values("stream_id").reset_index(drop=True),
        final_other_type_data.sort_values("stream_id").reset_index(drop=True),
        check_dtype=False,  # Focus on values
    )

    # 2. Check own type data reflects upserts
    final_own_type_data = db_state[db_state["source_type"] == test_source_type]
    # Manually construct expected state for typeX after upsert
    expected_s1_row = valid_input_data[valid_input_data["stream_id"] == "s1"].iloc[0]
    expected_s3_row = initial_mixed_data[initial_mixed_data["stream_id"] == "s3"].iloc[
        0
    ]  # s3 was not in upsert, should be unchanged typeX
    expected_s_new_row = valid_input_data[valid_input_data["stream_id"] == "s_new"].iloc[0]

    assert len(final_own_type_data) == 3  # s1 (updated), s3 (original), s_new (inserted)
    # Check specific rows (more robust than full DataFrame comparison here)
    s1_final = final_own_type_data[final_own_type_data["stream_id"] == "s1"].iloc[0]
    s3_final = final_own_type_data[final_own_type_data["stream_id"] == "s3"].iloc[0]
    s_new_final = final_own_type_data[final_own_type_data["stream_id"] == "s_new"].iloc[0]

    assert s1_final["source_id"] == expected_s1_row["source_id"]
    assert s3_final["source_id"] == expected_s3_row["source_id"]  # Should match initial s3
    assert s_new_final["source_id"] == expected_s_new_row["source_id"]


def test_upsert_sources_empty_input_does_nothing(
    db_session: Session,
    db_engine: Engine,
    sql_descriptor: SqlAlchemySourceDescriptor,
    initial_mixed_data: pd.DataFrame,
    empty_input_data: DataFrame[PrimitiveSourceDataModel],
):
    """Test upsert_sources with empty input leaves DB unchanged."""
    # Setup: Insert mixed data
    setup_table_with_data(db_session, db_engine, initial_mixed_data)
    initial_db_state = get_all_data_from_db(db_session)

    # Act: Upsert empty data
    sql_descriptor.upsert_sources(empty_input_data)

    # Assert: DB state is identical
    final_db_state = get_all_data_from_db(db_session)
    pd.testing.assert_frame_equal(
        initial_db_state.sort_values(["source_type", "stream_id"]).reset_index(drop=True),
        final_db_state.sort_values(["source_type", "stream_id"]).reset_index(drop=True),
        check_dtype=False,  # Focus on values
    )


def test_upsert_sources_raises_on_mismatched_input(
    sql_descriptor: SqlAlchemySourceDescriptor, mismatched_input_data: DataFrame[PrimitiveSourceDataModel]
):
    """Test upsert_sources raises ValueError if input contains wrong source_type."""
    with pytest.raises(ValueError, match="Input descriptor contains rows with source_type"):
        sql_descriptor.upsert_sources(mismatched_input_data)
