"""
Tests for the SqlAlchemyDeploymentState block.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from pandera.typing import DataFrame
from prefect_sqlalchemy import SqlAlchemyConnector  # type: ignore
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine

from tests.fixtures.test_sql import DB_CONFIG
from tsn_adapters.blocks.deployment_state import DeploymentStateModel
from tsn_adapters.blocks.models.sql_models import primitive_sources_table
from tsn_adapters.blocks.sql_deployment_state import SqlAlchemyDeploymentState


@pytest.fixture
def db_url() -> str:
    """Constructs the database URL from DB_CONFIG."""
    return (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )


@pytest.fixture
def sql_connector(db_url: str) -> SqlAlchemyConnector:
    """Provides a configured SqlAlchemyConnector instance using a DB URL."""
    connector = SqlAlchemyConnector(connection_info=db_url)
    return connector


@pytest.fixture
def test_source_type() -> str:
    """Defines the source type used for testing the blocks."""
    return "typeX"


@pytest.fixture
def sql_deployment_state(sql_connector: SqlAlchemyConnector, test_source_type: str) -> SqlAlchemyDeploymentState:
    """Creates a SqlAlchemyDeploymentState instance configured for a specific source type."""
    return SqlAlchemyDeploymentState(sql_connector=sql_connector, source_type=test_source_type)


@pytest.fixture
def initial_source_data() -> pd.DataFrame:
    """Initial data for the primitive_sources table with mixed source_types."""
    data = {
        "stream_id": ["s1", "s2", "s3", "s4"],
        "source_id": ["srcA", "srcB", "srcC", "srcD"],
        "source_type": ["typeX", "typeY", "typeX", "typeZ"],
        # is_deployed and deployed_at default to False/None when inserted
    }
    return pd.DataFrame(data)


@pytest.fixture
def deployed_timestamp() -> datetime:
    """A consistent, timezone-aware timestamp for testing."""
    return datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


# --- Helper functions ---
def setup_table_with_data(session: Session, engine: Engine, data: pd.DataFrame):
    """Sets up the test table with the provided data."""
    with engine.connect() as connection:
        with connection.begin():
            # Clear existing data
            connection.execute(primitive_sources_table.delete())
            # Insert new data
            data.to_sql(primitive_sources_table.name, connection, if_exists="append", index=False)


def get_db_state(session: Session) -> pd.DataFrame:
    """Directly query the table to check its state."""
    query = "SELECT stream_id, source_id, source_type, is_deployed, deployed_at FROM primitive_sources"
    df = pd.read_sql(text(query), session.connection())
    # Standardize datetime for comparison and preserve timezone
    if "deployed_at" in df.columns:
        # Make sure deployed_at is timezone-aware (UTC) to match the model definition
        df["deployed_at"] = pd.to_datetime(df["deployed_at"], utc=True)
    return df


# --- Read Methods Tests ---
def test_read_methods_initial_state(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState,
    initial_source_data: pd.DataFrame,
):
    """Test read methods when no streams are marked (respects source_type)."""
    engine = sql_deployment_state._get_engine() # type: ignore
    setup_table_with_data(db_session, engine, initial_source_data)

    # Test has_been_deployed
    assert not sql_deployment_state.has_been_deployed("s1")  # typeX, expect False
    assert not sql_deployment_state.has_been_deployed("s3")  # typeX, expect False
    assert not sql_deployment_state.has_been_deployed("s2")  # typeY, expect False for typeX block
    assert not sql_deployment_state.has_been_deployed("s_nonexistent")  # Non-existent, expect False

    # Test check_multiple_streams
    stream_ids = ["s1", "s2", "s3", "s4", "s_nonexistent"]
    expected = {"s1": False, "s2": False, "s3": False, "s4": False, "s_nonexistent": False}
    result = sql_deployment_state.check_multiple_streams(stream_ids)
    assert result == expected

    # Test get_deployment_states - checks initial state for the correct source_type
    result_df = sql_deployment_state.get_deployment_states()
    DeploymentStateModel.validate(result_df)
    
    # Expecting the typeX streams (s1, s3) with null/NaT timestamps initially
    expected_df = pd.DataFrame({
        "stream_id": ["s1", "s3"],
        "deployment_timestamp": [pd.NaT, pd.NaT]
    })
    # Model uses DateTime(timezone=True), so expect UTC timestamps
    expected_df["deployment_timestamp"] = pd.to_datetime(expected_df["deployment_timestamp"], utc=True)
    expected_df["stream_id"] = expected_df["stream_id"].astype(str)
    
    pd.testing.assert_frame_equal(
        result_df.sort_values("stream_id").reset_index(drop=True),
        expected_df.sort_values("stream_id").reset_index(drop=True),
        check_dtype=True,
    )


# --- Write Method Tests ---
def test_mark_as_deployed_success(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState,
    initial_source_data: pd.DataFrame,
    deployed_timestamp: datetime,
):
    """Test mark_as_deployed correctly updates an existing stream of the correct type."""
    engine = sql_deployment_state._get_engine() # type: ignore
    setup_table_with_data(db_session, engine, initial_source_data)

    # Act: Mark 's1' which is typeX
    sql_deployment_state.mark_as_deployed("s1", deployed_timestamp)

    # Assert
    db_state = get_db_state(db_session)
    s1_row = db_state[db_state["stream_id"] == "s1"].iloc[0]
    
    # Check s1 (correct type) was updated
    assert s1_row["is_deployed"] == True
    assert s1_row["deployed_at"] == deployed_timestamp
    
    # Check other rows remain unchanged
    for stream_id in ["s2", "s3", "s4"]:
        row = db_state[db_state["stream_id"] == stream_id].iloc[0]
        assert row["is_deployed"] == False
        assert pd.isna(row["deployed_at"])


def test_mark_operations_respect_source_type(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState,
    initial_source_data: pd.DataFrame,
    deployed_timestamp: datetime,
):
    """Test mark operations only affect streams of the correct type."""
    engine = sql_deployment_state._get_engine() # type: ignore
    setup_table_with_data(db_session, engine, initial_source_data)
    initial_db_state = get_db_state(db_session).copy()

    # Try to mark 's2' (typeY - wrong type)
    sql_deployment_state.mark_as_deployed("s2", deployed_timestamp)

    # Try to mark non-existent stream
    sql_deployment_state.mark_as_deployed("s_nonexistent", deployed_timestamp)

    # DB state should be unchanged for both operations
    final_db_state = get_db_state(db_session)
    pd.testing.assert_frame_equal(
        initial_db_state.sort_values("stream_id").reset_index(drop=True),
        final_db_state.sort_values("stream_id").reset_index(drop=True),
        check_datetimelike_compat=True,
        check_dtype=False,
    )


def test_mark_multiple_operations(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState,
    initial_source_data: pd.DataFrame,
    deployed_timestamp: datetime,
):
    """Test multi-write methods update correct streams and ignore others."""
    engine = sql_deployment_state._get_engine() # type: ignore
    setup_table_with_data(db_session, engine, initial_source_data)
    
    # Mixed list of stream_ids: correct type, wrong type, non-existent
    streams_to_mark = ["s1", "s2", "s_nonexistent", "s3", "s4"]
    
    # Act
    sql_deployment_state.mark_multiple_as_deployed(streams_to_mark, deployed_timestamp)
    
    # Assert
    db_state = get_db_state(db_session)
    
    # Only typeX streams should be updated
    for stream_id in ["s1", "s3"]:
        row = db_state[db_state["stream_id"] == stream_id].iloc[0]
        assert row["is_deployed"] == True
        assert row["deployed_at"] == deployed_timestamp
    
    # Other types and non-existent should be unchanged
    for stream_id in ["s2", "s4"]:
        row = db_state[db_state["stream_id"] == stream_id].iloc[0]
        assert row["is_deployed"] == False
        assert pd.isna(row["deployed_at"])


def test_empty_inputs_do_nothing(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState,
    initial_source_data: pd.DataFrame,
    deployed_timestamp: datetime,
):
    """Test operations with empty inputs do nothing."""
    engine = sql_deployment_state._get_engine() # type: ignore
    setup_table_with_data(db_session, engine, initial_source_data)
    initial_db_state = get_db_state(db_session).copy()
    
    # Empty operations
    sql_deployment_state.mark_as_deployed("", deployed_timestamp)
    sql_deployment_state.mark_multiple_as_deployed([], deployed_timestamp)
    empty_df = DataFrame[DeploymentStateModel](columns=["stream_id", "deployment_timestamp"])
    empty_df["deployment_timestamp"] = pd.Series(dtype="datetime64[ns, UTC]")
    empty_df["stream_id"] = pd.Series(dtype="str")
    sql_deployment_state.update_deployment_states(empty_df)
    
    # DB state should be unchanged
    final_db_state = get_db_state(db_session)
    pd.testing.assert_frame_equal(
        initial_db_state.sort_values("stream_id").reset_index(drop=True),
        final_db_state.sort_values("stream_id").reset_index(drop=True),
        check_datetimelike_compat=True,
        check_dtype=False,
    )


def test_read_after_write(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState,
    initial_source_data: pd.DataFrame,
    deployed_timestamp: datetime,
):
    """Test read methods reflect state correctly after marking (respects source_type)."""
    engine = sql_deployment_state._get_engine() # type: ignore
    setup_table_with_data(db_session, engine, initial_source_data)
    
    # Mark the typeX streams
    sql_deployment_state.mark_as_deployed("s1", deployed_timestamp - timedelta(days=1))
    sql_deployment_state.mark_as_deployed("s3", deployed_timestamp)
    
    # Test has_been_deployed
    assert sql_deployment_state.has_been_deployed("s1") == True
    assert sql_deployment_state.has_been_deployed("s3") == True
    assert sql_deployment_state.has_been_deployed("s2") == False
    assert sql_deployment_state.has_been_deployed("s_nonexistent") == False
    
    # Test check_multiple_streams
    check_ids = ["s1", "s2", "s3", "s4", "s_nonexistent"]
    expected_check = {"s1": True, "s2": False, "s3": True, "s4": False, "s_nonexistent": False}
    assert sql_deployment_state.check_multiple_streams(check_ids) == expected_check
    
    # Test get_deployment_states
    deployed_states_df = sql_deployment_state.get_deployment_states()
    DeploymentStateModel.validate(deployed_states_df)
    
    # Should only contain the deployed typeX streams (s1, s3) with correct timestamps
    assert len(deployed_states_df) == 2
    expected_df = pd.DataFrame({
        "stream_id": ["s1", "s3"],
        "deployment_timestamp": [deployed_timestamp - timedelta(days=1), deployed_timestamp]
    })
    # Ensure expected timestamps are timezone-aware (UTC) to match implementation
    expected_df["deployment_timestamp"] = pd.to_datetime(expected_df["deployment_timestamp"], utc=True)
    expected_df["stream_id"] = expected_df["stream_id"].astype(str)
    
    pd.testing.assert_frame_equal(
        deployed_states_df.sort_values("stream_id").reset_index(drop=True),
        expected_df.sort_values("stream_id").reset_index(drop=True),
        check_dtype=True, # Expect correct dtypes including timezone
    )
