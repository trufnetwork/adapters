"""
Tests for the SqlAlchemyDeploymentState block.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
from pandera.typing import DataFrame
from prefect_sqlalchemy import SqlAlchemyConnector  # type: ignore
import pytest
from sqlalchemy.orm import Session

# Import DB_CONFIG and table from fixtures/models
from tests.fixtures.test_sql import DB_CONFIG

from tsn_adapters.blocks.deployment_state import DeploymentStateModel
from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel
from tsn_adapters.blocks.sql_deployment_state import SqlAlchemyDeploymentState
from tsn_adapters.blocks.sql_source_descriptor import SqlAlchemySourceDescriptor  # To insert base data


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
    # Use the database URL string for connection_info
    connector = SqlAlchemyConnector(connection_info=db_url)
    return connector


@pytest.fixture
def test_source_type() -> str:
    """Defines the source type used for testing the blocks."""
    return "typeX"


@pytest.fixture
def sql_deployment_state(sql_connector: SqlAlchemyConnector, test_source_type: str) -> SqlAlchemyDeploymentState:
    """Creates a SqlAlchemyDeploymentState instance configured for a specific source type."""
    # Provide the mandatory source_type
    return SqlAlchemyDeploymentState(sql_connector=sql_connector, source_type=test_source_type)


@pytest.fixture
def sql_source_descriptor(sql_connector: SqlAlchemyConnector, test_source_type: str) -> SqlAlchemySourceDescriptor:
    """Creates a SqlAlchemySourceDescriptor instance configured for a specific source type."""
    # Provide the mandatory source_type
    return SqlAlchemySourceDescriptor(sql_connector=sql_connector, source_type=test_source_type)


# --- Test Setup Data ---
@pytest.fixture
def initial_source_data() -> DataFrame[PrimitiveSourceDataModel]:
    """Initial data for the primitive_sources table with mixed source_types."""
    data = {
        "stream_id": ["s1", "s2", "s3", "s4"],
        "source_id": ["srcA", "srcB", "srcC", "srcD"],
        "source_type": ["typeX", "typeY", "typeX", "typeZ"],
        # is_deployed and deployed_at will be default (False, None)
    }
    # Validate and return the DataFrame with the correct type hint
    return DataFrame[PrimitiveSourceDataModel](pd.DataFrame(data))


# --- Test Cases for Read Methods ---


def test_has_been_deployed_initial(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState,
    sql_source_descriptor: SqlAlchemySourceDescriptor,
    initial_source_data: DataFrame[PrimitiveSourceDataModel],
):
    """Test has_been_deployed when no streams are marked (respects source_type)."""
    # Set up initial data using the *correctly typed* source descriptor
    # Note: set_sources will overwrite based on its configured source_type if not careful.
    # For setup, we might want a helper that inserts directly or uses a broader descriptor.
    # Let's assume sql_source_descriptor fixture is used only for ensuring table exists,
    # and we insert data directly for clarity.
    engine = sql_source_descriptor._get_engine() # type: ignore
    initial_source_data.to_sql(sql_deployment_state.table_name, engine, if_exists="replace", index=False)

    # Block is configured for typeX
    assert not sql_deployment_state.has_been_deployed("s1") # typeX, not deployed
    assert not sql_deployment_state.has_been_deployed("s3") # typeX, not deployed
    # Read operations check the type, so s2 (typeY) should also return False
    assert not sql_deployment_state.has_been_deployed("s2") # typeY, block only cares about typeX status
    assert not sql_deployment_state.has_been_deployed("s_nonexistent")  # Test non-existent stream


def test_check_multiple_streams_initial(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState,
    sql_source_descriptor: SqlAlchemySourceDescriptor, # Used implicitly for table access
    initial_source_data: DataFrame[PrimitiveSourceDataModel],
):
    """Test check_multiple_streams initial state (respects source_type)."""
    engine = sql_deployment_state._get_engine() # type: ignore
    initial_source_data.to_sql(sql_deployment_state.table_name, engine, if_exists="replace", index=False)

    stream_ids = ["s1", "s2", "s3", "s4", "s_nonexistent"]
    # The block returns status for all requested IDs, filtered by its source_type ('typeX')
    # So, s1 & s3 (typeX) are False because not deployed.
    # s2 & s4 have different types, so their deployment status *for typeX* is effectively False.
    # s_nonexistent is also False.
    expected = {"s1": False, "s2": False, "s3": False, "s4": False, "s_nonexistent": False}
    result = sql_deployment_state.check_multiple_streams(stream_ids)
    assert result == expected


def test_get_deployment_states_initial(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState,
    sql_source_descriptor: SqlAlchemySourceDescriptor, # Used implicitly for table access
    initial_source_data: DataFrame[PrimitiveSourceDataModel],
):
    """Test get_deployment_states when no 'typeX' streams are deployed."""
    engine = sql_deployment_state._get_engine() # type: ignore
    initial_source_data.to_sql(sql_deployment_state.table_name, engine, if_exists="replace", index=False)

    # Block is typeX, no typeX streams are deployed initially
    result = sql_deployment_state.get_deployment_states()
    assert result.empty
    # Validate schema of empty dataframe
    DeploymentStateModel.validate(result)


# --- Helper to check DB state directly ---
def get_db_state(session: Session, table_name: str = "primitive_sources", source_type_filter: str | None = None) -> pd.DataFrame:
    """Directly query the table to check its state, optionally filtering by source_type."""
    from sqlalchemy import text
    query = f"SELECT stream_id, source_type, is_deployed, deployed_at FROM {table_name}"
    if source_type_filter:
        query += f" WHERE source_type = :stype"
        params = {"stype": source_type_filter}
    else:
        params = None
    return pd.read_sql(text(query), session.connection(), params=params)


# --- Test Cases for Write Methods ---


@pytest.fixture
def deployed_timestamp() -> datetime:
    """A consistent, timezone-aware timestamp for testing."""
    return datetime.now(timezone.utc).replace(microsecond=0)  # Avoid microsecond precision issues


def test_mark_as_deployed_updates_existing_correct_type(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState, # Configured for typeX
    sql_source_descriptor: SqlAlchemySourceDescriptor, # Used implicitly for table access
    initial_source_data: DataFrame[PrimitiveSourceDataModel],
    deployed_timestamp: datetime,
    test_source_type: str, # Should be "typeX"
):
    """Test mark_as_deployed correctly updates an existing stream of the correct type."""
    engine = sql_deployment_state._get_engine() # type: ignore
    initial_source_data.to_sql(sql_deployment_state.table_name, engine, if_exists="replace", index=False)

    # Act: Mark 's1' which is typeX
    sql_deployment_state.mark_as_deployed("s1", deployed_timestamp)

    # Assert
    db_state = get_db_state(db_session)
    s1_row = db_state[db_state["stream_id"] == "s1"].iloc[0]
    assert s1_row["is_deployed"] == True
    assert s1_row["source_type"] == test_source_type # Verify it's the correct type
    db_ts = pd.Timestamp(s1_row["deployed_at"]).tz_convert("UTC")
    assert db_ts == deployed_timestamp
    # Check others remain unchanged (s3 is typeX but wasn't marked, s2/s4 are other types)
    assert db_state[db_state["stream_id"] == "s3"].iloc[0]["is_deployed"] == False
    assert db_state[db_state["stream_id"] == "s2"].iloc[0]["is_deployed"] == False


def test_mark_as_deployed_ignores_wrong_type(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState, # Configured for typeX
    sql_source_descriptor: SqlAlchemySourceDescriptor, # Used implicitly for table access
    initial_source_data: DataFrame[PrimitiveSourceDataModel],
    deployed_timestamp: datetime,
    test_source_type: str, # Should be "typeX"
):
    """Test mark_as_deployed ignores streams of the wrong type."""
    engine = sql_deployment_state._get_engine() # type: ignore
    initial_source_data.to_sql(sql_deployment_state.table_name, engine, if_exists="replace", index=False)
    initial_db_state = get_db_state(db_session).copy()

    # Act: Try to mark 's2' which is typeY
    sql_deployment_state.mark_as_deployed("s2", deployed_timestamp)

    # Assert: DB state should be unchanged
    final_db_state = get_db_state(db_session)
    pd.testing.assert_frame_equal(
        initial_db_state.sort_values("stream_id").reset_index(drop=True),
        final_db_state.sort_values("stream_id").reset_index(drop=True),
        check_datetimelike_compat=True # Allow for NaT comparison
    )


def test_mark_as_deployed_ignores_nonexistent(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState, # Configured for typeX
    sql_source_descriptor: SqlAlchemySourceDescriptor, # Used implicitly for table access
    initial_source_data: DataFrame[PrimitiveSourceDataModel],
    deployed_timestamp: datetime,
):
    """Test mark_as_deployed does nothing for a non-existent stream_id."""
    engine = sql_deployment_state._get_engine() # type: ignore
    initial_source_data.to_sql(sql_deployment_state.table_name, engine, if_exists="replace", index=False)
    initial_db_state = get_db_state(db_session).copy()

    # Act
    sql_deployment_state.mark_as_deployed("s_nonexistent", deployed_timestamp)

    # Assert: DB state should be unchanged
    final_db_state = get_db_state(db_session)
    pd.testing.assert_frame_equal(
        initial_db_state.sort_values("stream_id").reset_index(drop=True),
        final_db_state.sort_values("stream_id").reset_index(drop=True),
        check_datetimelike_compat=True
    )


def test_mark_multiple_as_deployed_updates_existing_correct_type(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState, # Configured for typeX
    sql_source_descriptor: SqlAlchemySourceDescriptor, # Used implicitly for table access
    initial_source_data: DataFrame[PrimitiveSourceDataModel],
    deployed_timestamp: datetime,
    test_source_type: str, # Should be "typeX"
):
    """Test mark_multiple_as_deployed updates multiple existing streams of correct type."""
    engine = sql_deployment_state._get_engine() # type: ignore
    initial_source_data.to_sql(sql_deployment_state.table_name, engine, if_exists="replace", index=False)
    streams_to_mark = ["s1", "s3"] # Both are typeX

    # Act
    sql_deployment_state.mark_multiple_as_deployed(streams_to_mark, deployed_timestamp)

    # Assert
    db_state = get_db_state(db_session)
    for stream_id in streams_to_mark:
        row = db_state[db_state["stream_id"] == stream_id].iloc[0]
        assert row["is_deployed"] == True
        assert row["source_type"] == test_source_type
        db_ts = pd.Timestamp(row["deployed_at"]).tz_convert("UTC")
        assert db_ts == deployed_timestamp
    # Check others remain unchanged
    assert db_state[db_state["stream_id"] == "s2"].iloc[0]["is_deployed"] == False
    assert db_state[db_state["stream_id"] == "s4"].iloc[0]["is_deployed"] == False


def test_mark_multiple_as_deployed_mixed_types_and_existence(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState, # Configured for typeX
    sql_source_descriptor: SqlAlchemySourceDescriptor, # Used implicitly for table access
    initial_source_data: DataFrame[PrimitiveSourceDataModel],
    deployed_timestamp: datetime,
    test_source_type: str, # Should be "typeX"
):
    """Test mark_multiple handles mix of correct type, wrong type, and non-existent IDs."""
    engine = sql_deployment_state._get_engine() # type: ignore
    initial_source_data.to_sql(sql_deployment_state.table_name, engine, if_exists="replace", index=False)
    streams_to_mark = ["s1", "s2", "s_nonexistent", "s3", "s4"]
    expected_updated = ["s1", "s3"] # Only typeX streams should be updated
    expected_unchanged_other_type = ["s2", "s4"] # These shouldn't be touched by typeX block

    # Act
    sql_deployment_state.mark_multiple_as_deployed(streams_to_mark, deployed_timestamp)

    # Assert
    db_state = get_db_state(db_session)
    # Check updated rows (s1, s3)
    for stream_id in expected_updated:
        row = db_state[db_state["stream_id"] == stream_id].iloc[0]
        assert row["is_deployed"] == True
        assert row["source_type"] == test_source_type
        db_ts = pd.Timestamp(row["deployed_at"]).tz_convert("UTC")
        assert db_ts == deployed_timestamp
    # Check other type rows remain unchanged
    for stream_id in expected_unchanged_other_type:
        row = db_state[db_state["stream_id"] == stream_id].iloc[0]
        assert row["is_deployed"] == False # Should remain False
        assert row["deployed_at"] is pd.NaT
    # Check table size hasn't changed (no insertions)
    assert len(db_state) == len(initial_source_data)


def test_update_deployment_states_updates_correct_type_and_ignores_others(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState, # Configured for typeX
    sql_source_descriptor: SqlAlchemySourceDescriptor, # Used implicitly for table access
    initial_source_data: DataFrame[PrimitiveSourceDataModel],
    deployed_timestamp: datetime,
    test_source_type: str, # Should be "typeX"
):
    """Test update_deployment_states updates correct type, ignores wrong type and non-existent."""
    engine = sql_deployment_state._get_engine() # type: ignore
    initial_source_data.to_sql(sql_deployment_state.table_name, engine, if_exists="replace", index=False)
    ts1 = deployed_timestamp - timedelta(days=1)
    ts2 = deployed_timestamp
    ts3 = deployed_timestamp + timedelta(days=1)
    # s1=typeX, s4=typeZ, s_nonexistent=N/A, s3=typeX
    update_df = DataFrame[DeploymentStateModel]({
        "stream_id": ["s1", "s4", "s_nonexistent", "s3"],
        "deployment_timestamp": [ts1, ts2, ts2, ts3]
    })

    # Act
    sql_deployment_state.update_deployment_states(update_df)

    # Assert
    db_state = get_db_state(db_session)
    # Check s1 (typeX) - should be updated
    s1_row = db_state[db_state["stream_id"] == "s1"].iloc[0]
    assert s1_row["is_deployed"] == True
    assert s1_row["source_type"] == test_source_type
    db_ts1 = pd.Timestamp(s1_row["deployed_at"]).tz_convert("UTC")
    assert db_ts1 == ts1
    # Check s3 (typeX) - should be updated
    s3_row = db_state[db_state["stream_id"] == "s3"].iloc[0]
    assert s3_row["is_deployed"] == True
    assert s3_row["source_type"] == test_source_type
    db_ts3 = pd.Timestamp(s3_row["deployed_at"]).tz_convert("UTC")
    assert db_ts3 == ts3
    # Check s4 (typeZ) - should NOT be updated by typeX block
    s4_row = db_state[db_state["stream_id"] == "s4"].iloc[0]
    assert s4_row["is_deployed"] == False
    assert s4_row["deployed_at"] is pd.NaT
    # Check s2 (typeY) - also unchanged
    s2_row = db_state[db_state["stream_id"] == "s2"].iloc[0]
    assert s2_row["is_deployed"] == False
    # Check table size
    assert len(db_state) == len(initial_source_data)


def test_update_deployment_states_ignores_nonexistent_correct_type(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState, # Configured for typeX
    sql_source_descriptor: SqlAlchemySourceDescriptor, # Used implicitly for table access
    initial_source_data: DataFrame[PrimitiveSourceDataModel],
    deployed_timestamp: datetime,
    test_source_type: str, # Should be "typeX"
):
    """Test update_deployment_states ignores non-existent stream_ids even if type matches."""
    engine = sql_deployment_state._get_engine() # type: ignore
    initial_source_data.to_sql(sql_deployment_state.table_name, engine, if_exists="replace", index=False)
    initial_db_state_len = len(get_db_state(db_session))

    # s_nonexistent would be typeX if it existed, s3 is typeX
    update_df = DataFrame[DeploymentStateModel](
        {
            "stream_id": ["s_nonexistent", "s3"],
            "deployment_timestamp": [deployed_timestamp, deployed_timestamp],
        }
    )

    # Act
    sql_deployment_state.update_deployment_states(update_df)

    # Assert
    final_db_state = get_db_state(db_session)
    # Check s3 was updated
    s3_row = final_db_state[final_db_state["stream_id"] == "s3"].iloc[0]
    assert s3_row["is_deployed"] == True
    assert s3_row["source_type"] == test_source_type
    db_ts3 = pd.Timestamp(s3_row["deployed_at"]).tz_convert("UTC")
    assert db_ts3 == deployed_timestamp
    # Check s1 and s2 remain unchanged
    assert final_db_state[final_db_state["stream_id"] == "s1"].iloc[0]["is_deployed"] == False
    assert final_db_state[final_db_state["stream_id"] == "s2"].iloc[0]["is_deployed"] == False
    # Check nonexistent wasn't added
    assert len(final_db_state) == initial_db_state_len


def test_update_deployment_states_empty_df(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState, # Configured for typeX
    sql_source_descriptor: SqlAlchemySourceDescriptor, # Used implicitly for table access
    initial_source_data: DataFrame[PrimitiveSourceDataModel],
):
    """Test update_deployment_states does nothing when passed an empty DataFrame."""
    engine = sql_deployment_state._get_engine() # type: ignore
    initial_source_data.to_sql(sql_deployment_state.table_name, engine, if_exists="replace", index=False)
    initial_db_state = get_db_state(db_session).copy()
    empty_df = DataFrame[DeploymentStateModel](columns=["stream_id", "deployment_timestamp"])
    # Ensure correct dtypes for empty DataFrame to pass validation if needed by the method
    empty_df["deployment_timestamp"] = pd.Series(dtype="datetime64[ns, UTC]")
    empty_df["stream_id"] = pd.Series(dtype="str")

    # Act
    sql_deployment_state.update_deployment_states(empty_df)

    # Assert: DB state should be unchanged
    final_db_state = get_db_state(db_session)
    pd.testing.assert_frame_equal(
        initial_db_state.sort_values("stream_id").reset_index(drop=True),
        final_db_state.sort_values("stream_id").reset_index(drop=True),
        check_datetimelike_compat=True
    )


# Add test for read methods after marking some as deployed
def test_read_methods_after_mark(
    db_session: Session,
    sql_deployment_state: SqlAlchemyDeploymentState, # Configured for typeX
    sql_source_descriptor: SqlAlchemySourceDescriptor, # Used implicitly for table access
    initial_source_data: DataFrame[PrimitiveSourceDataModel],
    deployed_timestamp: datetime,
    test_source_type: str, # Should be "typeX"
):
    """Test read methods correctly reflect state after marking (respects source_type)."""
    engine = sql_deployment_state._get_engine() # type: ignore
    initial_source_data.to_sql(sql_deployment_state.table_name, engine, if_exists="replace", index=False)
    streams_to_mark = ["s1", "s3"] # Mark the typeX streams
    sql_deployment_state.mark_multiple_as_deployed(streams_to_mark, deployed_timestamp)

    # Test has_been_deployed (block is typeX)
    assert sql_deployment_state.has_been_deployed("s1") == True  # typeX, marked
    assert sql_deployment_state.has_been_deployed("s3") == True  # typeX, marked
    assert sql_deployment_state.has_been_deployed("s2") == False # typeY, read reflects typeX status
    assert sql_deployment_state.has_been_deployed("s4") == False # typeZ, read reflects typeX status

    # Test check_multiple_streams (block is typeX)
    check_ids = ["s1", "s2", "s3", "s4", "s_nonexistent"]
    expected_check = {"s1": True, "s2": False, "s3": True, "s4": False, "s_nonexistent": False}
    assert sql_deployment_state.check_multiple_streams(check_ids) == expected_check

    # Test get_deployment_states (block is typeX)
    deployed_states_df = sql_deployment_state.get_deployment_states()
    DeploymentStateModel.validate(deployed_states_df) # Validate structure
    # Should only contain the deployed typeX streams (s1, s3)
    assert len(deployed_states_df) == 2
    expected_df = pd.DataFrame(
        {"stream_id": ["s1", "s3"], "deployment_timestamp": [deployed_timestamp, deployed_timestamp]}
    )
    # Ensure correct dtype for comparison
    expected_df["deployment_timestamp"] = pd.to_datetime(expected_df["deployment_timestamp"], utc=True)
    # Compare DataFrames (sort first for consistent order)
    pd.testing.assert_frame_equal(
        deployed_states_df.sort_values("stream_id").reset_index(drop=True),
        expected_df.sort_values("stream_id").reset_index(drop=True),
        check_dtype=True, # Check dtypes match
    )
