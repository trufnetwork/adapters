from datetime import datetime, timezone
from typing import cast

import pandas as pd
from pandera.typing import DataFrame
from prefect_sqlalchemy import SqlAlchemyConnector  # type: ignore
from sqlalchemy import Table, and_, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from tsn_adapters.blocks.deployment_state import DeploymentStateBlock, DeploymentStateModel
from tsn_adapters.blocks.models import primitive_sources_table
from tsn_adapters.utils.logging import get_logger_safe


class SqlAlchemyDeploymentState(DeploymentStateBlock):
    """
    Manages deployment state (is_deployed, deployed_at) stored in a SQL database table,
    sharing the table with SqlAlchemySourceDescriptor.

    This block uses a SQLAlchemy engine provided by a Prefect SqlAlchemyConnector
    to interact with the specified table. Write operations ONLY update existing rows.
    Operations are filtered by the configured `source_type`.

    Attributes:
        sql_connector: Prefect block providing SQLAlchemy connection details.
        table_name: The name of the database table to interact with.
                      Defaults to "primitive_sources".
        source_type: The type of the source to filter operations by.
    """

    _block_type_name = "SQLAlchemy Deployment State"

    sql_connector: SqlAlchemyConnector
    table_name: str = "primitive_sources"  # Default table name matches descriptor
    source_type: str  # Add mandatory source_type parameter

    @property
    def logger(self):
        if not hasattr(self, "_logger"):
            self._logger = get_logger_safe(__name__)
        return self._logger

    @property
    def _table(self) -> Table:
        """Returns the SQLAlchemy Table object based on the table_name."""
        if self.table_name != primitive_sources_table.name:
            self.logger.warning(
                f"Configured table_name '{self.table_name}' differs from imported table name "
                f"'{primitive_sources_table.name}'. Using imported table definition."
            )
        return primitive_sources_table

    def _get_engine(self) -> Engine:
        """Retrieves the SQLAlchemy engine from the connector."""
        try:
            engine: Engine = self.sql_connector.get_engine()
            return engine
        except Exception as e:
            self.logger.error(f"Failed to get SQLAlchemy engine from connector: {e}", exc_info=True)
            raise

    # --- Read Methods  ---
    def has_been_deployed(self, stream_id: str) -> bool:
        """Check if the deployment has been performed for a given stream_id."""
        engine = self._get_engine()
        stmt = select(self._table.c.is_deployed).where(  # type: ignore[arg-type]
            and_(
                self._table.c.stream_id == stream_id,
                self._table.c.source_type == self.source_type,  # Filter by source_type
            )
        )
        try:
            with engine.connect() as connection:
                result = connection.execute(stmt).scalar_one_or_none()
                return bool(result)  # Return False if None (not found) or False
        except SQLAlchemyError as e:
            self.logger.error(f"Database error checking deployment for '{stream_id}': {e}", exc_info=True)
            raise  # Re-raise after logging
        except Exception as e:
            self.logger.error(f"Unexpected error checking deployment for '{stream_id}': {e}", exc_info=True)
            raise  # Re-raise after logging

    def check_multiple_streams(self, stream_ids: list[str]) -> dict[str, bool]:
        """Check deployment status for multiple stream_ids."""
        if not stream_ids:
            return {}
        engine = self._get_engine()
        unique_ids = list(set(filter(None, stream_ids)))
        if not unique_ids:
            return {}

        stmt = select(self._table.c.stream_id, self._table.c.is_deployed).where(  # type: ignore[arg-type]
            and_(
                self._table.c.stream_id.in_(unique_ids),
                self._table.c.source_type == self.source_type,  # Filter by source_type
            )
        )
        results = {stream_id: False for stream_id in unique_ids}  # Initialize all as False
        try:
            with engine.connect() as connection:
                db_results = connection.execute(stmt).fetchall()
                for stream_id, is_deployed in db_results:
                    results[stream_id] = bool(is_deployed)
            return results
        except SQLAlchemyError as e:
            self.logger.error(f"Database error checking multiple deployments: {e}", exc_info=True)
            raise  # Re-raise after logging
        except Exception as e:
            self.logger.error(f"Unexpected error checking multiple deployments: {e}", exc_info=True)
            raise  # Re-raise after logging

    def get_deployment_states(self) -> DataFrame[DeploymentStateModel]:
        """Retrieve the deployment states (stream_id and nullable deployment_timestamp)
        for all streams matching the block's source_type.
        """
        engine = self._get_engine()
        # Select stream_id and deployed_at for all streams of the specified type
        stmt = (
            select(self._table.c.stream_id, self._table.c.deployed_at.label("deployment_timestamp")).where(  # type: ignore[arg-type]
                self._table.c.source_type == self.source_type
            )  # Filter by source_type
        )
        try:
            with engine.connect() as connection:
                df = pd.read_sql(sql=stmt, con=connection)
                # Ensure timestamp column is UTC if not already
                if not df.empty and "deployment_timestamp" in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df["deployment_timestamp"]):
                        if df["deployment_timestamp"].dt.tz is None:
                            df["deployment_timestamp"] = df["deployment_timestamp"].dt.tz_localize("UTC")
                        else:
                            df["deployment_timestamp"] = df["deployment_timestamp"].dt.tz_convert("UTC")
                    else:
                        # Handle cases where it might not be a datetime object initially
                        # Ensure NaT is preserved if converting from other types
                        df["deployment_timestamp"] = pd.to_datetime(
                            df["deployment_timestamp"], utc=True, errors="coerce"
                        )
                # Ensure column exists with correct dtype even if empty or all null
                elif "deployment_timestamp" not in df.columns:
                    df["deployment_timestamp"] = pd.Series(dtype="datetime64[ns, UTC]")
                else:  # Column exists but might be wrong type
                    df["deployment_timestamp"] = pd.to_datetime(df["deployment_timestamp"], utc=True, errors="coerce")

                # Validate final structure
                return DataFrame[DeploymentStateModel](df)
        except SQLAlchemyError as e:
            self.logger.error(f"Database error getting deployment states: {e}", exc_info=True)
        except Exception as e:
            self.logger.error(f"Unexpected error getting deployment states: {e}", exc_info=True)

        # Return validated empty DataFrame on error
        empty_df = pd.DataFrame(columns=["stream_id", "deployment_timestamp"])
        empty_df["deployment_timestamp"] = pd.Series(dtype="datetime64[ns, UTC]")
        empty_df["stream_id"] = pd.Series(dtype="str")
        return cast(DataFrame[DeploymentStateModel], empty_df)

    # --- Write Methods ---

    def _validate_timestamp(self, timestamp: datetime) -> datetime:
        """Validate and ensure timestamp is timezone-aware UTC."""
        if timestamp.tzinfo is None:
            self.logger.warning(f"Timestamp {timestamp} lacks timezone info, assuming UTC.")
            return timestamp.replace(tzinfo=timezone.utc)
        elif timestamp.tzinfo != timezone.utc:
            self.logger.warning(f"Timestamp {timestamp} is not UTC, converting to UTC.")
            return timestamp.astimezone(timezone.utc)
        return timestamp  # Already UTC

    def mark_as_deployed(self, stream_id: str, timestamp: datetime) -> None:
        """Mark a single stream_id as deployed at the given timestamp (updates only)."""
        if not stream_id:
            self.logger.warning("mark_as_deployed called with empty stream_id. Skipping.")
            return

        engine = self._get_engine()
        valid_timestamp = self._validate_timestamp(timestamp)

        stmt = (
            update(self._table)
            .where(
                and_(
                    self._table.c.stream_id == stream_id,
                    self._table.c.source_type == self.source_type,
                )
            )
            .values(is_deployed=True, deployed_at=valid_timestamp)
        )

        try:
            with engine.begin() as connection:
                result = connection.execute(stmt)
                if result.rowcount == 0:
                    self.logger.warning(
                        f"Stream ID '{stream_id}' not found in table '{self.table_name}'. "
                        f"No update performed for mark_as_deployed."
                    )
                elif result.rowcount == 1:
                    self.logger.info(f"Marked stream ID '{stream_id}' as deployed.")
                else:
                    # Should not happen with unique primary key
                    self.logger.error(f"Unexpected row count ({result.rowcount}) updating stream ID '{stream_id}'.")
        except SQLAlchemyError as e:
            self.logger.error(f"Database error marking stream '{stream_id}' as deployed: {e}", exc_info=True)
            raise  # Re-raise after logging
        except Exception as e:
            self.logger.error(f"Unexpected error marking stream '{stream_id}' as deployed: {e}", exc_info=True)
            raise  # Re-raise after logging

    def mark_multiple_as_deployed(self, stream_ids: list[str], timestamp: datetime) -> None:
        """Mark multiple stream_ids as deployed at the given timestamp (updates only)."""
        if not stream_ids:
            self.logger.info("mark_multiple_as_deployed called with empty list. Skipping.")
            return

        engine = self._get_engine()
        valid_timestamp = self._validate_timestamp(timestamp)
        unique_stream_ids = list(set(filter(None, stream_ids)))  # Remove empty strings and duplicates

        if not unique_stream_ids:
            self.logger.warning("mark_multiple_as_deployed called with list containing only empty strings. Skipping.")
            return

        stmt = (
            update(self._table)
            .where(and_(self._table.c.stream_id.in_(unique_stream_ids), self._table.c.source_type == self.source_type))
            .values(is_deployed=True, deployed_at=valid_timestamp)
        )

        try:
            with engine.begin() as connection:
                result = connection.execute(stmt)
                self.logger.info(
                    f"Attempted to mark {len(unique_stream_ids)} stream IDs as deployed. "
                    f"{result.rowcount} rows actually updated in table '{self.table_name}'."
                )
                if result.rowcount < len(unique_stream_ids):
                    self.logger.warning(
                        f"{len(unique_stream_ids) - result.rowcount} stream IDs were not found or already marked."
                    )
        except SQLAlchemyError as e:
            self.logger.error(f"Database error marking multiple streams as deployed: {e}", exc_info=True)
            raise  # Re-raise after logging
        except Exception as e:
            self.logger.error(f"Unexpected error marking multiple streams as deployed: {e}", exc_info=True)
            raise  # Re-raise after logging

    def update_deployment_states(self, states: DataFrame[DeploymentStateModel]) -> None:
        """
        Updates deployment states based on the input DataFrame (updates only).

        Iterates through the provided DataFrame and updates the `is_deployed`
        and `deployed_at` fields for matching `stream_id`s found in the database table.
        Rows in the DataFrame whose `stream_id` does not exist in the table are ignored.

        Args:
            states: DataFrame conforming to DeploymentStateModel. Note: The model expects
                    `deployment_timestamp`, but this method will use it to update the
                    `deployed_at` column in the database and set `is_deployed` to True.
        """
        if states.empty:
            self.logger.info("update_deployment_states called with empty DataFrame. Skipping.")
            return

        engine = self._get_engine()
        validated_states = DeploymentStateModel.validate(states)
        updated_count = 0
        skipped_count = 0

        # Ensure timestamp column is tz-aware UTC
        if not pd.api.types.is_datetime64tz_dtype(validated_states["deployment_timestamp"]):  # type: ignore
            self.logger.warning("Input DataFrame timestamps are not timezone-aware UTC. Converting.")
            try:
                # Use utc=True for conversion
                validated_states["deployment_timestamp"] = pd.to_datetime(
                    validated_states["deployment_timestamp"], utc=True
                )
            except Exception as e:
                self.logger.error(f"Failed to convert deployment_timestamp to UTC: {e}", exc_info=True)
                raise ValueError("Could not convert deployment_timestamp to UTC-aware datetime") from e

        # Start transaction
        with engine.begin() as connection:
            for _, row in validated_states.iterrows():
                stream_id = row["stream_id"]
                timestamp = row["deployment_timestamp"]

                # Prepare update statement
                stmt = (
                    update(self._table)
                    .where(self._table.c.stream_id == stream_id)
                    .values(is_deployed=True, deployed_at=timestamp)
                )

                try:
                    result = connection.execute(stmt)
                    if result.rowcount == 1:
                        updated_count += 1
                    else:
                        # stream_id not found in the table, or multiple updated (shouldn't happen)
                        skipped_count += 1
                        if result.rowcount > 1:
                            self.logger.error(f"Unexpected row count ({result.rowcount}) updating '{stream_id}'.")
                        # No warning needed for skipped_count == 0 rowcount, as per spec.

                except SQLAlchemyError as e:
                    self.logger.error(
                        f"Database error updating deployment state for stream '{stream_id}': {e}", exc_info=True
                    )
                    # Decide on error handling: re-raise, or log and continue?
                    # Re-raising ensures transactional integrity if one update fails.
                    raise

        self.logger.info(
            f"update_deployment_states finished. Updated: {updated_count}, Skipped (not found): {skipped_count}."
        )
