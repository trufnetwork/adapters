"""
Prefect block for interacting with primitive source descriptor data stored in a SQL database.
"""

from typing import cast

import pandas as pd
from pandera.typing import DataFrame
from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import Table, delete, select
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import Select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tsn_adapters.blocks.models import primitive_sources_table
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    WritableSourceDescriptorBlock,
)
from tsn_adapters.utils.logging import get_logger_safe


class SqlAlchemySourceDescriptor(WritableSourceDescriptorBlock):
    """
    Manages primitive source descriptors stored in a SQL database table.

    This block uses a SQLAlchemy engine provided by a Prefect SqlAlchemyConnector
    to interact with a specified table.

    Attributes:
        sql_connector: Prefect block providing SQLAlchemy connection details.
        table_name: The name of the database table to interact with.
                      Defaults to "primitive_sources".
    """

    _block_type_name = "SQLAlchemy Source Descriptor"
    # _logo_url = "URL_TO_LOGO"  # Optional: Add a logo URL if available
    # _documentation_url = "URL_TO_DOCS" # Optional: Link to docs

    sql_connector: SqlAlchemyConnector
    table_name: str = "primitive_sources" # Default table name

    @property
    def logger(self):
        if not hasattr(self, "_logger"):
            self._logger = get_logger_safe(__name__)
        return self._logger

    @property
    def _table(self) -> Table:
        """Returns the SQLAlchemy Table object based on the table_name."""
        # Assuming primitive_sources_table is imported and represents the correct structure
        if self.table_name != primitive_sources_table.name:
            self.logger.warning(
                f"Configured table_name '{self.table_name}' differs from imported table name "
                f"'{primitive_sources_table.name}'. Using imported table definition."
            )
            # For robustness, ensure the imported table object is returned
            # This assumes the schema is consistent enough for the block's operations.
            # A more robust solution might involve dynamically reflecting the table 
            # if names differ significantly, but that adds complexity.
        return primitive_sources_table 

    def _get_engine(self) -> Engine:
        """Retrieves the SQLAlchemy engine from the connector."""
        try:
            # Prefect blocks often have async methods, but get_engine might be sync
            # Ensure we handle potential async if needed (though it seems sync here)
            engine: Engine = self.sql_connector.get_engine() 
            return engine
        except Exception as e:
            self.logger.error(f"Failed to get SQLAlchemy engine from connector: {e}", exc_info=True)
            raise

    def get_descriptor(self) -> DataFrame[PrimitiveSourceDataModel]:
        """
        Retrieves the entire source descriptor table from the database.

        Returns:
            A DataFrame conforming to PrimitiveSourceDataModel containing all rows
            from the table, or an empty DataFrame if the table is empty or an error occurs.
        """
        engine = self._get_engine()
        try:
            with engine.connect() as connection:
                stmt: Select = select(self._table)
                # Use specific dtypes for read_sql for better type inference
                df = pd.read_sql(sql=stmt, con=connection)
                # Ensure correct dtypes, especially if reading an empty table
                for col, pa_dtype in PrimitiveSourceDataModel.to_schema().columns.items():
                    pd_dtype = pa_dtype.dtype.type if pa_dtype.dtype else object
                    if col not in df.columns:
                        df[col] = pd.Series(dtype=pd_dtype)
                    else:
                        if not pd.api.types.is_dtype_equal(df[col].dtype, pd_dtype):
                            try:
                                # Use astype with error handling
                                df[col] = df[col].astype(pd_dtype)
                            except Exception as e:
                                self.logger.warning(f"Could not cast column '{col}' from {df[col].dtype} to {pd_dtype}: {e}")
                # Validate and return
                return DataFrame[PrimitiveSourceDataModel](df)
        except SQLAlchemyError as e:
            self.logger.error(f"Error querying table '{self.table_name}': {e}", exc_info=True)
        except Exception as e:
            self.logger.error(
                f"An unexpected error occurred retrieving descriptor from table '{self.table_name}': {e}",
                exc_info=True,
            )
        # Return empty DataFrame in case of any error
        empty_df = pd.DataFrame(columns=list(PrimitiveSourceDataModel.to_schema().columns.keys()))
        # Ensure empty df also has correct types before casting
        for col, pa_dtype in PrimitiveSourceDataModel.to_schema().columns.items():
            pd_dtype = pa_dtype.dtype.type if pa_dtype.dtype else object
            if col not in empty_df.columns:
                 empty_df[col] = pd.Series(dtype=pd_dtype)
        return cast(DataFrame[PrimitiveSourceDataModel], empty_df)

    def set_sources(self, descriptor: DataFrame[PrimitiveSourceDataModel]) -> None:
        """
        Overwrites the entire source descriptor table with the provided data.

        This performs a DELETE operation followed by an INSERT operation.
        Use with caution, as it removes all existing data in the table.

        Args:
            descriptor: A DataFrame conforming to PrimitiveSourceDataModel
                        containing the data to write.
        """
        engine = self._get_engine()
        validated_descriptor = PrimitiveSourceDataModel.validate(descriptor)
        
        try:
            with engine.begin() as connection: # type: Connection
                # Delete all existing rows
                self.logger.info(f"Deleting all existing rows from table '{self.table_name}'.")
                delete_stmt = delete(self._table)
                connection.execute(delete_stmt)
                self.logger.info("Existing rows deleted.")
                
                # Insert new data
                self.logger.info(f"Inserting {len(validated_descriptor)} new rows into table '{self.table_name}'.")
                # Use connection from the transaction context for to_sql
                validated_descriptor.to_sql(
                    name=self.table_name,
                    con=connection,
                    if_exists="append", # Should be safe after delete
                    index=False,
                    # Consider specifying dtype mapping if needed
                )
                self.logger.info("New rows inserted successfully.")
        except SQLAlchemyError as e:
            self.logger.error(
                f"Database error during set_sources for table '{self.table_name}': {e}", exc_info=True
            )
            raise # Re-raise after logging
        except Exception as e:
            self.logger.error(
                f"An unexpected error occurred during set_sources for table '{self.table_name}': {e}",
                exc_info=True,
            )
            raise # Re-raise after logging

    def upsert_sources(self, descriptor: DataFrame[PrimitiveSourceDataModel]) -> None:
        """
        Inserts new sources or updates existing ones based on stream_id.

        Leverages PostgreSQL's INSERT ... ON CONFLICT DO UPDATE.
        Updates only occur if source_id or source_type differ from the
        existing database record.

        Args:
            descriptor: A DataFrame conforming to PrimitiveSourceDataModel
                        containing the data to upsert.

        Raises:
            SQLAlchemyError: If a database error occurs during the upsert.
            Exception: For other unexpected errors.
        """
        engine = self._get_engine()
        validated_descriptor = PrimitiveSourceDataModel.validate(descriptor)

        if validated_descriptor.empty:
            self.logger.info("Input descriptor is empty, nothing to upsert.")
            return

        # Convert DataFrame to list of dictionaries for SQLAlchemy Core API
        rows_to_upsert = validated_descriptor.to_dict(orient="records")

        # Construct the base insert statement
        insert_stmt = pg_insert(self._table).values(rows_to_upsert)

        # Define the ON CONFLICT DO UPDATE clause
        # Specify the conflict target (the primary key column)
        # Use the 'excluded' pseudo-table to refer to the values proposed for insertion
        update_columns = {
            # Update 'source_id' and 'source_type' if they differ from existing values
            # Also update 'updated_at' automatically via the table definition
            "source_id": insert_stmt.excluded.source_id,
            "source_type": insert_stmt.excluded.source_type,
            "updated_at": func.now(), # Explicitly update updated_at
        }
        
        # The WHERE clause ensures updates only happen if values actually changed
        # Accessing the table columns directly for the comparison
        table = self._table
        where_clause = (table.c.source_id != insert_stmt.excluded.source_id) | \\
                       (table.c.source_type != insert_stmt.excluded.source_type)

        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["stream_id"],  # Conflict target constraint (PK)
            set_=update_columns,           # Columns to update
            where=where_clause              # Condition for update
        )

        try:
            with engine.begin() as connection: # type: Connection
                self.logger.info(f"Executing upsert for {len(rows_to_upsert)} rows into table '{self.table_name}'.")
                connection.execute(upsert_stmt)
                self.logger.info("Upsert operation completed successfully.")
        except SQLAlchemyError as e:
            self.logger.error(
                f"Database error during upsert for table '{self.table_name}': {e}", exc_info=True
            )
            raise # Re-raise after logging
        except Exception as e:
            self.logger.error(
                f"An unexpected error occurred during upsert for table '{self.table_name}': {e}",
                exc_info=True,
            )
            raise # Re-raise after logging
