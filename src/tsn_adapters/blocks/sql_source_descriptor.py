"""
Prefect block for interacting with primitive source descriptor data stored in a SQL database.
"""

from typing import cast

import pandas as pd
from pandera.typing import DataFrame
from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import Table, delete, select, Column
from sqlalchemy.engine import Engine, Connection, Transaction
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import Select, func, ColumnElement
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
        source_type: The type of the source.
    """

    _block_type_name = "SQLAlchemy Source Descriptor"
    # _logo_url = "URL_TO_LOGO"  # Optional: Add a logo URL if available
    # _documentation_url = "URL_TO_DOCS" # Optional: Link to docs

    sql_connector: SqlAlchemyConnector
    table_name: str = "primitive_sources" # Default table name
    source_type: str # Add mandatory source_type parameter

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
            # Use engine.connect() which yields a Connection
            with engine.connect() as connection: # type: Connection
                # Select specific columns for clarity and correctness
                # Ensure columns exist on the table object
                stmt: Select = select(
                    self._table.c.stream_id,
                    self._table.c.source_id,
                    self._table.c.source_type
                    # Add other necessary columns if PrimitiveSourceDataModel requires them
                ).where(self._table.c.source_type == self.source_type) # Filter by source_type
                
                self.logger.debug(f"Querying table '{self.table_name}' for source_type '{self.source_type}'.")
                # Use specific dtypes for read_sql for better type inference
                df = pd.read_sql(sql=stmt, con=connection) # read_sql needs a Connection

                # --- DataFrame Validation and Type Correction ---
                # Define expected columns based on the Pandera model
                expected_cols = list(PrimitiveSourceDataModel.to_schema().columns.keys())
                
                # Ensure all expected columns exist, adding missing ones with correct dtype
                for col in expected_cols:
                    pa_dtype = PrimitiveSourceDataModel.to_schema().columns[col]
                    pd_dtype = pa_dtype.dtype.type if pa_dtype.dtype else object
                    if col not in df.columns:
                        df[col] = pd.Series(dtype=pd_dtype)
                    elif not pd.api.types.is_dtype_equal(df[col].dtype, pd_dtype):
                        try:
                            # Attempt conversion, handle potential errors gracefully
                            df[col] = df[col].astype(pd_dtype)
                        except (ValueError, TypeError) as e:
                            self.logger.warning(f"Could not cast column '{col}' from {df[col].dtype} to {pd_dtype}. Error: {e}. Setting to object type.")
                            df[col] = df[col].astype(object) # Fallback to object
                
                # Filter DataFrame to only include expected columns (handles extra columns from DB)
                df = df[expected_cols]

                # Validate final DataFrame structure and types with Pandera
                return DataFrame[PrimitiveSourceDataModel](df)
                # --- End Validation ---

        except SQLAlchemyError as e:
            self.logger.error(f"Error querying table '{self.table_name}': {e}", exc_info=True)
        except Exception as e:
            self.logger.error(
                f"An unexpected error occurred retrieving descriptor from table '{self.table_name}': {e}",
                exc_info=True,
            )
            
        # Return validated empty DataFrame in case of any error
        empty_df = pd.DataFrame(columns=list(PrimitiveSourceDataModel.to_schema().columns.keys()))
        for col, pa_dtype in PrimitiveSourceDataModel.to_schema().columns.items():
            pd_dtype = pa_dtype.dtype.type if pa_dtype.dtype else object
            if col not in empty_df.columns:
                 empty_df[col] = pd.Series(dtype=pd_dtype)
        return cast(DataFrame[PrimitiveSourceDataModel], empty_df)

    def set_sources(self, descriptor: DataFrame[PrimitiveSourceDataModel]) -> None:
        """
        Overwrites the entire source descriptor table with the provided data.

        This performs a DELETE operation followed by an INSERT operation within a transaction.
        Use with caution, as it removes all existing data in the table.

        Args:
            descriptor: A DataFrame conforming to PrimitiveSourceDataModel
                        containing the data to write.
        """
        engine = self._get_engine()
        validated_descriptor = PrimitiveSourceDataModel.validate(descriptor)

        # Input Validation: Check if all incoming rows match self.source_type
        if not validated_descriptor.empty and not (validated_descriptor["source_type"] == self.source_type).all():
            mismatched = validated_descriptor[validated_descriptor["source_type"] != self.source_type]["source_type"].unique()
            error_msg = f"Input descriptor contains rows with source_type(s) {list(mismatched)} which do not match the block's configured source_type '{self.source_type}'."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            # Use engine.begin() which yields a Connection within a Transaction context
            with engine.begin() as connection:
                # Pre-Delete Check: Ensure no conflicting source_types exist in the table
                check_stmt = select(func.count(self._table.c.stream_id)).where(
                    self._table.c.source_type != self.source_type
                )
                conflicting_count_result = connection.execute(check_stmt).scalar()
                conflicting_count = conflicting_count_result if conflicting_count_result is not None else 0

                if conflicting_count > 0:
                    error_msg = f"Cannot overwrite table '{self.table_name}' because it contains {conflicting_count} rows with source_type other than '{self.source_type}'."
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                else:
                    self.logger.debug(f"Pre-delete check passed: No conflicting source_types found in '{self.table_name}'.")

                # Delete only rows matching the configured source_type
                self.logger.info(f"Deleting existing rows with source_type '{self.source_type}' from table '{self.table_name}'.")
                delete_stmt = delete(self._table).where(self._table.c.source_type == self.source_type)
                delete_result = connection.execute(delete_stmt) 
                self.logger.info(f"Deleted {delete_result.rowcount} existing rows with source_type '{self.source_type}'.")

                # Insert new data if the descriptor is not empty
                if not validated_descriptor.empty:
                    self.logger.info(f"Inserting {len(validated_descriptor)} new rows into table '{self.table_name}'.")
                    # Use the connection provided by engine.begin() for pandas to_sql
                    validated_descriptor.to_sql(
                        name=self.table_name,
                        con=connection, # Pass the connection
                        if_exists="append", # Safe after delete
                        index=False,
                        # dtype=PrimitiveSourceDataModel.to_sqlalchemy_types() # Optional: Specify types if needed
                    )
                    self.logger.info("New rows inserted successfully.")
                else:
                    self.logger.info("Input descriptor is empty, no rows to insert after delete.")
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
            ValueError: If the input descriptor contains source_types 
                        mismatched with the block's configured source_type.
            SQLAlchemyError: If a database error occurs during the upsert.
            Exception: For other unexpected errors.
        """
        engine = self._get_engine()
        validated_descriptor = PrimitiveSourceDataModel.validate(descriptor)

        if validated_descriptor.empty:
            self.logger.info("Input descriptor is empty, nothing to upsert.")
            return

        # Input Validation: Check if all incoming rows match self.source_type
        if not (validated_descriptor["source_type"] == self.source_type).all():
            mismatched = validated_descriptor[validated_descriptor["source_type"] != self.source_type]["source_type"].unique()
            error_msg = f"Input descriptor contains rows with source_type(s) {list(mismatched)} which do not match the block's configured source_type '{self.source_type}'. Upsert aborted."
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # Convert DataFrame to list of dictionaries for SQLAlchemy Core API
        # Ensure only columns present in the table model are included
        table_cols = [c.name for c in self._table.columns]
        # Filter DataFrame columns before converting to dict
        cols_to_upsert = [col for col in validated_descriptor.columns if col in table_cols]
        rows_to_upsert = validated_descriptor[cols_to_upsert].to_dict(orient="records")
        
        if not rows_to_upsert:
             self.logger.warning("DataFrame columns do not match table columns, nothing to upsert.")
             return

        # Construct the base insert statement
        insert_stmt = pg_insert(self._table).values(rows_to_upsert)

        # Define the ON CONFLICT DO UPDATE clause
        # Use the 'excluded' pseudo-table to refer to the values proposed for insertion
        excluded = insert_stmt.excluded
        update_columns = {
            # Update 'source_id' and 'source_type' if they differ from existing values
            # Use excluded.column_name to reference incoming values
            "source_id": excluded.source_id,
            "source_type": excluded.source_type,
            "updated_at": func.now(), # Explicitly update updated_at
        }

        # The WHERE clause ensures updates only happen if values actually changed
        # Accessing the table columns directly for the comparison
        table: Table = self._table
        # Corrected where_clause without syntax error
        where_clause: ColumnElement[bool] = (table.c.source_id != excluded.source_id) | \
                       (table.c.source_type != excluded.source_type)

        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=[table.c.stream_id],  # Use Column object for index_elements (PK)
            set_=update_columns,                 # Columns to update
            where=where_clause                    # Condition for update
        )

        try:
            # Use engine.begin() which yields a Connection within a Transaction context
            with engine.begin() as connection: # type: Connection
                self.logger.info(f"Executing upsert for {len(rows_to_upsert)} rows into table '{self.table_name}'.")
                connection.execute(upsert_stmt) # Execute on the connection
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
