from abc import ABC, abstractmethod
from datetime import datetime, timezone
import io

import pandas as pd
from pandera import DataFrameModel, Field  # type: ignore
from pandera.typing import DataFrame, Series
from prefect import Task
from prefect.blocks.core import Block
from prefect_aws import S3Bucket  # type: ignore
from pydantic import ConfigDict

from tsn_adapters.utils.deroutine import deroutine


class DeploymentStateModel(DataFrameModel):
    """Pandera DataFrameModel for tracking deployment state.

    Fields:
        stream_id: Non-nullable string identifier for the stream.
        deployment_timestamp: Non-nullable pandas Timestamp. Must be timezone-aware (UTC).
    """
    stream_id: Series[str] = Field(nullable=False)
    deployment_timestamp: Series[pd.Timestamp] = Field(nullable=False)

    class Config:  # type: ignore
        strict = "filter"
        coerce = True


class DeploymentStateBlock(Block, ABC):
    model_config = ConfigDict(ignored_types=(Task,))

    @abstractmethod
    def has_been_deployed(self, stream_id: str) -> bool:
        """Check if the deployment has been performed for a given stream_id."""
        raise NotImplementedError

    @abstractmethod
    def check_multiple_streams(self, stream_ids: list[str]) -> dict[str, bool]:
        """Check deployment status for multiple stream_ids and return a dictionary mapping each stream_id to its deployment status."""
        raise NotImplementedError

    @abstractmethod
    def mark_as_deployed(self, stream_id: str, timestamp: datetime) -> None:
        """Mark a single stream_id as deployed at the given timestamp."""
        raise NotImplementedError

    @abstractmethod
    def mark_multiple_as_deployed(self, stream_ids: list[str], timestamp: datetime) -> None:
        """Mark multiple stream_ids as deployed at the given timestamp."""
        raise NotImplementedError

    @abstractmethod
    def get_deployment_states(self) -> DataFrame[DeploymentStateModel]:
        """Retrieve the deployment states as a Pandera DataFrameModel."""
        raise NotImplementedError

    @abstractmethod
    def update_deployment_states(self, states: DataFrame[DeploymentStateModel]) -> None:
        """Update the deployment states with the provided DataFrame of DeploymentStateModel."""
        raise NotImplementedError


class S3DeploymentStateBlock(DeploymentStateBlock):
    """S3-backed deployment state tracking block.
    
    This block uses S3 to store deployment states in a unified parquet file.
    All stream states are stored in a single file for better consistency and atomic updates.
    """
    s3_bucket: S3Bucket
    file_path: str
    model_config = ConfigDict(ignored_types=(Task,))

    def _validate_timestamp(self, timestamp: datetime) -> None:
        """Validate that a timestamp is timezone-aware and in UTC.
        
        Args:
            timestamp: The timestamp to validate.
            
        Raises:
            TypeError: If timestamp is not a datetime object.
            ValueError: If the timestamp is not timezone-aware or not in UTC.
        """
        if timestamp.tzinfo is None:
            raise ValueError("Timestamp must be timezone-aware")
        if timestamp.tzinfo != timezone.utc:
            raise ValueError("Timestamp must be in UTC")

    def _validate_stream_id(self, stream_id: str) -> None:
        """Validate that a stream ID is a non-empty string.
        
        Args:
            stream_id: The stream ID to validate.
            
        Raises:
            TypeError: If stream_id is not a string.
            ValueError: If stream_id is empty.
        """
        if not stream_id:
            raise ValueError("Stream ID cannot be empty")

    def _validate_stream_ids(self, stream_ids: list[str]) -> None:
        """Validate that a list of stream IDs contains only non-empty strings.
        
        Args:
            stream_ids: The list of stream IDs to validate.
            
        Raises:
            TypeError: If stream_ids is not a list or if any stream ID is not a string.
            ValueError: If any stream ID is empty.
        """
        for stream_id in stream_ids:
            self._validate_stream_id(stream_id)

    def mark_as_deployed(self, stream_id: str, timestamp: datetime) -> None:
        self._validate_timestamp(timestamp)
        self._validate_stream_id(stream_id)

        # Attempt to load existing data from S3
        try:
            content = deroutine(self.s3_bucket.read_path(self.file_path))
            buffer = io.BytesIO(content)
            df = pd.read_parquet(buffer, engine='pyarrow')
        except Exception:
            # If file doesn't exist or can't be read, create a new DataFrame
            df = pd.DataFrame(columns=['stream_id', 'deployment_timestamp'])

        # Append the new deployment record
        new_row = {'stream_id': stream_id, 'deployment_timestamp': timestamp}
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        # Write the updated DataFrame back to S3
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
        buffer.seek(0)
        try:
            deroutine(self.s3_bucket.write_path(self.file_path, buffer.getvalue()))
        except Exception as exc:
            raise Exception(f"Failed to write deployment state to S3: {exc}")

    def has_been_deployed(self, stream_id: str) -> bool:
        """Check if the deployment has been performed for a given stream_id.
        
        Args:
            stream_id: The stream ID to check.
            
        Returns:
            bool: True if the stream has been deployed, False otherwise.
            
        Raises:
            TypeError: If the stream ID is not a string.
            ValueError: If the stream ID is empty.
        """
        self._validate_stream_id(stream_id)
        try:
            content = deroutine(self.s3_bucket.read_path(self.file_path))
            buffer = io.BytesIO(content)
            df = pd.read_parquet(buffer, engine='pyarrow')
            return stream_id in df['stream_id'].values
        except ValueError:  # File doesn't exist
            return False
        except Exception as exc:
            raise Exception(f"Failed to read deployment states from S3: {exc}")

    def check_multiple_streams(self, stream_ids: list[str]) -> dict[str, bool]:
        """Check deployment status for multiple stream_ids.
        
        Args:
            stream_ids: List of stream IDs to check.
            
        Returns:
            dict[str, bool]: A dictionary mapping each stream_id to its deployment status.
            
        Raises:
            TypeError: If stream_ids is not a list or if any stream ID is not a string.
            ValueError: If any stream ID is empty.
            Exception: If there is an error reading from S3.
        """
        self._validate_stream_ids(stream_ids)
        if not stream_ids:
            return {}

        try:
            content = deroutine(self.s3_bucket.read_path(self.file_path))
            buffer = io.BytesIO(content)
            df = pd.read_parquet(buffer, engine='pyarrow')
            deployed_streams = set(df['stream_id'].values)
            return {stream_id: stream_id in deployed_streams for stream_id in stream_ids}
        except ValueError:  # File doesn't exist
            return {stream_id: False for stream_id in stream_ids}
        except Exception as exc:
            raise Exception(f"Failed to read deployment states from S3: {exc}")

    def mark_multiple_as_deployed(self, stream_ids: list[str], timestamp: datetime) -> None:
        """Mark multiple stream_ids as deployed at the given timestamp.
        
        Args:
            stream_ids: List of stream IDs to mark as deployed.
            timestamp: The UTC timestamp when the streams were deployed.
            
        Raises:
            TypeError: If stream_ids is not a list, if any stream ID is not a string,
                    or if timestamp is not a datetime object.
            ValueError: If any stream ID is empty, or if the timestamp is not in UTC.
            Exception: If there is an error reading from or writing to S3.
        """
        self._validate_stream_ids(stream_ids)
        self._validate_timestamp(timestamp)

        if not stream_ids:
            return

        try:
            content = deroutine(self.s3_bucket.read_path(self.file_path))
            buffer = io.BytesIO(content)
            df = pd.read_parquet(buffer, engine='pyarrow')
        except ValueError:  # File doesn't exist
            df = pd.DataFrame(columns=['stream_id', 'deployment_timestamp'])
        except Exception as exc:
            raise Exception(f"Failed to read deployment states from S3: {exc}")

        # Create new rows for all stream IDs
        new_rows = [{'stream_id': stream_id, 'deployment_timestamp': timestamp} for stream_id in stream_ids]
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

        # Write the updated DataFrame back to S3
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
        buffer.seek(0)
        try:
            deroutine(self.s3_bucket.write_path(self.file_path, buffer.getvalue()))
        except Exception as exc:
            raise Exception(f"Failed to write deployment states to S3: {exc}")

    def get_deployment_states(self) -> DataFrame[DeploymentStateModel]:
        """Retrieve the deployment states as a Pandera DataFrameModel.
        
        Returns:
            DataFrame[DeploymentStateModel]: A DataFrame containing all deployment states.
            If no states exist, returns an empty DataFrame with the correct schema.
        
        Raises:
            Exception: If there is an error reading from S3 (except for non-existent file).
        """
        try:
            content = deroutine(self.s3_bucket.read_path(self.file_path))
            buffer = io.BytesIO(content)
            df = pd.read_parquet(buffer, engine='pyarrow')
            # Convert timestamps to timezone-aware UTC if they aren't already
            if df['deployment_timestamp'].dt.tz is None:
                df['deployment_timestamp'] = pd.to_datetime(df['deployment_timestamp']).dt.tz_localize('UTC')
            else:
                df['deployment_timestamp'] = pd.to_datetime(df['deployment_timestamp']).dt.tz_convert('UTC')
            # Ensure the DataFrame matches our model schema
            return DataFrame[DeploymentStateModel](df)
        except ValueError:  # File doesn't exist
            # Create an empty DataFrame with the correct schema
            empty_df = pd.DataFrame(columns=['stream_id', 'deployment_timestamp'])
            return DataFrame[DeploymentStateModel](empty_df)
        except Exception as exc:
            raise Exception(f"Failed to read deployment states from S3: {exc}")

    def update_deployment_states(self, states: DataFrame[DeploymentStateModel]) -> None:
        """Update the deployment states with the provided DataFrame of DeploymentStateModel.
        
        This method completely replaces the existing deployment states with the new ones.
        The provided DataFrame must conform to the DeploymentStateModel schema.
        
        Args:
            states: DataFrame[DeploymentStateModel] containing the new deployment states.
            Must have 'stream_id' and 'deployment_timestamp' columns with correct types.
        
        Raises:
            Exception: If there is an error writing to S3 or if the DataFrame doesn't match the schema.
        """
        # The type annotation already ensures states is a DataFrame[DeploymentStateModel]
        # which means it has already been validated against the schema
        
        # Write the DataFrame to S3, ensuring timestamps are in UTC
        df = states.copy()
        if df['deployment_timestamp'].dt.tz is None:
            df['deployment_timestamp'] = pd.to_datetime(df['deployment_timestamp']).dt.tz_localize('UTC')
        else:
            df['deployment_timestamp'] = pd.to_datetime(df['deployment_timestamp']).dt.tz_convert('UTC')
        
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
        buffer.seek(0)
        try:
            deroutine(self.s3_bucket.write_path(self.file_path, buffer.getvalue()))
        except Exception as exc:
            raise Exception(f"Failed to write deployment states to S3: {exc}") 