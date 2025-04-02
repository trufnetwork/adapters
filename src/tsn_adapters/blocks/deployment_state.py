"""
Deployment State Tracking Module

This module defines the Pandera DataFrameModel for deployment state tracking,
an abstract base class DeploymentStateBlock, and a concrete implementation
S3DeploymentStateBlock that uses an S3 bucket to store deployment state information
in a unified parquet file.

Usage Example:
    Before using this block:
    1. Create an S3Bucket block in Prefect (UI or programmatically)
    2. Register this DeploymentState block in Prefect (UI or programmatically)
    3. Configure an instance with your S3 bucket details

    # Example using Prefect's recommended loading method:
    from prefect_aws import S3Bucket
    from tsn_adapters.blocks.deployment_state import S3DeploymentStateBlock
    from datetime import datetime, timezone

    # Load your pre-configured blocks
    deployment_block = S3DeploymentStateBlock.load("your-deployment-block-name")
    timestamp = datetime.now(timezone.utc)
    deployment_block.mark_as_deployed("stream_id_example", timestamp)

    # Alternatively, if creating programmatically:
    s3_bucket = S3Bucket.load("your-bucket-block-name")
    deployment_block = S3DeploymentStateBlock(
        s3_bucket=s3_bucket,
        file_path="deployment_states/all_streams.parquet"
    )
    deployment_block.save("your-deployment-block-name")
"""

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
    deployment_timestamp: Series[pd.Timestamp] = Field(nullable=True)

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
        """Check deployment status for multiple stream_ids and return a dictionary
        mapping each stream_id to its deployment status."""
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

    Attributes:
        s3_bucket: The S3 bucket to store deployment states in.
        file_path: The path within the bucket where the parquet file is stored.
    """
    s3_bucket: S3Bucket
    file_path: str
    model_config = ConfigDict(ignored_types=(Task,))

    def _create_empty_df(self) -> pd.DataFrame:
        """Create an empty DataFrame with the correct schema for deployment states.
        
        Returns:
            pd.DataFrame: An empty DataFrame with 'stream_id' and 'deployment_timestamp' columns.
        """
        return pd.DataFrame({
            'stream_id': pd.Series(dtype='str'),
            'deployment_timestamp': pd.Series(dtype='datetime64[ns, UTC]')
        })

    def _ensure_utc_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all timestamps in the DataFrame are timezone-aware UTC.
        
        Args:
            df: DataFrame containing deployment timestamps.
            
        Returns:
            pd.DataFrame: DataFrame with UTC-aware timestamps.
        """
        if not df.empty and 'deployment_timestamp' in df.columns:
            if df['deployment_timestamp'].dt.tz is None:
                df['deployment_timestamp'] = df['deployment_timestamp'].dt.tz_localize('UTC')
            else:
                df['deployment_timestamp'] = df['deployment_timestamp'].dt.tz_convert('UTC')
        return df

    def _validate_timestamp(self, timestamp: datetime) -> None:
        """Validate that a timestamp is timezone-aware and in UTC.
        
        Args:
            timestamp: The timestamp to validate.
            
        Raises:
            ValueError: If the timestamp is not timezone-aware or not in UTC.
        """
        if timestamp.tzinfo is None:
            raise ValueError("Timestamp must be timezone-aware and in UTC.")
        if timestamp.tzinfo != timezone.utc:
            raise ValueError("Timestamp must be in UTC timezone.")

    def _validate_stream_id(self, stream_id: str) -> None:
        """Validate that a stream ID is a non-empty string.
        
        Args:
            stream_id: The stream ID to validate.
            
        Raises:
            ValueError: If stream_id is empty.
        """
        if not stream_id:
            raise ValueError("Stream ID cannot be empty.")

    def _validate_stream_ids(self, stream_ids: list[str]) -> None:
        """Validate that a list of stream IDs contains only non-empty strings.
        
        Args:
            stream_ids: The list of stream IDs to validate.
            
        Raises:
            ValueError: If any stream ID is empty.
        """
        for stream_id in stream_ids:
            self._validate_stream_id(stream_id)

    def _read_deployment_df(self, ignore_errors: bool = True) -> pd.DataFrame:
        """Read the deployment states DataFrame from S3.
        
        Args:
            ignore_errors: If True, return an empty DataFrame on errors instead of raising exceptions.
            
        Returns:
            pd.DataFrame: The deployment states DataFrame.
            
        Raises:
            Exception: If ignore_errors is False and there's an error reading from S3.
        """
        try:
            content = deroutine(self.s3_bucket.read_path(self.file_path))
            buffer = io.BytesIO(content)
            df = pd.read_parquet(buffer, engine='pyarrow')
            return self._ensure_utc_timestamps(df)
        except ValueError:  # File doesn't exist
            return self._create_empty_df()
        except Exception as exc:
            if ignore_errors:
                return self._create_empty_df()
            else:
                raise Exception(f"Failed to read deployment states from S3: {str(exc)}")

    def _write_deployment_df(self, df: pd.DataFrame) -> None:
        """Write the deployment states DataFrame to S3.
        
        Args:
            df: The DataFrame to write.
            
        Raises:
            Exception: If there's an error writing to S3.
        """
        try:
            df = self._ensure_utc_timestamps(df)
            buffer = io.BytesIO()
            df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
            buffer.seek(0)
            deroutine(self.s3_bucket.write_path(self.file_path, buffer.getvalue()))
        except Exception as exc:
            raise Exception(f"Failed to write deployment states to S3: {str(exc)}")

    def _update_deployment_records(self, df: pd.DataFrame, stream_ids: list[str], timestamp: datetime) -> pd.DataFrame:
        """Update deployment records, ensuring uniqueness of stream_ids.
        
        This method removes any existing records for the given stream_ids and adds
        new records with the provided timestamp.
        
        Args:
            df: Existing DataFrame of deployment records
            stream_ids: List of stream IDs to update
            timestamp: The UTC timestamp to use for the new records
            
        Returns:
            pd.DataFrame: Updated DataFrame with unique stream_ids
        """
        # Remove existing records for these stream_ids
        df = df[~df['stream_id'].astype(str).isin([str(sid) for sid in stream_ids])]
        
        # Create new records
        new_records = pd.DataFrame({
            'stream_id': pd.Series(stream_ids, dtype=str),
            'deployment_timestamp': pd.Series([timestamp] * len(stream_ids), dtype='datetime64[ns, UTC]')
        })
        
        # Concatenate with existing records
        return pd.concat([df, new_records], ignore_index=True)

    def mark_as_deployed(self, stream_id: str, timestamp: datetime) -> None:
        """Mark a single stream as deployed at the given timestamp.
        
        This method updates the deployment record for the specified stream. If the stream
        was previously marked as deployed, the old record will be replaced with the new timestamp.
        
        Args:
            stream_id: The unique identifier for the stream.
            timestamp: The UTC timestamp when the stream was deployed.
            
        Raises:
            ValueError: If the stream_id is empty or if the timestamp is not timezone-aware UTC.
        """
        self._validate_timestamp(timestamp)
        self._validate_stream_id(stream_id)
        df = self._read_deployment_df(ignore_errors=True)
        df = self._update_deployment_records(df, [stream_id], timestamp)
        self._write_deployment_df(df)

    def has_been_deployed(self, stream_id: str) -> bool:
        """Check if a stream has ever been deployed.
        
        Args:
            stream_id: The unique identifier for the stream to check.
            
        Returns:
            bool: True if the stream has been deployed at least once, False otherwise.
            
        Raises:
            ValueError: If the stream_id is empty.
        """
        self._validate_stream_id(stream_id)
        df = self._read_deployment_df(ignore_errors=True)
        deployed_streams = set(df['stream_id'].values)
        return stream_id in deployed_streams

    def check_multiple_streams(self, stream_ids: list[str]) -> dict[str, bool]:
        """Check deployment status for multiple streams simultaneously.
        
        This method is more efficient than calling has_been_deployed multiple times
        as it only reads the deployment state file once.
        
        Args:
            stream_ids: List of stream identifiers to check.
            
        Returns:
            dict[str, bool]: A dictionary mapping each stream_id to its deployment status.
            
        Raises:
            ValueError: If any stream_id in the list is empty.
        """
        self._validate_stream_ids(stream_ids)
        df = self._read_deployment_df(ignore_errors=True)
        deployed_streams = set(df['stream_id'].values)
        return {stream_id: stream_id in deployed_streams for stream_id in stream_ids}

    def mark_multiple_as_deployed(self, stream_ids: list[str], timestamp: datetime) -> None:
        """Mark multiple streams as deployed at the same timestamp.
        
        This method updates the deployment records for the specified streams. If any stream
        was previously marked as deployed, its old record will be replaced with the new timestamp.
        
        Args:
            stream_ids: List of stream identifiers to mark as deployed.
            timestamp: The UTC timestamp when the streams were deployed.
            
        Raises:
            ValueError: If any stream_id is empty or if the timestamp is not timezone-aware UTC.
        """
        self._validate_timestamp(timestamp)
        self._validate_stream_ids(stream_ids)
        if not stream_ids:
            return
        df = self._read_deployment_df(ignore_errors=True)
        df = self._update_deployment_records(df, stream_ids, timestamp)
        self._write_deployment_df(df)

    def get_deployment_states(self) -> DataFrame[DeploymentStateModel]:
        """Retrieve all deployment states.
        
        This method returns a DataFrame containing all deployment records. The DataFrame
        is validated against the DeploymentStateModel schema to ensure data consistency.
        
        Returns:
            DataFrame[DeploymentStateModel]: A DataFrame containing all deployment states.
            If no states exist, returns an empty DataFrame with the correct schema.
            
        Raises:
            Exception: If there is an error reading from S3 and the data cannot be retrieved.
        """
        try:
            df = self._read_deployment_df(ignore_errors=False)
            if df.empty:
                df = self._create_empty_df()
            return DataFrame[DeploymentStateModel](df)
        except Exception as exc:
            raise Exception(f"Failed to read deployment states from S3: {str(exc)}")

    def update_deployment_states(self, states: DataFrame[DeploymentStateModel]) -> None:
        """Update all deployment states with a new DataFrame.
        
        This method completely replaces the existing deployment states with the new ones.
        The provided DataFrame must conform to the DeploymentStateModel schema.
        
        Args:
            states: DataFrame containing the new deployment states. Must have 'stream_id'
                   and 'deployment_timestamp' columns with correct types.
            
        Raises:
            Exception: If there is an error writing to S3.
        """
        self._write_deployment_df(states) 