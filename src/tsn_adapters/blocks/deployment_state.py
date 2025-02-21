from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd
from pandera import DataFrameModel, Field
from pandera.typing import DataFrame, Series
from prefect.blocks.core import Block


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