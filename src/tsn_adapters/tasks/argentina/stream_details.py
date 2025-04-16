"""
Stream details fetcher implementations.
"""

from typing import Literal, TypeGuard, cast

from prefect import task

from tsn_adapters.blocks.primitive_source_descriptor import (
    GithubPrimitiveSourcesDescriptor,
    UrlPrimitiveSourcesDescriptor,
    get_descriptor_from_github,
    get_descriptor_from_url,
)
from tsn_adapters.common.interfaces.provider import IStreamSourceMapFetcher
from tsn_adapters.tasks.argentina.models.stream_source import StreamSourceMetadataModel
from tsn_adapters.tasks.argentina.types import StreamSourceMapDF

PrimitiveSourcesTypeStr = Literal["url", "github"]

def is_valid_source_type(source_type: str) -> TypeGuard[PrimitiveSourcesTypeStr]:
    """Check if the source type is valid."""
    valid_types = ("url", "github")
    return source_type in valid_types

class GitHubStreamDetailsFetcher(IStreamSourceMapFetcher):
    """Fetches stream details from a GitHub source."""

    def __init__(self, block_name: str):
        """
        Initialize with a GitHub primitive sources descriptor block name.

        Args:
            block_name: Name of the GithubPrimitiveSourcesDescriptor block
        """
        self.block_name = block_name
        self.block = GithubPrimitiveSourcesDescriptor.load(block_name)

    def get_streams(self) -> StreamSourceMapDF:
        """
        Fetch stream metadata from GitHub.

        Returns:
            StreamMetadataDF: DataFrame containing stream metadata
        """
        # Get stream metadata from GitHub
        source_metadata_df = get_descriptor_from_github(block=self.block)
        if source_metadata_df is None:
            raise ValueError("Source metadata is None")

        # Validate and coerce using the model
        validated_df = StreamSourceMetadataModel.validate(source_metadata_df)
        return validated_df


class URLStreamDetailsFetcher(IStreamSourceMapFetcher):
    """Fetches stream details from a URL source."""

    def __init__(self, block_name: str):
        """
        Initialize with a URL primitive sources descriptor block name.

        Args:
            block_name: Name of the UrlPrimitiveSourcesDescriptor block
        """
        self.block_name = block_name
        self.block = UrlPrimitiveSourcesDescriptor.load(block_name)

    def get_streams(self) -> StreamSourceMapDF:
        """
        Fetch stream metadata from URL.

        Returns:
            StreamMetadataDF: DataFrame containing stream metadata
        """
        # Get stream metadata from URL
        source_metadata_df = get_descriptor_from_url(block=self.block)
        if source_metadata_df is None:
            raise ValueError("Source metadata is None")

        # Validate and coerce using the model
        validated_df = StreamSourceMetadataModel.validate(source_metadata_df)
        return validated_df


@task(name="Create Stream Details Fetcher")
def create_stream_details_fetcher(source_type: PrimitiveSourcesTypeStr, block_name: str) -> IStreamSourceMapFetcher:
    """
    Factory function to create the appropriate stream details fetcher.

    Args:
        source_type: Type of source ("url" or "github")
        block_name: Name of the source descriptor block

    Returns:
        IStreamDetailsFetcher: The appropriate fetcher implementation
    """
    if source_type == "github":
        return GitHubStreamDetailsFetcher(block_name)
    elif source_type == "url":
        return URLStreamDetailsFetcher(block_name)
    else:
        raise ValueError(f"Invalid source type: {source_type}")
