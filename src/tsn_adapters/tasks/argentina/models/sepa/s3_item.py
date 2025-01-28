"""
S3-based SEPA data provider implementation.
"""

from collections.abc import Coroutine

from prefect_aws.s3 import S3Bucket
from pydantic import BaseModel, field_validator

from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaDataItem
from tsn_adapters.tasks.argentina.models.sepa.website_item import SepaWebsiteDataItem


class SepaS3RawDataItem(SepaDataItem, BaseModel):
    """Represents a SEPA data item stored in S3."""

    key: str
    bucket: str = "argentina-sepa"
    block: S3Bucket
    item_reported_date: str

    @field_validator("item_reported_date")
    @classmethod
    def validate_date(cls, v: str) -> str:
        """Validate date format YYYY-MM-DD."""
        # Reuse the same validation as SepaHistoricalDataItem
        return SepaWebsiteDataItem.validate_date(v)

    @classmethod
    def create(cls, block: S3Bucket, key: str, item_reported_date: str) -> "SepaS3RawDataItem":
        """Factory method to create a SepaS3DataItem instance."""
        return cls(
            block=block,
            key=key,
            item_reported_date=item_reported_date,
        )

    def fetch_into_memory(self) -> bytes:
        """
        Fetch the zip file content from S3.


        Returns:
            bytes: The file contents
        """
        result = self.block.read_path(self.key)
        if isinstance(result, Coroutine):
            raise ValueError("Task returned a coroutine")
        return result
