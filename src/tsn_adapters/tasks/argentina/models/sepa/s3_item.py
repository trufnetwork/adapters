"""
S3 data item model for SEPA data.
"""

from prefect_aws import S3Bucket
from pydantic import BaseModel

from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaDataItem
from tsn_adapters.tasks.argentina.models.sepa.website_item import SepaWebsiteDataItem
from tsn_adapters.utils import deroutine


class SepaS3RawDataItem(SepaDataItem, BaseModel):
    """Represents a SEPA data item stored in S3."""

    key: str
    block: S3Bucket

    @property
    def reported_date(self) -> str:
        """Get the reported date from the key."""
        return self.key.split("_")[-1].split(".")[0]

    @property
    def item_reported_date(self) -> str:
        """Get the reported date for this item."""
        return SepaWebsiteDataItem.validate_date(self.reported_date)

    @classmethod
    def create(cls, block: S3Bucket, key: str, item_reported_date: str) -> "SepaS3RawDataItem":
        """Factory method to create a SepaS3RawDataItem instance."""
        return cls(
            block=block,
            key=key,
        )

    def fetch_into_memory(self) -> bytes:
        """
        Fetch the zip file content from S3.


        Returns:
            bytes: The file contents
        """
        return deroutine(self.block.read_path(self.key))
