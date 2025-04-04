"""
S3 data item model for SEPA data.
"""

from prefect_aws import S3Bucket
from pydantic import BaseModel

from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaDataItem
from tsn_adapters.utils import force_sync


class SepaS3RawDataItem(SepaDataItem, BaseModel):
    """Represents a SEPA data item stored in S3."""

    key: str
    block: S3Bucket

    @classmethod
    def create(cls, block: S3Bucket, key: str) -> "SepaS3RawDataItem":
        """Factory method to create a SepaS3RawDataItem instance."""
        item_reported_date = key.split("_")[-1].split(".")[0]
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
        return force_sync(self.block.read_path)(self.key)
