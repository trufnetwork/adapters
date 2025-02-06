"""
Factory functions for creating SEPA data providers.
"""

from pandera.typing import DataFrame
from prefect import task
from prefect_aws import S3Bucket

from tsn_adapters.common.interfaces.provider import IProviderGetter
from tsn_adapters.tasks.argentina.provider.s3 import ProcessedDataProvider
from tsn_adapters.tasks.argentina.types import AggregatedPricesDF, DateStr


@task(name="Create SEPA Provider")
def create_sepa_processed_provider(block: S3Bucket) -> IProviderGetter[DateStr, DataFrame[AggregatedPricesDF]]:
    """
    Create a SEPA provider instance.

    Returns:
        IProviderGetter: The provider instance
    """
    return ProcessedDataProvider(s3_block=block)
