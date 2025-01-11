"""
Factory for creating SEPA data providers.
"""

from prefect import task
from prefect_aws import S3Bucket

from tsn_adapters.tasks.argentina.provider.interfaces import IProviderGetter
from tsn_adapters.tasks.argentina.provider.s3 import create_sepa_s3_provider
from tsn_adapters.tasks.argentina.provider.website import create_sepa_website_provider
from tsn_adapters.tasks.argentina.types import DateStr, SepaDF


@task(name="Create SEPA Provider")
def create_sepa_provider(
    provider_type: str,
    s3_block: S3Bucket | None = None,
    s3_prefix: str = "source_data/",
    delay_seconds: float = 0.1,
    show_progress_bar: bool = False
) -> IProviderGetter[DateStr, SepaDF]:
    """
    Factory function to create a provider based on provider_type.

    Args:
        provider_type: Type of provider to create ('website' or 's3')
        s3_block: The S3 block to use (required for 's3' provider)
        s3_prefix: The prefix for S3 keys (only used for 's3' provider)
        delay_seconds: Delay between requests for website scraping (only used for 'website' provider)
        show_progress_bar: Whether to show progress bars during downloads

    Returns:
        IProviderGetter: The provider instance

    Raises:
        ValueError: If provider_type is unknown or if s3_block is missing for 's3' provider
    """
    if provider_type == "website":
        return create_sepa_website_provider(
            delay_seconds=delay_seconds,
            show_progress_bar=show_progress_bar
        )
    elif provider_type == "s3":
        if s3_block is None:
            raise ValueError("s3_block is required for 's3' provider")
        return create_sepa_s3_provider(
            s3_block=s3_block,
            prefix=s3_prefix
        )
    else:
        raise ValueError(f"Unknown provider_type: {provider_type}") 