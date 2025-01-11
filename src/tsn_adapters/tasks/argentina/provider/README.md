# SEPA Data Providers

This directory contains the implementation of different data providers for the SEPA (Sistema Electrónico de Publicidad de Precios Argentinos) dataset.

## Structure

- `base.py`: Base interfaces and types
- `website.py`: Website scraper-based provider
- `s3.py`: S3-based provider
- `utils.py`: Shared utilities for data processing
- `factory.py`: Factory function for creating providers

## Usage

The providers are designed to be interchangeable. You can use either the website scraper or S3 provider with the same interface:

```python
from prefect import flow
from prefect_aws import S3Bucket

from tsn_adapters.tasks.argentina.provider.factory import create_sepa_provider

@flow
def my_flow(provider_type: str = "website"):
    # Create a provider (website or S3)
    if provider_type == "website":
        provider = create_sepa_provider("website", show_progress_bar=True)
    else:
        s3_block = S3Bucket.load("my-bucket")
        provider = create_sepa_provider("s3", s3_block=s3_block)

    # Use the provider
    available_dates = provider.list_available_keys()
    for date in available_dates:
        df = provider.get_data_for(date)
        # Process df...
```

## Provider Types

### Website Provider

The website provider scrapes data directly from the SEPA website. It's useful for:
- Initial data collection
- Verifying data availability
- Testing without S3 access

```python
provider = create_sepa_provider(
    "website",
    delay_seconds=0.1,  # Delay between requests
    show_progress_bar=True
)
```

### S3 Provider

The S3 provider reads data from an S3 bucket. It's recommended for:
- Production use
- Faster access
- Reduced load on the source website

```python
provider = create_sepa_provider(
    "s3",
    s3_block=my_s3_block,
    s3_prefix="source_data/"
)
```

## Interface

Both providers implement the `IProviderGetter[DateStr, SepaDF]` interface:

```python
def list_available_keys(self) -> list[DateStr]:
    """Return a list of available dates."""
    pass

def get_data_for(self, key: DateStr) -> SepaDF:
    """Get data for a specific date."""
    pass
```

This ensures that your code can work with any provider implementation without modification. 