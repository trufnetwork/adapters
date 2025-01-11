# Argentina SEPA Ingestion Module

This module implements Argentina's SEPA (Sistema Electrónico de Publicidad de Precios Argentinos) data ingestion using the TSN Adapters pipeline architecture.

## Overview

The Argentina module demonstrates a specialized implementation of the TSN Adapters pipeline for ingesting and processing SEPA pricing data. It showcases how to adapt the core pipeline interfaces for this specific data source.

### Key Components

- **Data Providers** (`provider/`): Implementations for fetching SEPA data
  - Website scraper
  - S3 bucket integration
  
- **Models** (`models/`): Pandera schemas for data validation
  - SEPA-specific data models
  - Category mapping schemas
  
- **Transformers** (`transformers/`): Data transformation logic
  - Raw SEPA → TSN-compatible format
  - Category-specific transformations

- **Reconciliation** (`reconciliation/`): Strategies for data consistency
  - Date-based reconciliation
  - Partial data handling

- **Aggregation** (`aggregate/`): Price aggregation logic
  - Category-level computations
  - Statistical processing

## Usage

### Prerequisites

1. Configure required Prefect blocks:
   - `TNAccessBlock` for TSN network access
   - Data source descriptor (e.g., `S3Block` or `UrlBlock`)

### Basic Implementation

```python
from tsn_adapters.tasks.argentina.flows import argentina_ingestor_flow

# Run the ingestion flow
argentina_ingestor_flow(
    source_descriptor_type="github",  # or "s3", "url"
    source_descriptor_block_name="your-block-name",
    trufnetwork_access_block_name="your-tn-block",
    product_category_map_url="your-category-map",
    data_provider="your-provider-id"
)
```

### Monitoring

- Track progress via Prefect UI
- Review logs for validation results
- Monitor TSN network for successful data insertion

## See Also

- [TSN Adapters Documentation](../../../../README.md)
- [Prefect Documentation](https://docs.prefect.io/)
- [Pandera Documentation](https://pandera.readthedocs.io/)
