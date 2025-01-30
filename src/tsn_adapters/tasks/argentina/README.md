# Argentina SEPA Processing Module

This module implements Argentina's SEPA (Sistema Electrónico de Publicidad de Precios Argentinos) data processing using the TSN Adapters pipeline architecture.

## Overview

The Argentina module demonstrates a specialized implementation of the TSN Adapters pipeline for processing SEPA pricing data. It features a two-stage pipeline architecture:

1. **Preprocessing Flow** - Data validation and aggregation
2. **Ingestion Flow** - Network insertion and synchronization

### Key Components

- **Data Providers** (`provider/`):
  - `RawDataProvider`: Handles raw ZIP files from S3 (source_data/)
  - `ProcessedDataProvider`: Manages processed data (processed/)
  
- **Processing Flows** (`flows/`):
  - `PreprocessFlow`: Validation, aggregation, and S3 storage
  - `IngestFlow`: Network stream management and batch inserts

- **Data Models** (`models/`):
  - Raw data schemas (SepaProductosDataModel)
  - Processed data schemas (SepaAggregatedPricesModel)
  - Category mapping schemas

- **Core Logic**:
  - `aggregate/`: Price aggregation by category
  - `transformers/`: Data format conversions
  - `reconciliation/`: Data consistency strategies

## Usage

### Prerequisites

1. Configure Prefect blocks:
   ```bash
   # S3 storage
   prefect block register -m prefect_aws.s3
   
   # TSN network access
   prefect block register -m tsn_adapters.blocks.tn_access
   ```

### Basic Implementation

```python
from tsn_adapters.tasks.argentina.flows import preprocess_flow, ingest_flow

# Run preprocessing
preprocess_flow(
    date="2024-01-01",
    product_category_map_url="<your-category-map-url>",
    s3_block_name="argentina-sepa"
)

# Run ingestion
ingest_flow(
    source_descriptor_type="github",
    source_descriptor_block_name="argentina-sepa-source-descriptor",
    trufnetwork_access_block_name="default",
    s3_block_name="argentina-sepa"
)
```

### S3 Data Structure
```text
s3://argentina-sepa/
├── source_data/          # Original ZIP files
│   └── sepa_2024-01-01.zip
└── processed/            # Processed outputs
    └── 2024-01-01/
        ├── data.zip      # Aggregated prices
        ├── uncategorized.zip  # Unmapped products
        └── logs.zip      # Processing logs
```

## Monitoring

- **Preprocessing Artifacts**:
  - Markdown summaries in Prefect UI
  - S3 processed/ directory structure
  - Uncategorized products reports

- **Ingestion Tracking**:
  - TSN network transaction logs
  - Stream synchronization status
  - Network insertion metrics

## See Also

- [TSN Adapters Documentation](../../../../README.md)
- [Prefect AWS Documentation](https://prefecthq.github.io/prefect-aws/)
- [Pandera Validation](https://pandera.readthedocs.io/)
