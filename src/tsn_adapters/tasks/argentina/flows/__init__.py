"""Argentina SEPA Data Processing Flows

This package contains the flows for processing Argentina SEPA data:

1. Preprocessing Flow:
   - Validates raw data from S3
   - Applies category mapping
   - Aggregates prices
   - Stores processed data back to S3

2. Ingestion Flow:
   - Loads preprocessed data from S3
   - Manages network streams
   - Handles batch insertions to Truf.network

Usage:
    from tsn_adapters.tasks.argentina.flows import preprocess_flow, ingest_flow

    # Run preprocessing
    preprocess_flow(
        date="2024-01-01",
        product_category_map_url="...",
        s3_block_name="argentina-sepa"
    )

    # Run ingestion
    ingest_flow(
        source_descriptor_type="github",
        source_descriptor_block_name="argentina-sepa-source-descriptor",
        trufnetwork_access_block_name="default",
        s3_block_name="argentina-sepa"
    )
"""

from .preprocess_flow import preprocess_flow, PreprocessFlow
from .ingest_flow import ingest_flow, IngestFlow

__all__ = [
    "preprocess_flow",
    "PreprocessFlow",
    "ingest_flow",
    "IngestFlow",
]
