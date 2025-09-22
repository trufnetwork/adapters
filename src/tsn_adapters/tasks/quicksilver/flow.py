"""
Quicksilver Data Flow

Simple three-step flow for Quicksilver API data:
1. Fetch data from Quicksilver API for single asset
2. Transform to TrufNetwork format  
3. Send to TrufNetwork

Simplified and deterministic for single asset processing.
"""

import os
from typing import Any, Optional, TypedDict

from pandera.typing import DataFrame
from prefect import flow, task

from tsn_adapters.blocks.quicksilver import QuicksilverBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel

# Import TN components with fallback for POC testing
try:
    from tsn_adapters.blocks.tn_access import TNAccessBlock
    from tsn_adapters.common.trufnetwork.tasks.insert import task_split_and_insert_records
except ImportError:
    # Mock implementations for when trufnetwork-sdk-py is not available
    TNAccessBlock = Any  # Accept any object as TN block
    def task_split_and_insert_records(*args, **kwargs):
        """Mock implementation - should not be called in dry_run mode."""
        raise RuntimeError("Real TN insertion attempted without trufnetwork-sdk-py installed. Use dry_run=True.")
            
from tsn_adapters.utils.logging import get_logger_safe

from .provider import QuicksilverProvider
from .transformer import QuicksilverDataTransformer
from .types import QuicksilverDataDF


class QuicksilverFlowResult(TypedDict):
    success: bool
    records_fetched: int
    records_inserted: int
    errors: list[str]


@task(name="Fetch Quicksilver Data", retries=2)
def task_fetch_quicksilver_data(provider: QuicksilverProvider) -> QuicksilverDataDF:
    """
    Fetch data from Quicksilver API for the configured asset.
    
    Args:
        provider: Configured Quicksilver provider
        
    Returns:
        DataFrame containing Quicksilver data
    """
    logger = get_logger_safe(__name__)
    logger.info("Fetching latest Quicksilver data")
    
    data = provider.get_latest_data()
    logger.info(f"Successfully fetched {len(data)} records from Quicksilver")
    return data


@task(name="Transform Quicksilver Data", retries=2)
def task_transform_quicksilver_data(
    data: QuicksilverDataDF,
    stream_id: str,
    data_provider: str = "quicksilver"
) -> DataFrame[TnDataRowModel]:
    """
    Transform Quicksilver data to TrufNetwork format.
    
    Args:
        data: Raw Quicksilver data
        stream_id: TrufNetwork stream ID for this asset
        data_provider: Data provider name
        
    Returns:
        DataFrame in TrufNetwork format
    """
    logger = get_logger_safe(__name__)
    logger.info(f"Transforming {len(data)} Quicksilver records")
    
    transformer = QuicksilverDataTransformer(
        stream_id=stream_id,
        data_provider=data_provider
    )
    
    transformed_data = transformer.transform(data)
    logger.info(f"Successfully transformed {len(transformed_data)} records")
    return transformed_data


@flow(name="Quicksilver Data Flow")
def quicksilver_flow(
    quicksilver_block: QuicksilverBlock,
    tn_block: Any,  # TNAccessBlock or compatible mock object
    asset_id: str,
    stream_id: str,
    data_provider: str = "quicksilver",
    endpoint_path: Optional[str] = None,
    max_batch_size: int = 25000,
    dry_run: bool = False
) -> QuicksilverFlowResult:
    """
    Main flow to fetch Quicksilver data and insert into TrufNetwork.
    
    Args:
        quicksilver_block: Configured Quicksilver API block
        tn_block: TrufNetwork access block for data insertion
        asset_id: Asset ID to fetch (e.g., "bitcoin", "ethereum")
        stream_id: TrufNetwork stream ID for this asset
        data_provider: Name of the data provider (default: "quicksilver")
        endpoint_path: API endpoint path (uses QUICKSILVER_ENDPOINT_PATH env var if None)
        max_batch_size: Maximum batch size for TN insertion (default: 25000)
        dry_run: If True, simulate TN insertion without actually sending data (default: False)
        
    Returns:
        QuicksilverFlowResult with execution statistics and error details
    """
    logger = get_logger_safe(__name__)
    logger.info(f"Starting Quicksilver data flow for asset: {asset_id}")
    
    errors: list[str] = []
    records_fetched = 0
    records_inserted = 0
    
    try:
        logger.info("Step 1: Fetching data from Quicksilver API")
        provider = QuicksilverProvider(
            quicksilver_block=quicksilver_block,
            asset_id=asset_id,
            endpoint_path=endpoint_path
        )
        
        quicksilver_data = task_fetch_quicksilver_data(provider)
        records_fetched = len(quicksilver_data)
        
        if quicksilver_data.empty:
            logger.warning("No data received from Quicksilver API")
            return QuicksilverFlowResult(
                success=False,
                records_fetched=0,
                records_inserted=0,
                errors=["No data available from Quicksilver API"]
            )
        
        logger.info("Step 2: Transforming data to TrufNetwork format")
        tn_data = task_transform_quicksilver_data(
            data=quicksilver_data,
            stream_id=stream_id,
            data_provider=data_provider
        )
        
        if tn_data.empty:
            logger.warning("No data after transformation")
            return QuicksilverFlowResult(
                success=False,
                records_fetched=records_fetched,
                records_inserted=0,
                errors=["No data after transformation"]
            )
        
        logger.info("Step 3: Inserting data into TrufNetwork")
        if dry_run:
            logger.info("🧪 DRY RUN MODE: Simulating TrufNetwork insertion")
            records_inserted = len(tn_data)
        else:
            insertion_results = task_split_and_insert_records(
                tn_block=tn_block,
                data=tn_data,
                max_batch_size=max_batch_size
            )
            records_inserted = insertion_results.get("records_inserted", 0)
        
        logger.info(f"Flow completed successfully: {records_fetched} fetched, {records_inserted} inserted")
        return QuicksilverFlowResult(
            success=True,
            records_fetched=records_fetched,
            records_inserted=records_inserted,
            errors=errors
        )
        
    except Exception as e:
        error_msg = f"Flow failed: {e}"
        logger.error(error_msg)
        errors.append(error_msg)
        
        return QuicksilverFlowResult(
            success=False,
            records_fetched=records_fetched,
            records_inserted=records_inserted,
            errors=errors
        )