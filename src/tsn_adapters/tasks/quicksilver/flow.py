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
    return provider.get_latest_data()


@task(name="Transform Quicksilver Data", retries=2)
def task_transform_quicksilver_data(
    data: QuicksilverDataDF,
    stream_id: str,
    data_provider: str = "quicksilver"
) -> DataFrame[TnDataRowModel]:
    transformer = QuicksilverDataTransformer(stream_id=stream_id, data_provider=data_provider)
    return transformer.transform(data)


@flow(name="Quicksilver Data Flow")
def quicksilver_flow(
    quicksilver_block: QuicksilverBlock,
    tn_block: Any,
    ticker: str,
    stream_id: str,
    data_provider: str = "quicksilver",
    endpoint_path: Optional[str] = None,
    max_batch_size: int = 25000,
    dry_run: bool = False
) -> QuicksilverFlowResult:
    logger = get_logger_safe(__name__)
    errors: list[str] = []
    records_fetched = 0
    records_inserted = 0
    
    try:
        provider = QuicksilverProvider(
            quicksilver_block=quicksilver_block,
            ticker=ticker,
            endpoint_path=endpoint_path
        )
        
        quicksilver_data = task_fetch_quicksilver_data(provider)
        records_fetched = len(quicksilver_data)
        
        if quicksilver_data.empty:
            return QuicksilverFlowResult(success=False, records_fetched=0, records_inserted=0, errors=["No data"])
        
        tn_data = task_transform_quicksilver_data(quicksilver_data, stream_id, data_provider)
        
        if tn_data.empty:
            return QuicksilverFlowResult(success=False, records_fetched=records_fetched, records_inserted=0, errors=["Transform failed"])
        
        if dry_run:
            records_inserted = len(tn_data)
        else:
            insertion_results = task_split_and_insert_records(tn_block=tn_block, data=tn_data, max_batch_size=max_batch_size)
            records_inserted = insertion_results.get("records_inserted", 0)
        
        return QuicksilverFlowResult(success=True, records_fetched=records_fetched, records_inserted=records_inserted, errors=errors)
        
    except Exception as e:
        return QuicksilverFlowResult(success=False, records_fetched=records_fetched, records_inserted=records_inserted, errors=[str(e)])