#!/usr/bin/env python3
"""
Quicksilver POC - Single Asset Data Integration

Fetches asset data from Quicksilver API using ID-based filtering.

Features:
- Single asset focus (configurable via QUICKSILVER_ASSET_ID)
- Dry-run mode for safe testing
"""

import os
from env_utils import load_environment, get_quicksilver_config

env_status = load_environment()
config = get_quicksilver_config()

ASSET_ID = config['asset_id']
STREAM_ID = config['stream_id']

from tsn_adapters.blocks.quicksilver import QuicksilverBlock
from tsn_adapters.tasks.quicksilver.flow import quicksilver_flow
from mock_tn_block import MockTNAccessBlock


def main():
    print("🚀 Quicksilver POC - Single Asset Integration")
    print("=" * 60)
    print(f"Asset: {ASSET_ID}")
    print(f"Stream ID: {STREAM_ID}")
    print()
    
    print("📡 Step 1: Setting up Quicksilver API connection...")
    
    try:
        quicksilver_block = QuicksilverBlock(
            base_url=config['base_url'],
            timeout=30
        )
        print("✅ API block configured")
    except Exception as e:
        print(f"❌ Failed to configure API block: {e}")
        return
    
    print(f"\n📊 Step 2: Fetching {ASSET_ID} market data...")
    
    try:
        endpoint_path = config['endpoint_path']
        market_data = quicksilver_block.fetch_data(
            endpoint_path=endpoint_path,
            asset_id=ASSET_ID,
            params={"source": "pg", "limit": "1"}
        )
        
        print(f"✅ Successfully fetched {len(market_data)} record!")
        print(f"\n📋 Sample data for {ASSET_ID}:")
        if len(market_data) > 0:
            record = market_data.iloc[0]
            print(f"   ID: {record['id']}")
            print(f"   Symbol: {record['symbol']}")
            print(f"   Price: ${record['current_price']}")
            print(f"   Last Updated: {record['last_updated']}")
        
    except Exception as e:
        print(f"❌ Failed to fetch sample data: {e}")
        return
    
    print(f"\n🔄 Step 3: Testing complete flow (dry-run mode)...")
    
    try:
        tn_block = MockTNAccessBlock()
        
        result = quicksilver_flow(
            quicksilver_block=quicksilver_block,
            tn_block=tn_block,
            asset_id=ASSET_ID,
            stream_id=STREAM_ID,
            data_provider="quicksilver-poc",
            dry_run=True
        )
        
        print("✅ Complete flow executed successfully!")
        print(f"\n📊 Flow Results:")
        print(f"   Success: {result['success']}")
        print(f"   Records Fetched: {result['records_fetched']}")
        print(f"   Records Ready for TN: {result['records_inserted']}")
        
        if result['errors']:
            print(f"   Errors: {result['errors']}")
        
    except Exception as e:
        print(f"❌ Flow execution failed: {e}")
        return

if __name__ == "__main__":
    main()