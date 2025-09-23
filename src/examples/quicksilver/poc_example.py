#!/usr/bin/env python3
"""
Quicksilver POC - Data Integration

Fetches data from Quicksilver API using ticker-based filtering.
"""

import os
from env_utils import load_environment, get_quicksilver_config

env_status = load_environment()
config = get_quicksilver_config()

TICKER = config['ticker']
STREAM_ID = config['stream_id']

from tsn_adapters.blocks.quicksilver import QuicksilverBlock
from tsn_adapters.tasks.quicksilver.flow import quicksilver_flow
from mock_tn_block import MockTNAccessBlock


def main():
    print("🚀 Quicksilver POC - Data Integration")
    print("=" * 50)
    print(f"This POC demonstrates:")
    print(f"1. 📡 Connect to Quicksilver API")
    print(f"2. 📊 Fetch data for ticker: {TICKER}")
    print(f"3. 🔄 Transform data to TrufNetwork format")
    print(f"4. 🧪 Run complete flow in dry-run mode")
    print()
    
    print("📡 Step 1: Setting up Quicksilver API connection...")
    try:
        quicksilver_block = QuicksilverBlock(base_url=config['base_url'], timeout=30)
        print("✅ API block configured successfully")
    except Exception as e:
        print(f"❌ Failed to configure API block: {e}")
        return
    
    print(f"\n📊 Step 2: Fetching data for ticker {TICKER}...")
    try:
        data = quicksilver_block.fetch_data(
            endpoint_path=config['endpoint_path'],
            ticker=TICKER,
            params={"source": "yahoo", "limit": "10"}
        )
        
        print(f"✅ Successfully fetched {len(data)} record(s)")
        print(f"\n📋 Sample data for {TICKER}:")
        if len(data) > 0:
            record = data.iloc[0]
            print(f"   Ticker: {record['ticker']}")
            print(f"   Price: {record['price']}")
        
    except Exception as e:
        print(f"❌ Failed to fetch sample data: {e}")
        return
    
    print(f"\n🔄 Step 3: Testing complete flow (dry-run mode)...")
    try:
        tn_block = MockTNAccessBlock()
        
        result = quicksilver_flow(
            quicksilver_block=quicksilver_block,
            tn_block=tn_block,
            ticker=TICKER,
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
        else:
            print("   No errors detected")
        
        print(f"\n🎯 POC completed! The adapter is ready to:")
        print(f"   • Fetch data from any Quicksilver endpoint")
        print(f"   • Transform data for TrufNetwork")
        print(f"   • Handle different tickers by changing QUICKSILVER_TICKER")
        
    except Exception as e:
        print(f"❌ Flow execution failed: {e}")
        return

if __name__ == "__main__":
    main()