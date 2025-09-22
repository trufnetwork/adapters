# Quicksilver API Adapter

A complete adapter for integrating Quicksilver REST API data into TrufNetwork.

## Overview

The Quicksilver adapter implements the standard TSN adapter pattern with three main steps:

1. **Fetch** - Retrieve data from Quicksilver API
2. **Normalize** - Transform data to TrufNetwork format
3. **Send** - Insert data into TrufNetwork

## Architecture

### Components

- **`QuicksilverBlock`** - Prefect block for API authentication and HTTP requests
- **`QuicksilverProvider`** - Implements `IProviderGetter` for data fetching
- **`QuicksilverDataTransformer`** - Implements `IDataTransformer` for format conversion
- **`quicksilver_flow`** - Main Prefect flow orchestrating the three steps

## Quick Start

### POC - Try It Now

1. **Set up environment variables:**

   ```bash
   # Add following variables to .env with your actual API details
   # QUICKSILVER_BASE_URL=https://your-quicksilver-api.com/
   # QUICKSILVER_ENDPOINT_PATH=/api/gateway/your-id/coingekoMarket/findMany
   # QUICKSILVER_ASSET_ID=bitcoin
   # QUICKSILVER_STREAM_ID=stream_bitcoin_price
   ```

2. **Run the POC example:**

   ```bash
   cd src/examples/quicksilver/
   python poc_example.py
   ```

This will:

- ✅ Connect to the real Quicksilver API
- ✅ Fetch live cryptocurrency market data
- ✅ Transform it to TrufNetwork format
- ✅ Run the complete flow in dry-run mode
