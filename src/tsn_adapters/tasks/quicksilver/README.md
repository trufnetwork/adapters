# Quicksilver API Adapter

A complete adapter for integrating Quicksilver REST API data into TrufNetwork.

## Overview

The Quicksilver adapter implements the standard TSN adapter pattern:

1. **Fetch** - Retrieve data from Quicksilver API
2. **Transform** - Convert data to TrufNetwork format  
3. **Insert** - Send data to TrufNetwork

## Architecture

### Components

- **`QuicksilverBlock`** - Prefect block for HTTP requests
- **`QuicksilverProvider`** - Data fetching implementation
- **`QuicksilverDataTransformer`** - Format conversion
- **`quicksilver_flow`** - Main Prefect flow

### Data Model

```python
class QuicksilverDataModel:
    ticker: str    # Ticker symbol
    price: str     # Current price
```

## Quick Start

### POC Example

1. **Set environment variables:**

   ```bash
   # .env file
   QUICKSILVER_BASE_URL=https://api.example.com/
   QUICKSILVER_ENDPOINT_PATH=/api/gateway/example/findMany
   QUICKSILVER_TICKER=AAPL
   QUICKSILVER_STREAM_ID=stream_aapl_price
   ```

2. **Run the POC:**

   ```bash
   cd src/examples/quicksilver/
   python poc_example.py
   ```

This demonstrates:

- ✅ API connection and data fetching
- ✅ Data transformation to TrufNetwork format
- ✅ Complete flow execution in dry-run mode

## Usage

```python
from tsn_adapters.blocks.quicksilver import QuicksilverBlock
from tsn_adapters.tasks.quicksilver.flow import quicksilver_flow

# Configure API access
block = QuicksilverBlock(base_url="https://api.example.com/")

# Run flow
result = quicksilver_flow(
    quicksilver_block=block,
    tn_block=tn_block,
    ticker="AAPL",
    stream_id="stream_aapl_price",
    dry_run=True
)
```
