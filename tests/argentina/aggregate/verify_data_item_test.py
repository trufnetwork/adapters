"""
Manual test to verify end-to-end data fetching and aggregation for a single date.

Usage:
    python test_manual_provider.py

This test:
1. Creates a SEPA provider (website or s3).
2. Lists available keys.
3. Filters for an arbitrary date.
4. Fetches the data and transforms it.
5. Aggregates prices by category.
"""

import sys

from prefect_aws import S3Bucket


def manual_test(provider_type: str = "website", test_date: str = "2025-01-04"):
    """
    Manually test fetching SEPA data and aggregating it for a single date.

    provider_type : "website" or "s3"
    test_date     : The date string (YYYY-MM-DD) to test
    """
    from tsn_adapters.tasks.argentina.aggregate.category_price_aggregator import aggregate_prices_by_category
    from tsn_adapters.tasks.argentina.models.category_map import SepaProductCategoryMapModel
    from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
    from tsn_adapters.tasks.argentina.provider.factory import create_sepa_provider

    # 1. Create the provider
    if provider_type == "s3":
        # For S3, you'll need a valid S3Block name, e.g. "argentina-sepa-bucket"
        s3_block_name = "argentina-sepa"
        s3_block = S3Bucket.load(s3_block_name)
        provider = create_sepa_provider(provider_type="s3", s3_block=s3_block, s3_prefix="source_data/")
    else:
        # Website provider
        provider = create_sepa_provider(provider_type="website", delay_seconds=0.1, show_progress_bar=True)

    # 2. List available keys
    print(f"Listing available keys from provider {provider_type}...")
    keys = provider.list_available_keys()
    print(f"Available dates (truncated): {keys[:10]}... (total={len(keys)})")

    # 3. Filter for a specific date
    if test_date not in keys:
        print(f"Date {test_date} not found in provider keys! Exiting.")
        return

    # 4. Fetch data
    print(f"\nFetching SEPA data for date: {test_date}")
    sepa_df = provider.get_data_for(test_date)
    print(f"Fetched {len(sepa_df)} rows for {test_date}")

    if sepa_df.empty:
        print("No data found—nothing to transform or aggregate.")
        return

    # 5. Transform into average prices by product
    print("Loading product-category map from a known URL...")
    category_map_url = (
        "https://drive.usercontent.google.com/u/2/uc?id=1nfcAjCF-BYU5-rrWJW9eFqCw2AjNpc1x&export=download"
    )
    category_map_df = SepaProductCategoryMapModel.from_url(category_map_url, sep="|", compression="zip")
    print(f"Loaded {len(category_map_df)} category-map rows.")

    print("Computing average prices by product...")
    avg_price_df = SepaAvgPriceProductModel.from_sepa_product_data(sepa_df)
    print(f"Got {len(avg_price_df)} rows of average prices.")

    # 6. Aggregate by category
    print("Aggregating average prices by category...")
    aggregated_df = aggregate_prices_by_category(category_map_df, avg_price_df)
    print(f"Aggregated to {len(aggregated_df)} category-date rows.")

    # Show final result head
    print("Sample of the aggregated output:")
    print(aggregated_df.head(10))

    print("\nManual test completed successfully.")


if __name__ == "__main__":
    # You could parse CLI arguments here if you wish.
    # e.g. python test_manual_provider.py website 2025-01-04
    # or python test_manual_provider.py s3 2024-12-31

    args = sys.argv[1:]
    if len(args) == 2:
        provider_type_arg = args[0]
        date_arg = args[1]
    elif len(args) == 1:
        provider_type_arg = args[0]
        date_arg = "2025-01-04"
    else:
        provider_type_arg = "s3"
        date_arg = "2025-01-04"

    manual_test(provider_type_arg, date_arg)
