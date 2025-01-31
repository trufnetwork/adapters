"""
Manual test to verify end-to-end data fetching and aggregation for a single date.

Usage:
    python test_manual_provider.py

This test:
1. Creates a SEPA provider.
2. Lists available keys.
3. Filters for an arbitrary date.
4. Fetches the data and transforms it.
5. Aggregates prices by category.
"""

from prefect_aws import S3Bucket

from tsn_adapters.tasks.argentina.provider.s3 import ProcessedDataProvider, RawDataProvider
from tsn_adapters.utils.deroutine import deroutine


def manual_test(test_date: str = "2025-01-04"):
    """
    Manually test fetching SEPA data and aggregating it for a single date.

    test_date     : The date string (YYYY-MM-DD) to test
    """
    from tsn_adapters.tasks.argentina.aggregate.category_price_aggregator import aggregate_prices_by_category
    from tsn_adapters.tasks.argentina.models.category_map import SepaProductCategoryMapModel
    from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel

    # 1. Create the provider
    s3_block_name = "argentina-sepa"
    s3_block = deroutine(S3Bucket.load(s3_block_name))
    raw_provider = RawDataProvider(s3_block=s3_block)
    ProcessedDataProvider(s3_block=s3_block)

    # 2. List available keys
    print("Listing available keys from provider...")
    keys = raw_provider.list_available_keys()
    print(f"Available dates (truncated): {keys[:10]}... (total={len(keys)})")

    # 3. Filter for a specific date
    if test_date not in keys:
        print(f"Date {test_date} not found in provider keys! Exiting.")
        return

    # 4. Fetch data
    print(f"\nFetching SEPA data for date: {test_date}")
    sepa_df = raw_provider.get_raw_data_for(test_date)
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
    aggregated_df, uncategorized_df = aggregate_prices_by_category(category_map_df, avg_price_df)
    print(f"Aggregated to {len(aggregated_df)} category-date rows.")

    # Show final result head
    print("Sample of the aggregated output:")
    print(aggregated_df.head(10))

    print("Sample of the uncategorized output:")
    print(uncategorized_df.head(10))
    print(f"There are {len(uncategorized_df)} rows of uncategorized data.")

    print("\nManual test completed successfully.")


if __name__ == "__main__":
    manual_test("2025-01-04")
