"""
Simple tests for streaming processing - focuses on correctness and basic memory benefits.
"""

from typing import Any

import pandas as pd
from pandera.typing import DataFrame
import pytest

from tsn_adapters.tasks.argentina.flows.preprocess_flow import process_raw_data, process_raw_data_streaming
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaProductosDataModel
from tsn_adapters.tasks.argentina.types import CategoryMapDF


def test_streaming_equivalence():
    """Test that streaming processing produces identical results to original processing."""
    # Simple test data
    raw_data = generate_test_data(product_count=5, price_range=100, duplicate_factor=2)
    category_data = generate_category_map(5)

    # Original processing
    original_result = process_raw_data(raw_data, category_data)

    # Streaming processing with single chunk (should be identical)
    def single_chunk_stream():
        yield raw_data

    streaming_result = process_raw_data_streaming(single_chunk_stream(), category_data)

    # Verify identical results
    assert_results_equivalent(original_result, streaming_result)


@pytest.mark.parametrize("chunk_size", [1, 10, 100, 1000])
def test_chunk_size_robustness(chunk_size: int):
    """Test that different chunk sizes all produce the same results."""
    test_data = generate_test_data(product_count=3, price_range=100, duplicate_factor=2)
    category_map = generate_category_map(3)

    # Original result
    original_result = process_raw_data(test_data, category_map)

    # Streaming with different chunk sizes
    def chunked_stream():
        for i in range(0, len(test_data), chunk_size):
            chunk = test_data.iloc[i : i + chunk_size]
            if not chunk.empty:
                yield chunk

    streaming_result = process_raw_data_streaming(chunked_stream(), category_map, chunk_size=chunk_size)

    # Should produce same results regardless of chunk size
    assert_results_equivalent(original_result, streaming_result)


def test_memory_benefit_exists():
    """Basic test that streaming doesn't use more memory than original (smoke test)."""
    # This is just a smoke test - we can't easily measure exact memory usage
    # but we can verify streaming works without errors on larger datasets
    large_dataset = generate_test_data(product_count=50, price_range=1000, duplicate_factor=10)  # ~500 rows
    category_map = generate_category_map(50)

    def chunked_stream():
        chunk_size = 10
        for i in range(0, len(large_dataset), chunk_size):
            yield large_dataset.iloc[i : i + chunk_size]

    # Should complete without memory errors
    result = process_raw_data_streaming(chunked_stream(), category_map, chunk_size=10)
    
    # Basic sanity check
    assert len(result[2]) > 0, "Should produce some weighted average results"


# =============================================================================
# Helper Functions
# =============================================================================


def generate_test_data(product_count: int, price_range: int, duplicate_factor: int) -> DataFrame[SepaProductosDataModel]:
    """Generate simple test data."""
    if product_count <= 0:
        return DataFrame[SepaProductosDataModel](pd.DataFrame())

    data = []
    for i in range(product_count):
        for _ in range(duplicate_factor):
            data.append({
                "id_producto": f"P{i:03d}",
                "productos_descripcion": f"Product {i}",
                "productos_precio_lista": float(price_range * (i + 1) / product_count),
                "date": "2024-01-01",
            })

    return DataFrame[SepaProductosDataModel](pd.DataFrame(data))


def generate_category_map(product_count: int) -> CategoryMapDF:
    """Generate simple category mapping."""
    if product_count <= 0:
        return CategoryMapDF(pd.DataFrame())

    data = []
    for i in range(product_count):
        data.append({
            "id_producto": f"P{i:03d}",
            "productos_descripcion": f"Product {i}",
            "category_id": f"C{i % 3:03d}",  # Rotate through 3 categories
            "category_name": f"Category {i % 3}",
        })

    return CategoryMapDF(pd.DataFrame(data))


def assert_results_equivalent(original_result: Any, streaming_result: Any):
    """Assert that two results are equivalent - using actual return types."""
    orig_cat, orig_uncat, orig_weighted = original_result
    stream_cat, stream_uncat, stream_weighted = streaming_result

    # Same aggregated categories
    pd.testing.assert_frame_equal(
        orig_cat.sort_values(["category_id", "date"]).reset_index(drop=True),
        stream_cat.sort_values(["category_id", "date"]).reset_index(drop=True),
    )

    # Same uncategorized products
    pd.testing.assert_frame_equal(
        orig_uncat.sort_values(["id_producto"]).reset_index(drop=True),
        stream_uncat.sort_values(["id_producto"]).reset_index(drop=True),
    )

    # Same product averages (comparing the core price columns)
    orig_avg_subset = orig_weighted[["id_producto", "productos_precio_lista_avg", "date"]]
    stream_avg_subset = stream_weighted[["id_producto", "productos_precio_lista_avg", "date"]]

    pd.testing.assert_frame_equal(
        orig_avg_subset.sort_values(["id_producto", "date"]).reset_index(drop=True),
        stream_avg_subset.sort_values(["id_producto", "date"]).reset_index(drop=True),
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
