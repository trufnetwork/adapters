"""
Tests for the category price aggregator functionality.

This module contains tests that verify the correct behavior of the price
aggregation logic, focusing on the happy path scenarios where input data
is well-formed and valid.
"""

import pandas as pd
from pandera.typing import DataFrame as PaDataFrame
import pytest

from tsn_adapters.tasks.argentina.aggregate.category_price_aggregator import (
    aggregate_prices_by_category,
)
from tsn_adapters.tasks.argentina.models import (
    SepaAvgPriceProductModel,
    SepaProductCategoryMapModel,
)


@pytest.fixture
def product_category_map_df() -> PaDataFrame[SepaProductCategoryMapModel]:
    """
    Fixture providing a small, well-formed DataFrame mapping products to categories.
    """
    data = {
        "id_producto": ["P01", "P02", "P03", "P04"],
        "productos_descripcion": ["Milk 1L", "Milk 2L", "Bread White", "Bread Wheat"],
        "category_id": ["C-MILK", "C-MILK", "C-BREAD", "C-BREAD"],
        "category_name": ["Milk", "Milk", "Bread", "Bread"],
    }
    df = pd.DataFrame(data)
    # Pandera coerces & validates it against SepaProductCategoryMapModel schema
    return PaDataFrame[SepaProductCategoryMapModel](df)


@pytest.fixture
def avg_price_product_df() -> PaDataFrame[SepaAvgPriceProductModel]:
    """
    Fixture providing a small DataFrame of average prices per product per date.
    """
    data = {
        "id_producto": ["P01", "P02", "P03", "P04", "P01", "P03"],
        "date": ["2025-01-01", "2025-01-01", "2025-01-01", "2025-01-01", "2025-01-02", "2025-01-02"],  # 2 dates
        "productos_precio_lista_avg": [100.0, 120.0, 50.0, 55.0, 110.0, 60.0],
        "productos_descripcion": ["Milk 1L", "Milk 2L", "Bread White", "Bread Wheat", "Milk 1L", "Bread White"],
    }
    df = pd.DataFrame(data)
    # Pandera coerces & validates it against SepaAvgPriceProductModel schema
    return PaDataFrame[SepaAvgPriceProductModel](df)


def test_aggregate_prices_simple_case(
    product_category_map_df: PaDataFrame[SepaProductCategoryMapModel],
    avg_price_product_df: PaDataFrame[SepaAvgPriceProductModel],
):
    """
    Tests a straightforward scenario with multiple categories and two dates,
    verifying that the function merges and averages correctly.
    """
    # WHEN
    aggregated_df = aggregate_prices_by_category(product_category_map_df, avg_price_product_df)

    # THEN
    # The aggregator should group by (category_id, date) and compute avg over 'productos_precio_lista_avg'
    # Let's convert it to a Python list of dicts for easy checking
    results = aggregated_df.to_dict(orient="records")

    # Expected computations:
    #   For 2025-01-01:
    #     C-MILK => mean(100.0, 120.0) = 110.0
    #     C-BREAD => mean(50.0, 55.0) = 52.5
    #   For 2025-01-02:
    #     C-MILK => mean(110.0) = 110.0  (only P01 on that date)
    #     C-BREAD => mean(60.0) = 60.0   (only P03 on that date)
    expected = [
        {"category_id": "C-MILK", "date": "2025-01-01", "avg_price": 110.0},
        {"category_id": "C-BREAD", "date": "2025-01-01", "avg_price": 52.5},
        {"category_id": "C-MILK", "date": "2025-01-02", "avg_price": 110.0},
        {"category_id": "C-BREAD", "date": "2025-01-02", "avg_price": 60.0},
    ]

    # Sort both lists to compare easily
    results_sorted = sorted(results, key=lambda x: (x["category_id"], x["date"]))
    expected_sorted = sorted(expected, key=lambda x: (x["category_id"], x["date"]))

    assert results_sorted == expected_sorted, f"Aggregated results differ. Got: {results_sorted}"


def test_aggregate_prices_single_category_single_date():
    """
    Verifies the aggregator works even if there's only one date and one category.
    """
    # Single category => 'C-CHOCOLATE'
    category_map_data = {
        "id_producto": ["CHOC01", "CHOC02"],
        "productos_descripcion": ["Chocolate 1", "Chocolate 2"],
        "category_id": ["C-CHOCOLATE", "C-CHOCOLATE"],
        "category_name": ["Chocolate", "Chocolate"],
    }
    category_map_df = pd.DataFrame(category_map_data)
    category_map_df = PaDataFrame[SepaProductCategoryMapModel](category_map_df)

    avg_price_data = {
        "id_producto": ["CHOC01", "CHOC02"],
        "date": ["2025-02-10", "2025-02-10"],
        "productos_precio_lista_avg": [10.0, 20.0],
        "productos_descripcion": ["Chocolate 1", "Chocolate 2"],
    }
    avg_price_df = pd.DataFrame(avg_price_data)
    avg_price_df = PaDataFrame[SepaAvgPriceProductModel](avg_price_df)

    # WHEN
    aggregated_df = aggregate_prices_by_category(category_map_df, avg_price_df)

    # THEN
    results = aggregated_df.to_dict(orient="records")

    # For a single date: 2025-02-10, single category: C-CHOCOLATE
    # => average(10.0, 20.0) = 15.0
    expected = [
        {
            "category_id": "C-CHOCOLATE",
            "date": "2025-02-10",
            "avg_price": 15.0,
        }
    ]

    assert results == expected, f"Expected {expected}, got {results}"


def test_aggregate_prices_multiple_dates_across_multiple_categories():
    """
    Checks that grouping works when some categories appear on certain dates
    and skip others. Still a happy path: all products exist in the category map.
    """
    # Setup: Two categories (C-FRUIT, C-VEG) on multiple dates, not every category has data on all dates
    category_map_data = {
        "id_producto": ["APPLE", "BANANA", "CARROT", "TOMATO"],
        "productos_descripcion": ["Apple", "Banana", "Carrot", "Tomato"],
        "category_id": ["C-FRUIT", "C-FRUIT", "C-VEG", "C-VEG"],
        "category_name": ["Fruit", "Fruit", "Vegetable", "Vegetable"],
    }
    category_map_df = PaDataFrame[SepaProductCategoryMapModel](pd.DataFrame(category_map_data))

    avg_price_data = {
        "id_producto": ["APPLE", "BANANA", "APPLE", "CARROT", "TOMATO"],
        "date": ["2025-03-01", "2025-03-01", "2025-03-02", "2025-03-01", "2025-03-02"],
        "productos_precio_lista_avg": [5.0, 6.0, 7.0, 3.0, 4.0],
        "productos_descripcion": ["Apple", "Banana", "Apple", "Carrot", "Tomato"],
    }
    avg_price_df = PaDataFrame[SepaAvgPriceProductModel](pd.DataFrame(avg_price_data))

    # WHEN
    aggregated_df = aggregate_prices_by_category(category_map_df, avg_price_df)
    results = aggregated_df.to_dict(orient="records")

    # THEN
    # 2025-03-01 =>
    #   C-FRUIT => average(5.0 [Apple], 6.0 [Banana]) = 5.5
    #   C-VEG   => average(3.0 [Carrot]) = 3.0
    #
    # 2025-03-02 =>
    #   C-FRUIT => average(7.0 [Apple]) = 7.0
    #   C-VEG   => average(4.0 [Tomato]) = 4.0
    expected = [
        {"category_id": "C-FRUIT", "date": "2025-03-01", "avg_price": 5.5},
        {"category_id": "C-VEG", "date": "2025-03-01", "avg_price": 3.0},
        {"category_id": "C-FRUIT", "date": "2025-03-02", "avg_price": 7.0},
        {"category_id": "C-VEG", "date": "2025-03-02", "avg_price": 4.0},
    ]

    # Sort them for easy comparison
    results_sorted = sorted(results, key=lambda x: (x["category_id"], x["date"]))
    expected_sorted = sorted(expected, key=lambda x: (x["category_id"], x["date"]))

    assert results_sorted == expected_sorted, f"Expected {expected_sorted}, got {results_sorted}"
