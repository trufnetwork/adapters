"""
Unit tests for the Argentina SEPA product aggregation data models.
"""

import json

import pandas as pd
from pandera.errors import SchemaError
from pydantic import ValidationError
import pytest

from tsn_adapters.tasks.argentina.models.aggregate_products_models import (
    DynamicPrimitiveSourceModel,
    ProductAggregationMetadata,
)

# --- Tests for ProductAggregationMetadata ---


def test_metadata_instantiation_defaults():
    """Test instantiation with default values."""
    metadata = ProductAggregationMetadata()
    assert metadata.last_processed_date == "1970-01-01"
    assert metadata.total_products_count == 0


def test_metadata_instantiation_valid():
    """Test instantiation with valid data."""
    metadata = ProductAggregationMetadata(last_processed_date="2023-10-26", total_products_count=150)
    assert metadata.last_processed_date == "2023-10-26"
    assert metadata.total_products_count == 150


def test_metadata_instantiation_invalid_date():
    """Test instantiation with invalid date format raises ValidationError."""
    with pytest.raises(ValidationError, match="last_processed_date must be in YYYY-MM-DD format"):
        ProductAggregationMetadata(last_processed_date="26-10-2023", total_products_count=10)


def test_metadata_serialization_deserialization():
    """Test serialization to dict/json and deserialization."""
    metadata = ProductAggregationMetadata(last_processed_date="2024-01-15", total_products_count=500)
    metadata_dict = metadata.model_dump()
    assert metadata_dict == {"last_processed_date": "2024-01-15", "total_products_count": 500}

    metadata_json = metadata.model_dump_json()
    assert json.loads(metadata_json) == metadata_dict

    # Deserialize from dict
    deserialized_from_dict = ProductAggregationMetadata(**metadata_dict)
    assert deserialized_from_dict == metadata

    # Deserialize from json
    deserialized_from_json = ProductAggregationMetadata.model_validate_json(metadata_json)
    assert deserialized_from_json == metadata


# --- Tests for DynamicPrimitiveSourceModel ---


@pytest.fixture
def valid_dynamic_source_data() -> pd.DataFrame:
    """Fixture for a valid DataFrame conforming to DynamicPrimitiveSourceModel."""
    return pd.DataFrame(
        {
            "stream_id": ["arg_sepa_prod_123", "arg_sepa_prod_456"],
            "source_id": ["123", "456"],
            "source_type": ["argentina_sepa_product", "argentina_sepa_product"],
            "productos_descripcion": ["Product A", "Product B"],
            "first_shown_at": ["2023-11-01", "2023-11-02"],
        }
    )


@pytest.fixture
def invalid_dynamic_source_data_date_format() -> pd.DataFrame:
    """Fixture for an invalid DataFrame (incorrect date format)."""
    return pd.DataFrame(
        {
            "stream_id": ["arg_sepa_prod_789"],
            "source_id": ["789"],
            "source_type": ["argentina_sepa_product"],
            "productos_descripcion": ["Product C"],
            "first_shown_at": ["03/11/2023"],  # Invalid format
        }
    )


@pytest.fixture
def invalid_dynamic_source_data_missing_col() -> pd.DataFrame:
    """Fixture for an invalid DataFrame (missing required column)."""
    return pd.DataFrame(
        {
            "stream_id": ["arg_sepa_prod_101"],
            "source_id": ["101"],
            # "source_type": ["argentina_sepa_product"], # Missing
            "productos_descripcion": ["Product D"],
            "first_shown_at": ["2023-11-04"],
        }
    )


def test_dynamic_source_model_valid_data(valid_dynamic_source_data: pd.DataFrame):
    """Test validation with a valid DataFrame."""
    try:
        DynamicPrimitiveSourceModel.validate(valid_dynamic_source_data, lazy=True)
    except SchemaError as e:
        pytest.fail(f"Validation failed unexpectedly for valid data: {e}")


def test_dynamic_source_model_invalid_date_format(invalid_dynamic_source_data_date_format: pd.DataFrame):
    """Test validation filters rows with incorrect date format."""
    # With strict="filter" and the check applied to the type hint, invalid rows should be dropped.
    validated_df = DynamicPrimitiveSourceModel.validate(invalid_dynamic_source_data_date_format, lazy=True)
    assert validated_df.empty, "DataFrame with invalid date format should be filtered out"


def test_dynamic_source_model_missing_column(invalid_dynamic_source_data_missing_col: pd.DataFrame):
    """Test validation fails when a required column is missing."""
    with pytest.raises(Exception):
        # Need to use lazy=True because drop_invalid_rows=True requires it
        # But we get another kind of error due to a bug in pandera
        DynamicPrimitiveSourceModel.validate(invalid_dynamic_source_data_missing_col, lazy=True)


def test_dynamic_source_model_extra_column_filtered(valid_dynamic_source_data: pd.DataFrame):
    """Test that extra columns are filtered out due to strict='filter'."""
    df_with_extra = valid_dynamic_source_data.copy()
    df_with_extra["extra_col"] = "should_be_removed"
    validated_df = DynamicPrimitiveSourceModel.validate(df_with_extra, lazy=True)
    assert "extra_col" not in validated_df.columns
    assert list(validated_df.columns) == list(DynamicPrimitiveSourceModel.to_schema().columns.keys())


def test_dynamic_source_model_coercion(valid_dynamic_source_data: pd.DataFrame):
    """Test type coercion works as expected."""
    df_mixed_types = valid_dynamic_source_data.copy()
    # Change source_id to numeric, should be coerced to string
    df_mixed_types["source_id"] = [123, 456]
    validated_df = DynamicPrimitiveSourceModel.validate(df_mixed_types, lazy=True)
    assert validated_df["source_id"].dtype == "object"  # Pandas uses 'object' for strings
    assert validated_df["source_id"].tolist() == ["123", "456"]
