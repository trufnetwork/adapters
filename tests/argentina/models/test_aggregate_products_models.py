"""
Unit tests for the Argentina SEPA product aggregation data models.
"""

import json
import re

import pandas as pd
from pandera.errors import SchemaError
from pydantic import ValidationError
import pytest

from tsn_adapters.tasks.argentina.models.aggregate_products_models import (
    ArgentinaProductStateMetadata,
    DynamicPrimitiveSourceModel,
)

# --- Tests for ProductAggregationMetadata ---


def test_metadata_instantiation_defaults():
    """Test instantiation with default values."""
    metadata = ArgentinaProductStateMetadata()
    assert metadata.last_aggregation_processed_date == "1970-01-01"
    assert metadata.last_product_deployment_date == "1970-01-01"
    assert metadata.last_insertion_processed_date == "1970-01-01"
    assert metadata.total_products_count == 0


def test_metadata_instantiation_valid():
    """Test instantiation with valid data for all fields."""
    metadata = ArgentinaProductStateMetadata(
        last_aggregation_processed_date="2023-10-26",
        last_product_deployment_date="2023-11-15",
        last_insertion_processed_date="2023-11-01",
        total_products_count=150,
    )
    assert metadata.last_aggregation_processed_date == "2023-10-26"
    assert metadata.last_product_deployment_date == "2023-11-15"
    assert metadata.last_insertion_processed_date == "2023-11-01"
    assert metadata.total_products_count == 150


@pytest.mark.parametrize(
    "field_name, invalid_date",
    [
        ("last_aggregation_processed_date", "26-10-2023"),
        ("last_product_deployment_date", "2023/11/15"),
        ("last_insertion_processed_date", "Nov 1, 2023"),
        ("last_aggregation_processed_date", ""), # Test empty string
        ("last_product_deployment_date", "invalid"), # Test non-date string
    ],
)
def test_metadata_instantiation_invalid_date(field_name: str, invalid_date: str):
    """Test instantiation with invalid date format raises ValidationError for each date field."""
    valid_data = {
        "last_aggregation_processed_date": "2023-01-01",
        "last_product_deployment_date": "2023-01-01",
        "last_insertion_processed_date": "2023-01-01",
        "total_products_count": 10,
    }
    invalid_data = {**valid_data, field_name: invalid_date}

    # Use re.compile with DOTALL to match field name and message across lines
    with pytest.raises(ValidationError, match=re.compile(f"{field_name}.*Date must be in YYYY-MM-DD format", re.DOTALL)):
        ArgentinaProductStateMetadata(**invalid_data) # type: ignore[arg-type]


def test_metadata_serialization_deserialization():
    """Test serialization to dict/json and deserialization including new fields."""
    metadata = ArgentinaProductStateMetadata(
        last_aggregation_processed_date="2024-01-15",
        last_product_deployment_date="2024-02-01",
        last_insertion_processed_date="2024-01-20",
        total_products_count=500,
    )
    metadata_dict = metadata.model_dump()
    expected_dict = {
        "last_aggregation_processed_date": "2024-01-15",
        "last_product_deployment_date": "2024-02-01",
        "last_insertion_processed_date": "2024-01-20",
        "total_products_count": 500,
    }
    assert metadata_dict == expected_dict

    metadata_json = metadata.model_dump_json()
    assert json.loads(metadata_json) == expected_dict

    # Deserialize from dict
    deserialized_from_dict = ArgentinaProductStateMetadata(**metadata_dict)
    assert deserialized_from_dict == metadata

    # Deserialize from json
    deserialized_from_json = ArgentinaProductStateMetadata.model_validate_json(metadata_json)
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


def test_dynamic_source_model_empty_dataframe():
    """Test validation with an empty DataFrame."""
    empty_df = pd.DataFrame(columns=list(DynamicPrimitiveSourceModel.to_schema().columns.keys()))
    try:
        validated_df = DynamicPrimitiveSourceModel.validate(empty_df, lazy=True)
        assert validated_df.empty, "Validation of empty DataFrame should result in an empty DataFrame"
        assert list(validated_df.columns) == list(DynamicPrimitiveSourceModel.to_schema().columns.keys())
    except SchemaError as e:
        pytest.fail(f"Validation of empty DataFrame failed unexpectedly: {e}")


def test_dynamic_source_model_with_nulls_filtered(valid_dynamic_source_data: pd.DataFrame):
    """Test that rows with nulls in non-nullable columns are filtered out due to strict='filter'."""
    df_with_nulls = valid_dynamic_source_data.copy()
    # Introduce None/NaN into required columns (non-nullable)
    df_with_nulls.loc[0, "stream_id"] = None  # stream_id is non-nullable
    df_with_nulls.loc[1, "source_id"] = pd.NA  # source_id is non-nullable

    # Add a fully valid row to ensure the filtering happens per-row
    valid_row = pd.DataFrame(
        {
            "stream_id": ["arg_sepa_prod_789"],
            "source_id": ["789"],
            "source_type": ["argentina_sepa_product"],
            "productos_descripcion": ["Product C"],
            "first_shown_at": ["2023-11-03"],
        }
    )
    df_to_validate = pd.concat([df_with_nulls, valid_row], ignore_index=True)

    validated_df = DynamicPrimitiveSourceModel.validate(df_to_validate, lazy=True)

    # Expected: Only the fully valid row should remain
    assert len(validated_df) == 1
    assert validated_df.iloc[0]["source_id"] == "789"
    assert "stream_id" in validated_df.columns # Ensure column still exists
