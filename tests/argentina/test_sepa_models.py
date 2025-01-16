import pandas as pd
import pytest

from tsn_adapters.tasks.argentina.models.sepa.sepa_models import (
    ProductDescriptionModel,
    SepaAvgPriceProductModel,
    SepaProductosDataModel,
    SepaProductosDataModelAlt1,
)


@pytest.fixture
def sample_core_data():
    """Fixture providing sample data in core model format."""
    data = {
        "id_producto": ["1", "2", "3"],
        "productos_descripcion": ["Product A", "Product B", "Product C"],
        "productos_precio_lista": [100.0, 200.0, 300.0],
        "date": ["2024-01-01", "2024-01-01", "2024-01-01"],
    }
    df = pd.DataFrame(data)
    return df


@pytest.fixture
def sample_alt1_data():
    """Fixture providing sample data in alternative model 1 format."""
    data = {
        "id_producto": ["1", "2", "3"],
        "productos_descripcion": ["Product A", "Product B", "Product C"],
        "precio_unitario_bulto_por_unidad_venta_con_iva": [100.0, 200.0, 300.0],
        "date": ["2024-01-01", "2024-01-01", "2024-01-01"],
    }
    df = pd.DataFrame(data)
    return df


def test_alt1_to_core_model_conversion(sample_alt1_data):
    """Test conversion from alternative model 1 to core model."""
    # Convert alt1 to core model
    core_df = SepaProductosDataModelAlt1.to_core_model(sample_alt1_data)

    # Verify the conversion maintains data integrity
    assert len(core_df) == len(sample_alt1_data)
    assert all(core_df.productos_precio_lista == sample_alt1_data.precio_unitario_bulto_por_unidad_venta_con_iva)
    assert all(core_df.productos_descripcion == sample_alt1_data.productos_descripcion)
    assert all(core_df.id_producto == sample_alt1_data.id_producto)


def test_product_description_model_creation(sample_core_data):
    """Test creation of ProductDescriptionModel from core data."""
    desc_df = ProductDescriptionModel.from_sepa_product_data(sample_core_data)

    # Verify only required columns are present
    assert set(desc_df.columns) == {"id_producto", "productos_descripcion"}

    # Verify no duplicates
    assert len(desc_df) == len(desc_df.drop_duplicates())


def test_avg_price_product_model_creation(sample_core_data):
    """Test creation of SepaAvgPriceProductModel from core data."""
    avg_price_df = SepaAvgPriceProductModel.from_sepa_product_data(sample_core_data)

    # Verify required columns are present
    expected_columns = {"id_producto", "productos_descripcion", "productos_precio_lista_avg", "date"}
    assert set(avg_price_df.columns) == expected_columns

    # Verify price averaging
    original_prices = sample_core_data.productos_precio_lista.tolist()
    avg_prices = avg_price_df.productos_precio_lista_avg.tolist()
    assert avg_prices == original_prices  # In this case they're the same since we have no duplicates


def test_model_validation():
    """Test model validation with invalid data."""
    invalid_data = {
        "id_producto": ["1"],
        "productos_descripcion": ["Product A"],
        "productos_precio_lista": ["invalid"],  # Should be float
        "date": ["2024-01-01"],
    }
    df = pd.DataFrame(invalid_data)

    # Verify validation fails for invalid data
    with pytest.raises(Exception):
        SepaProductosDataModel.validate(df)


def test_has_columns_detection():
    """Test the column detection functionality."""
    core_header = "id_producto|productos_descripcion|productos_precio_lista|date"
    alt1_header = "id_producto|productos_descripcion|precio_unitario_bulto_por_unidad_venta_con_iva|date"

    assert SepaProductosDataModel.has_columns(core_header)
    assert not SepaProductosDataModel.has_columns(alt1_header)
    assert SepaProductosDataModelAlt1.has_columns(alt1_header)
    assert not SepaProductosDataModelAlt1.has_columns(core_header)
