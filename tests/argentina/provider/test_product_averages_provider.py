import io
from unittest.mock import MagicMock, patch

import pandas as pd
from pandera.typing import DataFrame
from prefect_aws import S3Bucket
import pytest

from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaWeightedAvgPriceProductModel
from tsn_adapters.tasks.argentina.provider.product_averages import ProductAveragesProvider
from tsn_adapters.tasks.argentina.types import DateStr


@pytest.fixture
def mock_s3_block() -> MagicMock:
    """Fixture for a mocked S3Bucket."""
    return MagicMock(spec=S3Bucket)


@pytest.fixture
def provider(mock_s3_block: MagicMock) -> ProductAveragesProvider:
    """Fixture for ProductAveragesProvider instance with mocked S3 block."""
    return ProductAveragesProvider(s3_block=mock_s3_block)


@pytest.fixture
def sample_avg_price_data() -> DataFrame[SepaWeightedAvgPriceProductModel]:
    """Fixture for sample SepaWeightedAvgPriceProductModel data."""
    data = {
        "id_producto": ["prod1", "prod2"],
        "productos_descripcion": ["Product 1", "Product 2"],
        "productos_precio_lista_avg": [100.50, 200.75],
        "date": ["2023-01-01", "2023-01-01"],
        "product_count": [1, 1],  # Add required product_count column
    }
    # Cast to the specific Pandera DataFrame type
    return DataFrame[SepaWeightedAvgPriceProductModel](pd.DataFrame(data))


def test_to_product_averages_file_key():
    """Test the static method generates the correct relative S3 key."""
    test_date: DateStr = DateStr("2023-10-26")
    # The static method should return the key *relative* to the prefix
    expected_relative_key = "2023-10-26/product_averages.zip"
    assert ProductAveragesProvider.to_product_averages_file_key(test_date) == expected_relative_key


@patch("tsn_adapters.tasks.argentina.provider.base.SepaS3BaseProvider.write_csv")
def test_save_product_averages(
    mock_write_csv: MagicMock,
    provider: ProductAveragesProvider,
    sample_avg_price_data: DataFrame[SepaWeightedAvgPriceProductModel],
):
    """Test that save_product_averages calls write_csv with correct relative key and data."""
    test_date: DateStr = DateStr("2023-01-01")
    expected_relative_key = "2023-01-01/product_averages.zip" # Expect relative key

    provider.save_product_averages(test_date, sample_avg_price_data)

    mock_write_csv.assert_called_once()
    call_args = mock_write_csv.call_args[0]
    assert call_args[0] == expected_relative_key # Assert relative key
    pd.testing.assert_frame_equal(call_args[1], sample_avg_price_data)


@patch("tsn_adapters.tasks.argentina.provider.base.SepaS3BaseProvider.read_csv")
def test_get_product_averages_for(
    mock_read_csv: MagicMock,
    provider: ProductAveragesProvider,
    sample_avg_price_data: DataFrame[SepaWeightedAvgPriceProductModel],
):
    """Test that get_product_averages_for calls read_csv with correct relative key and returns data."""
    test_date: DateStr = DateStr("2023-01-01")
    expected_relative_key = "2023-01-01/product_averages.zip" # Expect relative key
    mock_read_csv.return_value = pd.DataFrame(sample_avg_price_data)

    result = provider.get_product_averages_for(test_date)

    mock_read_csv.assert_called_once_with(expected_relative_key) # Assert relative key
    SepaWeightedAvgPriceProductModel.validate(result)
    pd.testing.assert_frame_equal(result, sample_avg_price_data)


@patch("tsn_adapters.tasks.argentina.provider.base.SepaS3BaseProvider.path_exists")
def test_exists_true(mock_path_exists: MagicMock, provider: ProductAveragesProvider):
    """Test exists method calls path_exists with correct relative key and returns True."""
    test_date: DateStr = DateStr("2023-01-01")
    expected_relative_key = "2023-01-01/product_averages.zip" # Expect relative key
    mock_path_exists.return_value = True

    result = provider.exists(test_date)

    mock_path_exists.assert_called_once_with(expected_relative_key) # Assert relative key
    assert result is True


@patch("tsn_adapters.tasks.argentina.provider.base.SepaS3BaseProvider.path_exists")
def test_exists_false(mock_path_exists: MagicMock, provider: ProductAveragesProvider):
    """Test exists method calls path_exists with correct relative key and returns False."""
    test_date: DateStr = DateStr("2023-01-01")
    expected_relative_key = "2023-01-01/product_averages.zip" # Expect relative key
    mock_path_exists.return_value = False

    result = provider.exists(test_date)

    mock_path_exists.assert_called_once_with(expected_relative_key) # Assert relative key
    assert result is False


def _build_zip_csv_bytes(csv_text: str) -> bytes:
    buf = io.BytesIO()
    pd.read_csv(io.StringIO(csv_text)).to_csv(buf, index=False, compression="zip")
    return buf.getvalue()


def test_read_csv_strips_dot_zero_from_id_producto(provider: ProductAveragesProvider, mock_s3_block: MagicMock):
    # Real-world fingerprint: one `.0`-tainted row poisons pandas dtype inference
    # for the whole column. The reader must normalize so the downstream join
    # against the descriptor's clean integer-strings does not collapse.
    csv_text = (
        "id_producto,productos_descripcion,productos_precio_lista_avg,date,product_count\n"
        "100119,Item A,10.5,2025-12-23,1\n"
        "7790260013058.0,Item B,20.0,2025-12-23,2\n"
        "7791290795013.0,Item C,30.0,2025-12-23,3\n"
    )
    mock_s3_block.read_path = MagicMock(return_value=_build_zip_csv_bytes(csv_text))

    df = provider.read_csv("2025-12-23/product_averages.zip")

    assert df["id_producto"].tolist() == ["100119", "7790260013058", "7791290795013"]


def test_read_csv_passes_clean_integer_ids_through_unchanged(
    provider: ProductAveragesProvider, mock_s3_block: MagicMock
):
    csv_text = (
        "id_producto,productos_descripcion,productos_precio_lista_avg,date,product_count\n"
        "100119,Item A,10.5,2025-11-19,1\n"
        "100121,Item B,20.0,2025-11-19,2\n"
    )
    mock_s3_block.read_path = MagicMock(return_value=_build_zip_csv_bytes(csv_text))

    df = provider.read_csv("2025-11-19/product_averages.zip")

    assert df["id_producto"].tolist() == ["100119", "100121"]


def test_read_csv_without_id_producto_column_is_unchanged(
    provider: ProductAveragesProvider, mock_s3_block: MagicMock
):
    csv_text = "some_other_col,value\nalpha,1\nbeta,2\n"
    mock_s3_block.read_path = MagicMock(return_value=_build_zip_csv_bytes(csv_text))

    df = provider.read_csv("anything.zip")

    assert "id_producto" not in df.columns
    assert df["some_other_col"].tolist() == ["alpha", "beta"]