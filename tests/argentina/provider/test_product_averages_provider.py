from unittest.mock import MagicMock, patch

import pandas as pd
from pandera.typing import DataFrame
from prefect_aws import S3Bucket
import pytest

from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
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
def sample_avg_price_data() -> DataFrame[SepaAvgPriceProductModel]:
    """Fixture for sample SepaAvgPriceProductModel data."""
    data = {
        "id_producto": ["prod1", "prod2"],
        "productos_descripcion": ["Product 1", "Product 2"],
        "productos_precio_lista_avg": [100.50, 200.75],
        "date": ["2023-01-01", "2023-01-01"],
    }
    # Cast to the specific Pandera DataFrame type
    return DataFrame[SepaAvgPriceProductModel](pd.DataFrame(data))


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
    sample_avg_price_data: DataFrame[SepaAvgPriceProductModel],
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
    sample_avg_price_data: DataFrame[SepaAvgPriceProductModel],
):
    """Test that get_product_averages_for calls read_csv with correct relative key and returns data."""
    test_date: DateStr = DateStr("2023-01-01")
    expected_relative_key = "2023-01-01/product_averages.zip" # Expect relative key
    mock_read_csv.return_value = pd.DataFrame(sample_avg_price_data)

    result = provider.get_product_averages_for(test_date)

    mock_read_csv.assert_called_once_with(expected_relative_key) # Assert relative key
    SepaAvgPriceProductModel.validate(result)
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