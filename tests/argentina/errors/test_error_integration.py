"""
Integration tests for Argentina SEPA error handling system.

This test suite focuses on critical integration tests and complex error handling scenarios,
validating the complete data processing pipeline and error accumulation mechanisms.

Key Test Areas:
1. Data Processing Pipeline - Complete flow validation
2. Error Handling - Complex error scenarios and recovery
3. Concurrency - Error isolation and accumulation
4. System Resilience - Partial processing and recovery
"""

from collections.abc import Generator
import os
import re
from typing import Any, Callable, Optional, TypeVar
from unittest.mock import MagicMock, patch
import zipfile

import pandas as pd
from prefect import flow
import pytest

from tsn_adapters.tasks.argentina.base_types import DateStr
from tsn_adapters.tasks.argentina.errors.accumulator import error_collection
from tsn_adapters.tasks.argentina.errors.context_helper import ArgentinaErrorContext
from tsn_adapters.tasks.argentina.errors.errors import (
    AccountableRole,
    ArgentinaSEPAError,
    DateMismatchError,
    EmptyCategoryMapError,
    InvalidCSVSchemaError,
    InvalidStructureZIPError,
    InvalidProductsError,
    MissingProductosCSVError,
    InvalidDateFormatError,
)
from tsn_adapters.tasks.argentina.provider.data_processor import process_sepa_zip
from tsn_adapters.tasks.argentina.types import SepaDF

T = TypeVar("T", bound=ArgentinaSEPAError)

# --------------------------------------------------
# Fixtures & Test Data
# --------------------------------------------------


@pytest.fixture
def error_context_fixture():
    """Fixture for managing error context and cleanup."""
    with error_collection() as accumulator:
        yield accumulator


@pytest.fixture
def mock_s3_block():
    """Mock S3 block with configurable responses."""
    mock = MagicMock()
    mock.list_available_keys.return_value = ["2024-01-01"]
    mock.get_raw_data_for.return_value = pd.DataFrame(
        {
            "id_producto": ["P1", "P2"],
            "productos_descripcion": ["Product 1", "Product 2"],
            "productos_precio_lista": [100.0, 200.0],
            "date": ["2024-01-01", "2024-01-01"],
        }
    )
    mock.bucket_name = "mock-bucket"
    mock.bucket_folder = "mock-folder"
    mock.credentials = MagicMock()
    mock.read_path.return_value = b"mock_s3_content"
    return mock


@pytest.fixture
def valid_sepa_data() -> pd.DataFrame:
    """Valid SEPA data fixture with realistic product data."""
    df = pd.DataFrame(
        {
            "date": ["2024-01-01"] * 5,
            "id_producto": ["P1", None, "", None, "P5"],
            "productos_descripcion": [
                "Leche Entera 1L",
                "Pan Francés 1kg",
                "Aceite Girasol 900ml",
                "Arroz Largo 1kg",
                "Azúcar Blanca 1kg",
            ],
            "productos_precio_lista": [350.0, 800.0, 1200.0, 900.0, 600.0],
        }
    )
    return df


@pytest.fixture
def mock_zip_factory():
    """Factory fixture for creating test ZIP files with various scenarios."""

    def create_zip(scenario: str, *, date: Optional[str] = None, df: Optional[pd.DataFrame] = None) -> bytes:
        if scenario == "invalid":
            return b"This is not a valid ZIP file"

        # Create a temporary ZIP file in memory
        from io import BytesIO

        zip_buffer = BytesIO()

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            if scenario == "invalid_date":
                # Create a directory with invalid date format
                base_dir = "sepa_1_comercio-sepa-1_2024/01/01_00-00-00"
            else:
                base_dir = "sepa_1_comercio-sepa-1_2024-01-01_00-00-00"

            # Create the base directory marker
            zip_file.writestr(f"{base_dir}/", "")

            if scenario == "missing_productos":
                # Create a ZIP with wrong file but correct directory structure
                zip_file.writestr(f"{base_dir}/wrong.csv", "some content")
            elif scenario == "wrong_date":
                content_date = date or "2024-01-02"
                content = (
                    "id_producto|productos_descripcion|productos_precio_lista|date\n"
                    f"P1|Product 1|100.0|{content_date}\n"
                )
                zip_file.writestr(f"{base_dir}/productos.csv", content)
            elif scenario == "mixed_errors":
                # Create an invalid CSV schema
                content = (
                    "wrong_column|productos_descripcion|productos_precio_lista|date\n"
                    "P1|Product 1|100.0|2024-01-01\n"
                )
                zip_file.writestr(f"{base_dir}/productos.csv", content)
            elif scenario == "partial_valid":
                if df is not None:
                    content = df.to_csv(sep="|", index=False)
                else:
                    content = (
                        "id_producto|productos_descripcion|productos_precio_lista|date\n"
                        "|Product 1|100.0|2024-01-01\n"  # Missing ID
                        "|Product 2|200.0|2024-01-01\n"  # Missing ID
                        "|Product 3|300.0|2024-01-01\n"  # Missing ID
                        "P4|Product 4|400.0|2024-01-01\n"  # Valid
                        "P5|Product 5|500.0|2024-01-01\n"  # Valid
                    )
                zip_file.writestr(f"{base_dir}/productos.csv", content)
            else:
                # Default valid content
                content = (
                    "id_producto|productos_descripcion|productos_precio_lista|date\n"
                    "P1|Product 1|100.0|2024-01-01\n"
                )
                zip_file.writestr(f"{base_dir}/productos.csv", content)

        return zip_buffer.getvalue()

    return create_zip


@pytest.fixture
def mock_filesystem(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Mock filesystem operations for testing."""
    created_dirs: set[str] = set()

    def mock_makedirs(path: str | os.PathLike[str], exist_ok: bool = False) -> None:
        """Mock os.makedirs to track created directories."""
        path_str = str(path)
        if path_str not in created_dirs:
            created_dirs.add(path_str)

    with patch.object(os, "makedirs", mock_makedirs):
        yield


@pytest.fixture(autouse=True)
def mock_network():
    """Mock all network operations."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"mock_content"
    mock_response.text = "mock_text"

    with (
        patch("requests.get", return_value=mock_response),
        patch("requests.post", return_value=mock_response),
        patch("requests.Session", return_value=MagicMock()),
        patch("prefect_aws.S3Bucket.read_path", return_value=b"mock_s3_content"),
        patch("prefect_aws.S3Bucket.download_object_to_path", return_value=None),
        patch("prefect_aws.S3Bucket.list_objects", return_value=["mock_key"]),
    ):
        yield mock_response


# --------------------------------------------------
# Integration Test Classes
# --------------------------------------------------


class TestDataProcessingPipeline:
    """Integration tests for the complete data processing pipeline."""

    @pytest.mark.usefixtures("prefect_test_fixture")
    @flow(name="test-complete-pipeline")
    def test_complete_processing_flow(
        self,
        mock_s3_block: MagicMock,
        valid_sepa_data: pd.DataFrame,
        mock_zip_factory: Callable[..., bytes],
        error_context_fixture: Any,
    ):
        """Test the complete data processing pipeline with mixed valid/invalid data."""
        # Setup test data with mixed scenarios
        zip_content = mock_zip_factory("partial_valid", df=valid_sepa_data)

        def mock_reader() -> Generator[bytes, None, None]:
            yield zip_content

        # Process data and verify results
        result = process_sepa_zip(mock_reader(), DateStr("2024-01-01"), "test_pipeline")

        # Verify error handling
        assert len(error_context_fixture.errors) > 0
        error = error_context_fixture.errors[0]
        assert isinstance(error, InvalidProductsError)
        assert error.context["missing_count"] == "3"

        # Verify successful processing of valid records
        assert len(result) == 2
        assert all(pd.notna(result["id_producto"].astype(str)))

    @pytest.mark.usefixtures("prefect_test_fixture")
    @pytest.mark.parametrize(
        "error_scenario,expected_error,validation_details",
        [
            (
                "invalid_structure",
                InvalidStructureZIPError,
                {
                    "code": "ARG-100",
                    "message": "Invalid ZIP file structure - cannot extract files",
                    "responsibility": AccountableRole.DATA_PROVIDER,
                    "context_keys": ["date", "error"],
                    "context_values": {"date": "2024-01-01"},
                },
            ),
            (
                "missing_productos",
                MissingProductosCSVError,
                {
                    "code": "ARG-102",
                    "message": "Missing productos.csv in ZIP archive",
                    "responsibility": AccountableRole.DATA_PROVIDER,
                    "context_keys": ["directory", "available_files"],
                    "context_pattern": r"/tmp/tmp[^/]+/data/sepa_1_comercio-sepa-1_2024-01-01_00-00-00",
                },
            ),
            (
                "invalid_date",
                InvalidDateFormatError,
                {
                    "code": "ARG-101",
                    "message": "Invalid date format: 2024/01/01 - must be YYYY-MM-DD",
                    "responsibility": AccountableRole.SYSTEM,
                    "context_keys": ["invalid_date"],
                    "context_values": {"invalid_date": "2024/01/01"},
                },
            ),
        ],
    )
    @flow(name="test-error-scenarios")
    def test_error_scenarios(
        self,
        mock_zip_factory: Callable[..., bytes],
        error_scenario: str,
        expected_error: type[T],
        validation_details: dict[str, Any],
        error_context_fixture: Any,
    ):
        """Test various error scenarios in the processing pipeline."""
        try:
            if error_scenario == "invalid_structure":
                zip_content = mock_zip_factory("invalid")
                # Set up context for InvalidStructureZIPError
                ctx = ArgentinaErrorContext()
                ctx.store_id = "test"
                ctx.date = "2024-01-01"
            elif error_scenario == "invalid_date":
                zip_content = mock_zip_factory("invalid_date")
            else:
                zip_content = mock_zip_factory("missing_productos")

            def mock_reader() -> Generator[bytes, None, None]:
                yield zip_content

            process_sepa_zip(mock_reader(), DateStr("2024-01-01"), "test")
            pytest.fail(f"Expected {expected_error.__name__} to be raised")
        except expected_error as e:
            # Verify error code
            assert e.code == validation_details["code"]
            # Verify error message
            assert e.message == validation_details["message"]
            # Verify responsibility
            assert e.responsibility == validation_details["responsibility"]
            
            # Verify context has all required keys
            for key in validation_details["context_keys"]:
                assert key in e.context
            
            # Verify specific context values if provided
            if "context_values" in validation_details:
                for key, value in validation_details["context_values"].items():
                    assert e.context[key] == value
            
            # Verify context patterns if provided
            if "context_pattern" in validation_details:
                assert re.match(validation_details["context_pattern"], e.context["directory"])

    @pytest.mark.usefixtures("prefect_test_fixture")
    @flow(name="test-invalid-structure-scenarios")
    def test_invalid_structure_error_scenarios(self, error_context_fixture: Any):
        """Test various scenarios for InvalidStructureZIPError."""
        # Test with different error messages
        error_messages = [
            "File is not a zip file",
            "Bad CRC-32 for file",
            "Bad password for file",
            "Truncated file header",
        ]

        for error_msg in error_messages:
            # Set up context
            ctx = ArgentinaErrorContext()
            ctx.date = "2024-01-01"
            
            # Create and verify error
            error = InvalidStructureZIPError(error_msg)
            
            # Verify basic error properties
            assert error.code == "ARG-100"
            assert error.message == "Invalid ZIP file structure - cannot extract files"
            assert error.responsibility == AccountableRole.DATA_PROVIDER
            
            # Verify context
            assert "date" in error.context
            assert error.context["date"] == "2024-01-01"
            assert "error" in error.context
            assert error.context["error"] == error_msg

        # Test without context date
        error = InvalidStructureZIPError("test error")
        assert "date" in error.context
        assert error.context["date"] is None

    @pytest.mark.usefixtures("prefect_test_fixture")
    @flow(name="test-date-format-scenarios")
    def test_date_format_error_scenarios(self, error_context_fixture: Any):
        """Test various scenarios for InvalidDateFormatError."""
        # Test with different invalid date formats
        invalid_dates = [
            "2024/01/01",  # Wrong separator
            "24-01-01",    # Wrong year format
            "2024-1-1",    # Missing padding
            "2024-13-01",  # Invalid month
            "2024-01-32",  # Invalid day
            "01-01-2024",  # Wrong order
            "2024-01",     # Incomplete
            "not-a-date",  # Invalid format
        ]

        for invalid_date in invalid_dates:
            # Create and verify error
            error = InvalidDateFormatError(invalid_date)
            
            # Verify basic error properties
            assert error.code == "ARG-101"
            assert error.message == f"Invalid date format: {invalid_date} - must be YYYY-MM-DD"
            assert error.responsibility == AccountableRole.SYSTEM
            
            # Verify context
            assert "invalid_date" in error.context
            assert error.context["invalid_date"] == invalid_date


class TestErrorHandlingSystem:
    """Integration tests for the error handling and accumulation system."""

    @pytest.mark.usefixtures("prefect_test_fixture")
    @flow(name="test-error-accumulation")
    def test_error_accumulation_and_isolation(self, error_context_fixture: Any):
        """Test error accumulation, ordering, and isolation."""
        # Test concurrent error accumulation
        with error_collection() as flow1_errors:
            with error_collection() as flow2_errors:
                # Add errors to different flows
                flow1_errors.add_error(EmptyCategoryMapError(url="map1"))
                # Set up context for DateMismatchError
                ctx = ArgentinaErrorContext()
                ctx.date = "2024-01-01"
                flow2_errors.add_error(DateMismatchError(internal_date="2024-01-02"))

                # Verify flow2 errors
                assert len(flow2_errors.errors) == 1
                assert isinstance(flow2_errors.errors[0], DateMismatchError)
                assert flow2_errors.errors[0].context["external_date"] == "2024-01-01"

            # Verify flow1 errors
            assert len(flow1_errors.errors) == 1
            assert isinstance(flow1_errors.errors[0], EmptyCategoryMapError)
            assert flow1_errors.errors[0].context["url"] == "map1"

    @pytest.mark.usefixtures("prefect_test_fixture")
    @flow(name="test-error-recovery")
    def test_error_recovery_mechanisms(
        self,
        mock_zip_factory: Callable[..., bytes],
        valid_sepa_data: SepaDF,
        error_context_fixture: Any,
    ):
        """Test system recovery from various error conditions."""
        # Test partial processing recovery
        zip_content = mock_zip_factory("partial_valid", df=valid_sepa_data)

        def mock_reader() -> Generator[bytes, None, None]:
            yield zip_content

        result = process_sepa_zip(mock_reader(), DateStr("2024-01-01"), "test_recovery")

        # Verify error capture
        assert len(error_context_fixture.errors) > 0
        error = error_context_fixture.errors[0]
        assert isinstance(error, InvalidProductsError)

        # Verify successful partial processing
        assert len(result) > 0
        assert all(pd.notna(result["id_producto"].astype(str)))

    @pytest.mark.usefixtures("prefect_test_fixture")
    @flow(name="test-complex-errors")
    def test_complex_error_scenarios(
        self,
        mock_zip_factory: Callable[..., bytes],
        error_context_fixture: Any,
    ):
        """Test handling of complex error scenarios with multiple error types."""
        # Create data with multiple error types
        zip_content = mock_zip_factory("mixed_errors")

        def mock_reader() -> Generator[bytes, None, None]:
            yield zip_content

        try:
            process_sepa_zip(mock_reader(), DateStr("2024-01-01"), "test_complex")
            pytest.fail("Expected InvalidCSVSchemaError to be raised")
        except InvalidCSVSchemaError as e:
            assert e.code == "ARG-102"
            assert e.responsibility == AccountableRole.DATA_PROVIDER
            assert len(error_context_fixture.errors) == 1
            assert error_context_fixture.errors[0] == e


if __name__ == "__main__":
    pytest.main(["-v", __file__])
