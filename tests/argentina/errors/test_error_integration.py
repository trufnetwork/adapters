"""
Integration tests for Argentina SEPA error handling system.

This test suite validates error handling across the data processing pipeline,
focusing on business-critical validations and proper error accumulation.

Key Test Areas:
1. Structure Validation - ZIP and CSV file integrity
2. Input Validation - Date formats and file requirements
3. Data Processing - Schema validation and content processing
4. Category Mapping - Product categorization and mapping validation
5. Error Recovery - System resilience and error isolation
6. Error Accumulation - Multi-error handling and context management

Each test focuses on real-world scenarios and validates the complete error lifecycle:
- Error detection and creation
- Context preservation
- Error accumulation
- Final error reporting
"""

import io
import re
from typing import cast
from unittest.mock import MagicMock, patch, mock_open
import zipfile
import logging
import tempfile
import os
import shutil

import pandas as pd
import pytest
from prefect.testing.utilities import prefect_test_harness

from tsn_adapters.tasks.argentina.base_types import DateStr
from tsn_adapters.tasks.argentina.errors.errors import (
    AccountableRole,
    DateMismatchError,
    EmptyCategoryMapError,
    InvalidCSVSchemaError,
    InvalidDateFormatError,
    InvalidStructureZIPError,
    MissingProductIDError,
    MissingProductosCSVError,
    UncategorizedProductsError,
)
from tsn_adapters.tasks.argentina.errors.accumulator import error_collection
from tsn_adapters.tasks.argentina.flows.preprocess_flow import PreprocessFlow
from tsn_adapters.tasks.argentina.provider.data_processor import process_sepa_zip


# --------------------------------------------------
# Fixtures & Test Data
# --------------------------------------------------
@pytest.fixture
def mock_s3_block():
    """Mock S3 block for testing with configurable responses"""
    mock = MagicMock()
    
    # Mock basic S3 operations
    mock.list_available_keys.return_value = ["2024-01-01"]
    mock.get_raw_data_for.return_value = pd.DataFrame({
        'id_producto': ['P1', 'P2'],
        'productos_descripcion': ['Product 1', 'Product 2'],
        'productos_precio_lista': [100.0, 200.0],
        'date': ['2024-01-01', '2024-01-01']
    })
    
    # Mock S3 bucket operations
    mock.bucket_name = "mock-bucket"
    mock.bucket_folder = "mock-folder"
    mock.credentials = MagicMock()
    
    # Mock S3 methods
    mock.read_path.return_value = b'mock_s3_content'
    mock.write_path.return_value = None
    mock.list_objects.return_value = ['mock_key']
    mock.download_object_to_path.return_value = None
    mock.upload_from_path.return_value = None
    
    return mock

@pytest.fixture
def valid_sepa_data():
    """Valid SEPA data fixture with realistic product data"""
    return pd.DataFrame({
        "date": ["2024-01-01"] * 5,
        "id_producto": ["P1", "P2", "P3", "P4", "P5"],
        "productos_descripcion": [
            "Leche Entera 1L",
            "Pan Francés 1kg",
            "Aceite Girasol 900ml",
            "Arroz Largo 1kg",
            "Azúcar Blanca 1kg"
        ],
        "precio": [350.0, 800.0, 1200.0, 900.0, 600.0],
    })

@pytest.fixture
def create_zip_file(mock_zip_operations):
    """Factory fixture for creating test ZIP files with various scenarios"""
    def _create_zip(content_type: str, **kwargs) -> bytes:
        if content_type == "invalid":
            # Mock invalid ZIP behavior
            with patch("zipfile.is_zipfile", return_value=False):
                return b'This is not a ZIP file'
        
        # For all other cases, prepare the data but don't actually create files
        if content_type == "missing_productos":
            mock_zip_operations.namelist.return_value = ["sepa_1_comercio-sepa-1_2024-01-01_00-00-00/wrong.csv"]
        elif content_type == "wrong_date":
            mock_zip_operations.read.return_value = (
                "id_producto|productos_descripcion|productos_precio_lista|date\n"
                f"P1|Product 1|100.0|{kwargs.get('date', '2024-01-02')}\n"
            ).encode()
        elif content_type == "mixed_errors":
            mock_zip_operations.read.return_value = (
                "id_producto|productos_descripcion|productos_precio_lista|date\n"
                "|Product 1|100.0|2024-01-01\n"  # Missing ID
                "|Product 2|200.0|2024-01-01\n"  # Missing ID
                "P3|Product 3|300.0|2024-01-02\n"  # Wrong date
                "P4|Product 4|400.0|2024-01-02\n"  # Wrong date
                "P5|Product 5|invalid|2024-01-01\n"  # Invalid price
                "P6|Product 6|invalid|2024-01-01\n"  # Invalid price
            ).encode()
        elif content_type == "partial_valid":
            mock_zip_operations.read.return_value = (
                "id_producto|productos_descripcion|productos_precio_lista|date\n"
                "|Product 1|100.0|2024-01-01\n"
                "|Product 2|200.0|2024-01-01\n"
                "|Product 3|300.0|2024-01-01\n"
                "P4|Product 4|400.0|2024-01-01\n"
                "P5|Product 5|500.0|2024-01-01\n"
            ).encode()
        
        return b'mock_zip_content'
    return _create_zip

@pytest.fixture
def mock_logger():
    """Mock logger for testing."""
    logger = MagicMock(spec=logging.Logger)
    with patch('tsn_adapters.tasks.argentina.flows.base.get_run_logger', return_value=logger):
        yield logger

@pytest.fixture(autouse=True)
def prefect_test_context():
    """Fixture to provide Prefect test context for all tests."""
    with prefect_test_harness():
        yield

@pytest.fixture(autouse=True)
def mock_filesystem():
    """Mock filesystem operations to speed up tests."""
    mock_temp_dir = "/tmp/mock_temp_dir"
    mock_temp = MagicMock()
    mock_temp.name = mock_temp_dir
    
    # Create the temp directory
    os.makedirs(mock_temp_dir, exist_ok=True)
    
    with patch("tempfile.mkdtemp", return_value=mock_temp_dir), \
         patch("os.path.join", lambda *args: "/".join(args)), \
         patch("os.listdir", return_value=["productos.csv"]), \
         patch("os.makedirs", return_value=None), \
         patch("os.path.exists", return_value=True), \
         patch("os.remove", return_value=None), \
         patch("builtins.open", mock_open(read_data="id_producto|productos_descripcion|productos_precio_lista|date\n")), \
         patch("shutil.rmtree", return_value=None):
        yield mock_temp_dir
        
        # Clean up
        try:
            shutil.rmtree(mock_temp_dir)
        except:
            pass

@pytest.fixture(autouse=True)
def mock_zip_operations():
    """Mock ZIP file operations to speed up tests."""
    mock_zip = MagicMock()
    mock_zip.namelist.return_value = ["sepa_1_comercio-sepa-1_2024-01-01_00-00-00/productos.csv"]
    
    with patch("zipfile.ZipFile", return_value=mock_zip), \
         patch("zipfile.is_zipfile", return_value=True):
        yield mock_zip

@pytest.fixture(autouse=True)
def mock_network_operations():
    """Mock all network operations including S3 and HTTP requests."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'mock_content'
    mock_response.text = 'mock_text'
    mock_response.raise_for_status.return_value = None
    
    with patch('requests.get', return_value=mock_response), \
         patch('requests.post', return_value=mock_response), \
         patch('requests.Session', return_value=MagicMock()), \
         patch('prefect_aws.S3Bucket.read_path', return_value=b'mock_s3_content'), \
         patch('prefect_aws.S3Bucket.download_object_to_path', return_value=None), \
         patch('prefect_aws.S3Bucket.list_objects', return_value=['mock_key']):
        yield mock_response

@pytest.fixture(autouse=True)
def mock_data_tasks():
    """Mock data processing tasks and heavy computations."""
    mock_df = pd.DataFrame({
        'id_producto': ['P1', 'P2'],
        'productos_descripcion': ['Product 1', 'Product 2'],
        'productos_precio_lista': [100.0, 200.0],
        'date': ['2024-01-01', '2024-01-01']
    })
    
    with patch('tsn_adapters.tasks.argentina.task_wrappers.task_create_stream_fetcher', return_value=mock_df), \
         patch('tsn_adapters.tasks.argentina.task_wrappers.task_get_streams', return_value=mock_df), \
         patch('tsn_adapters.tasks.argentina.task_wrappers.task_create_sepa_provider', return_value=mock_df), \
         patch('tsn_adapters.tasks.argentina.task_wrappers.task_get_data_for_date', return_value=mock_df):
        yield mock_df

@pytest.fixture(autouse=True)
def mock_prefect_operations():
    """Mock Prefect operations to avoid actual task runs and flow executions."""
    mock_state = MagicMock()
    mock_state.is_completed.return_value = True
    mock_state.result.return_value = None
    
    with patch('prefect.task', lambda *args, **kwargs: lambda f: f), \
         patch('prefect.flow', lambda *args, **kwargs: lambda f: f), \
         patch('prefect.get_run_logger', return_value=MagicMock()), \
         patch('prefect.context.get_run_context', return_value=MagicMock()):
        yield mock_state

# --------------------------------------------------
# 1. Structure Validation Tests
# --------------------------------------------------
@pytest.mark.parametrize("invalid_content,expected_error,expected_context", [
    ("invalid", InvalidStructureZIPError, {"source": "test", "date": "2024-01-01", "error": "File is not a zip file"}),
    ("missing_productos", MissingProductosCSVError, {
        "directory": r"/tmp/tmp[^/]+/data/sepa_1_comercio-sepa-1_2024-01-01_00-00-00",
        "available_files": ["wrong.csv"]
    }),
])
def test_zip_structure_validation(create_zip_file, invalid_content, expected_error, expected_context):
    """Validate ZIP file structure requirements with detailed context"""
    zip_content = create_zip_file(invalid_content)
    
    def mock_reader():
        yield zip_content

    with error_collection() as accumulator:
        try:
            process_sepa_zip(mock_reader(), DateStr("2024-01-01"), "test")
            pytest.fail(f"Expected {expected_error.__name__} to be raised")
        except expected_error as e:
            accumulator.add_error(e)
            assert isinstance(e, expected_error)
            assert e.responsibility == AccountableRole.DATA_PROVIDER
            # For directory paths, just check the pattern since temp dir will be different
            if "directory" in expected_context:
                assert re.match(expected_context["directory"], e.context["directory"])
                assert e.context["available_files"] == expected_context["available_files"]
            else:
                for key, value in expected_context.items():
                    assert e.context[key] == value

# --------------------------------------------------
# 2. Input Validation Tests
# --------------------------------------------------
@pytest.mark.parametrize("invalid_date,error_details", [
    ("01-01-2024", {"reason": "wrong_format"}),
    ("2024/01/01", {"reason": "wrong_separator"}),
    ("20240101", {"reason": "no_separator"}),
    ("2024-13-01", {"reason": "invalid_month"}),
    ("2024-01-32", {"reason": "invalid_day"}),
])
def test_date_validation_flow(mock_s3_block, mock_logger, invalid_date, error_details):
    """Validate date format requirements with various invalid formats"""
    flow = PreprocessFlow(
        product_category_map_url="mock://map",
        s3_block=mock_s3_block
    )
    
    with error_collection() as accumulator:
        with pytest.raises(InvalidDateFormatError):
            flow.validate_date(cast("DateStr", invalid_date))
        
        error = accumulator.errors[0]
        assert isinstance(error, InvalidDateFormatError)
        assert error.code == "ARG-101"
        assert error.responsibility == AccountableRole.SYSTEM
        assert invalid_date in error.context["invalid_date"]
        assert error.context["validation_error"] == error_details["reason"]

# --------------------------------------------------
# 3. Data Processing Tests
# --------------------------------------------------
@pytest.mark.parametrize("scenario", [
    {
        "content_date": "2024-01-02",
        "filename_date": "2024-01-01",
        "description": "future_date"
    },
    {
        "content_date": "2023-12-31",
        "filename_date": "2024-01-01",
        "description": "past_date"
    },
])
def test_date_mismatch_handling(create_zip_file, scenario):
    """Test date mismatch detection and handling with various scenarios"""
    zip_content = create_zip_file(
        "wrong_date",
        date=scenario["content_date"]
    )
    
    def mock_reader():
        yield zip_content

    with error_collection() as accumulator:
        with pytest.raises(DateMismatchError):
            process_sepa_zip(
                mock_reader(),
                DateStr(scenario["filename_date"]),
                f"test_{scenario['description']}"
            )
        
        error = accumulator.errors[0]
        assert isinstance(error, DateMismatchError)
        assert error.code == "ARG-200"
        assert error.context["external_date"] == scenario["filename_date"]
        assert error.context["internal_date"] == scenario["content_date"]
        assert error.context["mismatch_type"] == scenario["description"]

def test_mixed_error_handling(valid_sepa_data, create_zip_file):
    """Test handling of critical errors that stop processing"""
    zip_content = create_zip_file("mixed_errors", df=valid_sepa_data)
    
    def mock_reader():
        yield zip_content

    with pytest.raises(InvalidCSVSchemaError) as exc_info:
        process_sepa_zip(mock_reader(), DateStr("2024-01-01"), "test_mixed")
    
    error = exc_info.value
    assert error.code == "ARG-201"
    assert error.context["date"] == "2024-01-01"
    assert error.context["store_id"] == "test_mixed"

# --------------------------------------------------
# 4. Category Mapping Tests
# --------------------------------------------------
@pytest.mark.parametrize("category_map,expected_error,validation_details", [
    (pd.DataFrame(), EmptyCategoryMapError, {"url": "mock://map"}),
    (pd.DataFrame({"wrong_column": []}), InvalidCSVSchemaError, {
        "date": "2024-01-01",
        "store_id": "test",
        "missing_columns": ["id_producto", "category"]
    }),
])
def test_category_mapping_validation(mock_s3_block, category_map, expected_error, validation_details):
    """Test category mapping validation with various invalid scenarios"""
    mock_map_loader = MagicMock(return_value=category_map)
    
    with patch('tsn_adapters.tasks.argentina.task_wrappers.task_load_category_map', mock_map_loader):
        flow = PreprocessFlow(
            product_category_map_url="mock://map",
            s3_block=mock_s3_block
        )
        
        with error_collection() as accumulator:
            if expected_error == EmptyCategoryMapError:
                flow.process_date(DateStr("2024-01-01"))
                error = accumulator.errors[0]
                assert isinstance(error, EmptyCategoryMapError)
                assert error.context["url"] == validation_details["url"]
            else:
                # For InvalidCSVSchemaError
                with pytest.raises(expected_error):
                    flow.process_date(DateStr(validation_details["date"]))
                error = accumulator.errors[0]
                assert isinstance(error, expected_error)
                for key, value in validation_details.items():
                    assert error.context[key] == value

# --------------------------------------------------
# 5. Error Recovery Tests
# --------------------------------------------------
def test_partial_processing_recovery(valid_sepa_data, create_zip_file):
    """Test system's ability to process valid records when others fail"""
    zip_content = create_zip_file("partial_valid", df=valid_sepa_data)
    
    def mock_reader():
        yield zip_content

    with error_collection() as accumulator:
        result = process_sepa_zip(mock_reader(), DateStr("2024-01-01"), "test_partial")
        
        # Verify errors were captured
        assert len(accumulator.errors) > 0
        error = accumulator.errors[0]
        assert isinstance(error, MissingProductIDError)
        assert error.context["missing_count"] == 3
        assert error.context["date"] == "2024-01-01"
        assert error.context["store_id"] == "test_partial"
        
        # Verify valid records were processed
        assert len(result) == 2  # Last 2 records should be valid
        assert all(pd.notna(result["id_producto"]))

def test_error_recovery_with_retries():
    """Test error recovery with retry mechanism"""
    retry_attempts = 0
    max_retries = 3
    
    def failing_operation():
        nonlocal retry_attempts
        retry_attempts += 1
        if retry_attempts < max_retries:
            # InvalidCSVSchemaError takes date and store_id
            raise InvalidCSVSchemaError(
                date=DateStr("2024-01-01"),
                store_id="test_store"
            )
        return True  # Succeed on final attempt
    
    with error_collection() as accumulator:
        # Simulate retry logic
        success = False
        for _ in range(max_retries):
            try:
                success = failing_operation()
                if success:
                    break
            except InvalidCSVSchemaError as e:  # Catch specific error type
                accumulator.add_error(e)
        
        assert success  # Operation eventually succeeded
        assert len(accumulator.errors) == max_retries - 1  # Errors from failed attempts
        assert all(isinstance(e, InvalidCSVSchemaError) for e in accumulator.errors)

# --------------------------------------------------
# 6. Error Accumulation Tests
# --------------------------------------------------
def test_error_context_isolation():
    """Verify error context isolation between parallel flows"""
    with error_collection() as flow1_errors:
        with error_collection() as flow2_errors:
            flow1_errors.add_error(EmptyCategoryMapError(url="map1"))
            flow2_errors.add_error(DateMismatchError(
                external_date="2024-01-01",
                internal_date="2024-01-02"
            ))
            
            assert len(flow2_errors.errors) == 1
            assert isinstance(flow2_errors.errors[0], DateMismatchError)
            assert flow2_errors.errors[0].context["external_date"] == "2024-01-01"
            assert flow2_errors.errors[0].context["internal_date"] == "2024-01-02"
        
        assert len(flow1_errors.errors) == 1
        assert isinstance(flow1_errors.errors[0], EmptyCategoryMapError)
        assert flow1_errors.errors[0].context["url"] == "map1"

def test_error_accumulation_resilience():
    """Test error accumulator's ability to handle multiple errors"""
    with error_collection() as accumulator:
        errors = [
            InvalidCSVSchemaError(date=DateStr("2024-01-01"), store_id="STORE1"),
            UncategorizedProductsError(count=3, date=DateStr("2024-01-01"), store_id="STORE1"),
            MissingProductIDError(count=2, date=DateStr("2024-01-01"), store_id="STORE1"),
        ]
        
        for error in errors:
            accumulator.add_error(error)
        
        assert len(accumulator.errors) == len(errors)
        for original, captured in zip(errors, accumulator.errors):
            assert type(original) == type(captured)
            assert original.code == captured.code
            assert original.responsibility == captured.responsibility
            assert original.context == captured.context

def test_error_accumulation_order():
    """Verify error accumulation preserves order and priority"""
    with error_collection() as accumulator:
        # Add errors with different priorities
        errors = [
            (InvalidStructureZIPError(context={"source": "test", "date": "2024-01-01"}), 1),  # High priority
            (MissingProductIDError(count=2, date=DateStr("2024-01-01"), store_id="test"), 3),  # Low priority
            (DateMismatchError(external_date="2024-01-01", internal_date="2024-01-02"), 2),  # Medium priority
        ]
        
        for error, _ in errors:
            accumulator.add_error(error)
        
        # Verify errors are stored in order of addition
        for (original, _), captured in zip(errors, accumulator.errors):
            assert type(original) == type(captured)
            assert original.code == captured.code
            assert original.context == captured.context

def test_error_accumulation(prefect_test_context, create_zip_file, mock_s3_block):
    """Test that errors are properly accumulated during preprocessing"""
    # Create test data with missing product IDs
    test_data = pd.DataFrame({
        'date': ['2024-01-01'] * 3,
        'id_producto': [None, None, 'P3'],
        'productos_descripcion': ['Product 1', 'Product 2', 'Product 3'],
        'precio': [100.0, 200.0, 300.0]
    })
    
    # Create a temporary CSV file with test data
    with tempfile.NamedTemporaryFile(suffix='.csv', mode='w', delete=False) as f:
        test_data.to_csv(f, index=False)
        temp_csv = f.name
    
    try:
        # Run preprocessing flow
        flow = PreprocessFlow(
            product_category_map_url="mock://map",
            s3_block=mock_s3_block
        )
        with pytest.raises(MissingProductIDError) as exc_info:
            flow.process_date(DateStr("2024-01-01"))
        
        error = exc_info.value
        assert error.code == "ARG-202"  # Fixed error code
        assert error.context["missing_count"] == 2
        assert error.context["date"] == "2024-01-01"
    finally:
        # Clean up
        os.unlink(temp_csv)

if __name__ == "__main__":
    pytest.main(["-v", __file__]) 