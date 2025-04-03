"""
Integration tests for the Argentina SEPA Product Insertion Flow.
"""

from collections.abc import Generator
from contextlib import ExitStack
from datetime import datetime, timezone
import os
from typing import Any
from unittest.mock import (
    ANY,
    AsyncMock,
    MagicMock,
    patch,
)

import boto3  # type: ignore
from moto import mock_aws
from mypy_boto3_s3 import S3Client
import pandas as pd
from pandera.typing import DataFrame
from prefect.exceptions import UpstreamTaskError
from prefect_aws import S3Bucket  # type: ignore
import pytest

from tsn_adapters.blocks.deployment_state import DeploymentStateBlock
from tsn_adapters.blocks.primitive_source_descriptor import (
    PrimitiveSourceDataModel,
    PrimitiveSourcesDescriptorBlock,
)
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.tasks.argentina.config import ArgentinaFlowVariableNames
from tsn_adapters.tasks.argentina.flows.insert_products_flow import DeploymentCheckError, insert_argentina_products_flow
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.tasks.argentina.tasks.date_processing_tasks import DailyAverageLoadingError, MappingIntegrityError
from tsn_adapters.tasks.argentina.tasks.descriptor_tasks import DescriptorError
from tsn_adapters.tasks.argentina.types import DateStr

# --- Constants ---
TEST_E2E_BUCKET_NAME = "test-e2e-insertion-bucket"
BASE_AGG_PATH = "aggregated"
BASE_PROC_PATH = "processed"
STATE_FILE_PATH = f"{BASE_AGG_PATH}/argentina_product_state.json"
DESCRIPTOR_FILE_PATH = f"{BASE_AGG_PATH}/argentina_products.csv.zip"

# --- Centralized Flow Context Fixture ---


@pytest.fixture
def mocked_flow_context() -> Generator[dict[str, MagicMock], None, None]:
    """
    Fixture to patch all external dependencies of the insert_argentina_products_flow.

    Yields a dictionary containing the mocks for further configuration in tests.
    """
    flow_path = "tsn_adapters.tasks.argentina.flows.insert_products_flow"
    mocks = {}
    with ExitStack() as stack:
        mocks["provider_cls"] = stack.enter_context(patch(f"{flow_path}.ProductAveragesProvider"))
        mocks["var_get"] = stack.enter_context(patch("prefect.variables.Variable.get"))
        mocks["var_aset"] = stack.enter_context(patch("prefect.variables.Variable.aset"))
        mocks["load_daily"] = stack.enter_context(patch(f"{flow_path}.load_daily_averages", new_callable=AsyncMock))
        mocks["transform"] = stack.enter_context(patch(f"{flow_path}.transform_product_data", new_callable=AsyncMock))
        mocks["insert"] = stack.enter_context(patch(f"{flow_path}.task_split_and_insert_records"))
        mocks["create_artifact"] = stack.enter_context(patch(f"{flow_path}.create_markdown_artifact"))

        # Add mock provider instance for convenience
        mocks["provider_instance"] = mocks["provider_cls"].return_value
        yield mocks


# --- Fixtures for Mocked Blocks ---


@pytest.fixture
def mock_s3_block() -> MagicMock:
    return MagicMock(spec=S3Bucket)


@pytest.fixture
def mock_tn_block() -> MagicMock:
    return MagicMock(spec=TNAccessBlock)


@pytest.fixture
def mock_descriptor_block() -> MagicMock:
    mock = MagicMock(spec=PrimitiveSourcesDescriptorBlock)
    mock.get_descriptor = MagicMock(spec=PrimitiveSourcesDescriptorBlock.get_descriptor)
    return mock


@pytest.fixture
def mock_deployment_state_block() -> MagicMock:
    mock = MagicMock(spec=DeploymentStateBlock)
    mock.check_multiple_streams = MagicMock(spec=DeploymentStateBlock.check_multiple_streams)
    return mock


# --- Fixtures for Moto-based S3 Interaction ---


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def real_s3_bucket_block(aws_credentials: None, prefect_test_fixture: Any) -> Generator[S3Bucket, None, None]:
    _ = aws_credentials
    _ = prefect_test_fixture
    with mock_aws():
        s3_block = S3Bucket(bucket_name=TEST_E2E_BUCKET_NAME)
        s3_client: S3Client = s3_block._get_s3_client()  # type: ignore
        s3_client.create_bucket(Bucket=TEST_E2E_BUCKET_NAME)
        yield s3_block


# --- Sample Data Fixtures ---


@pytest.fixture
def sample_descriptor_df() -> DataFrame[PrimitiveSourceDataModel]:
    data = {
        "stream_id": ["stream-1", "stream-2"],
        "source_id": ["p1", "p2"],
        "source_type": ["arg_sepa_prod"] * 2,
    }
    return PrimitiveSourceDataModel.validate(pd.DataFrame(data), lazy=True)


@pytest.fixture
def sample_daily_avg_df_date1() -> DataFrame[SepaAvgPriceProductModel]:
    data = {
        "id_producto": ["p1", "p2"],
        "productos_descripcion": ["Prod 1", "Prod 2"],
        "productos_precio_lista_avg": [10.5, 20.25],
        "date": ["2024-03-15"] * 2,
    }
    return SepaAvgPriceProductModel.validate(pd.DataFrame(data), lazy=True)


@pytest.fixture
def sample_transformed_df_date1() -> DataFrame[TnDataRowModel]:
    ts = int(datetime(2024, 3, 15, 0, 0, 0, tzinfo=timezone.utc).timestamp())
    data = {
        "stream_id": ["stream-1", "stream-2"],
        "date": [ts, ts],
        "value": ["10.5", "20.25"],
        "data_provider": [None, None],
    }
    return TnDataRowModel.validate(pd.DataFrame(data), lazy=True)


# --- Helper Functions ---
# (Keep setup_initial_s3_state if needed for e2e tests, otherwise remove)

# --- Successful Run Test ---


@pytest.mark.asyncio
async def test_insert_flow_successful_run(
    prefect_test_fixture: Any,
    mocked_flow_context: dict[str, MagicMock],
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    mock_deployment_state_block: MagicMock,
    sample_descriptor_df: DataFrame[PrimitiveSourceDataModel],
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],
    sample_transformed_df_date1: DataFrame[TnDataRowModel],
):
    """Test a successful run of the flow for a single date."""
    # Arrange
    date_to_process = "2024-03-15"
    batch_size = 500
    mocks = mocked_flow_context

    # Configure mocks specific to this test
    mocks["provider_instance"].list_available_keys.return_value = [date_to_process]

    def var_get_side_effect(name: str, default: Any = None) -> str | None:
        if name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE:
            return date_to_process
        if name == ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE:
            return "2024-03-14"
        return default

    mocks["var_get"].side_effect = var_get_side_effect

    mock_descriptor_block.get_descriptor.return_value = sample_descriptor_df
    required_streams = sample_descriptor_df["stream_id"].tolist()
    mock_deployment_state_block.check_multiple_streams.return_value = {s: True for s in required_streams}
    mocks["load_daily"].return_value = sample_daily_avg_df_date1
    mocks["transform"].return_value = sample_transformed_df_date1

    # Act
    await insert_argentina_products_flow(
        s3_block=mock_s3_block,
        tn_block=mock_tn_block,
        descriptor_block=mock_descriptor_block,
        deployment_state=mock_deployment_state_block,
        batch_size=batch_size,
    )

    # Assert
    mock_descriptor_block.get_descriptor.assert_called_once()
    mocks["provider_cls"].assert_called_once_with(s3_block=mock_s3_block)
    mocks["provider_instance"].list_available_keys.assert_called_once()
    mocks["var_get"].assert_any_call(ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE, default=ANY)
    mocks["var_get"].assert_any_call(ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE, default=ANY)

    mocks["load_daily"].assert_called_once()
    assert mocks["load_daily"].call_args[1]["date_str"] == date_to_process

    assert mock_deployment_state_block.check_multiple_streams.call_count == 1
    call_args, _ = mock_deployment_state_block.check_multiple_streams.call_args  # Ignore kwargs
    assert call_args[0] == required_streams

    mocks["transform"].assert_called_once_with(
        daily_avg_df=sample_daily_avg_df_date1,
        descriptor_df=sample_descriptor_df,
        date_str=date_to_process,
    )
    mocks["insert"].assert_called_once_with(
        block=mock_tn_block, records=sample_transformed_df_date1, batch_size=batch_size
    )
    mocks["var_set"].assert_called_once_with(ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE, date_to_process)
    mocks["create_artifact"].assert_called_once()


# --- Parametrized Scenarios ---

flow_run_scenarios = [
    ("single_date", ["2024-03-15"], ["2024-03-15"], "2024-03-15", "2024-03-14"),
    ("two_dates", ["2024-03-15", "2024-03-16"], ["2024-03-15", "2024-03-16"], "2024-03-16", "2024-03-14"),
    ("no_new_dates", [], [], "2024-03-16", "2024-03-16"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "test_id, dates_to_return, expected_processed_dates, initial_agg_date_mock, initial_ins_date_mock",
    flow_run_scenarios,
)
async def test_insert_flow_basic_scenarios(
    test_id: str,
    dates_to_return: list[DateStr],
    expected_processed_dates: list[DateStr],
    initial_agg_date_mock: str,
    initial_ins_date_mock: str,
    prefect_test_fixture: Any,
    mocked_flow_context: dict[str, MagicMock],
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    mock_deployment_state_block: MagicMock,
    sample_descriptor_df: DataFrame[PrimitiveSourceDataModel],
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],
    sample_transformed_df_date1: DataFrame[TnDataRowModel],
):
    # Arrange
    batch_size = 500
    mocks = mocked_flow_context
    desc_df = sample_descriptor_df.copy()
    daily_avg_df = sample_daily_avg_df_date1.copy()
    transformed_df = sample_transformed_df_date1.copy()

    mocks["provider_instance"].list_available_keys.return_value = dates_to_return
    mock_descriptor_block.get_descriptor.return_value = desc_df
    mock_deployment_state_block.check_multiple_streams.return_value = {s: True for s in desc_df["stream_id"]}

    def var_get_side_effect(name: str, default: Any = None) -> str | None:
        if name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE:
            return initial_agg_date_mock
        if name == ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE:
            return initial_ins_date_mock
        return default

    mocks["var_get"].side_effect = var_get_side_effect

    mocks["load_daily"].return_value = daily_avg_df
    mocks["transform"].return_value = transformed_df

    # Act
    await insert_argentina_products_flow(
        s3_block=mock_s3_block,
        tn_block=mock_tn_block,
        descriptor_block=mock_descriptor_block,
        deployment_state=mock_deployment_state_block,
        batch_size=batch_size,
    )

    # Assert
    mock_descriptor_block.get_descriptor.assert_called_once()
    mocks["provider_instance"].list_available_keys.assert_called_once()
    mocks["var_get"].assert_any_call(ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE, default=ANY)
    mocks["var_get"].assert_any_call(ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE, default=ANY)

    num_expected_calls = len(expected_processed_dates)
    assert mocks["load_daily"].call_count == num_expected_calls
    assert mocks["transform"].call_count == num_expected_calls
    assert mocks["insert"].call_count == num_expected_calls
    assert mock_deployment_state_block.check_multiple_streams.call_count == num_expected_calls

    if num_expected_calls > 0:
        mocks["var_set"].assert_called_with(
            ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE, expected_processed_dates[-1]
        )
        assert mocks["var_set"].call_count == num_expected_calls
    else:
        mocks["var_set"].assert_not_called()

    mocks["create_artifact"].assert_called_once()
    artifact_md = mocks["create_artifact"].call_args.kwargs.get("markdown", "")
    if num_expected_calls > 0:
        assert f"({num_expected_calls} dates)" in artifact_md
        assert f"Total Records Transformed & Submitted:** {len(transformed_df) * num_expected_calls}" in artifact_md
    else:
        assert "No new dates available to process" in artifact_md


# --- Fatal Error Scenario Tests ---


@pytest.mark.asyncio
async def test_insert_flow_fatal_error_load_descriptor(
    prefect_test_fixture: Any,
    mocked_flow_context: dict[str, MagicMock],
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    mock_deployment_state_block: MagicMock,
):
    """Test flow fails if loading descriptor fails."""
    mocks = mocked_flow_context
    test_exception = DescriptorError("Failed to get descriptor")
    mock_descriptor_block.get_descriptor.side_effect = test_exception
    mocks["var_get"].return_value = "2023-01-01"  # Allow flow to reach descriptor load

    with pytest.raises(RuntimeError) as exc_info:
        await insert_argentina_products_flow(
            s3_block=mock_s3_block,
            tn_block=mock_tn_block,
            descriptor_block=mock_descriptor_block,
            deployment_state=mock_deployment_state_block,
        )
    assert isinstance(exc_info.value.__cause__, DescriptorError)
    mock_descriptor_block.get_descriptor.assert_called_once()


@pytest.mark.asyncio
async def test_insert_flow_fatal_error_load_daily(
    prefect_test_fixture: Any,
    mocked_flow_context: dict[str, MagicMock],
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    mock_deployment_state_block: MagicMock,
    sample_descriptor_df: DataFrame[PrimitiveSourceDataModel],
):
    """Test flow fails if loading daily averages fails within the loop."""
    mocks = mocked_flow_context
    test_exception = DailyAverageLoadingError("Failed to load daily")
    date_to_fail = "2024-03-15"

    mocks["provider_instance"].list_available_keys.return_value = [date_to_fail]
    def var_get_side_effect(name: str, default: Any = None) -> str | None:
        if name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE: return date_to_fail # Allow processing date_to_fail
        if name == ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE: return "2024-03-14" # Assume day before
        return default
    mocks["var_get"].side_effect = var_get_side_effect

    mock_descriptor_block.get_descriptor.return_value = sample_descriptor_df
    mock_deployment_state_block.check_multiple_streams.return_value = {
        s: True for s in sample_descriptor_df["stream_id"]
    }
    mocks["load_daily"].side_effect = test_exception

    with pytest.raises(DailyAverageLoadingError) as exc_info:
        await insert_argentina_products_flow(
            s3_block=mock_s3_block,
            tn_block=mock_tn_block,
            descriptor_block=mock_descriptor_block,
            deployment_state=mock_deployment_state_block,
        )

    assert exc_info.value is test_exception
    mock_descriptor_block.get_descriptor.assert_called_once()
    mocks["load_daily"].assert_called_once_with(provider=ANY, date_str=date_to_fail)


@pytest.mark.asyncio
async def test_insert_flow_fatal_error_transform(
    prefect_test_fixture: Any,
    mocked_flow_context: dict[str, MagicMock],
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    mock_deployment_state_block: MagicMock,
    sample_descriptor_df: DataFrame[PrimitiveSourceDataModel],
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],
):
    """Test flow fails if transformation fails."""
    mocks = mocked_flow_context
    test_exception = MappingIntegrityError("Missing product mapping")
    date_to_process = "2024-03-15"

    mocks["provider_instance"].list_available_keys.return_value = [date_to_process]
    def var_get_side_effect(name: str, default: Any = None) -> str | None:
        if name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE: return date_to_process
        if name == ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE: return "2024-03-14"
        return default
    mocks["var_get"].side_effect = var_get_side_effect

    mock_descriptor_block.get_descriptor.return_value = sample_descriptor_df
    mock_deployment_state_block.check_multiple_streams.return_value = {
        s: True for s in sample_descriptor_df["stream_id"]
    }
    mocks["load_daily"].return_value = sample_daily_avg_df_date1
    mocks["transform"].side_effect = test_exception

    with pytest.raises(MappingIntegrityError) as exc_info:
        await insert_argentina_products_flow(
            s3_block=mock_s3_block,
            tn_block=mock_tn_block,
            descriptor_block=mock_descriptor_block,
            deployment_state=mock_deployment_state_block,
        )

    assert exc_info.value is test_exception
    mock_descriptor_block.get_descriptor.assert_called_once()
    mocks["load_daily"].assert_called_once()
    mocks["transform"].assert_called_once()
    mocks["insert"].assert_not_called()


@pytest.mark.asyncio
async def test_insert_flow_fatal_error_insert(
    prefect_test_fixture: Any,
    mocked_flow_context: dict[str, MagicMock],
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    mock_deployment_state_block: MagicMock,
    sample_descriptor_df: DataFrame[PrimitiveSourceDataModel],
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],
    sample_transformed_df_date1: DataFrame[TnDataRowModel],
):
    """Test flow fails if TN insertion task fails."""
    mocks = mocked_flow_context
    test_exception = UpstreamTaskError("TN insertion failed")
    date_to_process = "2024-03-15"

    mocks["provider_instance"].list_available_keys.return_value = [date_to_process]
    def var_get_side_effect(name: str, default: Any = None) -> str | None:
        if name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE: return date_to_process
        if name == ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE: return "2024-03-14"
        return default
    mocks["var_get"].side_effect = var_get_side_effect

    mock_descriptor_block.get_descriptor.return_value = sample_descriptor_df
    mock_deployment_state_block.check_multiple_streams.return_value = {
        s: True for s in sample_descriptor_df["stream_id"]
    }
    mocks["load_daily"].return_value = sample_daily_avg_df_date1
    mocks["transform"].return_value = sample_transformed_df_date1
    mocks["insert"].side_effect = test_exception

    with pytest.raises(UpstreamTaskError) as exc_info:
        await insert_argentina_products_flow(
            s3_block=mock_s3_block,
            tn_block=mock_tn_block,
            descriptor_block=mock_descriptor_block,
            deployment_state=mock_deployment_state_block,
        )

    assert exc_info.value is test_exception
    mocks["load_daily"].assert_called_once()
    mocks["transform"].assert_called_once()
    mocks["insert"].assert_called_once()


@pytest.mark.asyncio
async def test_insert_flow_fatal_error_save_state(
    prefect_test_fixture: Any,
    mocked_flow_context: dict[str, MagicMock],
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    mock_deployment_state_block: MagicMock,
    sample_descriptor_df: DataFrame[PrimitiveSourceDataModel],
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],
    sample_transformed_df_date1: DataFrame[TnDataRowModel],
):
    """Test flow fails if saving state fails after successful insertion."""
    mocks = mocked_flow_context
    test_exception = OSError("Variable set failed")
    date_to_process = "2024-03-15"

    mocks["provider_instance"].list_available_keys.return_value = [date_to_process]
    def var_get_side_effect(name: str, default: Any = None) -> str | None:
        if name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE: return date_to_process
        if name == ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE: return "2024-03-14"
        return default
    mocks["var_get"].side_effect = var_get_side_effect

    mock_descriptor_block.get_descriptor.return_value = sample_descriptor_df
    mock_deployment_state_block.check_multiple_streams.return_value = {
        s: True for s in sample_descriptor_df["stream_id"]
    }
    mocks["load_daily"].return_value = sample_daily_avg_df_date1
    mocks["transform"].return_value = sample_transformed_df_date1
    mocks["insert"].return_value = None  # Insertion succeeds
    mocks["var_set"].side_effect = test_exception

    with pytest.raises(OSError) as exc_info:
        await insert_argentina_products_flow(
            s3_block=mock_s3_block,
            tn_block=mock_tn_block,
            descriptor_block=mock_descriptor_block,
            deployment_state=mock_deployment_state_block,
        )

    assert exc_info.value is test_exception
    mocks["load_daily"].assert_called_once()
    mocks["transform"].assert_called_once()
    mocks["insert"].assert_called_once()
    mocks["var_set"].assert_called_once()  # Should be called once before failing
    mocks["create_artifact"].assert_called_once()


# --- Test for Deployment Check Failure ---


@pytest.mark.asyncio
async def test_insert_flow_halts_on_undeployed_streams(
    prefect_test_fixture: Any,
    mocked_flow_context: dict[str, MagicMock],
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    mock_deployment_state_block: MagicMock,
    sample_descriptor_df: DataFrame[PrimitiveSourceDataModel],
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],
):
    """Test flow halts with DeploymentCheckError if streams are not deployed."""
    mocks = mocked_flow_context
    date_to_process = "2024-03-15"
    undeployed_stream = "stream-1"
    required_streams = sample_descriptor_df["stream_id"].tolist()
    assert undeployed_stream in required_streams

    mocks["provider_instance"].list_available_keys.return_value = [date_to_process]

    # Define var_get here or use the one from successful run if applicable
    def var_get_side_effect(name: str, default: Any = None) -> str | None:
        if name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE:
            return date_to_process
        if name == ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE:
            return "2024-03-14"
        return default

    mocks["var_get"].side_effect = var_get_side_effect

    mock_descriptor_block.get_descriptor.return_value = sample_descriptor_df
    mocks["load_daily"].return_value = sample_daily_avg_df_date1

    deployment_status = {s: True for s in required_streams}
    deployment_status[undeployed_stream] = False
    mock_deployment_state_block.check_multiple_streams.return_value = deployment_status

    with pytest.raises(DeploymentCheckError) as exc_info:
        await insert_argentina_products_flow(
            s3_block=mock_s3_block,
            tn_block=mock_tn_block,
            descriptor_block=mock_descriptor_block,
            deployment_state=mock_deployment_state_block,
        )

    assert f"halting flow: date {date_to_process} cannot be processed" in str(exc_info.value).lower()
    assert undeployed_stream in str(exc_info.value)

    mocks["provider_instance"].list_available_keys.assert_called_once()
    mocks["var_get"].assert_any_call(ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE, default=ANY)
    mock_descriptor_block.get_descriptor.assert_called_once()
    mocks["load_daily"].assert_called_once_with(provider=ANY, date_str=date_to_process)
    assert mock_deployment_state_block.check_multiple_streams.call_count == 1
    call_args, _ = mock_deployment_state_block.check_multiple_streams.call_args
    assert call_args[0] == required_streams

    mocks["transform"].assert_not_called()
    mocks["insert"].assert_not_called()
    mocks["var_set"].assert_not_called()
    mocks["create_artifact"].assert_not_called()


# --- Reporting Test ---


@pytest.mark.asyncio
async def test_insert_flow_reporting(
    prefect_test_fixture: Any,
    mocked_flow_context: dict[str, MagicMock],
    mock_s3_block: MagicMock,
    mock_tn_block: MagicMock,
    mock_descriptor_block: MagicMock,
    mock_deployment_state_block: MagicMock,
    sample_descriptor_df: DataFrame[PrimitiveSourceDataModel],
    sample_daily_avg_df_date1: DataFrame[SepaAvgPriceProductModel],
    sample_transformed_df_date1: DataFrame[TnDataRowModel],
):
    """Test that the summary artifact is created correctly after processing dates."""
    mocks = mocked_flow_context
    dates_to_process = ["2024-03-15", "2024-03-16"]
    total_records_per_date = len(sample_transformed_df_date1)
    expected_total_records = total_records_per_date * len(dates_to_process)

    mocks["provider_instance"].list_available_keys.return_value = dates_to_process
    def var_get_side_effect(name: str, default: Any = None) -> str | None:
        if name == ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE: return dates_to_process[-1] # Use last date in list
        if name == ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE: return "2024-03-14" # Assume day before first date
        return default
    mocks["var_get"].side_effect = var_get_side_effect

    mock_descriptor_block.get_descriptor.return_value = sample_descriptor_df
    mock_deployment_state_block.check_multiple_streams.return_value = {
        s: True for s in sample_descriptor_df["stream_id"]
    }
    mocks["load_daily"].return_value = sample_daily_avg_df_date1
    mocks["transform"].return_value = sample_transformed_df_date1
    mocks["insert"].return_value = None  # Simulate successful insert

    await insert_argentina_products_flow(
        s3_block=mock_s3_block,
        tn_block=mock_tn_block,
        descriptor_block=mock_descriptor_block,
        deployment_state=mock_deployment_state_block,
    )

    assert mocks["insert"].call_count == len(dates_to_process)
    mocks["create_artifact"].assert_called_once()
    artifact_md = mocks["create_artifact"].call_args.kwargs.get("markdown", "")
    assert f"Processed Date Range:** {dates_to_process[0]} to {dates_to_process[-1]}" in artifact_md
    assert f"({len(dates_to_process)} dates)" in artifact_md
    assert f"Total Records Transformed & Submitted:** {expected_total_records}" in artifact_md


# --- Test Removed ---
# @pytest.mark.asyncio
# async def test_insert_flow_fatal_error_load_state(...) - Removed as state loading changed.
