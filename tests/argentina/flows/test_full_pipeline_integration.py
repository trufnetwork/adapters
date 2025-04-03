"""
End-to-end integration test for the Argentina product pipeline.

This test executes the aggregation, deployment, and insertion flows sequentially,
verifying the state of S3, database, TN, and Prefect variables at each stage.
"""

from datetime import datetime, timezone
from functools import partial  # To pass verification args
import inspect
from typing import Any, Callable, Optional

from mypy_boto3_s3 import S3Client
import pandas as pd
from pandera.typing import DataFrame
from prefect import Flow
import prefect.variables as variables
from prefect_aws import S3Bucket
import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session as SqlaSession
from src.tsn_adapters.flows.stream_deploy_flow import deploy_streams_flow
from tests.argentina.helpers import upload_df_to_s3_csv_zip

from tsn_adapters.blocks.models.sql_models import primitive_sources_table  # For DB verification
from tsn_adapters.blocks.sql_deployment_state import SqlAlchemyDeploymentState
from tsn_adapters.blocks.sql_source_descriptor import SqlAlchemySourceDescriptor
from tsn_adapters.blocks.tn_access import TNAccessBlock
from tsn_adapters.common.trufnetwork.models.tn_models import TnRecordModel  # Import model for read_records
from tsn_adapters.tasks.argentina.config import ArgentinaFlowVariableNames
from tsn_adapters.tasks.argentina.flows.aggregate_products_flow import aggregate_argentina_products_flow
from tsn_adapters.tasks.argentina.flows.insert_products_flow import insert_argentina_products_flow
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import SepaAvgPriceProductModel
from tsn_adapters.utils.logging import get_logger_safe

# === Helper Functions for Verification ===


def _dt_to_ts(date_str: str) -> int:
    """Convert YYYY-MM-DD string to UTC midnight POSIX timestamp integer."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    dt_utc = dt.replace(tzinfo=timezone.utc)
    return int(dt_utc.timestamp())


def _generate_expected_tn_records(
    source_id_to_data: dict[str, list[tuple[str, float]]],
) -> dict[str, list[tuple[int, str]]]:
    """Generates the expected TN records dict from product data."""
    expected_records = {}
    for source_id, data_list in source_id_to_data.items():
        expected_records[source_id] = sorted(
            [(_dt_to_ts(date_str), str(price)) for date_str, price in data_list], key=lambda x: x[0]
        )  # Sort by timestamp
    return expected_records


def _verify_db_state(
    db_session: SqlaSession,
    phase_name: str,
    expected_total_rows: int,
    expected_source_ids: set[str],
    expected_deployed_ids: set[str],
) -> pd.DataFrame:
    """Verify the state of the primitive_sources table."""
    logger = get_logger_safe(__name__)
    logger.info(f"Verifying database state after {phase_name}...")
    try:
        with db_session.begin():  # Ensure transaction context for query
            stmt = text(f"SELECT * FROM {primitive_sources_table.name}")
            result = db_session.execute(stmt)
            db_rows = result.fetchall()
            db_df = pd.DataFrame(db_rows, columns=result.keys())

        # Assert row count and source IDs
        assert (
            len(db_df) == expected_total_rows
        ), f"[{phase_name}] Expected {expected_total_rows} rows in DB, found {len(db_df)}"
        actual_source_ids = set(db_df["source_id"])
        assert (
            actual_source_ids == expected_source_ids
        ), f"[{phase_name}] Mismatch in source IDs. Expected: {expected_source_ids}, Got: {actual_source_ids}"

        # Assert properties for all rows based on deployment expectation
        for _, row in db_df.iterrows():
            source_id = row["source_id"]
            is_expected_deployed = source_id in expected_deployed_ids

            assert (
                row["source_type"] == "argentina_sepa_product"
            ), f"[{phase_name}] Incorrect source_type found for {source_id}"
            assert (
                row["is_deployed"] == is_expected_deployed
            ), f"[{phase_name}] is_deployed mismatch for {source_id}. Expected: {is_expected_deployed}, Got: {row['is_deployed']}"
            if is_expected_deployed:
                assert pd.notnull(
                    row["deployed_at"]
                ), f"[{phase_name}] deployed_at should not be NULL for deployed {source_id}"
            else:
                assert pd.isnull(
                    row["deployed_at"]
                ), f"[{phase_name}] deployed_at should be NULL for non-deployed {source_id}"

            # Verify stream_id is a non-empty string, likely starting with 'st'
            stream_id = row["stream_id"]
            assert (
                isinstance(stream_id, str) and stream_id
            ), f"[{phase_name}] stream_id should be a non-empty string for {source_id}, got: {stream_id!r}"
            assert stream_id.startswith(
                "st"
            ), f"[{phase_name}] stream_id should start with 'st' for {source_id}, got: {stream_id!r}"
            # todo add inline verification with generate_stream_id sdk helper

        logger.info(f"Database state verification successful after {phase_name}.")
        return db_df  # Return for potential reuse

    except Exception as e:
        pytest.fail(f"Database verification failed after {phase_name}: {e}")


async def _verify_variable_state(
    phase_name: str,
    expected_agg_date: Optional[str],
    expected_ins_date: Optional[str],
):
    """Verify the state of the Prefect variables."""
    logger = get_logger_safe(__name__)
    logger.info(f"Verifying variable state after {phase_name}...")
    if expected_agg_date:
        actual_agg_date = await variables.Variable.aget(
            ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE, default=ArgentinaFlowVariableNames.DEFAULT_DATE
        )
        assert (
            actual_agg_date == expected_agg_date
        ), f"[{phase_name}] Expected LAST_AGGREGATION_SUCCESS_DATE: '{expected_agg_date}', Got: '{actual_agg_date}'"
    if expected_ins_date:
        actual_ins_date = await variables.Variable.aget(
            ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE, default=ArgentinaFlowVariableNames.DEFAULT_DATE
        )
        assert (
            actual_ins_date == expected_ins_date
        ), f"[{phase_name}] Expected LAST_INSERTION_SUCCESS_DATE: '{expected_ins_date}', Got: '{actual_ins_date}'"
    logger.info(f"Variable state verification successful after {phase_name}.")


def _verify_tn_records(
    tn_block: TNAccessBlock,
    phase_name: str,
    id_map: dict[str, str],
    expected_records: dict[str, list[tuple[int, str]]],
    check_source_ids: list[str],
):
    """Verify records for specific source IDs in TrufNetwork."""
    logger = get_logger_safe(__name__)
    logger.info(f"Verifying TN records for {check_source_ids} after {phase_name}...")

    for source_id in check_source_ids:
        stream_id = id_map.get(source_id)
        assert stream_id is not None, f"[{phase_name}] Stream ID not found for source ID {source_id} in mapping"
        expected_data = expected_records.get(source_id, [])  # Default to empty list if source_id not expected
        logger.debug(f"[{phase_name}] Verifying records for {source_id} (stream: {stream_id})...")
        try:
            # Read records (returns DataFrame[TnRecordModel])
            tn_records_df: DataFrame[TnRecordModel] = tn_block.read_records(stream_id=stream_id, is_unix=True)

            # Prepare expected DataFrame
            expected_df = pd.DataFrame(expected_data, columns=["timestamp", "value"])
            # Ensure correct types for comparison
            expected_df["timestamp"] = pd.to_numeric(expected_df["timestamp"])
            expected_df["value"] = expected_df["value"].astype(str)
            expected_df = DataFrame[TnRecordModel](expected_df)

            # Sort DataFrames by timestamp
            tn_records_sorted_df = tn_records_df.sort_values(by="timestamp").reset_index(drop=True)
            expected_sorted_df = expected_df.sort_values(by="timestamp").reset_index(drop=True)

            # Compare using pandas testing function for better error messages
            try:
                pd.testing.assert_frame_equal(tn_records_sorted_df, expected_sorted_df, check_dtype=True)
            except AssertionError as df_diff:
                pytest.fail(f"[{phase_name}] TN Record mismatch for {source_id} ({stream_id}):\n{df_diff}")

            logger.debug(f"[{phase_name}] Records verified successfully for {source_id}.")
        except Exception as e:
            pytest.fail(f"[{phase_name}] TN record verification failed for {source_id} ({stream_id}): {e}")
    logger.info(f"TrufNetwork record verification successful after {phase_name}.")


def _verify_stream_existence(
    tn_block: TNAccessBlock,
    phase_name: str,
    id_map: dict[str, str],
    account: str,
    should_exist_ids: list[str],
):
    """Verify existence/non-existence of streams in TrufNetwork."""
    logger = get_logger_safe(__name__)
    logger.info(f"Verifying stream existence after {phase_name}...")
    missing_streams = []

    for source_id in should_exist_ids:
        stream_id = id_map.get(source_id)
        assert stream_id is not None, f"[{phase_name}] Stream ID missing for {source_id} in id_map"
        try:
            if not tn_block.stream_exists(account, stream_id):
                logger.error(
                    f"[{phase_name}] Verification failed: Expected stream '{stream_id}' ({source_id}) does not exist in TN."
                )
                missing_streams.append(stream_id)
        except Exception as e:
            pytest.fail(f"[{phase_name}] tn_block.stream_exists failed for expected stream '{stream_id}': {e}")

    assert not missing_streams, f"[{phase_name}] Expected streams not found in TN: {missing_streams}"
    logger.info(f"TrufNetwork stream existence verification successful after {phase_name}.")


# === Flow Execution Helper ===


async def _run_flow_and_verify(
    phase_name: str,
    flow_func: Flow[Any, Any],
    flow_args: dict[str, Any],
    verifications: list[tuple[Callable[..., Any], dict[str, Any]]] = [],
) -> dict[str, Any]:
    """
    Runs an async flow, handles errors, and performs specified verifications.

    Args:
        phase_name: Name of the phase for logging.
        flow_func: The async flow function to execute.
        flow_args: Dictionary of arguments to pass to the flow function.
        verifications: List of tuples, where each tuple contains:
                         - verification_func: The verification function to call.
                         - verification_args: Dictionary of arguments for the verification func.

    Returns:
        A dictionary containing any non-None results from verification functions,
        keyed by the verification function's name.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"--- Starting {phase_name} --- ")

    # Execute Flow
    logger.info(f"Executing flow: {flow_func.name}...")
    try:
        # Check if the flow function itself is async
        if inspect.iscoroutinefunction(flow_func.fn): # Check the underlying function
            await flow_func(**flow_args)
        else:
            flow_func(**flow_args) # Run synchronous flows directly
        
        # Optional: Add a small delay if needed for async operations within sync flows to potentially settle
        # await asyncio.sleep(1) 

        logger.info(f"Flow {flow_func.name} executed successfully for {phase_name}.")

        # Debug: Print current variable state (Keep this for now)
        if ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE in str(flow_func.name): # Check name
            agg_date = await variables.Variable.aget(
                ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE, 
                default=ArgentinaFlowVariableNames.DEFAULT_DATE
            )
            logger.info(f"DEBUG - After flow execution, LAST_AGGREGATION_SUCCESS_DATE = {agg_date}")
    except Exception as e:
        pytest.fail(f"Flow {flow_func.name} failed during {phase_name}: {e}")

    # Perform Verifications
    verification_results = {}
    logger.info(f"Performing verifications for {phase_name}...")
    for verify_func, verify_args in verifications:
        # Determine the actual function to inspect (handle partials)
        actual_func_to_inspect = getattr(verify_func, "func", verify_func)
        func_name = actual_func_to_inspect.__name__

        try:
            # Call the verification function (await if it's async)
            if inspect.iscoroutinefunction(actual_func_to_inspect):
                result = await verify_func(phase_name=phase_name, **verify_args)
            else:
                result = verify_func(phase_name=phase_name, **verify_args)
            
            verification_results[func_name] = result  # Store result if not None
        except Exception as e:
            pytest.fail(f"Verification function {func_name} failed during {phase_name}: {e}")
    logger.info(f"Verifications complete for {phase_name}.")
    return verification_results


# Fixtures are automatically discovered by pytest from:
# - tests/argentina/conftest.py (s3_bucket_block, sql_descriptor_block, sql_deployment_state)
# - tests/fixtures/test_sql.py (db_session, db_engine)
# - tests/fixtures/test_trufnetwork.py (tn_access_block as tn_block)
# - prefect.testing.utilities (prefect_test_fixture)


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.asyncio
async def test_full_argentina_pipeline(
    prefect_test_fixture: Any,
    s3_bucket_block: S3Bucket,
    db_session: SqlaSession,
    tn_block: TNAccessBlock,
    sql_descriptor_block: SqlAlchemySourceDescriptor,
    sql_deployment_state: SqlAlchemyDeploymentState,
    # sql_connector is implicitly used by descriptor and deployment state fixtures
):
    """
    Execute the full Argentina pipeline end-to-end using a helper function.

    This test simulates multiple runs of the pipeline flows:
    1. Initial run (Aggregate -> Deploy -> Insert) for data D1, D2, D3.
    2. Resumption setup: New data (D4, D5) uploaded, PREPROCESS variable updated.
    3. Out-of-order insertion attempt (should skip D4, D5).
    4. Resumption run (Aggregate -> Deploy -> Insert) for data D4, D5.
    Verifies database state, TN state (streams and records), and Prefect variables
    at each critical step.
    """
    # === Phase 1: Setup ===
    logger = get_logger_safe(__name__)
    logger.info("--- Starting Phase 1: Initial Setup ---")
    account = tn_block.client.get_current_account()

    # 1. Define and upload initial test data (D1, D2, D3)
    # This simulates the output of a preceding preprocessing step.
    source_data_d1 = {"P001": ("2024-05-01", 10.50), "P002": ("2024-05-01", 20.00), "P003": ("2024-05-01", 15.75)}
    source_data_d2 = {
        "P002": ("2024-05-02", 21.00),
        "P003": ("2024-05-02", 16.00),
        "P004": ("2024-05-02", 50.25),
        "P005": ("2024-05-02", 5.00),
    }
    source_data_d3 = {
        "P001": ("2024-05-03", 11.00),
        "P003": ("2024-05-03", 16.50),
        "P006": ("2024-05-03", 99.99),
        "P007": ("2024-05-03", 75.00),
    }
    # (DataFrame creation and validation...)
    data_d1 = pd.DataFrame(
        [
            {
                "id_producto": k,
                "productos_descripcion": f"Product {k}",
                "productos_precio_lista_avg": v[1],
                "date": v[0],
            }
            for k, v in source_data_d1.items()
        ]
    )
    data_d2 = pd.DataFrame(
        [
            {
                "id_producto": k,
                "productos_descripcion": f"Product {k}",
                "productos_precio_lista_avg": v[1],
                "date": v[0],
            }
            for k, v in source_data_d2.items()
        ]
    )
    data_d3 = pd.DataFrame(
        [
            {
                "id_producto": k,
                "productos_descripcion": f"Product {k}",
                "productos_precio_lista_avg": v[1],
                "date": v[0],
            }
            for k, v in source_data_d3.items()
        ]
    )
    data_d1 = SepaAvgPriceProductModel.validate(data_d1)
    data_d2 = SepaAvgPriceProductModel.validate(data_d2)
    data_d3 = SepaAvgPriceProductModel.validate(data_d3)

    s3_base_path = "processed"
    datasets = {"2024-05-01": data_d1, "2024-05-02": data_d2, "2024-05-03": data_d3}
    s3_client: S3Client = s3_bucket_block._get_s3_client()  # type: ignore
    paths_to_check = []
    for date_str, df in datasets.items():
        s3_path = f"{s3_base_path}/{date_str}/product_averages.zip"
        logger.info(f"Uploading test data for {date_str} to s3://{s3_bucket_block.bucket_name}/{s3_path}")
        upload_df_to_s3_csv_zip(s3_bucket_block, s3_path, df)
        paths_to_check.append(s3_path)

    # 3. Verify S3 Uploads (Quick check to ensure setup is correct)
    logger.info("Verifying initial files exist in Moto S3...")
    uploaded_keys = []
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket_block.bucket_name, Prefix=s3_base_path)
        uploaded_keys = [key for item in response.get("Contents", []) if (key := item.get("Key")) is not None]
    except Exception as e:
        pytest.fail(f"Failed to list objects in mock S3 bucket: {e}")
    for expected_path in paths_to_check:
        assert expected_path in uploaded_keys, f"File {expected_path} not found in S3 bucket"
    logger.info("S3 Upload verification successful.")

    # 4. Initialize Prefect Variables
    # Aggregation and Insertion start from scratch (1970-01-01).
    # Preprocessing is assumed complete up to the latest data (D3).
    _ = await _verify_variable_state("Initial Setup (Before Set)", "1970-01-01", "1970-01-01")  # Check defaults first
    logger.info("Initializing Prefect variables for first run...")
    await variables.Variable.aset(ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE, "2024-05-03", overwrite=True)
    await variables.Variable.aset(ArgentinaFlowVariableNames.LAST_AGGREGATION_SUCCESS_DATE, "1970-01-01", overwrite=True)
    await variables.Variable.aset(ArgentinaFlowVariableNames.LAST_INSERTION_SUCCESS_DATE, "1970-01-01", overwrite=True)
    # Verify the state after setting
    _ = await _verify_variable_state(
        "Initial Setup (After Set)", "1970-01-01", "1970-01-01"
    )  # Agg/Ins should still be default
    assert await variables.Variable.aget(ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE) == "2024-05-03"
    logger.info("Initial Prefect variable state verified.")

    # Define expected IDs
    ids_d1_d3 = {f"P{i:03d}" for i in range(1, 8)}  # P001-P007 from first 3 days
    ids_d1_d5 = {f"P{i:03d}" for i in range(1, 10)}  # P001-P009 including resumption data

    # --- Run Pipeline Phases using Helper ---

    # Phase 2: Aggregation (First Run)
    agg_results = await _run_flow_and_verify(
        phase_name="Phase 2 (Aggregation)",
        flow_func=aggregate_argentina_products_flow,
        flow_args={"s3_block": s3_bucket_block, "descriptor_block": sql_descriptor_block, "force_reprocess": False},
        verifications=[
            (partial(_verify_variable_state, expected_agg_date="2024-05-03", expected_ins_date="1970-01-01"), {}),
            (
                partial(
                    _verify_db_state, expected_total_rows=7, expected_source_ids=ids_d1_d3, expected_deployed_ids=set()
                ),
                {"db_session": db_session},
            ),
        ],
    )
    db_df = agg_results["_verify_db_state"]  # Get DB state for next phase
    id_map_d1_d3 = db_df.set_index("source_id")["stream_id"].to_dict()

    # Phase 3: Deployment (First Run)
    _ = await _run_flow_and_verify(
        phase_name="Phase 3 (Deployment)",
        flow_func=deploy_streams_flow,
        flow_args={"psd_block": sql_descriptor_block, "tna_block": tn_block, "deployment_state": sql_deployment_state},
        verifications=[
            (partial(_verify_variable_state, expected_agg_date="2024-05-03", expected_ins_date="1970-01-01"), {}),
            (
                partial(
                    _verify_db_state,
                    expected_total_rows=7,
                    expected_source_ids=ids_d1_d3,
                    expected_deployed_ids=ids_d1_d3,
                ),
                {"db_session": db_session},
            ),
            (
                partial(
                    _verify_stream_existence, id_map=id_map_d1_d3, account=account, should_exist_ids=list(ids_d1_d3)
                ),
                {"tn_block": tn_block},
            ),
        ],
    )

    # Phase 4: Insertion (First Run)
    initial_source_data = {**source_data_d1, **source_data_d2, **source_data_d3}
    records_d1_d3_data = {}
    for pid, (date, price) in initial_source_data.items():
        records_d1_d3_data.setdefault(pid, []).append((date, price))
    expected_records_d1_d3 = _generate_expected_tn_records(records_d1_d3_data)

    await _run_flow_and_verify(
        phase_name="Phase 4 (Insertion)",
        flow_func=insert_argentina_products_flow,
        flow_args={
            "s3_block": s3_bucket_block,
            "tn_block": tn_block,
            "descriptor_block": sql_descriptor_block,
            "deployment_state": sql_deployment_state,
        },
        verifications=[
            (partial(_verify_variable_state, expected_agg_date="2024-05-03", expected_ins_date="2024-05-03"), {}),
            (
                partial(
                    _verify_tn_records,
                    id_map=id_map_d1_d3,
                    expected_records=expected_records_d1_d3,
                    check_source_ids=list(ids_d1_d3),
                ),
                {"tn_block": tn_block},
            ),
        ],
    )

    # === Phase 5: Resumption Setup and Out-of-Order Insertion Test ===
    logger.info("--- Starting Phase 5: Resumption Setup & Out-of-Order Test ---")
    # 1. Define and upload new data (D4, D5)
    source_data_d4 = {"P007": ("2024-05-04", 76.00), "P008": ("2024-05-04", 88.88)}
    source_data_d5 = {"P001": ("2024-05-05", 11.50), "P009": ("2024-05-05", 9.99)}
    data_d4 = pd.DataFrame(
        [
            {
                "id_producto": k,
                "productos_descripcion": f"Product {k}",
                "productos_precio_lista_avg": v[1],
                "date": v[0],
            }
            for k, v in source_data_d4.items()
        ]
    )
    data_d5 = pd.DataFrame(
        [
            {
                "id_producto": k,
                "productos_descripcion": f"Product {k}",
                "productos_precio_lista_avg": v[1],
                "date": v[0],
            }
            for k, v in source_data_d5.items()
        ]
    )
    data_d4 = SepaAvgPriceProductModel.validate(data_d4)
    data_d5 = SepaAvgPriceProductModel.validate(data_d5)
    logger.info("Uploading resumption data (D4, D5) to S3...")
    upload_df_to_s3_csv_zip(s3_bucket_block, f"{s3_base_path}/2024-05-04/product_averages.zip", data_d4)
    upload_df_to_s3_csv_zip(s3_bucket_block, f"{s3_base_path}/2024-05-05/product_averages.zip", data_d5)
    logger.info("Resumption data uploaded.")

    # 2. Update Variables for Resumption Test
    logger.info("Updating variables for resumption test (PREPROCESS -> D5)...")
    await variables.Variable.aset(ArgentinaFlowVariableNames.LAST_PREPROCESS_SUCCESS_DATE, "2024-05-05")
    _ = await _verify_variable_state("Phase 5 (Skip Setup)", "2024-05-03", "2024-05-03")
    logger.info("Variables updated for resumption.")

    # 3/4. Attempt insertion flow again (expect skip) and verify
    await _run_flow_and_verify(
        phase_name="Phase 5 (Skip Attempt)",
        flow_func=insert_argentina_products_flow,
        flow_args={
            "s3_block": s3_bucket_block,
            "tn_block": tn_block,
            "descriptor_block": sql_descriptor_block,
            "deployment_state": sql_deployment_state,
        },
        verifications=[
            (
                partial(_verify_variable_state, expected_agg_date="2024-05-03", expected_ins_date="2024-05-03"),
                {},
            ),  # Unchanged
            (
                partial(
                    _verify_tn_records,
                    id_map=id_map_d1_d3,
                    expected_records=expected_records_d1_d3,
                    check_source_ids=list(ids_d1_d3),
                ),
                {"tn_block": tn_block},
            ),  # Unchanged
            (
                partial(
                    _verify_stream_existence, id_map=id_map_d1_d3, account=account, should_exist_ids=list(ids_d1_d3)
                ),
                {"tn_block": tn_block},
            ),  # P008/9 still shouldn't exist
        ],
    )

    # === Phase 6: Aggregation (Resumption) ===
    agg_results_resume = await _run_flow_and_verify(
        phase_name="Phase 6 (Resumption Agg)",
        flow_func=aggregate_argentina_products_flow,
        flow_args={"s3_block": s3_bucket_block, "descriptor_block": sql_descriptor_block, "force_reprocess": False},
        verifications=[
            (
                partial(_verify_variable_state, expected_agg_date="2024-05-05", expected_ins_date="2024-05-03"),
                {},
            ),  # Agg updated
            (
                partial(
                    _verify_db_state,
                    expected_total_rows=9,
                    expected_source_ids=ids_d1_d5,
                    expected_deployed_ids=ids_d1_d3,
                ),
                {"db_session": db_session},
            ),  # P008/9 added, not deployed
        ],
    )
    db_df_after_resumption_agg = agg_results_resume["_verify_db_state"]
    id_map_d1_d5 = db_df_after_resumption_agg.set_index("source_id")["stream_id"].to_dict()

    # === Phase 7: Deployment (Resumption) ===
    await _run_flow_and_verify(
        phase_name="Phase 7 (Resumption Deploy)",
        flow_func=deploy_streams_flow,
        flow_args={"psd_block": sql_descriptor_block, "tna_block": tn_block, "deployment_state": sql_deployment_state},
        verifications=[
            (
                partial(_verify_variable_state, expected_agg_date="2024-05-05", expected_ins_date="2024-05-03"),
                {},
            ),  # Unchanged
            (
                partial(
                    _verify_db_state,
                    expected_total_rows=9,
                    expected_source_ids=ids_d1_d5,
                    expected_deployed_ids=ids_d1_d5,
                ),
                {"db_session": db_session},
            ),  # P008/9 deployed
            (
                partial(
                    _verify_stream_existence, id_map=id_map_d1_d5, account=account, should_exist_ids=list(ids_d1_d5)
                ),
                {"tn_block": tn_block},
            ),  # P008/9 exist
        ],
    )

    # === Phase 8: Insertion (Resumption) ===
    final_source_data = {**source_data_d1, **source_data_d2, **source_data_d3, **source_data_d4, **source_data_d5}
    records_d1_d5_data = {}
    for pid, (date, price) in final_source_data.items():
        records_d1_d5_data.setdefault(pid, []).append((date, price))
    expected_records_d1_d5 = _generate_expected_tn_records(records_d1_d5_data)

    await _run_flow_and_verify(
        phase_name="Phase 8 (Resumption Insert)",
        flow_func=insert_argentina_products_flow,
        flow_args={
            "s3_block": s3_bucket_block,
            "tn_block": tn_block,
            "descriptor_block": sql_descriptor_block,
            "deployment_state": sql_deployment_state,
        },
        verifications=[
            (
                partial(_verify_variable_state, expected_agg_date="2024-05-05", expected_ins_date="2024-05-05"),
                {},
            ),  # Ins updated
            (
                partial(
                    _verify_tn_records,
                    id_map=id_map_d1_d5,
                    expected_records=expected_records_d1_d5,
                    check_source_ids=list(ids_d1_d5),
                ),
                {"tn_block": tn_block},
            ),  # All records D1-D5 present
        ],
    )

    # Phase 9: Cleanup (Handled by fixtures)
    logger.info("--- Test finished successfully ---")
