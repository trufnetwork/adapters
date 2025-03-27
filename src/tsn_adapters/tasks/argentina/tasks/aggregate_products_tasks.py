"""
Prefect tasks for loading Argentina SEPA product aggregation state from S3.

Handles state loading (metadata JSON, aggregated products CSV) including
validation and default creation if files are missing.
"""

import io
import json
from typing import Any, TypeVar

from botocore.exceptions import ClientError  # type: ignore
import pandas as pd
from pandera.errors import SchemaError, SchemaDefinitionError
from pandera.typing import DataFrame
from prefect import get_run_logger, task
from prefect_aws import S3Bucket  # type: ignore

from tsn_adapters.tasks.argentina.models.aggregate_products_models import (
    DynamicPrimitiveSourceModel,
    ProductAggregationMetadata,
)

T = TypeVar("T")


# --- State Loading Helper Functions ---

def _is_client_error_not_found(exception: ClientError) -> bool:
    """Checks if a botocore ClientError is an S3 'Not Found' (404 or NoSuchKey)."""
    # Safely access potentially untyped response dictionary elements
    error_dict: dict[str, Any] = exception.response
    error_code = error_dict.get("Error", {}).get("Code", "")
    status_code_str = error_dict.get("ResponseMetadata", {}).get("HTTPStatusCode")
    status_code = int(status_code_str) if status_code_str is not None else None
    return error_code == "NoSuchKey" or status_code == 404


async def _load_metadata_from_s3(
    s3_block: S3Bucket, metadata_path: str
) -> ProductAggregationMetadata:
    """
    Loads ProductAggregationMetadata from JSON in S3.

    Returns default metadata if the file is not found (404/NoSuchKey).

    Args:
        s3_block: S3Bucket block for S3 access.
        metadata_path: Path to the metadata JSON file in the bucket.

    Returns:
        Loaded or default ProductAggregationMetadata.

    Raises:
        ClientError: For non-404 S3 errors.
        json.JSONDecodeError: For invalid JSON.
        Exception: For other unexpected errors.
    """
    logger = get_run_logger()
    try:
        logger.info(f"Loading metadata: {metadata_path}")
        metadata_bytes = await s3_block.aread_path(metadata_path)
        metadata = ProductAggregationMetadata(**json.loads(metadata_bytes.decode("utf-8")))
        logger.info("Metadata loaded successfully.")
        return metadata
    except ClientError as e:
        if _is_client_error_not_found(e):
            default_metadata = ProductAggregationMetadata()
            logger.warning(f"Metadata file not found. Using defaults: {default_metadata}")
            return default_metadata
        else:
            logger.error(f"S3 Error loading metadata: {e}", exc_info=True)
            raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in metadata file: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading metadata: {e}", exc_info=True)
        raise


def _create_empty_aggregated_data() -> pd.DataFrame:
    """Creates an empty DataFrame matching DynamicPrimitiveSourceModel columns."""
    columns = list(DynamicPrimitiveSourceModel.to_schema().columns.keys())
    df = pd.DataFrame(columns=columns)
    # Attempt setting dtypes on the empty DataFrame based on the model
    for col, props in DynamicPrimitiveSourceModel.to_schema().columns.items():
        if col in df.columns and props.dtype:
            try:
                df[col] = df[col].astype(props.dtype.type) # type: ignore
            except Exception:
                df[col] = df[col].astype(object) # type: ignore
    return df


async def _load_data_from_s3(
    s3_block: S3Bucket, data_path: str
) -> DataFrame[DynamicPrimitiveSourceModel]:
    """
    Loads and validates aggregated product data from a gzipped CSV in S3.

    Returns an empty, validated DataFrame if the file is not found.
    Validates loaded data using Pandera, passing `lazy=True` as required by model config.

    Args:
        s3_block: S3Bucket block for S3 access.
        data_path: Path to the gzipped CSV data file in the bucket.

    Returns:
        Validated DataFrame matching DynamicPrimitiveSourceModel (can be empty).

    Raises:
        ClientError: For non-404 S3 errors.
        SchemaError, SchemaDefinitionError: For Pandera validation issues.
        pd.errors.ParserError: For malformed CSV.
        Exception: For other unexpected errors.
    """
    logger = get_run_logger()
    try:
        logger.info(f"Loading aggregated data: {data_path}")
        data_bytes = await s3_block.aread_path(data_path)
        data_buffer = io.BytesIO(data_bytes)

        # Prepare dtypes based on Pandera model for robust parsing
        dtypes = {
            col: props.dtype.type if props.dtype else object
            for col, props in DynamicPrimitiveSourceModel.to_schema().columns.items()
        }
        for col in DynamicPrimitiveSourceModel.to_schema().columns.keys():
             if col not in dtypes: dtypes[col] = object

        # Note: type checker might flag read_csv with buffer due to complex overloads
        loaded_df: pd.DataFrame = pd.read_csv( # type: ignore[call-overload]
            data_buffer,
            compression="gzip",
            dtype=dtypes,
            keep_default_na=False,
            na_values=[""]
        )

        logger.info("Validating loaded data...")
        # Validate with lazy=True (required as model uses strict="filter")
        validated_df = DynamicPrimitiveSourceModel.validate(loaded_df, lazy=True)
        logger.info(f"Data loaded and validated ({len(validated_df)} records).")
        return validated_df

    except ClientError as e:
        if _is_client_error_not_found(e):
            logger.warning(f"Data file not found. Returning empty DataFrame.")
            empty_df = _create_empty_aggregated_data()
            try:
                # Must validate empty DF with lazy=True due to model config
                return DynamicPrimitiveSourceModel.validate(empty_df, lazy=True)
            except SchemaDefinitionError as sde:
                 logger.error(f"Pandera definition error validating empty DF: {sde}", exc_info=True)
                 raise sde # Indicates a code/model definition issue
        else:
            logger.error(f"S3 Error loading data: {e}", exc_info=True)
            raise
    except (SchemaError, SchemaDefinitionError) as e:
        logger.error(f"Pandera validation failed for data: {e}", exc_info=True)
        raise
    except pd.errors.ParserError as e:
        logger.error(f"CSV parsing error for data: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error loading/validating data: {e}", exc_info=True)
        raise


# --- Main Aggregation Task ---

@task(name="Load Aggregation State from S3")
async def load_aggregation_state(
    s3_block: S3Bucket, base_path: str = "aggregated"
) -> tuple[DataFrame[DynamicPrimitiveSourceModel], ProductAggregationMetadata]:
    """
    Loads aggregation state (metadata and data) from S3.

    Orchestrates calls to helper functions for loading and validation.

    Args:
        s3_block: Configured Prefect S3Bucket block.
        base_path: Base S3 directory path for aggregation files.

    Returns:
        Tuple: (validated product DataFrame, loaded metadata).

    Raises:
        Exception: Propagated from helper functions on failure.
    """
    logger = get_run_logger()
    logger.info(f"Starting state load from S3 path: {base_path}")

    metadata_path = f"{base_path}/argentina_products_metadata.json"
    data_path = f"{base_path}/argentina_products.csv.gz"

    # Load metadata (or defaults)
    metadata = await _load_metadata_from_s3(s3_block, metadata_path)

    # Load data (or empty) and validate
    aggregated_data = await _load_data_from_s3(s3_block, data_path)

    logger.info("Aggregation state load completed.")
    return aggregated_data, metadata
