"""
Tasks related to loading and handling product descriptors for Argentina SEPA data.
"""

from pandera.typing import DataFrame
from prefect import task

from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel, S3SourceDescriptor
from tsn_adapters.utils.logging import get_logger_safe


class DescriptorError(ValueError):
    """Custom exception for descriptor loading failures."""
    pass


@task(name="Load Product Descriptor")
async def load_product_descriptor(
    descriptor_block: S3SourceDescriptor,
) -> DataFrame[PrimitiveSourceDataModel]:
    """
    Loads the aggregated product descriptor using the provided S3SourceDescriptor block.

    This task wraps the block's get_descriptor method and enforces that a failure
    to load a non-empty, valid descriptor is treated as a fatal error for the flow.

    Args:
        descriptor_block: Configured S3SourceDescriptor block instance pointing to the descriptor file.

    Returns:
        A validated DataFrame conforming to PrimitiveSourceDataModel.

    Raises:
        DescriptorError: If the descriptor cannot be loaded, is empty, or fails validation within the block.
    """
    logger = get_logger_safe(__name__)
    logger.info(f"Attempting to load product descriptor from block: {descriptor_block.file_path}")

    try:
        # The get_descriptor method handles reading, decompression, parsing, and basic validation
        descriptor_df: DataFrame[PrimitiveSourceDataModel] = descriptor_block.get_descriptor()

        if descriptor_df.empty:
            msg = f"Product descriptor loaded from {descriptor_block.file_path} is empty. This is treated as a fatal error."
            logger.error(msg)
            raise DescriptorError(msg)

        logger.info(f"Successfully loaded and validated product descriptor with {len(descriptor_df)} entries.")
        return descriptor_df

    except Exception as e:
        # Catch exceptions from get_descriptor (e.g., S3 issues, parsing errors, validation errors)
        # or the empty check above.
        msg = f"Failed to load or validate product descriptor from {descriptor_block.file_path}: {e}"
        logger.error(msg, exc_info=True)
        # Re-raise as DescriptorError to signal fatal flow failure clearly
        raise DescriptorError(msg) from e 