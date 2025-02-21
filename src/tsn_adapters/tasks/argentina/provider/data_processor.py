"""
Shared utilities for SEPA data processing.
"""

from collections.abc import Generator
import os
import tempfile
from typing import cast

from prefect.concurrency.sync import concurrency

from tsn_adapters.tasks.argentina.errors.errors import DateMismatchError, InvalidStructureZIPError
from tsn_adapters.tasks.argentina.types import DateStr, SepaDF
from tsn_adapters.tasks.argentina.utils.processors import SepaDirectoryProcessor


def process_sepa_zip(
    zip_reader: Generator[bytes, None, None],
    reported_date: DateStr,
    source_name: str,
) -> SepaDF:
    """
    Process SEPA data from a data item.

    Args:
        zip_reader: Generator yielding bytes of the ZIP file
        source_name: Name of the source (for error messages)
        reported_date: The date reported by the source

    Returns:
        DataFrame: The processed SEPA data

    Raises:
        DateMismatchError: If the date in the data doesn't match the reported date
        InvalidStructureZIPError: If the ZIP file structure is invalid
        ValueError: If the data is invalid
    """
    # Create a temporary directory for extraction
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download the zip file
        # ~ 200 MB
        with concurrency("network-usage", 200):
            zip_content = b"".join(zip_reader)

        # Create a temporary file for the zip
        with tempfile.NamedTemporaryFile(suffix=".zip", dir=temp_dir, delete=False) as temp_zip:
            temp_zip.write(zip_content)
            temp_zip_path = temp_zip.name

        # Process the data
        extract_dir = os.path.join(temp_dir, "data")
        os.makedirs(extract_dir, exist_ok=True)
        try:
            processor = SepaDirectoryProcessor.from_zip_path(temp_zip_path, extract_dir)
            data = processor.get_all_products_data_merged()

            # Validate date matches
            if not data.empty:
                data_date: str = cast(str, data["date"].iloc[0])
                if str(data_date) != str(reported_date):
                    raise DateMismatchError(internal_date=data_date)

            return data
        except Exception as e:
            if isinstance(e, DateMismatchError):
                raise
            raise InvalidStructureZIPError(str(e)) from e
