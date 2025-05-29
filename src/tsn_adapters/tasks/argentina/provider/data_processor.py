"""
Shared utilities for SEPA data processing.
"""

from collections.abc import Generator
import os
import tempfile
from typing import Iterator, cast

import pandas as pd
from prefect.concurrency.sync import concurrency

from tsn_adapters.tasks.argentina.types import DateStr, SepaDF
from tsn_adapters.tasks.argentina.utils.processors import SepaDirectoryProcessor


class DatesNotMatchError(Exception):
    """Exception raised when the date does not match."""

    def __init__(self, real_date: DateStr, reported_date: DateStr, source: str):
        """Initialize the exception."""
        self.real_date = real_date
        self.reported_date = reported_date
        self.source = source
        super().__init__(f"Real date {real_date} does not match {source} date {reported_date}")


def process_sepa_zip_streaming(
    zip_reader: Generator[bytes, None, None],
    reported_date: DateStr,
    source_name: str,
    chunk_size: int = 100000,
) -> Iterator[SepaDF]:
    """
    Process SEPA data from a ZIP file in chunks for memory efficiency.

    Args:
        zip_reader: Generator yielding zip file bytes
        reported_date: The date reported by the source
        source_name: Name of the source (for error messages)
        chunk_size: Number of rows per chunk

    Yields:
        SepaDF chunks of the specified size

    Raises:
        DatesNotMatchError: If the date in the data doesn't match the reported date
        ValueError: If the data is invalid
    """
    # Create a temporary directory for extraction
    with tempfile.TemporaryDirectory() as temp_dir:
        # Download the zip file
        # ~ 200 MB - TODO: Optimize this to stream extraction
        with concurrency("network-usage", 200):
            zip_content = b"".join(zip_reader)

        # Create a temporary file for the zip
        with tempfile.NamedTemporaryFile(suffix=".zip", dir=temp_dir, delete=False) as temp_zip:
            temp_zip.write(zip_content)
            temp_zip_path = temp_zip.name

        # Process the data in chunks
        extract_dir = os.path.join(temp_dir, "data")
        os.makedirs(extract_dir, exist_ok=True)
        processor = SepaDirectoryProcessor.from_zip_path(temp_zip_path, extract_dir)

        # Get data iterator instead of merged data
        first_chunk = True
        total_yielded = 0

        for store_data in processor.get_products_data():
            if store_data.empty:
                continue

            # Validate date on first chunk only
            if first_chunk:
                real_date = store_data["date"].iloc[0]
                if reported_date != real_date:
                    raise DatesNotMatchError(real_date, reported_date, source_name)
                first_chunk = False

            # Yield this store's data in chunks
            for i in range(0, len(store_data), chunk_size):
                chunk = store_data.iloc[i:i + chunk_size].copy()
                total_yielded += len(chunk)
                yield chunk

        if total_yielded == 0:
            # Return empty chunk if no data
            yield cast(SepaDF, pd.DataFrame())


def process_sepa_zip(
    zip_reader: Generator[bytes, None, None],
    reported_date: DateStr,
    source_name: str,
) -> SepaDF:
    """
    Process SEPA data from a data item - DEPRECATED: Use process_sepa_zip_streaming for better memory efficiency.

    Args:
        zip_reader: Generator yielding zip file bytes
        source_name: Name of the source (for error messages)
        reported_date: The date reported by the source

    Returns:
        DataFrame: The processed SEPA data

    Raises:
        DatesNotMatchError: If the date in the data doesn't match the reported date
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
        processor = SepaDirectoryProcessor.from_zip_path(temp_zip_path, extract_dir)
        df = processor.get_all_products_data_merged()

        # skip empty dataframes
        if df.empty:
            return cast(SepaDF, pd.DataFrame())

        # Validate the date matches
        real_date = df["date"].iloc[0]
        if reported_date != real_date:
            # we need to raise an error so cache is invalidated
            raise DatesNotMatchError(real_date, reported_date, source_name)

        return df
