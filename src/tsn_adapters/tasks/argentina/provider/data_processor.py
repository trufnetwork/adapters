"""
Shared utilities for SEPA data processing.
"""

import os
import tempfile
from typing import cast

import pandas as pd

from tsn_adapters.tasks.argentina.models.sepa import SepaDataItem
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


def process_sepa_data(
    data_item: SepaDataItem,
    source_name: str,
) -> SepaDF:
    """
    Process SEPA data from a data item.

    Args:
        data_item: The data item to process
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
        zip_content = data_item.fetch_into_memory()

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
        if data_item.item_reported_date != real_date:
            # we need to raise an error so cache is invalidated
            raise DatesNotMatchError(real_date, DateStr(data_item.item_reported_date), source_name)

        return cast(SepaDF, df)
