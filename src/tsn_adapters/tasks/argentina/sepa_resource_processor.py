from __future__ import annotations

import os
import re
import tempfile
from typing import Iterator, List, Optional
import pandas as pd
from io import StringIO
from pydantic import BaseModel, field_validator
from pandera.typing import DataFrame

from .models.sepa_models import (
    SepaProductosDataModel,
)
from .utils.archives import extract_zip


def extract_sepa_data(zip_path: str, extract_path: str) -> SepaDirectoryProcessor:
    """
    Extracts SEPA data from a ZIP file and returns a SepaDirectoryProcessor.
    """
    extract_zip(zip_path, extract_path, overwrite=True)
    return SepaDirectoryProcessor(extract_path)


class SepaDirectoryProcessor:
    """
    Processes extracted SEPA data directories, providing access to various data formats.
    """
    def __init__(self, dir_path: str):
        self.dir_path = dir_path

    def get_data_dirs(self) -> Iterator[SepaDataDirectory]:
        """
        Iterates over the data directories within the main directory.
        """
        for dir_name in os.listdir(self.dir_path):
            dir_path = os.path.join(self.dir_path, dir_name)
            if os.path.isdir(dir_path):
                try:
                    yield SepaDataDirectory.from_dir_path(dir_path)
                except ValueError as e:
                    print(f"Skipping invalid directory {dir_path}: {e}")

    def get_all_data_dirs(self) -> List[SepaDataDirectory]:
        """Returns a list of all valid data directories."""
        return list(self.get_data_dirs())

    def get_date(self) -> str:
        """Returns the date associated with the data."""
        data_dirs = list(self.get_data_dirs())
        if not data_dirs:
            raise ValueError("No valid data directories found.")
        return data_dirs[0].date

    def get_products_data(self) -> Iterator[DataFrame[SepaProductosDataModel]]:
        """
        Iterates over product data DataFrames from each data directory.
        """
        for data_dir in self.get_data_dirs():
            try:
                yield data_dir.load_products_data()
            except Exception as e:
                print(f"Error loading products data from {data_dir.dir_path}: {e}")

    def get_all_products_data_merged(self) -> DataFrame[SepaProductosDataModel]:
        """
        Loads and merges all product data into a single DataFrame.
        """
        all_data = [data for data in self.get_products_data()]
        if not all_data:
            return DataFrame[SepaProductosDataModel](pd.DataFrame())  # Return empty DataFrame if no data

        merged_df = pd.concat(all_data, ignore_index=True)
        return DataFrame[SepaProductosDataModel](merged_df)

    @staticmethod
    def from_zip_path(zip_path: str, extract_path: Optional[str] = None) -> "SepaDirectoryProcessor":
        """
        Creates a SepaDirectoryProcessor instance from a ZIP file path.
        """
        if extract_path is None:
            extract_path = tempfile.mkdtemp()
        return extract_sepa_data(zip_path, extract_path)


class SepaDataDirectory(BaseModel):
    """
    Represents a single directory containing SEPA data for a specific store/date.
    """
    id_bandera: str
    id_comercio: str
    date: str
    dir_path: str

    @field_validator("date")
    @classmethod
    def validate_date(cls, v: str) -> str:
        if not re.match(r"\d{4}-\d{2}-\d{2}", v):
            raise ValueError(f"Invalid date format: {v}")
        return v

    @staticmethod
    def from_dir_path(dir_path: str) -> "SepaDataDirectory":
        """Creates a SepaDataDirectory instance from a directory path."""
        regex = r"sepa_(\d+)_comercio-sepa-(\d+)_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})"
        file_name = os.path.basename(dir_path)

        match = re.match(regex, file_name)
        if not match:
            raise ValueError(f"Invalid directory name: {file_name}")

        id_bandera, id_comercio, date, _ = match.groups()

        return SepaDataDirectory(
            id_bandera=id_bandera,
            id_comercio=id_comercio,
            date=date,
            dir_path=dir_path
        )

    def load_products_data(self) -> DataFrame[SepaProductosDataModel]:
        """
        Loads product data from the 'productos.csv' file in the directory.
        """
        file_path = os.path.join(self.dir_path, "productos.csv")

        def read_file_iterator():
            with open(file_path, "r") as file:
                for line in file:
                    if "|" in line:
                        yield line.strip()

        text_lines = list(read_file_iterator())
        text_content = "\n".join(text_lines)
        content_io = StringIO(text_content)

        df = pd.read_csv(content_io, skip_blank_lines=True, sep="|")
        df["date"] = self.date
        return DataFrame[SepaProductosDataModel](df)
