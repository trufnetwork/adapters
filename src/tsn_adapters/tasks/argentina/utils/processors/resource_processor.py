from __future__ import annotations

from collections.abc import Iterator
import logging
import os
from pathlib import Path
import re
import tempfile
from typing import Any, ClassVar

import pandas as pd
from pandera.typing import DataFrame
from pydantic import BaseModel, field_validator

from tsn_adapters.tasks.argentina.errors.accumulator import ErrorAccumulator
from tsn_adapters.tasks.argentina.errors.errors import (
    InvalidCSVSchemaError,
    InvalidDateFormatError,
    MissingProductosCSVError,
)
from tsn_adapters.tasks.argentina.models.sepa.sepa_models import (
    SepaProductosAlternativeModel,
    SepaProductosDataModel,
    SepaProductosDataModelAlt1,
)
from tsn_adapters.tasks.argentina.utils.archives import extract_zip
from tsn_adapters.utils.logging import get_logger_safe


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
        self.logger = get_logger_safe(__name__)

    def get_data_dirs(self) -> Iterator[SepaDataDirectory]:
        """
        Iterates over the data directories within the main directory using pathlib.
        """
        root_path = Path(self.dir_path)
        sepa_dirs = root_path.glob("**/sepa_*_comercio-sepa-*_????-??-??_??-??-??")

        for sepa_dir in sepa_dirs:
            if sepa_dir.is_dir():
                try:
                    yield SepaDataDirectory.from_dir_path(str(sepa_dir))
                except ValueError as e:
                    self.logger.warning(f"Skipping invalid directory {sepa_dir}: {e}")

        sepa_zips = root_path.glob("**/sepa_*_comercio-sepa-*_????-??-??_??-??-??.zip")

        for sepa_zip in sepa_zips:
            if sepa_zip.is_file():
                try:
                    yield SepaDataDirectory.from_zip_path(str(sepa_zip))
                except ValueError as e:
                    self.logger.warning(f"Skipping invalid zip file {sepa_zip}: {e}")

    def get_all_data_dirs(self) -> list[SepaDataDirectory]:
        """Returns a list of all valid data directories."""
        dirs = list(self.get_data_dirs())
        if not dirs:
            self.logger.warning("No valid data directories found in the workspace")
        return dirs

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
            except MissingProductosCSVError as e:
                self.logger.error(f"Error loading products data from {data_dir.dir_path}: {e}")
                raise
            except Exception as e:
                self.logger.error(f"Error loading products data from {data_dir.dir_path}: {e}")

    def get_all_products_data_merged(self) -> DataFrame[SepaProductosDataModel]:
        """
        Loads and merges all product data into a single DataFrame.
        """
        all_data = list(self.get_products_data())
        if not all_data:
            self.logger.warning("No product data found in any directory")
            empty_df = pd.DataFrame(
                {
                    "id_producto": [],
                    "productos_descripcion": [],
                    "productos_precio_lista": [],
                    "date": [],
                }
            )
            return DataFrame[SepaProductosDataModel](empty_df)

        merged_df = pd.concat(all_data, ignore_index=True)
        self.logger.info(f"Successfully merged {len(all_data)} product data frames with {len(merged_df)} total rows")
        return DataFrame[SepaProductosDataModel](merged_df)

    @staticmethod
    def from_zip_path(zip_path: str, extract_path: str | None = None) -> SepaDirectoryProcessor:
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
    logger: ClassVar[logging.Logger] = get_logger_safe(__name__)

    @field_validator("date")
    @classmethod
    def validate_date(cls, v: str) -> str:
        if not re.match(r"\d{4}-\d{2}-\d{2}", v):
            raise InvalidDateFormatError(v)
        return v

    @staticmethod
    def from_dir_path(dir_path: str) -> SepaDataDirectory:
        """Creates a SepaDataDirectory instance from a directory path."""
        regex = r"sepa_(\d+)_comercio-sepa-(\d+)_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})"
        file_name = os.path.basename(dir_path)

        match = re.match(regex, file_name)
        if not match:
            raise ValueError(
                f"Invalid directory name: {file_name}. "
                f"Expected format: sepa_[bandera-id]_comercio-sepa-[comercio-id]_[date]_[time]. "
                f"Example: sepa_1_comercio-sepa-1_2024-01-04_00-00-00"
            )

        id_bandera, id_comercio, date, _ = match.groups()

        return SepaDataDirectory(id_bandera=id_bandera, id_comercio=id_comercio, date=date, dir_path=dir_path)

    @staticmethod
    def from_zip_path(zip_path: str) -> SepaDataDirectory:
        """
        Creates a SepaDataDirectory instance from a ZIP file path.
        """
        zip_name = os.path.basename(zip_path)
        zip_without_extension = os.path.splitext(zip_name)[0]
        zip_root_path = os.path.dirname(zip_path)
        extract_path = os.path.join(zip_root_path, zip_without_extension)

        extract_zip(zip_path, extract_path, overwrite=True)

        # delete the zip file
        os.remove(zip_path)

        return SepaDataDirectory.from_dir_path(extract_path)

    def load_products_data(self) -> DataFrame[SepaProductosDataModel]:
        """
        Loads product data from the 'productos.csv' file in the directory.
        """
        # file name might be:
        product_names = [
            "productos.csv",
            "Productos.csv",
        ]
        # find the file name
        # list the files in the directory
        files = os.listdir(self.dir_path)
        for file in files:
            if file in product_names:
                file_path = os.path.join(self.dir_path, file)
                break
        else:
            self.logger.error(f"No valid product file found in {self.dir_path}")
            raise MissingProductosCSVError(directory=self.dir_path, available_files=files)

        def read_file_iterator():
            with open(file_path) as file:
                for line in file:
                    if "|" in line:
                        yield line.strip()

        text_lines = list(read_file_iterator())
        if not text_lines:
            self.logger.warning(f"Empty product file found in {self.dir_path}")
            empty_df = pd.DataFrame(
                {
                    "id_producto": [],
                    "productos_descripcion": [],
                    "productos_precio_lista": [],
                    "date": [],
                }
            )
            return DataFrame[SepaProductosDataModel](empty_df)

        # models might be
        models: list[type[SepaProductosAlternativeModel]] = [
            SepaProductosDataModel,
            SepaProductosDataModelAlt1,
        ]

        for model in models:
            if model.has_columns(text_lines[0]):
                try:
                    # Parse and convert the data
                    raw_df: DataFrame[Any] = model.from_csv(self.date, text_lines, self.logger)
                    if model != SepaProductosDataModel:
                        self.logger.info(f"Converting {model} to core model")
                        return model.to_core_model(raw_df)
                    return raw_df
                except Exception as e:
                    self.logger.error(f"Failed to parse CSV with model {model}: {e}")
                    ErrorAccumulator.get_or_create_from_context().add_error(InvalidCSVSchemaError(error=str(e)))
                    continue

        raise InvalidCSVSchemaError(error="No valid model found")
