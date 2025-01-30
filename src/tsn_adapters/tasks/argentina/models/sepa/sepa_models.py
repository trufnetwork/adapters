from abc import ABC, abstractmethod
from datetime import datetime
from io import StringIO
from logging import Logger
from typing import TypeVar, cast

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series
from pydantic import BaseModel, field_validator

from tsn_adapters.utils.filter_failures import filter_failures

# Create type variables for the models
T = TypeVar("T", bound="ProductDescriptionModel")
S = TypeVar("S", bound="SepaProductosDataModel")


class SepaProductosAlternativeModel(type(BaseModel), pa.DataFrameModel):
    """
    Alternative model for SEPA productos data.
    """

    @classmethod
    @abstractmethod
    def get_differentiation_columns(cls) -> list[str]:
        """
        The columns that differentiate the alternative model from the core model.
        """
        raise NotImplementedError

    @classmethod
    def to_core_model(cls, df: DataFrame[T]) -> DataFrame["SepaProductosDataModel"]:
        """
        Convert the alternative model to the core model.
        """
        raise NotImplementedError

    @classmethod
    def has_columns(cls, header: str) -> bool:
        """
        Check if the header has the columns of the current class.
        """
        return all(column in header for column in cls.get_differentiation_columns())

    @classmethod
    def from_csv(
        cls,
        date: str,
        lines: list[str],
        logger: Logger,
    ) -> DataFrame[T]:
        """
        Read the CSV file and return the core model.
        """

        text_content = "\n".join(lines)
        content_io = StringIO(text_content)

        try:
            df = pd.read_csv(
                content_io,
                skip_blank_lines=True,
                sep="|",
                usecols=cls.get_differentiation_columns(),
            )
        except Exception as e:
            # if the error is about missing columns, print first 2 lines
            if "Usecols do not match columns, columns expected but not found" in str(e):
                logger.error(f"First 2 lines of {content_io}:\n{lines[:2]}")

            raise e

        df["date"] = date
        original_len = len(df)
        df = filter_failures(df, cls)
        if len(df) < original_len:
            logger.warning(f"Filtered out {original_len - len(df)} invalid rows")
        return cast(DataFrame[T], df)


class SepaProductosDataModel(SepaProductosAlternativeModel):
    """
    Pandera model for core SEPA productos data.
    """

    id_producto: Series[str]
    productos_descripcion: Series[str]
    productos_precio_lista: Series[float]
    date: Series[str]

    class Config(pa.DataFrameModel.Config):
        coerce = True
        strict = "filter"

    @classmethod
    def get_differentiation_columns(cls) -> list[str]:
        return ["productos_precio_lista", "productos_descripcion", "id_producto"]

    @classmethod
    def to_core_model(cls, df: DataFrame[T]) -> DataFrame["SepaProductosDataModel"]:
        """
        Convert the alternative model to the core model.
        """
        return cast(DataFrame[SepaProductosDataModel], df)


# ALTERNATIVE MODELS


class SepaProductosDataModelAlt1(SepaProductosAlternativeModel):
    """
    Alternative model for SEPA productos data.
    """

    id_producto: Series[str]
    productos_descripcion: Series[str]
    precio_unitario_bulto_por_unidad_venta_con_iva: Series[float]
    date: Series[str]

    class Config(SepaProductosDataModel.Config):
        strict = True
        coerce = True
        add_missing_columns = False

    @classmethod
    def get_differentiation_columns(cls) -> list[str]:
        return ["precio_unitario_bulto_por_unidad_venta_con_iva", "productos_descripcion", "id_producto"]

    @classmethod
    def to_core_model(cls, df: DataFrame[T]) -> DataFrame["SepaProductosDataModel"]:
        new_df = pd.DataFrame(
            {
                "id_producto": df.id_producto,
                "productos_descripcion": df.productos_descripcion,
                "productos_precio_lista": df.precio_unitario_bulto_por_unidad_venta_con_iva,
                "date": df.date,
            }
        )
        return filter_failures(new_df, SepaProductosDataModel)


# Other Models


class ProductDescriptionModel(pa.DataFrameModel):
    """
    Pandera model for unique product descriptions.
    """

    id_producto: Series[str]
    productos_descripcion: Series[str]

    class Config(pa.DataFrameModel.Config):
        coerce = True
        strict = "filter"

    @classmethod
    def from_sepa_product_data(
        cls: type[T],
        data: DataFrame[SepaProductosDataModel],
    ) -> DataFrame[T]:
        unique_data = data.drop_duplicates(subset="id_producto").reset_index(drop=True)
        original = unique_data[["id_producto", "productos_descripcion"]]
        df = filter_failures(original, cls)

        return df


class SepaAvgPriceProductModel(ProductDescriptionModel):
    """
    Pandera model for product descriptions with average prices.
    """

    productos_precio_lista_avg: Series[float]
    date: Series[str]

    class Config(ProductDescriptionModel.Config):
        strict = "filter"
        coerce = True

    @classmethod
    def from_sepa_product_data(
        cls: type[T],
        data: DataFrame[SepaProductosDataModel],
    ) -> DataFrame[T]:
        with_average_price = (
            data.groupby(["id_producto", "date"])
            .agg(
                {
                    "id_producto": "first",
                    "productos_precio_lista": "mean",
                    "productos_descripcion": "first",
                    "date": "first",
                }
            )
            .reset_index(drop=True)
        )

        with_average_price.rename(
            columns={
                "productos_precio_lista": "productos_precio_lista_avg",
            },
            inplace=True,
        )

        df = filter_failures(with_average_price, cls)

        return df


class SepaDataItem(ABC, BaseModel):
    """
    Generic data item from the dataset.

    This represet a fetchable unit of data. E.g. a file in S3, a file in the website, etc.
    """

    item_reported_date: str

    @field_validator("item_reported_date")
    @classmethod
    def _validate_date(cls, v: str) -> str:
        # Strict check for YYYY-MM-DD
        datetime.strptime(v, "%Y-%m-%d")
        return v

    @abstractmethod
    def fetch_into_memory(self) -> bytes:
        """
        Fetch the data into memory.
        """
