"""
Models for product-to-category mapping in the SEPA dataset.

This module contains the Pandera model for validating and typing the mapping between
products and their categories in the SEPA dataset.
"""

import pandas as pd
from pandera.typing import DataFrame, Series
from pandera import DataFrameModel
from pandas._typing import CompressionOptions

from .sepa_models import SepaProductosDataModel


class SepaProductCategoryMapModel(DataFrameModel):
    """
    Pandera model for the product-to-category mapping data.

    This model validates the structure of DataFrames containing mappings between
    SEPA products and their corresponding categories.

    Attributes
    ----------
    id_producto : Series[str]
        Unique identifier for each product
    productos_descripcion : Series[str]
        Description of the product
    category_id : Series[str]
        Identifier for the category this product belongs to
    category_name : Series[str]
        Human-readable name of the category
    """
    id_producto: Series[str]
    productos_descripcion: Series[str]
    category_id: Series[str]
    category_name: Series[str]

    @staticmethod
    def from_url(url: str, sep: str = '|', compression: CompressionOptions = None) -> DataFrame["SepaProductCategoryMapModel"]:
        """
        Create a validated DataFrame from a URL source.

        Parameters
        ----------
        url : str
            URL pointing to the CSV data source
        sep : str, optional
            Delimiter to use for the CSV file, by default '|', because some product ids contains commas
        compression : CompressionOptions, optional
            Compression type of the file, by default None

        Returns
        -------
        DataFrame[SepaProductCategoryMapModel]
            A validated DataFrame containing the product-category mapping
        """
        df = pd.read_csv(url, compression=compression, sep=sep)
        return DataFrame[SepaProductCategoryMapModel](df)

    
    class Config:
        strict = True
        coerce = True 


def get_uncategorized_products(data: DataFrame[SepaProductosDataModel], category_map: DataFrame[SepaProductCategoryMapModel]) -> DataFrame[SepaProductosDataModel]:
    """
    Get the products without category
    """
    diff_df = data[~data["id_producto"].isin(category_map["id_producto"])]
    return DataFrame[SepaProductosDataModel](diff_df)
