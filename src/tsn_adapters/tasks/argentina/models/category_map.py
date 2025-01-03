"""
Models for product-to-category mapping in the SEPA dataset.

This module contains the Pandera model for validating and typing the mapping between
products and their categories in the SEPA dataset.
"""

from datetime import timedelta
import pandas as pd
import pandera as pa
from pandera.typing import DataFrame, Series
from pandera import DataFrameModel
from pandas._typing import CompressionOptions
from prefect import task
from prefect.tasks import task_input_hash

from tsn_adapters.utils.filter_failures import filter_failures
from .sepa_models import SepaProductosDataModel
import tempfile
import requests
import os
from typing import TypeVar, Type

U = TypeVar("U", bound=pa.DataFrameModel)

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

    @classmethod
    def from_url(cls: Type[U], url: str, sep: str = '|', compression: CompressionOptions = None) -> DataFrame[U]:
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

        Raises
        ------
        ValueError
            If the file is empty, corrupted, or doesn't contain the expected columns
        requests.RequestException
            If there's an error downloading the file
        """
        required_columns = {"id_producto", "productos_descripcion", "category_id", "category_name"}
        
        # Create a temporary directory to store the downloaded file
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = os.path.join(temp_dir, "category_map.zip")
            
            # Download the file with proper headers
            try:
                response = requests.get(url, stream=True, allow_redirects=True)
                response.raise_for_status()
            except requests.RequestException as e:
                raise ValueError(f"Failed to download category map from {url}: {str(e)}")
            
            # Save the content to a temporary file
            try:
                with open(temp_file, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            except IOError as e:
                raise ValueError(f"Failed to save category map file: {str(e)}")
            
            # Read the CSV from the zip file
            try:
                df = pd.read_csv(temp_file, compression=compression, sep=sep)
            except Exception as e:
                raise ValueError(f"Failed to read category map as CSV: {str(e)}")
            
            # Verify the DataFrame is not empty and has required columns
            if df.empty:
                raise ValueError("The category map file is empty")
            
            missing_columns = required_columns - set(df.columns)
            if missing_columns:
                raise ValueError(f"Category map is missing required columns: {missing_columns}")
            
            # Verify we have actual data in required columns
            null_counts = df[list(required_columns)].isnull().sum()
            if null_counts.any():
                columns_with_nulls = null_counts[null_counts > 0].index.tolist()
                raise ValueError(f"Category map contains null values in columns: {columns_with_nulls}")
            
            # Verify we have at least one valid category
            unique_categories = df['category_id'].nunique()
            if unique_categories == 0:
                raise ValueError("Category map contains no valid categories")
                
            df = filter_failures(df, cls)
                
            return df

    # --- TASKIFIED METHODS ---
    @staticmethod
    @task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
    def task_from_url(url: str, sep: str = '|', compression: CompressionOptions = None) -> DataFrame["SepaProductCategoryMapModel"]:
        return SepaProductCategoryMapModel.from_url(url, sep, compression)

    class Config(pa.DataFrameModel.Config):
        strict = True
        coerce = True


def get_uncategorized_products(data: DataFrame[SepaProductosDataModel], category_map: DataFrame[SepaProductCategoryMapModel]) -> DataFrame[SepaProductosDataModel]:
    """
    Get the products without category
    """
    diff_df = data[~data["id_producto"].isin(category_map["id_producto"])]

    # get data without id_producto (=null)
    null_df = data[data["id_producto"].isnull()]
    
    return DataFrame[SepaProductosDataModel](diff_df)