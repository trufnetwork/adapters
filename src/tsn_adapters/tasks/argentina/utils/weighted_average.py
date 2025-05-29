"""
Utility functions for weighted average calculations.

This module provides functions to combine pre-aggregated data using weighted averages,
enabling memory-efficient processing of large datasets.
"""

from typing import Iterator, List

import pandas as pd
from pandera.typing import DataFrame

from tsn_adapters.tasks.argentina.models.sepa.sepa_models import (
    SepaProductosDataModel,
    SepaWeightedAvgPriceProductModel,
)
from tsn_adapters.utils.logging import get_logger_safe

logger = get_logger_safe(__name__)


def combine_weighted_averages(
    dataframes: List[DataFrame[SepaWeightedAvgPriceProductModel]]
) -> DataFrame[SepaWeightedAvgPriceProductModel]:
    """
    Combine multiple weighted average DataFrames into a single weighted average.
    
    This function is useful when you need to combine pre-aggregated data from multiple
    sources (e.g., different dates or batches) while maintaining accurate averages.
    
    Args:
        dataframes: List of DataFrames with weighted averages and counts
        
    Returns:
        Combined DataFrame with properly weighted averages
        
    Example:
        If you have two batches:
        - Batch 1: Product A with avg=10, count=5 (total=50)
        - Batch 2: Product A with avg=20, count=5 (total=100)
        Result: Product A with avg=15, count=10 (total=150)
    """
    if not dataframes:
        logger.warning("No dataframes provided for combination")
        return DataFrame[SepaWeightedAvgPriceProductModel](
            pd.DataFrame(columns=list(SepaWeightedAvgPriceProductModel.to_schema().columns.keys()))
        )
    
    if len(dataframes) == 1:
        return dataframes[0]
    
    # Concatenate all dataframes
    combined = pd.concat(dataframes, ignore_index=True)
    
    # Calculate weighted sums (price * count for each row)
    combined["weighted_sum"] = combined["productos_precio_lista_avg"] * combined["product_count"]
    
    # Group by product and date, summing the weighted values and counts
    result = combined.groupby(["id_producto", "date"]).agg({
        "productos_descripcion": "first",
        "weighted_sum": "sum",
        "product_count": "sum"
    }).reset_index()
    
    # Calculate the new weighted average
    result["productos_precio_lista_avg"] = result["weighted_sum"] / result["product_count"]
    
    # Drop the temporary column and reorder
    result = result[[
        "id_producto", 
        "productos_descripcion", 
        "productos_precio_lista_avg", 
        "date", 
        "product_count"
    ]]
    
    logger.info(f"Combined {len(dataframes)} dataframes into {len(result)} weighted averages")
    
    # Return properly typed DataFrame
    return DataFrame[SepaWeightedAvgPriceProductModel](result)


def batch_process_with_weighted_avg(
    raw_data_iterator: Iterator[DataFrame[SepaProductosDataModel]],
    batch_size: int = 100000
) -> DataFrame[SepaWeightedAvgPriceProductModel]:
    """
    Process raw data in batches and combine using weighted averages.
    
    This enables processing datasets larger than memory by processing chunks
    and combining the pre-aggregated results.
    
    Args:
        raw_data_iterator: Iterator that yields chunks of raw data
        batch_size: Number of rows to process at a time
        
    Returns:
        Final weighted average DataFrame
    """
    batch_results = []
    
    for i, batch in enumerate(raw_data_iterator):
        logger.info(f"Processing batch {i+1} with {len(batch)} rows")
        
        # Process this batch to get weighted averages
        weighted_avg = SepaWeightedAvgPriceProductModel.from_sepa_product_data(batch)
        batch_results.append(weighted_avg)
        
        # Optional: periodically combine results to free memory
        if len(batch_results) >= 10:
            logger.info("Combining intermediate batch results")
            combined = combine_weighted_averages(batch_results)
            batch_results = [combined]
    
    # Combine all batch results
    return combine_weighted_averages(batch_results) 