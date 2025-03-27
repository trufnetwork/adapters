"""
Data models for the Argentina SEPA product aggregation flow.
"""

from datetime import datetime

import pandera as pa
from pandera.typing import Series as PanderaSeries
from pandas import Series as PandasSeries
from pandera.api.extensions import register_check_method
from pydantic import BaseModel, Field, field_validator

from tsn_adapters.blocks.primitive_source_descriptor import PrimitiveSourceDataModel


class ProductAggregationMetadata(BaseModel):
    """
    Metadata for tracking the state of the product aggregation process.
    """

    last_processed_date: str = Field(
        default="1970-01-01", description="The date (YYYY-MM-DD) of the last successfully processed daily file."
    )
    total_products_count: int = Field(
        default=0, description="The total number of unique products currently in the aggregated list."
    )

    @field_validator("last_processed_date")
    @classmethod
    def _validate_date(cls, v: str) -> str:
        """Validate that the date string is in YYYY-MM-DD format."""
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError as e:
            raise ValueError("last_processed_date must be in YYYY-MM-DD format") from e


# Define and register the check
@register_check_method(
    check_type="vectorized",
    supported_types=(PandasSeries,),
    strategy="element_wise"
)
def check_yyyy_mm_dd(s: 'PandasSeries[str]') -> 'PandasSeries[bool]':
    """Check that the date string is in YYYY-MM-DD format."""
    return s.str.match(r'^\d{4}-\d{2}-\d{2}$', na=False)


class DynamicPrimitiveSourceModel(PrimitiveSourceDataModel):
    """
    Pandera DataFrameModel extending PrimitiveSourceDataModel for aggregated Argentina SEPA products.

    Includes product description and the date it was first observed.
    """

    # Inherited fields: stream_id, source_id, source_type
    productos_descripcion: PanderaSeries[str] = pa.Field(
        description="The description captured when the product was first seen."
    )
    first_shown_at: PanderaSeries[str] = pa.Field(
        description="The date (YYYY-MM-DD) the product was first encountered.",
        check_yyyy_mm_dd=True
    )

    class Config(PrimitiveSourceDataModel.Config):
        """
        Configuration for the Pandera model.

        Inherits strict='filter' and coerce=True from the base model.
        """
        # inherits strict = "filter" and coerce = True
        drop_invalid_rows = True
