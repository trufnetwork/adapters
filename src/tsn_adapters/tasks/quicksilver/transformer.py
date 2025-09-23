"""
Quicksilver data transformer implementation.
"""

import pandas as pd
from datetime import datetime
from pandera.typing import DataFrame

from tsn_adapters.common.interfaces.transformer import IDataTransformer
from tsn_adapters.common.trufnetwork.models.tn_models import TnDataRowModel
from tsn_adapters.utils.logging import get_logger_safe

from .types import QuicksilverDataDF, QuicksilverKey


class QuicksilverDataTransformer(IDataTransformer[QuicksilverDataDF]):
    """Transforms Quicksilver data into TrufNetwork records."""
    
    def __init__(self, stream_id: str, data_provider: str = "quicksilver"):
        self.stream_id = stream_id
        self.data_provider = data_provider
        self.logger = get_logger_safe(__name__)

    def transform(self, data: QuicksilverDataDF) -> DataFrame[TnDataRowModel]:
        if data.empty:
            return DataFrame[TnDataRowModel](pd.DataFrame(columns=["date", "value", "stream_id", "data_provider"]))
        
        self.logger.info(f"Transforming {len(data)} Quicksilver records")
        
        # Clean price data
        clean_prices = data['price'].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False)
        
        tn_data = pd.DataFrame({
            'date': int(datetime.now().timestamp()),
            'value': clean_prices,
            'stream_id': self.stream_id,
            'data_provider': self.data_provider
        })
        
        self.logger.info(f"Successfully transformed {len(tn_data)} records")
        return DataFrame[TnDataRowModel](tn_data)

