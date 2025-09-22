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
    """
    Transforms Quicksilver API data into TrufNetwork records.
    """
    
    def __init__(
        self, 
        stream_id: str,
        data_provider: str = "quicksilver"
    ):
        """
        Initialize transformer for single asset processing.
        
        Args:
            stream_id: TrufNetwork stream ID for this asset
            data_provider: Name of the data provider (default: "quicksilver")
        """
        self.stream_id = stream_id
        self.data_provider = data_provider
        self.logger = get_logger_safe(__name__)

    def transform(self, data: QuicksilverDataDF) -> DataFrame[TnDataRowModel]:
        """
        Transform Quicksilver data into TrufNetwork records.
        
        Args:
            data: The Quicksilver data to transform
            
        Returns:
            DataFrame containing TrufNetwork-formatted records
        """
        if data.empty:
            self.logger.info("Input data is empty, returning empty TN DataFrame")
            return DataFrame[TnDataRowModel](pd.DataFrame(columns=["date", "value", "stream_id", "data_provider"]))
        
        self.logger.info(f"Transforming {len(data)} Quicksilver records to TrufNetwork format")
        
        tn_data = pd.DataFrame({
            'date': pd.to_datetime(data['last_updated']).astype('int64') // 10**9,
            'value': data['current_price'].astype(str),
            'stream_id': self.stream_id,
            'data_provider': self.data_provider
        })
        
        self.logger.info(f"Successfully transformed {len(tn_data)} records")
        return DataFrame[TnDataRowModel](tn_data)

