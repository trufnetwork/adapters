"""
Quicksilver data provider implementation.
"""

import os
from typing import Any, Optional

from tsn_adapters.blocks.quicksilver import QuicksilverBlock
from tsn_adapters.common.interfaces.provider import IProviderGetter
from tsn_adapters.utils.logging import get_logger_safe

from .types import QuicksilverDataDF, QuicksilverKey


class QuicksilverProvider(IProviderGetter[QuicksilverKey, QuicksilverDataDF]):
    """Provider for fetching data from Quicksilver API."""
    
    def __init__(
        self, 
        quicksilver_block: QuicksilverBlock,
        ticker: str,
        endpoint_path: Optional[str] = None,
        default_params: Optional[dict[str, Any]] = None
    ):
        self.quicksilver_block = quicksilver_block
        self.ticker = ticker
        self.endpoint_path = endpoint_path or os.getenv("QUICKSILVER_ENDPOINT_PATH", "/api/gateway/example/findMany")
        self.default_params = default_params or {"source": "yahoo", "limit": "10"}
        self.logger = get_logger_safe(__name__)

    def list_available_keys(self) -> list[QuicksilverKey]:
        return [QuicksilverKey(self.ticker)]

    def get_data_for(self, key: QuicksilverKey) -> QuicksilverDataDF:
        if key != self.ticker:
            raise ValueError(f"Provider configured for '{self.ticker}', got '{key}'")
        
        return self.quicksilver_block.fetch_data(
            endpoint_path=self.endpoint_path,
            ticker=self.ticker,
            params=self.default_params
        )

    def get_latest_data(self) -> QuicksilverDataDF:
        return self.get_data_for(QuicksilverKey(self.ticker))