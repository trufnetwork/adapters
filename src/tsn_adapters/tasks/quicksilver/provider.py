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
    """
    Provider for fetching single asset data from Quicksilver API.
    """
    
    def __init__(
        self, 
        quicksilver_block: QuicksilverBlock,
        asset_id: str,
        endpoint_path: Optional[str] = None,
        default_params: Optional[dict[str, Any]] = None
    ):
        """
        Initialize Quicksilver provider for single asset.
        
        Args:
            quicksilver_block: Configured Quicksilver API block
            asset_id: Asset ID to fetch (e.g., "bitcoin", "ethereum")
            endpoint_path: API endpoint path (uses QUICKSILVER_ENDPOINT_PATH env var if not provided)
            default_params: Default query parameters for all requests
        """
        self.quicksilver_block = quicksilver_block
        self.asset_id = asset_id
        self.endpoint_path = endpoint_path or os.getenv(
            "QUICKSILVER_ENDPOINT_PATH", 
            "/api/gateway/example/coingekoMarket/findMany"
        )
        self.default_params = default_params or {"source": "pg", "limit": "1"}
        self.logger = get_logger_safe(__name__)

    def list_available_keys(self) -> list[QuicksilverKey]:
        """
        Return the configured asset key.
        
        Returns:
            List with single asset key
        """
        return [QuicksilverKey(self.asset_id)]

    def get_data_for(self, key: QuicksilverKey) -> QuicksilverDataDF:
        """
        Fetch data for the specified asset.
        
        Args:
            key: Asset key (should match configured asset_id)
            
        Returns:
            DataFrame containing asset data
            
        Raises:
            ValueError: If key doesn't match configured asset_id
        """
        if key != self.asset_id:
            raise ValueError(f"This provider is configured for asset '{self.asset_id}', got '{key}'")
        
        self.logger.info(f"Fetching data for asset: {self.asset_id}")
        
        # Fetch data with ID filtering
        return self.quicksilver_block.fetch_data(
            endpoint_path=self.endpoint_path,
            asset_id=self.asset_id,
            params=self.default_params
        )

    def get_latest_data(self) -> QuicksilverDataDF:
        """
        Get the latest data for the configured asset.
        
        Returns:
            DataFrame containing latest asset data
        """
        return self.get_data_for(QuicksilverKey(self.asset_id))