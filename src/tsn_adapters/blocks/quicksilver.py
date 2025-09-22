"""
Quicksilver API block for TSN adapters.
"""

import json
import logging
import os
import ssl
from typing import Any, Optional
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

import certifi
import pandas as pd
from pandera.typing import DataFrame
from prefect.blocks.core import Block
from pydantic import Field as PydanticField

from tsn_adapters.utils.create_empty_df import create_empty_df
from tsn_adapters.utils.logging import get_logger_safe


class QuicksilverEndpoint:
    """Represents a Quicksilver API endpoint with path and parameters."""
    
    def __init__(self, path: str, params: Optional[dict[str, Any]] = None):
        self.path = path
        self.params = params or {}
    
    def get_url(self, base_url: str) -> str:
        """Build the complete URL for this endpoint."""
        url = urljoin(base_url, self.path)
        if self.params:
            url += f"?{urlencode(self.params)}"
        return url




class QuicksilverBlock(Block):
    """
    Prefect block for accessing Quicksilver API.
    """
    
    base_url: str = PydanticField(
        default_factory=lambda: os.getenv("QUICKSILVER_BASE_URL", "https://api.example.com/"),
        description="Base URL for Quicksilver API (set via QUICKSILVER_BASE_URL env var)"
    )
    
    timeout: int = PydanticField(
        default=30,
        description="Request timeout in seconds"
    )

    @property
    def logger(self) -> logging.Logger:
        """Get logger instance."""
        if not hasattr(self, "_logger"):
            self._logger = get_logger_safe(__name__)
        return self._logger

    def _make_request(self, endpoint: QuicksilverEndpoint) -> dict[str, Any]:
        """
        Make HTTP request to Quicksilver API.
        
        Args:
            endpoint: The endpoint configuration
            
        Returns:
            Quicksilver API response: {"data": [...]}
            
        Raises:
            Exception: If request fails or returns invalid response
        """
        url = endpoint.get_url(self.base_url)
        
        # Create request with headers
        request = Request(url)
        request.add_header("Accept", "application/json")
        request.add_header("User-Agent", "TSN-Adapters/1.0")
        
        try:
            self.logger.debug(f"Making request to: {url}")
            
            # Create SSL context for secure connections
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            
            response = urlopen(request, timeout=self.timeout, context=ssl_context)
            data = response.read().decode("utf-8")
            parsed_data = json.loads(data)
            
            self.logger.debug(f"Successfully received {len(str(parsed_data))} bytes of data")
            return parsed_data
            
        except Exception as e:
            self.logger.error(f"Request failed for URL {url}: {e}")
            raise

    def fetch_data(
        self, 
        endpoint_path: str,
        asset_id: Optional[str] = None,
        params: Optional[dict[str, Any]] = None
    ) -> DataFrame:
        """
        Fetch data from Quicksilver API and return as typed DataFrame.
        
        Args:
            endpoint_path: API endpoint path (e.g., "/data/latest")
            asset_id: Optional asset ID for filtering (e.g., "bitcoin", "ethereum")
            params: Optional additional query parameters
            
        Returns:
            DataFrame containing the API response data
            
        Raises:
            ValueError: If API returns unexpected data format
            Exception: If request fails
        """
        # Import locally to avoid circular dependency
        from tsn_adapters.tasks.quicksilver.types import QuicksilverDataModel
        
        # Build query parameters
        query_params = params or {}
        
        # Add ID filter if specified (for deterministic single asset queries)
        if asset_id:
            import json
            where_clause = {"id": asset_id}
            query_params["where"] = json.dumps(where_clause)
        
        endpoint = QuicksilverEndpoint(endpoint_path, query_params)
        
        self.logger.info(f"Fetching data from Quicksilver: {endpoint_path}")
        
        try:
            raw_data = self._make_request(endpoint)
            
            # Extract data array from known response format: {"data": [...]}
            if not isinstance(raw_data, dict) or "data" not in raw_data:
                raise ValueError(f"Expected response with 'data' field, got: {type(raw_data)}")
            
            records = raw_data["data"]
            if not isinstance(records, list):
                raise ValueError(f"Expected 'data' to be a list, got: {type(records)}")
            
            if not records:
                self.logger.info("API returned empty data")
                return create_empty_df(QuicksilverDataModel)
            
            df = pd.DataFrame(records)
            self.logger.info(f"Successfully processed {len(df)} records")
            
            return DataFrame[QuicksilverDataModel](df)
            
        except Exception as e:
            error_msg = f"Failed to fetch data from {endpoint_path}: {e}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e

