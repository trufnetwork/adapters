"""
Environment utilities for Quicksilver POC.

Handles .env file loading.
"""

import os
from pathlib import Path
from typing import Optional


def load_env_file(env_path: Optional[Path] = None) -> bool:
    """
    Load .env file and set environment variables.
    
    Args:
        env_path: Path to .env file (defaults to project root)
        
    Returns:
        True if file was loaded, False if not found
    """
    if env_path is None:
        env_path = Path(__file__).parent.parent.parent.parent / ".env"
    
    if not env_path.exists():
        return False
    
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key] = value
    
    return True


def load_environment() -> str:
    """
    Load environment variables from .env file.
    
    Returns:
        String describing the result
    """
    env_path = Path(__file__).parent.parent.parent.parent / ".env"
    if load_env_file(env_path):
        return "✅ Loaded .env file"
    else:
        return "⚠️  No .env file found - using default values"


def get_quicksilver_config() -> dict[str, str]:
    """
    Get Quicksilver configuration from environment variables.
    
    Returns:
        Dictionary with configuration values
    """
    return {
        'ticker': os.getenv("QUICKSILVER_TICKER", "AAPL"),
        'stream_id': os.getenv("QUICKSILVER_STREAM_ID", "stream_aapl_price"),
        'base_url': os.getenv("QUICKSILVER_BASE_URL", "https://api.example.com/"),
        'endpoint_path': os.getenv("QUICKSILVER_ENDPOINT_PATH", "/api/gateway/example/findMany")
    }
