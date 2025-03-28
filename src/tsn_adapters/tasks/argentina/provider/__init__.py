"""SEPA Data Providers

This directory contains the implementation of different data providers for the SEPA dataset.

Directory Structure:
    - base.py: Base interfaces and types
    - website.py: Website scraper-based provider
    - s3.py: S3-based provider
    - utils.py: Shared utilities for data processing
    - factory.py: Factory function for creating providers

For detailed documentation and usage examples:
    - See factory.py for provider creation and usage examples
    - See s3.py and website.py for specific provider implementations
    - See docstrings in the source files for detailed API documentation
"""

from .s3 import RawDataProvider, ProcessedDataProvider
from .product_averages import ProductAveragesProvider

__all__ = [
    "RawDataProvider",
    "ProcessedDataProvider",
    "ProductAveragesProvider",
]
