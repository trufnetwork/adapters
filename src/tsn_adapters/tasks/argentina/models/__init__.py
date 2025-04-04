"""
Data models for Argentina SEPA data.
"""

from .sepa import SepaS3RawDataItem, SepaWebsiteDataItem

__all__ = [
    "SepaS3RawDataItem", 
    "SepaWebsiteDataItem", 
]