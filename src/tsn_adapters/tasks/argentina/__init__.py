"""
Argentina SEPA data ingestion pipeline.
"""

from tsn_adapters.tasks.argentina.models.sepa import SepaWebsiteDataItem
from tsn_adapters.tasks.argentina.models.sepa.website_item import SepaWebsiteScraper
from tsn_adapters.tasks.argentina.utils.processors import SepaDirectoryProcessor
from tsn_adapters.tasks.argentina.errors import errors

__all__ = [
    "SepaWebsiteDataItem",
    "SepaWebsiteScraper",
    "SepaDirectoryProcessor",
    "errors",
]
