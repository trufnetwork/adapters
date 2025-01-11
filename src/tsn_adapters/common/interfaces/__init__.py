"""
Common interfaces for TSN adapters.
"""

from .provider import IProviderGetter, IStreamSourceMapFetcher
from .reconciliation import IReconciliationStrategy
from .transformer import IDataTransformer
from .target import ITargetClient

__all__ = [
    "IProviderGetter",
    "IStreamSourceMapFetcher",
    "IReconciliationStrategy",
    "IDataTransformer",
    "ITargetClient",
] 