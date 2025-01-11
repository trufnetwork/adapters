"""Target system interfaces for Argentina data."""

from tsn_adapters.common.interfaces.target import ITargetClient
from .trufnetwork import create_trufnetwork_components

__all__ = ["ITargetClient", "create_trufnetwork_components"] 