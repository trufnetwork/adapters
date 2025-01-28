"""
Utility function to handle coroutine conversion in S3 operations.
"""

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")

def deroutine(item: T | Coroutine[Any, Any, T]) -> T:
    """Convert a coroutine to its result if needed.
    
    Args:
        item: Item that might be a coroutine
        
    Returns:
        The item itself if not a coroutine, or the coroutine's result
    """
    if isinstance(item, Coroutine):
        return asyncio.run(item)
    else:
        return item
