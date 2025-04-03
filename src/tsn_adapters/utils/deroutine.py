"""
Utility function to handle coroutine conversion in S3 operations.
"""

import asyncio
import inspect
from collections.abc import Coroutine
from functools import partial
from typing import Any, TypeVar, Callable, Union, ParamSpec

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

P = ParamSpec("P")
R = TypeVar("R")


def force_sync(fn: Callable[P, Union[R, Coroutine[Any, Any, R]]]) -> Callable[P, R]:
    """
    Force a function to run in the sync context, if the function is decorated by @async_dispatch
    
    it simply returns the same function with partial apply of _sync=True
    """
    if accepts_sync_param(fn):
        partial_fn = partial(fn, _sync=True)  # type: ignore
        return partial_fn # type: ignore
    else:
        return fn # type: ignore
    
def is_in_async():
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False

def accepts_sync_param(fn: Callable[..., Any]) -> bool:
    """
    Check if a function accepts a _sync parameter, which indicates
    it was likely decorated with @async_dispatch
    
    Args:
        fn: The function to check
        
    Returns:
        True if the function accepts a _sync parameter, False otherwise
    """
    sig = inspect.signature(fn)
    return '_sync' in sig.parameters
