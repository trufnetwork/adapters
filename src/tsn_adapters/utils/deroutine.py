import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")

def deroutine(item: T | Coroutine[Any, Any, T]) -> T:
    if isinstance(item, Coroutine):
        return asyncio.run(item)
    else:
        return item
