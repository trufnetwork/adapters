from typing import TypeVar, cast

from prefect.futures import PrefectFuture

T = TypeVar("T")

def cast_future(future: PrefectFuture[T]) -> T:
    return cast(T, future.result())