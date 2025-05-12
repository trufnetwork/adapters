# conftest.py
from functools import wraps
from glob import glob
import logging
from typing import Any, Callable
from unittest.mock import patch

import prefect
from prefect import Task

logger = logging.getLogger(__name__)
_original_task = prefect.task

def _strip_cache_and_retries(task_obj: Task[Any, Any]) -> Task[Any, Any]:
    """Remove cache + retry, and recursively patch .with_options()."""
    # zero-out caching & retries
    task_obj.cache_key_fn = None
    task_obj.cache_expiration = None
    if hasattr(task_obj, "cache_policy"):
        setattr(task_obj, "cache_policy", None)
    task_obj.retries = 0
    task_obj.retry_condition_fn = None

    # wrap its with_options so children are also stripped
    original_with_options = task_obj.with_options
    @wraps(original_with_options)
    def _patched_with_options(*args: Any, **kwargs: Any) -> Task[Any, Any]:
        new_task = original_with_options(*args, **kwargs)
        return _strip_cache_and_retries(new_task)
    task_obj.with_options = _patched_with_options          # type: ignore[attr-defined]

    return task_obj

def _patched_task(*d_args: Any, **d_kwargs: Any) -> Any:
    """Replacement for prefect.task that always returns a stripped Task."""
    # @task without args
    if len(d_args) == 1 and callable(d_args[0]) and not d_kwargs:
        return _strip_cache_and_retries(_original_task(d_args[0]))
    # @task(...) with args
    def decorator(fn: Callable[..., Any]) -> Task[Any, Any]:
        return _strip_cache_and_retries(_original_task(fn, **d_kwargs))
    return decorator

# apply patch early
_patcher = patch("prefect.task", new=_patched_task)
_patcher.start()

def pytest_sessionfinish(session: Any, exitstatus: Any):
    _patcher.stop()

# discover extra fixture files under tests/fixtures
def _path_to_module(path: str) -> str:
    return path.replace("/", ".").replace("\\", ".").replace(".py", "")

pytest_plugins = [
    _path_to_module(f) for f in glob("tests/fixtures/*.py") if "__" not in f
]
