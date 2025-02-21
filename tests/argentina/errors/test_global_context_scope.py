"""
Tests to verify the scoping of the global error context managed
by tsn_adapters/tasks/argentina/errors/context.py.

We cover:
  - Synchronous tasks that simply read the global context.
  - Tasks that add a local value via the error_context() context manager.
  - Asynchronous tasks that return the current context.
  - Tasks submitted via .submit().
  - A task that shows that a context set inside a block is rolled back after the block.
  - Concurrent tasks with different local context values to ensure isolation.
"""

import asyncio
from typing import Any

from prefect import flow, task

from tsn_adapters.tasks.argentina.errors.context import (
    clear_error_context,
    error_context,
    get_error_context,
    set_error_context,
)


@task
def get_context() -> dict[str, Any]:
    """Return the current global error context."""
    return get_error_context()


@task
def set_context_in_task(key: str, value: str) -> dict[str, Any]:
    """
    Set an additional context using error_context and return the merged context.
    This should merge any global context already present.
    """
    with error_context(**{key: value}):
        return get_error_context()


@task
async def async_get_context() -> dict[str, Any]:
    """Asynchronous task returning the current global error context after a short delay."""
    await asyncio.sleep(0.1)
    return get_error_context()


@task
def context_with_and_without(key: str, value: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Within a local error_context block, record the context then,
    afterwards, record the global context again to ensure the local value is not leaked.
    """
    with error_context(**{key: value}):
        inside = get_error_context()
    after = get_error_context()
    return inside, after


@flow
def global_context_flow() -> dict[str, Any]:
    """
    Flow that exercises various types of tasks to validate that the global error context is properly applied.

    First, we clear any previous context and set a global key.
    Then we run:
     - A normal task that simply returns the global context.
     - A task that adds a key inside an error_context block.
     - An async task.
     - A task launched via the .submit() method.
     - A task that shows a context value being set only within a with‐block.
    """
    # Reset and then set a global value:
    clear_error_context()
    set_error_context("global", "global_value")

    # 1. Normal task reading the global context:
    normal = get_context()

    # 2. Task that locally adds its own key:
    with_ctx = set_context_in_task("task_key", "task_value")

    # 3. Asynchronous task:
    async_result = asyncio.run(async_get_context())

    # 4. Task submitted via .submit()
    submit_future = set_context_in_task.submit("submit_key", "submit_value")
    submit_result = submit_future.result()

    # 5. Task that tests a context block's scoping:
    inside_after = context_with_and_without("block_key", "block_value")

    return {
        "normal": normal,
        "with_ctx": with_ctx,
        "async": async_result,
        "submit": submit_result,
        "inside_after": inside_after,
    }


@flow
def concurrent_context_flow() -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Flow that concurrently runs two tasks, each setting a different local error context.
    This test ensures that concurrently submitted tasks do not leak context across each other.
    """
    clear_error_context()
    # Launch two tasks concurrently, each using error_context to add a distinct key:
    future1 = set_context_in_task.submit("concurrent_key", "value1")
    future2 = set_context_in_task.submit("concurrent_key", "value2")
    result1 = future1.result()
    result2 = future2.result()
    return result1, result2


def test_global_context_scope(prefect_test_fixture: Any):
    """
    Validate that:
      - A normal task sees the global context.
      - A task wrapped with error_context correctly merges its key with the global context.
      - Asynchronous tasks and submitted tasks see the expected global values.
      - Context set within a with-block does not persist after the block.
    """
    result = global_context_flow()
    # Check that the normal task has the global value:
    assert result["normal"].get("global") == "global_value"

    # The task with additional context should include both the global and the local key:
    assert result["with_ctx"].get("global") == "global_value"
    assert result["with_ctx"].get("task_key") == "task_value"

    # Async and submitted tasks should see only the global context (unless they set their own):
    assert result["async"].get("global") == "global_value"
    assert result["submit"].get("global") == "global_value"
    assert result["submit"].get("submit_key") == "submit_value"

    # For the context_with_and_without task:
    inside, after = result["inside_after"]
    assert inside.get("block_key") == "block_value"
    # After leaving the block, "block_key" should no longer be present:
    assert "block_key" not in after


def test_concurrent_context_isolation(prefect_test_fixture: Any):
    """
    Validate that concurrently submitted tasks each carry their local error context independently.
    """
    result1, result2 = concurrent_context_flow()
    # Each task should have its own "concurrent_key" with distinct values.
    assert result1.get("concurrent_key") in ("value1", "value2")
    assert result2.get("concurrent_key") in ("value1", "value2")
    # The two results should not have the same value.
    assert result1.get("concurrent_key") != result2.get("concurrent_key")
