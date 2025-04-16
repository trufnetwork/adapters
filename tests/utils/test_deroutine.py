from functools import partial
from typing import Any
from unittest.mock import MagicMock

from prefect._internal.compatibility.async_dispatch import async_dispatch
import pytest
from pytest_mock import MockerFixture

from tsn_adapters.utils.deroutine import force_sync

# --- Test Functions ---


async def async_fn_base(x: Any) -> Any:
    return f"async_{x}"


def sync_fn_base(x: Any) -> Any:
    return f"sync_{x}"


@async_dispatch(async_fn_base)
def dispatched_fn(x: Any) -> Any:
    """Prefect async_dispatch wrapper."""
    return sync_fn_base(x)


def regular_fn(x: Any) -> Any:
    """Regular function (doesn't accept _sync)."""
    return f"regular_{x}"


def kwargs_fn(x: Any, **kwargs: Any) -> Any:
    """Function accepting **kwargs."""
    return f"kwargs_{x}_{kwargs.get('_sync')}"


# --- Tests ---


@pytest.fixture(autouse=True)
def mock_prefect_context(mocker: MockerFixture) -> None:
    """Ensure async_dispatch runs sync version by default in tests."""
    mocker.patch("prefect._internal.compatibility.async_dispatch.is_in_async_context", return_value=False)


def test_force_sync_with_dispatched_function() -> None:
    """Verify force_sync adds _sync=True and works with dispatched functions."""
    forced_dispatched = force_sync(dispatched_fn)
    assert isinstance(forced_dispatched, partial)
    assert forced_dispatched.keywords == {"_sync": True}
    # Verify it forces the sync path and runs correctly
    result = forced_dispatched(1)
    assert result == "sync_1"


def test_force_sync_with_regular_function_raises_error() -> None:
    """Verify force_sync adds _sync=True but calling it on a regular function raises TypeError."""
    forced_regular = force_sync(regular_fn)
    assert isinstance(forced_regular, partial)
    assert forced_regular.keywords == {"_sync": True}
    # Calling the partial on a function that can't handle _sync should raise TypeError
    with pytest.raises(TypeError, match="got an unexpected keyword argument '_sync'"):
        forced_regular(2)


def test_force_sync_with_kwargs_function() -> None:
    """Verify force_sync uses partial for functions accepting **kwargs."""
    # Function with **kwargs should get partial with _sync=True
    forced_kwargs = force_sync(kwargs_fn)
    assert isinstance(forced_kwargs, partial)
    assert forced_kwargs.keywords == {"_sync": True}
    
    # Should pass _sync=True when called
    result = forced_kwargs(2)
    assert result == "kwargs_2_True"


def test_force_sync_with_mock() -> None:
    """Verify force_sync leaves mock objects unchanged."""
    # Create a mock
    mock_fn = MagicMock(return_value="mock_result")
    
    # Apply force_sync
    forced_mock = force_sync(mock_fn)
    
    # Should return the original mock
    assert forced_mock is mock_fn
    
    # Mock should work normally
    assert forced_mock(3) == "mock_result"
    mock_fn.assert_called_once_with(3)
