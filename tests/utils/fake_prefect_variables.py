"""In-memory stand-in for `prefect.variables.Variable`, patched into a flow module."""

from __future__ import annotations

import copy
import json
from typing import Any

# prefect.types.MAX_VARIABLE_VALUE_LENGTH in Prefect 3.6.29
MAX_VALUE_LENGTH = 5000


class FakeVariables:
    """Mirrors `Variable.get(name, default)` and `Variable.set(name, value, overwrite=...)`.

    Values must be JSON and at most MAX_VALUE_LENGTH characters, as Prefect
    requires. `get_errors` / `set_errors` make a name's reads or writes raise.
    `calls` lists every ("get" | "set", name) in order.
    """

    def __init__(self, values: dict[str, Any] | None = None) -> None:
        self.values: dict[str, Any] = copy.deepcopy(values or {})
        self.get_errors: dict[str, Exception] = {}
        self.set_errors: dict[str, Exception] = {}
        self.calls: list[tuple[str, str]] = []

    def get(self, name: str, default: Any = None) -> Any:
        self.calls.append(("get", name))
        if name in self.get_errors:
            raise self.get_errors[name]
        return copy.deepcopy(self.values.get(name, default))

    def set(self, name: str, value: Any, tags: list[str] | None = None, overwrite: bool = False) -> None:
        self.calls.append(("set", name))
        if name in self.set_errors:
            raise self.set_errors[name]
        if name in self.values and not overwrite:
            raise ValueError(f"Variable {name!r} already exists. Use `overwrite=True` to update it.")
        if len(json.dumps(value)) > MAX_VALUE_LENGTH:
            raise ValueError(f"Variable value must be less than {MAX_VALUE_LENGTH} characters")
        self.values[name] = copy.deepcopy(value)

    def names_touched(self) -> set[str]:
        return {name for _, name in self.calls}
