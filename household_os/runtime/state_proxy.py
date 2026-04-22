from __future__ import annotations

from typing import Any, Callable

from household_os.runtime.state_firewall import FIREWALL
from household_os.runtime.state_firewall import StateMutationViolation


class StateProxy:
    """
    Prevents assignment to state fields at runtime.
    """

    def __init__(self, resolver: Callable[[str], str], object_id: str):
        self._resolver = resolver
        self._object_id = object_id

    @property
    def current_state(self) -> str:
        return self._resolver(self._object_id)

    @current_state.setter
    def current_state(self, value: Any) -> None:
        FIREWALL.block_direct_mutation(self, "current_state", value, source="StateProxy.current_state")

    def __setattr__(self, key: str, value: Any) -> None:
        if key.startswith("_"):
            super().__setattr__(key, value)
            return
        raise StateMutationViolation(f"Attempted illegal mutation: {key} = {value}")
