from __future__ import annotations

from typing import Any

from household_os.core.lifecycle_state import LifecycleState, assert_lifecycle_state


def enforce_lifecycle_integrity(state: Any) -> LifecycleState:
    """System-level lifecycle integrity gate.

    Raises a runtime error when lifecycle state is not canonical.
    """
    try:
        return assert_lifecycle_state(state)
    except TypeError as exc:
        raise RuntimeError(
            f"CRITICAL: Invalid lifecycle state detected: {state}"
        ) from exc
