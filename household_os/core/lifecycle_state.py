"""
Canonical Lifecycle State Model

This is the SINGLE source of truth for all action lifecycle states.
All state derivation, comparison, and persistence use only these values.


This enum replaces:
 - Legacy FSM state names ("executed", "ignored")
 - Legacy string literals throughout the codebase

Migration path:
 - OLD: state == "executed"
 - NEW: state == LifecycleState.COMMITTED

Enum values are strings for backward compatibility with serialization.
"""

from __future__ import annotations

from enum import Enum
from typing import Any


class LifecycleState(str, Enum):
    """
    Canonical lifecycle state enumeration.

    Inherits from str for compatibility with:
    - JSON serialization
    - String comparisons (via str.Enum)
    - Pydantic validation

    State transitions:
    - PROPOSED: Initial state when action is proposed
    - PENDING_APPROVAL: Internal state awaiting approval (no separate event)
    - APPROVED: Approved for execution
    - COMMITTED: Successfully executed (terminal)
    - REJECTED: Rejected by user (terminal)
    - FAILED: Execution failed (terminal)

    These states map to domain events:
    - PROPOSED      ← ACTION_PROPOSED
    - APPROVED      ← ACTION_APPROVED
    - COMMITTED     ← ACTION_COMMITTED
    - REJECTED      ← ACTION_REJECTED
    - FAILED        ← ACTION_FAILED
    - PENDING_APPROVAL: Internal state, no separate event
    """

    PROPOSED = "proposed"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    COMMITTED = "committed"
    REJECTED = "rejected"
    FAILED = "failed"

    def is_terminal(self) -> bool:
        """
        Check if this state is terminal (no further transitions possible).

        Returns:
            True if state is COMMITTED, REJECTED, or FAILED
        """
        return self in {LifecycleState.COMMITTED, LifecycleState.REJECTED, LifecycleState.FAILED}

    def is_pending(self) -> bool:
        """
        Check if action is still pending (awaiting action).

        Returns:
            True if state is PROPOSED or PENDING_APPROVAL
        """
        return self in {LifecycleState.PROPOSED, LifecycleState.PENDING_APPROVAL}

    def is_approved(self) -> bool:
        """
        Check if action is approved for execution.

        Returns:
            True if state is APPROVED or COMMITTED
        """
        return self in {LifecycleState.APPROVED, LifecycleState.COMMITTED}

def parse_lifecycle_state(value: Any) -> LifecycleState:
    """Parse and validate lifecycle state input with fail-fast semantics."""
    if isinstance(value, LifecycleState):
        return value

    if isinstance(value, str):
        try:
            return LifecycleState(value)
        except ValueError as exc:
            raise ValueError(f"Invalid lifecycle state: {value}") from exc

    raise TypeError(f"Unsupported lifecycle state type: {type(value)}")


def assert_lifecycle_state(value: Any) -> LifecycleState:
    """Require enum-only lifecycle state usage inside runtime logic."""
    if not isinstance(value, LifecycleState):
        raise TypeError(
            f"Lifecycle state must be LifecycleState enum, got {type(value)}"
        )
    return value


def enforce_boundary_state(value: Any) -> LifecycleState:
    """Zero-trust ingress parser for lifecycle state values."""
    return parse_lifecycle_state(value)
