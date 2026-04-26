from __future__ import annotations

from household_os.core.lifecycle_state import LifecycleState, assert_lifecycle_state


class LifecyclePresentationMapper:
    """Map internal lifecycle states to external presentation labels."""

    _API_STATE_BY_LIFECYCLE: dict[LifecycleState, str] = {
        LifecycleState.PROPOSED: LifecycleState.PROPOSED.value,
        LifecycleState.PENDING_APPROVAL: LifecycleState.PENDING_APPROVAL.value,
        LifecycleState.APPROVED: LifecycleState.APPROVED.value,
        LifecycleState.COMMITTED: "executed",
        LifecycleState.REJECTED: LifecycleState.REJECTED.value,
        LifecycleState.FAILED: LifecycleState.FAILED.value,
    }

    @classmethod
    def to_api_state(cls, state: LifecycleState) -> str:
        canonical_state = assert_lifecycle_state(state)
        return cls._API_STATE_BY_LIFECYCLE[canonical_state]
