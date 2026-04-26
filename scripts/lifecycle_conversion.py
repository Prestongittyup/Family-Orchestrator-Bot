from __future__ import annotations

from household_os.core.lifecycle_state import LifecycleState


# Compatibility map for legacy persisted values seen in migrations/backfills.
LEGACY_LIFECYCLE_MAP: dict[str, str] = {
    "executed": LifecycleState.COMMITTED.value,
    "ignored": LifecycleState.FAILED.value,
}


def normalize_lifecycle_literal(value: str) -> str:
    """Normalize a lifecycle literal to canonical persisted vocabulary."""
    lowered = value.strip().lower()
    return LEGACY_LIFECYCLE_MAP.get(lowered, lowered)
