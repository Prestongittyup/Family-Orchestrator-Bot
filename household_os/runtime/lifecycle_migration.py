"""
Lifecycle Migration Layer - Dual-Write Mode

During migration from mutation-based FSM to event-sourced architecture:
- FSM continues to run and mutate state (for backward compatibility)
- Events are also generated and stored (new system)
- System asserts FSM state == event-derived state
- Detects divergence immediately (safety guarantee)

This layer ensures:
1. No data loss during migration
2. Both systems stay synchronized
3. Divergence is caught and reported
4. Can roll back to FSM-only if needed
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Optional

from household_os.core.lifecycle_state import enforce_boundary_state

from household_os.runtime.command_handler import (
    Command,
    CommandError,
    CommandHandler,
    get_command_handler,
)
from household_os.runtime.domain_event import DomainEvent, LifecycleEventState
from household_os.runtime.event_store import EventStore, InMemoryEventStore
from household_os.runtime.state_reducer import reduce_state, StateReductionError


class MigrationError(Exception):
    """Raised when migration layer detects inconsistency."""

    pass


class DivergenceDetected(MigrationError):
    """Raised when FSM state diverges from event-derived state."""

    pass


@dataclass
class StateConsistencyReport:
    """
    Report of state consistency check between FSM and events.

    Used for monitoring and debugging during migration.
    """

    aggregate_id: str
    fsm_state: str | Literal["unknown"]
    event_derived_state: LifecycleEventState
    is_consistent: bool
    divergence_reason: str | None = None
    event_count: int = 0

    def __repr__(self) -> str:
        status = "✓ CONSISTENT" if self.is_consistent else "✗ DIVERGED"
        return (
            f"StateConsistencyReport("
            f"aggregate={self.aggregate_id}, "
            f"fsm={self.fsm_state}, "
            f"events={self.event_derived_state}, "
            f"{status}"
            f")"
        )


class LifecycleMigrationLayer:
    """
    Safe migration layer for FSM → Event-sourced transition.

    Runs both systems in parallel:
    - When applying transitions, emit events AND mutate FSM
    - When reading state, verify both systems agree
    - Fail fast if they diverge

    This design ensures:
    - No state loss during migration
    - Early detection of bugs
    - Easy rollback if needed
    - Data integrity maintained
    """

    def __init__(self, event_store: EventStore | None = None) -> None:
        """
        Initialize migration layer.

        Args:
            event_store: Event store instance (defaults to in-memory)
        """
        # Use explicit None check to avoid falsy empty event stores
        # (empty InMemoryEventStore has __len__=0 which is falsy)
        self.event_store = event_store if event_store is not None else InMemoryEventStore()
        self.command_handler = get_command_handler()

        # Monitoring
        self._divergence_count = 0
        self._sync_count = 0

    def process_command(
        self,
        command: Command,
        current_fsm_state: str,
        apply_fsm_transition: callable,
        aggregate_id: str,
    ) -> DomainEvent:
        """
        Process a command in hybrid mode (FSM + Events).

        This is the core migration method. It:
        1. Validates command using event history
        2. Generates event from command
        3. Appends event to store
        4. Applies FSM transition (for backward compatibility)
        5. Verifies consistency

        Args:
            command: Command to process
            current_fsm_state: Current FSM state (for comparison)
            apply_fsm_transition: Callback to apply FSM transition
            aggregate_id: ID of aggregate being modified

        Returns:
            Generated DomainEvent

        Raises:
            CommandError: If command is invalid
            DivergenceDetected: If FSM and events diverge
        """
        # Step 1: Get event history
        try:
            events = self.event_store.get_events(aggregate_id)
        except Exception:
            # First time - no prior events
            events = []

        # Step 2: Handle command to generate event
        try:
            event = self.command_handler.handle(command, events)
        except CommandError:
            raise

        # Step 3: Append event to store
        self.event_store.append(event)

        # Step 4: Apply FSM transition (callback)
        apply_fsm_transition(event)

        # Step 5: Verify consistency after transition
        new_events = self.event_store.get_events(aggregate_id)
        self._check_consistency(
            aggregate_id=aggregate_id,
            fsm_state=current_fsm_state,
            events=new_events,
            fail_on_divergence=True,
        )

        return event

    def verify_state_consistency(
        self, aggregate_id: str, fsm_state: str, fail_on_divergence: bool = False
    ) -> StateConsistencyReport:
        """
        Verify that FSM state and event-derived state match.

        Args:
            aggregate_id: ID of aggregate to check
            fsm_state: Current FSM state
            fail_on_divergence: Raise exception if divergence detected

        Returns:
            StateConsistencyReport with details

        Raises:
            DivergenceDetected: If states diverge and fail_on_divergence=True
        """
        try:
            events = self.event_store.get_events(aggregate_id)
        except Exception as e:
            # No events yet - shouldn't happen in normal flow
            msg = f"No events found for aggregate {aggregate_id}"
            if fail_on_divergence:
                raise DivergenceDetected(msg) from e
            return StateConsistencyReport(
                aggregate_id=aggregate_id,
                fsm_state=fsm_state,
                event_derived_state="unknown",  # type: ignore
                is_consistent=False,
                divergence_reason=msg,
            )

        return self._check_consistency(
            aggregate_id=aggregate_id,
            fsm_state=fsm_state,
            events=events,
            fail_on_divergence=fail_on_divergence,
        )

    def _check_consistency(
        self,
        aggregate_id: str,
        fsm_state: str,
        events: list[DomainEvent],
        fail_on_divergence: bool,
    ) -> StateConsistencyReport:
        """
        Internal consistency check.

        Args:
            aggregate_id: Aggregate ID
            fsm_state: FSM's current state
            events: Events from event store
            fail_on_divergence: Raise on divergence

        Returns:
            StateConsistencyReport

        Raises:
            DivergenceDetected: If states diverge and fail_on_divergence=True
        """
        # Compute event-derived state
        try:
            event_derived_state = reduce_state(events)
        except StateReductionError as e:
            raise MigrationError(f"Failed to reduce state for {aggregate_id}: {e}")

        parsed_fsm_state = enforce_boundary_state(fsm_state)

        # Check consistency
        is_consistent = parsed_fsm_state == event_derived_state

        report = StateConsistencyReport(
            aggregate_id=aggregate_id,
            fsm_state=parsed_fsm_state.value,
            event_derived_state=event_derived_state,
            is_consistent=is_consistent,
            event_count=len(events),
        )

        if is_consistent:
            self._sync_count += 1
        else:
            self._divergence_count += 1
            report.divergence_reason = (
                f"FSM state '{parsed_fsm_state}' != "
                f"event-derived state '{event_derived_state}'"
            )

            if fail_on_divergence:
                raise DivergenceDetected(report.divergence_reason)

        return report

    def get_event_history(self, aggregate_id: str) -> list[DomainEvent]:
        """Get all events for an aggregate."""
        return self.event_store.get_events(aggregate_id)

    def get_migration_stats(self) -> dict:
        """Get migration monitoring statistics."""
        return {
            "consistent_checks": self._sync_count,
            "divergence_detections": self._divergence_count,
            "divergence_rate": (
                self._divergence_count / (self._sync_count + self._divergence_count)
                if (self._sync_count + self._divergence_count) > 0
                else 0
            ),
        }

    def reset_stats(self) -> None:
        """Reset migration statistics (for testing)."""
        self._sync_count = 0
        self._divergence_count = 0


# Singleton instance
_migration_layer: LifecycleMigrationLayer | None = None


def get_migration_layer() -> LifecycleMigrationLayer:
    """Get or create singleton migration layer."""
    global _migration_layer
    if _migration_layer is None:
        _migration_layer = LifecycleMigrationLayer()
    return _migration_layer


def set_migration_layer(layer: LifecycleMigrationLayer) -> None:
    """Set a specific migration layer (for DI/testing)."""
    global _migration_layer
    _migration_layer = layer


def reset_migration_layer() -> None:
    """Reset singleton (for testing)."""
    global _migration_layer
    _migration_layer = None
