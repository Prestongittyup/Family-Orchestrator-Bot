"""
Command Handler - Transition Logic via Event Generation

Instead of mutating state directly, commands are processed to generate
domain events. The command handler:
- Validates the command against current state
- Checks for valid transitions
- Generates an event (or rejects with error)
- Never mutates state directly

This is the single point where lifecycle state changes are generated.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from household_os.core.lifecycle_state import LifecycleState, assert_lifecycle_state
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.state_reducer import (
    get_valid_next_events,
    is_terminal_state,
    reduce_state,
    StateReductionError,
)


class CommandError(Exception):
    """Base exception for command validation/execution errors."""

    pass


class InvalidTransitionError(CommandError):
    """Raised when a transition is not allowed from current state."""

    pass


class AggregateNotFoundError(CommandError):
    """Raised when aggregate (e.g., action) is not found."""

    pass


@dataclass(frozen=True)
class Command(ABC):
    """
    Base class for all commands.

    Commands represent intent to change state.
    Unlike events, commands can be rejected.
    """

    aggregate_id: str
    timestamp: datetime | None = None
    metadata: dict[str, Any] | None = None

    @property
    def ts(self) -> datetime:
        """Get timestamp, defaulting to now if not set."""
        return self.timestamp or datetime.now(UTC)

    @property
    def meta(self) -> dict[str, Any]:
        """Get metadata, defaulting to empty dict."""
        return self.metadata or {}


@dataclass(frozen=True)
class ApproveActionCommand(Command):
    """Command to approve an action."""

    request_id: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class RejectActionCommand(Command):
    """Command to reject an action."""

    request_id: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class CommitActionCommand(Command):
    """Command to commit an approved action."""

    request_id: str | None = None
    result: dict[str, Any] | None = None


@dataclass(frozen=True)
class FailActionCommand(Command):
    """Command to mark an action as failed."""

    request_id: str | None = None
    error: str | None = None
    error_code: str | None = None


class CommandHandler:
    """
    Handles command processing and event generation.

    Workflow:
    1. Validate command (check aggregate exists)
    2. Compute current state from events
    3. Check if transition is allowed
    4. Generate event if allowed
    5. Raise error if not allowed
    """

    def handle(
        self,
        command: Command,
        events: list[DomainEvent],
    ) -> DomainEvent:
        """
        Process a command and generate an event.

        Args:
            command: Command to process
            events: Current event history for aggregate

        Returns:
            Generated DomainEvent

        Raises:
            AggregateNotFoundError: If no events exist for aggregate
            InvalidTransitionError: If transition not allowed
            CommandError: Other validation failures
        """
        if not events:
            raise AggregateNotFoundError(
                f"No event history for aggregate {command.aggregate_id}"
            )

        # Compute current state
        try:
            current_state = reduce_state(events)
        except StateReductionError as e:
            raise CommandError(f"Invalid event history: {e}")
        current_state = assert_lifecycle_state(current_state)

        # Check if transition allowed
        if is_terminal_state(current_state):
            raise InvalidTransitionError(
                f"Cannot transition from terminal state {current_state}"
            )

        # Dispatch to specific handler
        if isinstance(command, ApproveActionCommand):
            return self._handle_approve(command, current_state)
        elif isinstance(command, RejectActionCommand):
            return self._handle_reject(command, current_state)
        elif isinstance(command, CommitActionCommand):
            return self._handle_commit(command, current_state)
        elif isinstance(command, FailActionCommand):
            return self._handle_fail(command, current_state)
        else:
            raise CommandError(f"Unknown command type: {type(command)}")

    def _handle_approve(
        self, command: ApproveActionCommand, current_state: LifecycleState
    ) -> DomainEvent:
        """Handle approve command."""
        valid_states = {LifecycleState.PROPOSED, LifecycleState.PENDING_APPROVAL}

        if current_state not in valid_states:
            raise InvalidTransitionError(
                f"Cannot approve from state {current_state}. "
                f"Valid states: {valid_states}"
            )

        return DomainEvent.create(
            aggregate_id=command.aggregate_id,
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            timestamp=command.ts,
            payload={"reason": command.reason},
            metadata={"request_id": command.request_id, **(command.meta or {})},
        )

    def _handle_reject(
        self, command: RejectActionCommand, current_state: LifecycleState
    ) -> DomainEvent:
        """Handle reject command."""
        valid_states = {LifecycleState.PROPOSED, LifecycleState.PENDING_APPROVAL}

        if current_state not in valid_states:
            raise InvalidTransitionError(
                f"Cannot reject from state {current_state}. "
                f"Valid states: {valid_states}"
            )

        return DomainEvent.create(
            aggregate_id=command.aggregate_id,
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_REJECTED"],
            timestamp=command.ts,
            payload={"reason": command.reason},
            metadata={"request_id": command.request_id, **(command.meta or {})},
        )

    def _handle_commit(
        self, command: CommitActionCommand, current_state: LifecycleState
    ) -> DomainEvent:
        """Handle commit command."""
        if current_state != LifecycleState.APPROVED:
            raise InvalidTransitionError(
                f"Can only commit from APPROVED state, current state: {current_state}"
            )

        return DomainEvent.create(
            aggregate_id=command.aggregate_id,
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],
            timestamp=command.ts,
            payload={"result": command.result},
            metadata={"request_id": command.request_id, **(command.meta or {})},
        )

    def _handle_fail(
        self, command: FailActionCommand, current_state: LifecycleState
    ) -> DomainEvent:
        """Handle fail command."""
        # Can fail from most states except terminal states
        if is_terminal_state(current_state):
            raise InvalidTransitionError(
                f"Cannot fail from terminal state {current_state}"
            )

        return DomainEvent.create(
            aggregate_id=command.aggregate_id,
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_FAILED"],
            timestamp=command.ts,
            payload={
                "error": command.error,
                "error_code": command.error_code,
            },
            metadata={"request_id": command.request_id, **(command.meta or {})},
        )


# Singleton instance
_handler: CommandHandler | None = None


def get_command_handler() -> CommandHandler:
    """Get or create singleton command handler."""
    global _handler
    if _handler is None:
        _handler = CommandHandler()
    return _handler
