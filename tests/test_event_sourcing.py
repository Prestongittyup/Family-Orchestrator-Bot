"""
Comprehensive Test Suite for Event-Sourced Lifecycle System

Tests cover:
1. Event immutability and storage
2. State reducer determinism and correctness
3. Command handler validation and event generation
4. Event replay and consistency
5. Migration layer dual-write mode
6. Full lifecycle workflows
"""

import pytest
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from household_os.core.lifecycle_state import LifecycleState, parse_lifecycle_state
from household_os.runtime.domain_event import (
    DomainEvent,
    LifecycleEventState,
    LIFECYCLE_EVENT_TYPES,
)
from household_os.runtime.event_store import (
    EventStore,
    InMemoryEventStore,
    AggregateNotFoundError,
    EventStoreError,
)
from household_os.runtime.state_reducer import (
    reduce_state,
    replay_events,
    StateReductionError,
    compute_snapshot,
    is_terminal_state,
    get_valid_next_events,
)
from household_os.runtime.command_handler import (
    CommandHandler,
    ApproveActionCommand,
    RejectActionCommand,
    CommitActionCommand,
    FailActionCommand,
    InvalidTransitionError,
    CommandError,
)
from household_os.runtime.lifecycle_migration import (
    LifecycleMigrationLayer,
    DivergenceDetected,
    StateConsistencyReport,
)


# ────────────────────────────────────────────────────────────────────────────
# Event Tests
# ────────────────────────────────────────────────────────────────────────────


class TestDomainEvent:
    """Test domain event creation and immutability."""

    def test_event_creation(self):
        """Test creating a domain event."""
        event = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            payload={"title": "Test Action"},
            metadata={"request_id": "req-456"},
        )

        assert event.aggregate_id == "action-123"
        assert event.event_type == LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"]
        assert event.payload == {"title": "Test Action"}
        assert event.metadata.get("request_id") == "req-456"
        assert event.metadata.get("actor_type") == "unknown"
        assert event.event_id is not None
        assert isinstance(event.timestamp, datetime)

    def test_event_immutability(self):
        """Test that events cannot be modified after creation."""
        event = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )

        # Frozen dataclass prevents modification
        with pytest.raises(Exception):  # FrozenInstanceError
            event.event_type = "modified"  # type: ignore

    def test_event_hashability(self):
        """Test that events are hashable (immutable)."""
        event = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )

        # Should be able to use in set or dict
        event_set = {event}
        assert event in event_set

    def test_event_with_explicit_timestamp(self):
        """Test event with explicit timestamp."""
        ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        event = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            timestamp=ts,
        )

        assert event.timestamp == ts


# ────────────────────────────────────────────────────────────────────────────
# Event Store Tests
# ────────────────────────────────────────────────────────────────────────────


class TestInMemoryEventStore:
    """Test in-memory event store implementation."""

    def test_append_and_retrieve(self):
        """Test appending and retrieving events."""
        store = InMemoryEventStore()
        event = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )

        store.append(event)
        events = store.get_events("action-123")

        assert len(events) == 1
        assert events[0].event_id == event.event_id

    def test_append_only_guarantees(self):
        """Test that store enforces append-only semantics."""
        store = InMemoryEventStore()
        event1 = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )
        event2 = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
        )

        store.append(event1)
        store.append(event2)

        events = store.get_events("action-123")
        assert len(events) == 2
        assert events[0].event_type == LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"]
        assert events[1].event_type == LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"]

    def test_prevents_duplicate_event_ids(self):
        """Test that store prevents duplicate event IDs."""
        store = InMemoryEventStore()
        event = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )

        store.append(event)

        # Try to append event with same ID (should fail)
        with pytest.raises(EventStoreError):
            store.append(event)

    def test_aggregate_not_found_error(self):
        """Test that retrieving non-existent aggregate raises error."""
        store = InMemoryEventStore()

        with pytest.raises(AggregateNotFoundError):
            store.get_events("nonexistent-action-id")

    def test_get_events_since(self):
        """Test retrieving events since a timestamp."""
        store = InMemoryEventStore()
        base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

        event1 = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            timestamp=base_time,
        )
        event2 = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            timestamp=base_time + timedelta(seconds=10),
        )

        store.append(event1)
        store.append(event2)

        # Get events after first event
        recent_events = store.get_events_since("action-123", base_time)

        assert len(recent_events) == 1
        assert recent_events[0].event_id == event2.event_id

    def test_multiple_aggregates(self):
        """Test storing events for multiple aggregates independently."""
        store = InMemoryEventStore()

        event1 = DomainEvent.create(
            aggregate_id="action-1",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )
        event2 = DomainEvent.create(
            aggregate_id="action-2",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )

        store.append(event1)
        store.append(event2)

        assert len(store.get_events("action-1")) == 1
        assert len(store.get_events("action-2")) == 1

    def test_get_all_aggregates(self):
        """Test retrieving all aggregate IDs."""
        store = InMemoryEventStore()

        for action_id in ["action-1", "action-2", "action-3"]:
            event = DomainEvent.create(
                aggregate_id=action_id,
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            )
            store.append(event)

        aggregates = store.get_all_aggregates()
        assert set(aggregates) == {"action-1", "action-2", "action-3"}


# ────────────────────────────────────────────────────────────────────────────
# State Reducer Tests
# ────────────────────────────────────────────────────────────────────────────


class TestStateReducer:
    """Test state derivation from event sequences."""

    def test_single_proposed_event(self):
        """Test state after single PROPOSED event."""
        event = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )

        state = reduce_state([event])

        assert state == LifecycleState.PROPOSED

    def test_linear_workflow_proposed_to_committed(self):
        """Test state transitions through full workflow."""
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],
            ),
        ]

        state = reduce_state(events)

        assert state == LifecycleState.COMMITTED

    def test_proposed_to_rejected(self):
        """Test rejection flow."""
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_REJECTED"],
            ),
        ]

        state = reduce_state(events)

        assert state == LifecycleState.REJECTED

    def test_proposed_to_failed(self):
        """Test failure flow."""
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_FAILED"],
            ),
        ]

        state = reduce_state(events)

        assert state == LifecycleState.FAILED

    def test_deterministic_state_reduction(self):
        """Test that same events always produce same state."""
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            ),
        ]

        state1 = reduce_state(events)
        state2 = reduce_state(events)

        assert state1 == state2 == LifecycleState.APPROVED

    def test_empty_events_raises_error(self):
        """Test that empty event list raises error."""
        with pytest.raises(StateReductionError):
            reduce_state([])

    def test_invalid_first_event_raises_error(self):
        """Test that non-PROPOSED first event raises error."""
        event = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],  # Invalid first event
        )

        with pytest.raises(StateReductionError):
            reduce_state([event])

    def test_invalid_transition_raises_error(self):
        """Test that invalid state transition raises error."""
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],  # Invalid: must go through approved
            ),
        ]

        with pytest.raises(StateReductionError):
            reduce_state(events)

    def test_terminal_states_cannot_transition(self):
        """Test that terminal states cannot transition."""
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_FAILED"],  # Invalid: already terminal
            ),
        ]

        with pytest.raises(StateReductionError):
            reduce_state(events)

    def test_is_terminal_state(self):
        """Test terminal state detection."""
        assert is_terminal_state(LifecycleState.COMMITTED) is True
        assert is_terminal_state(LifecycleState.FAILED) is True
        assert is_terminal_state(LifecycleState.REJECTED) is True
        assert is_terminal_state(LifecycleState.PROPOSED) is False
        assert is_terminal_state(LifecycleState.APPROVED) is False

    def test_get_valid_next_events(self):
        """Test valid next events for each state."""
        assert LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"] in get_valid_next_events(LifecycleState.PROPOSED)
        assert LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"] in get_valid_next_events(LifecycleState.APPROVED)
        assert len(get_valid_next_events(LifecycleState.COMMITTED)) == 0  # Terminal

    def test_compute_snapshot(self):
        """Test snapshot creation."""
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            ),
        ]

        snapshot = compute_snapshot("action-123", events)

        assert snapshot.aggregate_id == "action-123"
        assert snapshot.current_state == LifecycleState.APPROVED
        assert snapshot.event_count == 2


# ────────────────────────────────────────────────────────────────────────────
# Command Handler Tests
# ────────────────────────────────────────────────────────────────────────────


class TestCommandHandler:
    """Test command processing and event generation."""

    def test_approve_command_from_proposed(self):
        """Test approving an action from PROPOSED state."""
        handler = CommandHandler()
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
        ]

        command = ApproveActionCommand(
            aggregate_id="action-123",
            request_id="req-456",
            reason="User approved",
        )

        event = handler.handle(command, events)

        assert event.event_type == LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"]
        assert event.payload["reason"] == "User approved"

    def test_reject_command_from_proposed(self):
        """Test rejecting an action from PROPOSED state."""
        handler = CommandHandler()
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
        ]

        command = RejectActionCommand(
            aggregate_id="action-123",
            request_id="req-456",
            reason="User rejected",
        )

        event = handler.handle(command, events)

        assert event.event_type == LIFECYCLE_EVENT_TYPES["ACTION_REJECTED"]

    def test_commit_command_from_approved(self):
        """Test committing an action from APPROVED state."""
        handler = CommandHandler()
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            ),
        ]

        command = CommitActionCommand(
            aggregate_id="action-123",
            request_id="req-456",
            result={"execution_id": "exec-789"},
        )

        event = handler.handle(command, events)

        assert event.event_type == LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"]

    def test_commit_command_requires_approved_state(self):
        """Test that commit fails if not in APPROVED state."""
        handler = CommandHandler()
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
        ]

        command = CommitActionCommand(
            aggregate_id="action-123",
            request_id="req-456",
        )

        with pytest.raises(InvalidTransitionError):
            handler.handle(command, events)

    def test_fail_command(self):
        """Test failing an action."""
        handler = CommandHandler()
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            ),
        ]

        command = FailActionCommand(
            aggregate_id="action-123",
            request_id="req-456",
            error="Execution failed",
            error_code="EXEC_FAILED",
        )

        event = handler.handle(command, events)

        assert event.event_type == LIFECYCLE_EVENT_TYPES["ACTION_FAILED"]
        assert event.payload["error"] == "Execution failed"

    def test_command_on_empty_events_fails(self):
        """Test that command on aggregate with no events fails."""
        handler = CommandHandler()

        command = ApproveActionCommand(
            aggregate_id="action-123",
            request_id="req-456",
        )

        with pytest.raises(CommandError):
            handler.handle(command, [])

    def test_command_on_terminal_state_fails(self):
        """Test that command on terminal state fails."""
        handler = CommandHandler()
        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],
            ),
        ]

        command = ApproveActionCommand(
            aggregate_id="action-123",
            request_id="req-456",
        )

        with pytest.raises(InvalidTransitionError):
            handler.handle(command, events)


# ────────────────────────────────────────────────────────────────────────────
# Migration Layer Tests
# ────────────────────────────────────────────────────────────────────────────


class TestLifecycleMigrationLayer:
    """Test hybrid FSM + Event sourcing mode."""

    def test_consistency_check_matching_states(self):
        """Test consistency check when FSM and events match."""
        store = InMemoryEventStore()
        migration = LifecycleMigrationLayer(store)

        # Setup: single PROPOSED event
        event = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )
        store.append(event)

        # Check consistency
        report = migration.verify_state_consistency(
            aggregate_id="action-123",
            fsm_state=LifecycleState.PROPOSED.value,
        )

        assert report.is_consistent is True
        assert report.fsm_state == LifecycleState.PROPOSED.value
        assert report.event_derived_state == LifecycleState.PROPOSED

    def test_consistency_check_divergent_states(self):
        """Test consistency check when states diverge."""
        store = InMemoryEventStore()
        migration = LifecycleMigrationLayer(store)

        # Setup: events say "approved"
        event = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )
        store.append(event)

        # Check consistency - FSM says proposed but events say approved
        report = migration.verify_state_consistency(
            aggregate_id="action-123",
            fsm_state=LifecycleState.PROPOSED.value,
            fail_on_divergence=False,
        )

        assert report.is_consistent is True

    def test_divergence_detection_raises_error(self):
        """Test that divergence detection raises error when configured."""
        store = InMemoryEventStore()
        migration = LifecycleMigrationLayer(store)

        events = [
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            ),
            DomainEvent.create(
                aggregate_id="action-123",
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            ),
        ]

        for event in events:
            store.append(event)

        # FSM says proposed but events say approved
        with pytest.raises(DivergenceDetected):
            migration.verify_state_consistency(
                aggregate_id="action-123",
                fsm_state=LifecycleState.PROPOSED.value,
                fail_on_divergence=True,
            )

    def test_migration_stats_tracking(self):
        """Test migration layer statistics."""
        store = InMemoryEventStore()
        migration = LifecycleMigrationLayer(store)

        event = DomainEvent.create(
            aggregate_id="action-123",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )
        store.append(event)

        # Check consistency multiple times
        for _ in range(3):
            migration.verify_state_consistency(
                aggregate_id="action-123",
                fsm_state=LifecycleState.PROPOSED.value,
            )

        stats = migration.get_migration_stats()

        assert stats["consistent_checks"] == 3
        assert stats["divergence_detections"] == 0


# ────────────────────────────────────────────────────────────────────────────
# Integration Tests
# ────────────────────────────────────────────────────────────────────────────


class TestFullLifecycleWorkflows:
    """Test complete workflows end-to-end."""

    def test_full_workflow_proposed_to_committed(self):
        """Test complete workflow: propose → approve → commit."""
        store = InMemoryEventStore()
        handler = CommandHandler()

        action_id = "action-123"

        # Step 1: Create initial PROPOSED event
        proposed_event = DomainEvent.create(
            aggregate_id=action_id,
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            metadata={"request_id": "req-1"},
        )
        store.append(proposed_event)

        # Verify state
        events = store.get_events(action_id)
        assert reduce_state(events) == LifecycleState.PROPOSED

        # Step 2: Approve action
        approve_cmd = ApproveActionCommand(
            aggregate_id=action_id,
            request_id="req-1",
            reason="User approved",
        )
        approve_event = handler.handle(approve_cmd, events)
        store.append(approve_event)

        # Verify state
        events = store.get_events(action_id)
        assert reduce_state(events) == LifecycleState.APPROVED

        # Step 3: Commit action
        commit_cmd = CommitActionCommand(
            aggregate_id=action_id,
            request_id="req-1",
            result={"success": True},
        )
        commit_event = handler.handle(commit_cmd, events)
        store.append(commit_event)

        # Verify final state
        events = store.get_events(action_id)
        assert reduce_state(events) == LifecycleState.COMMITTED
        assert len(events) == 3

    def test_full_workflow_rejection(self):
        """Test workflow with rejection path."""
        store = InMemoryEventStore()
        handler = CommandHandler()

        action_id = "action-456"

        # Propose
        proposed_event = DomainEvent.create(
            aggregate_id=action_id,
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )
        store.append(proposed_event)

        # Reject
        events = store.get_events(action_id)
        reject_cmd = RejectActionCommand(
            aggregate_id=action_id,
            request_id="req-2",
            reason="Not now",
        )
        reject_event = handler.handle(reject_cmd, events)
        store.append(reject_event)

        # Verify final state
        events = store.get_events(action_id)
        assert reduce_state(events) == LifecycleState.REJECTED

    def test_migration_mode_consistency_throughout_workflow(self):
        """Test that hybrid mode maintains consistency throughout workflow."""
        store = InMemoryEventStore()
        migration = LifecycleMigrationLayer(store)
        handler = CommandHandler()

        action_id = "action-789"

        # Start workflow
        proposed = DomainEvent.create(
            aggregate_id=action_id,
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )
        store.append(proposed)

        # Check consistency after each step
        report1 = migration.verify_state_consistency(action_id, "proposed")
        assert report1.is_consistent is True

        # Approve
        events = store.get_events(action_id)
        approve_cmd = ApproveActionCommand(action_id, "req-456")
        approve_evt = handler.handle(approve_cmd, events)
        store.append(approve_evt)

        report2 = migration.verify_state_consistency(action_id, "approved")
        assert report2.is_consistent is True

        # Commit
        events = store.get_events(action_id)
        commit_cmd = CommitActionCommand(action_id, "req-456")
        commit_evt = handler.handle(commit_cmd, events)
        store.append(commit_evt)

        report3 = migration.verify_state_consistency(action_id, "committed")
        assert report3.is_consistent is True

        # Verify migration stats
        stats = migration.get_migration_stats()
        assert stats["consistent_checks"] == 3
        assert stats["divergence_detections"] == 0


def test_reducer_always_returns_enum():
    events = [
        DomainEvent.create(
            aggregate_id="invariant-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        )
    ]
    state = reduce_state(events)
    assert isinstance(state, LifecycleState)


def test_event_replay_returns_enum():
    events = [
        DomainEvent.create(
            aggregate_id="invariant-002",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        ),
        DomainEvent.create(
            aggregate_id="invariant-002",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
        ),
    ]
    state = replay_events(events)
    assert isinstance(state, LifecycleState)


def test_historical_replay_strict():
    legacy_event_stream = [
        DomainEvent(
            event_id=str(uuid4()),
            aggregate_id="legacy-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            timestamp=datetime.now(UTC),
            payload={"state": LifecycleState.PROPOSED.value},
            metadata={},
        ),
        DomainEvent(
            event_id=str(uuid4()),
            aggregate_id="legacy-001",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],
            timestamp=datetime.now(UTC),
            payload={"state": "executed"},
            metadata={},
        ),
    ]
    with pytest.raises(ValueError):
        replay_events(legacy_event_stream)


def test_invalid_state_rejected():
    with pytest.raises(ValueError):
        parse_lifecycle_state("executed")
