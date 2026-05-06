from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from decision_card_system.registry import (
    DECISION_CARD_ACKNOWLEDGED_EVENT_CANONICAL,
    DECISION_CARD_APPLIED_EVENT_CANONICAL,
    DECISION_CARD_GENERATED_EVENT_CANONICAL,
    DECISION_CARD_RESOLVED_EVENT_CANONICAL,
    DECISION_CARD_SURFACED_EVENT_CANONICAL,
    DecisionCardInvariantError,
    createDecisionCard,
    reduce_decision_card_projection,
)
from core.replay.event_replay_engine import ReplayValidationError, replay

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate]


def _event(
    *,
    event_id: str,
    event_type: str,
    timestamp: datetime,
    payload: dict[str, object],
    household_id: str = "household-decision-card",
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "household_id": household_id,
        "timestamp": timestamp,
        "payload": payload,
        "source": "tests.decision-card",
    }


def test_create_decision_card_is_stable_for_same_root_cause_and_contract() -> None:
    first = createDecisionCard(
        household_id="home-1",
        root_cause_key="Calendar conflict: dentist overlap",
        title="Resolve dentist overlap",
        actor_id="user-1",
        timestamp="2042-05-01T10:00:00Z",
    )
    second = createDecisionCard(
        household_id="home-1",
        root_cause_key="calendar conflict dentist overlap",
        title="Resolve dentist overlap",
        actor_id="user-1",
        timestamp="2042-05-01T10:00:01Z",
    )

    assert first["dedupe_key"] == second["dedupe_key"]
    assert first["decision_card_id"] == second["decision_card_id"]


def test_registry_enforces_single_unresolved_card_per_root_cause() -> None:
    base_cards = reduce_decision_card_projection(
        event_type=DECISION_CARD_GENERATED_EVENT_CANONICAL,
        payload=createDecisionCard(
            household_id="home-2",
            root_cause_key="meal-time-tradeoff",
            title="Choose meal or workout",
            actor_id="user-2",
            timestamp="2042-05-01T11:00:00Z",
            decision_card_id="dc-meal-1",
        ),
        recorded_at="2042-05-01T11:00:00Z",
        decision_cards={},
        strict=True,
    )

    duplicate_payload = createDecisionCard(
        household_id="home-2",
        root_cause_key="meal-time-tradeoff",
        title="Choose meal or workout",
        actor_id="user-2",
        timestamp="2042-05-01T11:05:00Z",
        decision_card_id="dc-meal-2",
    )

    with pytest.raises(DecisionCardInvariantError, match="duplicate unresolved decision card"):
        reduce_decision_card_projection(
            event_type=DECISION_CARD_GENERATED_EVENT_CANONICAL,
            payload=duplicate_payload,
            recorded_at="2042-05-01T11:05:00Z",
            decision_cards=base_cards,
            strict=True,
        )


def test_replay_decision_card_lifecycle_is_deterministic() -> None:
    t0 = datetime(2042, 5, 1, 12, 0, tzinfo=UTC)
    events = [
        _event(
            event_id="dc1",
            event_type=DECISION_CARD_GENERATED_EVENT_CANONICAL,
            timestamp=t0,
            payload=createDecisionCard(
                household_id="household-decision-card",
                root_cause_key="calendar_overlap",
                title="Resolve overlapping appointment",
                actor_id="user-a",
                timestamp=t0,
                decision_card_id="dc-100",
            ),
        ),
        _event(
            event_id="dc2",
            event_type=DECISION_CARD_SURFACED_EVENT_CANONICAL,
            timestamp=t0 + timedelta(seconds=1),
            payload={
                "decision_card_id": "dc-100",
                "actor_id": "user-a",
                "timestamp": "2042-05-01T12:00:01Z",
            },
        ),
        _event(
            event_id="dc3",
            event_type=DECISION_CARD_ACKNOWLEDGED_EVENT_CANONICAL,
            timestamp=t0 + timedelta(seconds=2),
            payload={
                "decision_card_id": "dc-100",
                "actor_id": "user-a",
                "timestamp": "2042-05-01T12:00:02Z",
            },
        ),
        _event(
            event_id="dc4",
            event_type=DECISION_CARD_RESOLVED_EVENT_CANONICAL,
            timestamp=t0 + timedelta(seconds=3),
            payload={
                "decision_card_id": "dc-100",
                "actor_id": "user-a",
                "timestamp": "2042-05-01T12:00:03Z",
            },
        ),
        _event(
            event_id="dc5",
            event_type=DECISION_CARD_APPLIED_EVENT_CANONICAL,
            timestamp=t0 + timedelta(seconds=4),
            payload={
                "decision_card_id": "dc-100",
                "actor_id": "user-a",
                "timestamp": "2042-05-01T12:00:04Z",
            },
        ),
    ]

    first = replay(events)
    second = replay(events)

    assert first == second
    assert first["derived_state"]["decision_cards"]["dc-100"]["state"] == "applied"


def test_replay_rejects_direct_generated_to_applied_transition() -> None:
    t0 = datetime(2042, 5, 1, 13, 0, tzinfo=UTC)
    events = [
        _event(
            event_id="dc10",
            event_type=DECISION_CARD_GENERATED_EVENT_CANONICAL,
            timestamp=t0,
            payload=createDecisionCard(
                household_id="household-decision-card",
                root_cause_key="inventory_gap",
                title="Resolve pantry inventory gap",
                actor_id="user-b",
                timestamp=t0,
                decision_card_id="dc-200",
            ),
        ),
        _event(
            event_id="dc11",
            event_type=DECISION_CARD_APPLIED_EVENT_CANONICAL,
            timestamp=t0 + timedelta(seconds=1),
            payload={
                "decision_card_id": "dc-200",
                "actor_id": "user-b",
                "timestamp": "2042-05-01T13:00:01Z",
            },
        ),
    ]

    with pytest.raises(ReplayValidationError, match="invalid lifecycle transition"):
        replay(events)


def test_replay_rejects_decision_completed_before_card_resolved() -> None:
    t0 = datetime(2042, 5, 1, 14, 0, tzinfo=UTC)
    events = [
        _event(
            event_id="dc20",
            event_type=DECISION_CARD_GENERATED_EVENT_CANONICAL,
            timestamp=t0,
            payload=createDecisionCard(
                household_id="household-decision-card",
                root_cause_key="school-dropoff-overlap",
                title="Resolve school dropoff overlap",
                actor_id="user-c",
                timestamp=t0,
                decision_card_id="dc-300",
            ),
        ),
        _event(
            event_id="dc21",
            event_type=DECISION_CARD_SURFACED_EVENT_CANONICAL,
            timestamp=t0 + timedelta(seconds=1),
            payload={
                "decision_card_id": "dc-300",
                "actor_id": "user-c",
                "timestamp": "2042-05-01T14:00:01Z",
            },
        ),
        _event(
            event_id="dc22",
            event_type=DECISION_CARD_ACKNOWLEDGED_EVENT_CANONICAL,
            timestamp=t0 + timedelta(seconds=2),
            payload={
                "decision_card_id": "dc-300",
                "actor_id": "user-c",
                "timestamp": "2042-05-01T14:00:02Z",
            },
        ),
        _event(
            event_id="dc23",
            event_type="DecisionCompleted",
            timestamp=t0 + timedelta(seconds=3),
            payload={
                "decision_id": "dc-300",
                "actor_id": "user-c",
                "timestamp": "2042-05-01T14:00:03Z",
            },
        ),
    ]

    with pytest.raises(ReplayValidationError, match="invalid lifecycle transition"):
        replay(events)
