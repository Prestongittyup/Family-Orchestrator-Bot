from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from decision_card_system.registry import createDecisionCard
from core.replay.event_replay_engine import replay


pytestmark = [pytest.mark.replay, pytest.mark.ci_gate]


def _event(
    *,
    event_id: str,
    event_type: str,
    timestamp: datetime,
    payload: dict[str, object],
    household_id: str = "household-replay-equivalence",
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "household_id": household_id,
        "payload": payload,
        "source": "tests.replay.test_decision_replay_equivalence",
    }


def _decision_event_stream() -> list[dict[str, object]]:
    household_id = "household-replay-equivalence"
    decision_id = "task-equivalence-1"
    actor_id = "user-equivalence"
    t0 = datetime(2040, 1, 3, 8, 0, tzinfo=UTC)

    generated_payload = createDecisionCard(
        household_id=household_id,
        root_cause_key="task:equivalence-1:conflict",
        title="Resolve replay equivalence decision",
        actor_id=actor_id,
        timestamp=t0 + timedelta(seconds=4),
        decision_card_id=decision_id,
    )

    generated_event_type = str(generated_payload.pop("event_type"))

    return [
        _event(
            event_id="eq-1",
            event_type="task.rules_evaluated",
            timestamp=t0,
            household_id=household_id,
            payload={"request_id": "req-equivalence-1", "rules_passed": True},
        ),
        _event(
            event_id="eq-2",
            event_type="task.risk_assessed",
            timestamp=t0 + timedelta(seconds=1),
            household_id=household_id,
            payload={"request_id": "req-equivalence-1", "risk": {"level": "low"}},
        ),
        _event(
            event_id="eq-3",
            event_type="task.fsm_transitioned",
            timestamp=t0 + timedelta(seconds=2),
            household_id=household_id,
            payload={
                "request_id": "req-equivalence-1",
                "task_id": decision_id,
                "current_state": "created",
                "transitions": [
                    {"index": 1, "from_state": "received", "to_state": "rules_passed"},
                    {"index": 2, "from_state": "rules_passed", "to_state": "risk_assessed"},
                    {"index": 3, "from_state": "risk_assessed", "to_state": "created"},
                ],
            },
        ),
        _event(
            event_id="eq-4",
            event_type="TaskCreated",
            timestamp=t0 + timedelta(seconds=3),
            household_id=household_id,
            payload={
                "request_id": "req-equivalence-1",
                "task": {
                    "task_id": decision_id,
                    "request_id": "req-equivalence-1",
                    "title": "Replay equivalence task",
                    "status": "pending",
                    "lifecycle_state": "created",
                },
            },
        ),
        _event(
            event_id="eq-5",
            event_type=generated_event_type,
            timestamp=t0 + timedelta(seconds=4),
            household_id=household_id,
            payload=generated_payload,
        ),
        _event(
            event_id="eq-6",
            event_type="DecisionCardSurfaced",
            timestamp=t0 + timedelta(seconds=5),
            household_id=household_id,
            payload={
                "decision_card_id": decision_id,
                "actor_id": actor_id,
                "timestamp": "2040-01-03T08:00:05Z",
            },
        ),
        _event(
            event_id="eq-7",
            event_type="DecisionCardAcknowledged",
            timestamp=t0 + timedelta(seconds=6),
            household_id=household_id,
            payload={
                "decision_card_id": decision_id,
                "actor_id": actor_id,
                "timestamp": "2040-01-03T08:00:06Z",
            },
        ),
        _event(
            event_id="eq-8",
            event_type="DecisionCardResolved",
            timestamp=t0 + timedelta(seconds=7),
            household_id=household_id,
            payload={
                "decision_card_id": decision_id,
                "actor_id": actor_id,
                "resolution_kind": "completed",
                "timestamp": "2040-01-03T08:00:07Z",
            },
        ),
        _event(
            event_id="eq-9",
            event_type="DecisionCardApplied",
            timestamp=t0 + timedelta(seconds=8),
            household_id=household_id,
            payload={
                "decision_card_id": decision_id,
                "actor_id": actor_id,
                "resolution_kind": "completed",
                "timestamp": "2040-01-03T08:00:08Z",
            },
        ),
        _event(
            event_id="eq-10",
            event_type="DecisionCompleted",
            timestamp=t0 + timedelta(seconds=9),
            household_id=household_id,
            payload={
                "decision_id": decision_id,
                "actor_id": actor_id,
                "timestamp": "2040-01-03T08:00:09Z",
            },
        ),
    ]


def test_replay_is_equivalent_for_identical_event_logs() -> None:
    events = _decision_event_stream()
    first = replay(events)
    second = replay(deepcopy(events))

    assert first == second
    assert first["replay_checksum"] == second["replay_checksum"]


def test_replay_output_does_not_depend_on_previous_runtime_memory() -> None:
    events = _decision_event_stream()
    baseline = replay(events)

    # Mutate a local copy to simulate external memory corruption after replay.
    corrupted = deepcopy(baseline)
    corrupted["derived_state"]["decision_cards"]["task-equivalence-1"]["state"] = "corrupted"

    rerun = replay(deepcopy(events))
    assert rerun["derived_state"]["decision_cards"]["task-equivalence-1"]["state"] == "applied"
    assert rerun["replay_checksum"] == baseline["replay_checksum"]
