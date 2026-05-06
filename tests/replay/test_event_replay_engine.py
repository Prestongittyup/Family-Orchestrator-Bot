from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from app.services.commands.runtime import CommandActor, CommandRuntimeService
from decision_card_system.registry import createDecisionCard
from core.replay.event_replay_engine import (
    ReplayValidationError,
    rebuild_fsm,
    replay,
    validate_replay,
)
from household_os.runtime.event_router import CanonicalEventEnvelope


# FEATURE_INTAKE:
#   projection_impact: yes
#   read_model_impact: no
#   kernel_interaction: none
FEATURE_INTAKE_DECLARATION = {
    "projection_impact": "yes",
    "read_model_impact": "no",
    "kernel_interaction": "none",
}


pytestmark = [pytest.mark.replay]


def _event(
    *,
    event_id: str,
    event_type: str,
    timestamp: datetime,
    payload: dict[str, Any],
    household_id: str = "household-1",
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "household_id": household_id,
        "timestamp": timestamp,
        "payload": payload,
        "source": "tests.replay",
    }


def _decision_card_generated_payload(
    *,
    household_id: str,
    decision_card_id: str,
    root_cause_key: str,
    title: str,
    actor_id: str,
    timestamp: datetime,
) -> tuple[str, dict[str, Any]]:
    payload = createDecisionCard(
        household_id=household_id,
        root_cause_key=root_cause_key,
        title=title,
        actor_id=actor_id,
        timestamp=timestamp,
        decision_card_id=decision_card_id,
    )
    event_type = str(payload.pop("event_type"))
    return event_type, payload


def _assert_replay_runs_identical(
    *,
    events: list[dict[str, Any]] | list[Any],
    runs: int = 5,
) -> dict[str, Any]:
    assert runs >= 5
    replay_runs = [replay(events) for _ in range(runs)]
    baseline = replay_runs[0]
    for replayed in replay_runs[1:]:
        assert replayed == baseline
    return baseline


def _assert_projection_reads_identical(
    *,
    runtime: CommandRuntimeService,
    household_id: str,
    force_replay: bool = False,
    runs: int = 5,
) -> dict[str, Any]:
    assert runs >= 5
    projection_runs = [
        runtime.get_projection(household_id, force_replay=force_replay)
        for _ in range(runs)
    ]
    baseline = projection_runs[0]
    for projection in projection_runs[1:]:
        assert projection == baseline
    return baseline


def _events_for_household(event_log: _InMemoryEventLogService, *, household_id: str) -> list[_FakeEventRow]:
    return [row for row in event_log.insert_order if row.household_id == household_id]


@pytest.mark.ci_gate
def test_event_replay_happy_path() -> None:
    t0 = datetime(2026, 4, 29, 10, 0, tzinfo=UTC)
    events = [
        _event(
            event_id="e1",
            event_type="task.rules_evaluated",
            timestamp=t0,
            payload={"request_id": "req-1", "rules_passed": True},
        ),
        _event(
            event_id="e2",
            event_type="task.risk_assessed",
            timestamp=t0 + timedelta(seconds=1),
            payload={"request_id": "req-1", "risk": {"level": "low"}},
        ),
        _event(
            event_id="e3",
            event_type="task.fsm_transitioned",
            timestamp=t0 + timedelta(seconds=2),
            payload={
                "request_id": "req-1",
                "task_id": "task-1",
                "current_state": "created",
                "transitions": [
                    {"index": 1, "from_state": "received", "to_state": "rules_passed"},
                    {"index": 2, "from_state": "rules_passed", "to_state": "risk_assessed"},
                    {"index": 3, "from_state": "risk_assessed", "to_state": "created"},
                ],
            },
        ),
        _event(
            event_id="e4",
            event_type="task.created",
            timestamp=t0 + timedelta(seconds=3),
            payload={
                "request_id": "req-1",
                "task": {
                    "task_id": "task-1",
                    "request_id": "req-1",
                    "title": "Buy groceries",
                    "lifecycle_state": "created",
                },
                "response": {
                    "request_id": "req-1",
                    "task": {"task_id": "task-1", "title": "Buy groceries"},
                },
            },
        ),
    ]

    result = _assert_replay_runs_identical(events=events)

    assert result["event_count"] == 4
    assert result["derived_state"]["tasks"]["task-1"]["title"] == "Buy groceries"
    assert result["fsm_state"]["tasks"]["task-1"]["current_state"] == "created"


@pytest.mark.reliability
def test_event_replay_out_of_order_events() -> None:
    t0 = datetime(2026, 4, 29, 10, 0, tzinfo=UTC)
    events = [
        _event(
            event_id="e1",
            event_type="task.rules_evaluated",
            timestamp=t0 + timedelta(seconds=2),
            payload={"request_id": "req-2", "rules_passed": True},
        ),
        _event(
            event_id="e2",
            event_type="task.risk_assessed",
            timestamp=t0 + timedelta(seconds=1),
            payload={"request_id": "req-2", "risk": {"level": "low"}},
        ),
    ]

    with pytest.raises(ReplayValidationError, match="Out-of-order timestamp"):
        replay(events)


@pytest.mark.ci_gate
def test_event_replay_missing_event_fails() -> None:
    t0 = datetime(2026, 4, 29, 10, 0, tzinfo=UTC)
    events = [
        _event(
            event_id="e1",
            event_type="task.rules_evaluated",
            timestamp=t0,
            payload={"request_id": "req-3", "rules_passed": True},
        ),
        _event(
            event_id="e2",
            event_type="task.fsm_transitioned",
            timestamp=t0 + timedelta(seconds=1),
            payload={
                "request_id": "req-3",
                "task_id": "task-missing",
                "current_state": "created",
                "transitions": [
                    {"index": 1, "from_state": "received", "to_state": "rules_passed"},
                    {"index": 2, "from_state": "rules_passed", "to_state": "risk_assessed"},
                    {"index": 3, "from_state": "risk_assessed", "to_state": "created"},
                ],
            },
        ),
    ]

    with pytest.raises(ReplayValidationError, match="Missing prerequisite task.risk_assessed"):
        replay(events)


@dataclass
class _FakeEventRow:
    event_id: str
    household_id: str
    timestamp: datetime
    type: str
    payload: dict[str, Any]


class _InMemoryEventLogService:
    def __init__(self) -> None:
        self.insert_order: list[_FakeEventRow] = []

    def append_envelope(self, envelope: CanonicalEventEnvelope) -> None:
        self.insert_order.append(
            _FakeEventRow(
                event_id=envelope.event_id,
                household_id=envelope.household_id,
                timestamp=envelope.timestamp,
                type=envelope.event_type,
                payload=dict(envelope.payload),
            )
        )

    def get_event_logs(
        self,
        *,
        household_id: str,
        user_id: str | None = None,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[_FakeEventRow]:
        rows = [row for row in self.insert_order if row.household_id == household_id]
        if event_type:
            rows = [row for row in rows if row.type == event_type]

        ordered = sorted(rows, key=lambda row: (row.timestamp, row.event_id), reverse=True)
        return ordered[: max(1, int(limit))]


class _InMemoryRouterService:
    def __init__(self, event_log: _InMemoryEventLogService) -> None:
        self._event_log = event_log

    def route(
        self,
        envelope: CanonicalEventEnvelope,
        *,
        persist: bool = True,
        dispatch: bool = True,
    ) -> dict[str, Any] | None:
        if persist:
            self._event_log.append_envelope(envelope)
        if not dispatch:
            return None
        return {"status": "persisted", "event_id": envelope.event_id}


@pytest.mark.migration
def test_replay_matches_runtime_state() -> None:
    event_log = _InMemoryEventLogService()
    runtime = CommandRuntimeService(
        router_service=_InMemoryRouterService(event_log),
        event_log_service=event_log,
    )

    household_id = "household-runtime"
    actor = CommandActor(actor_type="api_user", user_id="user-1")

    runtime.handle_command(
        command_type="task.create",
        household_id=household_id,
        actor=actor,
        payload={"title": "interleave-a one", "priority": "medium"},
        source="tests.replay",
    )

    projection_after_first_a = _assert_projection_reads_identical(
        runtime=runtime,
        household_id=household_id,
    )
    tasks_after_first_a = dict(projection_after_first_a.get("tasks", {}))

    runtime.handle_command(
        command_type="schedule.create",
        household_id=household_id,
        actor=actor,
        payload={
            "title": "interleave-b schedule",
            "start_at": "2036-02-01T08:00:00Z",
            "end_at": "2036-02-01T09:00:00Z",
        },
        source="tests.replay",
    )

    projection_after_b = _assert_projection_reads_identical(
        runtime=runtime,
        household_id=household_id,
    )
    assert projection_after_b.get("tasks", {}) == tasks_after_first_a

    runtime.handle_command(
        command_type="task.create",
        household_id=household_id,
        actor=actor,
        payload={"title": "interleave-a two", "priority": "medium"},
        source="tests.replay",
    )

    projection_after_second_a_cached = _assert_projection_reads_identical(
        runtime=runtime,
        household_id=household_id,
    )
    projection_after_second_a_recomputed = _assert_projection_reads_identical(
        runtime=runtime,
        household_id=household_id,
        force_replay=True,
    )
    assert projection_after_second_a_cached == projection_after_second_a_recomputed
    assert len(projection_after_second_a_recomputed.get("tasks", {})) == len(tasks_after_first_a) + 1

    runtime_ordered_events = [
        row.event_id
        for row in event_log.get_event_logs(household_id=household_id, limit=100)
    ]
    runtime_ordered_events_repeat = [
        row.event_id
        for row in event_log.get_event_logs(household_id=household_id, limit=100)
    ]
    assert runtime_ordered_events == runtime_ordered_events_repeat

    replayed = _assert_replay_runs_identical(events=event_log.insert_order)
    runtime_insert_order_ids = [row.event_id for row in event_log.insert_order]
    assert replayed["event_count"] == len(runtime_insert_order_ids)
    assert replayed["last_event_id"] == runtime_insert_order_ids[-1]

    live_projection = runtime.get_projection(household_id, force_replay=True)

    comparison = validate_replay(live_projection, replayed["derived_state"])
    assert comparison["matches"] is True
    assert comparison["drift"] == {
        "structural": False,
        "integrity": False,
        "causal": False,
    }
    assert live_projection == replayed["derived_state"]


@pytest.mark.ci_gate
def test_household_message_ingest_and_promotion_replay_equivalence() -> None:
    event_log = _InMemoryEventLogService()
    runtime = CommandRuntimeService(
        router_service=_InMemoryRouterService(event_log),
        event_log_service=event_log,
    )

    household_id = "household-message-replay"
    actor = CommandActor(actor_type="api_user", user_id="member-1")

    result = runtime.handle_command(
        command_type="household.message.ingest",
        household_id=household_id,
        actor=actor,
        payload={
            "source": "manual",
            "raw_content": "FYI pantry staples are fully stocked.",
            "created_at": "2026-05-05T09:00:00Z",
            "member_id": "member-1",
        },
        source="tests.replay",
    )

    assert result["status"] == "accepted"

    projection = runtime.get_projection(household_id, force_replay=True)
    replayed = replay(event_log.insert_order)
    replay_projection = replayed["derived_state"]

    assert projection == replay_projection
    assert len(projection["household_messages"]) == 1
    assert len(projection["household_promotions"]) == 1
    assert projection["household_promotions"][0]["source_message_id"] == projection["household_messages"][0]["message_id"]
    assert projection["household_promotions"][0]["promotion_target"] == "ignore"


@pytest.mark.reliability
def test_household_message_adversarial_stream_replay_equivalence() -> None:
    event_log = _InMemoryEventLogService()
    runtime = CommandRuntimeService(
        router_service=_InMemoryRouterService(event_log),
        event_log_service=event_log,
    )

    household_id = "household-message-adversarial-replay"
    actor = CommandActor(actor_type="api_user", user_id="member-adversarial")

    runtime.handle_command(
        command_type="schedule.create",
        household_id=household_id,
        actor=actor,
        payload={
            "schedule_id": "sched-adversarial-1",
            "title": "Soccer practice Tuesday",
            "start_at": "2026-05-12T17:00:00Z",
            "end_at": "2026-05-12T18:00:00Z",
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="schedule.create",
        household_id=household_id,
        actor=actor,
        payload={
            "schedule_id": "sched-adversarial-2",
            "title": "School pickup",
            "start_at": "2026-05-05T15:00:00Z",
            "end_at": "2026-05-05T16:00:00Z",
        },
        source="tests.replay",
    )

    inputs = [
        "Soccer practice cancelled Tuesday",
        "Game moved from 5pm to 7pm",
        "Pay $15 for picture day by Friday",
        "New meeting scheduled at 3pm",
        "Something changed for tomorrow",
        "Weekly newsletter update",
    ]

    for index, raw_content in enumerate(inputs):
        result = runtime.handle_command(
            command_type="household.message.ingest",
            household_id=household_id,
            actor=actor,
            payload={
                "source": "manual",
                "raw_content": raw_content,
                "created_at": f"2026-05-05T0{index}:00:00Z",
                "member_id": "member-adversarial",
                "message_id": f"msg-adversarial-{index}",
            },
            source="tests.replay",
        )
        assert result["status"] == "accepted"
        response = result.get("response")
        assert isinstance(response, dict)
        assert str(response.get("interpretation_type") or "").strip()
        assert str(response.get("promotion_reason") or "").strip()

    projection = runtime.get_projection(household_id, force_replay=True)
    replayed = _assert_replay_runs_identical(events=event_log.insert_order)
    replay_projection = replayed["derived_state"]

    assert projection == replay_projection
    promotions = projection.get("household_promotions")
    assert isinstance(promotions, list)
    assert len(promotions) == len(inputs)
    assert any(str(item.get("promotion_target") or "") == "calendar_update" for item in promotions)
    assert any(str(item.get("promotion_target") or "") == "decision" for item in promotions)
    assert all(str(item.get("interpretation_type") or "").strip() for item in promotions)
    assert all(str(item.get("promotion_reason") or "").strip() for item in promotions)


@pytest.mark.ci_gate
def test_fsm_rebuild_from_event_stream() -> None:
    t0 = datetime(2026, 4, 29, 10, 0, tzinfo=UTC)
    events = [
        _event(
            event_id="a1",
            event_type="assistant.response_proposed",
            timestamp=t0,
            payload={
                "request_id": "req-4",
                "response": {
                    "request_id": "req-4",
                    "recommended_action": {
                        "action_id": "action-1",
                        "approval_status": "pending",
                    },
                },
            },
        ),
        _event(
            event_id="a2",
            event_type="assistant.action_approved",
            timestamp=t0 + timedelta(seconds=1),
            payload={"request_id": "req-4", "action_ids": ["action-1"]},
        ),
        _event(
            event_id="a3",
            event_type="assistant.action_executed",
            timestamp=t0 + timedelta(seconds=2),
            payload={"request_id": "req-4", "action_id": "action-1"},
        ),
        _event(
            event_id="t1",
            event_type="task.rules_evaluated",
            timestamp=t0 + timedelta(seconds=3),
            payload={"request_id": "req-4-task", "rules_passed": True},
        ),
        _event(
            event_id="t2",
            event_type="task.risk_assessed",
            timestamp=t0 + timedelta(seconds=4),
            payload={"request_id": "req-4-task", "risk": {"level": "high"}},
        ),
        _event(
            event_id="t3",
            event_type="task.fsm_transitioned",
            timestamp=t0 + timedelta(seconds=5),
            payload={
                "request_id": "req-4-task",
                "task_id": "task-4",
                "current_state": "pending_approval",
                "transitions": [
                    {"index": 1, "from_state": "received", "to_state": "rules_passed"},
                    {"index": 2, "from_state": "rules_passed", "to_state": "risk_assessed"},
                    {"index": 3, "from_state": "risk_assessed", "to_state": "pending_approval"},
                ],
            },
        ),
    ]

    rebuilt = rebuild_fsm(events)

    assert rebuilt["actions"]["action-1"]["current_state"] == "committed"
    assert rebuilt["tasks"]["task-4"]["current_state"] == "pending_approval"


@pytest.mark.migration
def test_replay_consistency_for_interleaved_household_streams() -> None:
    event_log = _InMemoryEventLogService()
    runtime = CommandRuntimeService(
        router_service=_InMemoryRouterService(event_log),
        event_log_service=event_log,
    )
    actor = CommandActor(actor_type="api_user", user_id="user-1")

    household_a = "household-interleaved-replay-a"
    household_b = "household-interleaved-replay-b"

    runtime.handle_command(
        command_type="task.create",
        household_id=household_a,
        actor=actor,
        payload={"title": "A1", "priority": "medium"},
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="task.create",
        household_id=household_b,
        actor=actor,
        payload={"title": "B1", "priority": "medium"},
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="schedule.create",
        household_id=household_a,
        actor=actor,
        payload={
            "title": "A2",
            "start_at": "2038-01-01T08:00:00Z",
            "end_at": "2038-01-01T09:00:00Z",
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="schedule.create",
        household_id=household_b,
        actor=actor,
        payload={
            "title": "B2",
            "start_at": "2038-01-02T08:00:00Z",
            "end_at": "2038-01-02T09:00:00Z",
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="task.create",
        household_id=household_a,
        actor=actor,
        payload={"title": "A3", "priority": "low"},
        source="tests.replay",
    )

    events_a = _events_for_household(event_log, household_id=household_a)
    events_b = _events_for_household(event_log, household_id=household_b)
    assert events_a
    assert events_b

    ids_a = [row.event_id for row in events_a]
    ids_b = [row.event_id for row in events_b]

    assert ids_a == [row.event_id for row in _events_for_household(event_log, household_id=household_a)]
    assert ids_b == [row.event_id for row in _events_for_household(event_log, household_id=household_b)]

    assert all(events_a[index].timestamp <= events_a[index + 1].timestamp for index in range(len(events_a) - 1))
    assert all(events_b[index].timestamp <= events_b[index + 1].timestamp for index in range(len(events_b) - 1))

    replayed_a = _assert_replay_runs_identical(events=events_a)
    replayed_b = _assert_replay_runs_identical(events=events_b)

    runtime_projection_a = _assert_projection_reads_identical(
        runtime=runtime,
        household_id=household_a,
        force_replay=True,
    )
    runtime_projection_b = _assert_projection_reads_identical(
        runtime=runtime,
        household_id=household_b,
        force_replay=True,
    )

    assert replayed_a["derived_state"] == runtime_projection_a
    assert replayed_b["derived_state"] == runtime_projection_b
    assert replayed_a["last_event_id"] == ids_a[-1]
    assert replayed_b["last_event_id"] == ids_b[-1]

    validation_a = validate_replay(runtime_projection_a, replayed_a["derived_state"])
    validation_b = validate_replay(runtime_projection_b, replayed_b["derived_state"])
    assert validation_a["matches"] is True
    assert validation_b["matches"] is True


@pytest.mark.ci_gate
def test_family_coordination_surface_event_model_projection() -> None:
    event_log = _InMemoryEventLogService()
    runtime = CommandRuntimeService(
        router_service=_InMemoryRouterService(event_log),
        event_log_service=event_log,
    )
    actor = CommandActor(actor_type="api_user", user_id="coordinator-user")
    household_id = "household-surface-1"

    runtime.handle_command(
        command_type="household.member.add",
        household_id=household_id,
        actor=actor,
        payload={
            "member_id": "member-1",
            "display_name": "Alex",
            "role": "parent",
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="household.member.update",
        household_id=household_id,
        actor=actor,
        payload={
            "member_id": "member-1",
            "timezone": "America/New_York",
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="household.responsibility.create",
        household_id=household_id,
        actor=actor,
        payload={
            "responsibility_id": "resp-1",
            "title": "School pickup",
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="household.responsibility.assign",
        household_id=household_id,
        actor=actor,
        payload={
            "responsibility_id": "resp-1",
            "assigned_member_ids": ["member-1"],
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="household.responsibility.update",
        household_id=household_id,
        actor=actor,
        payload={
            "responsibility_id": "resp-1",
            "priority": "high",
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="household.event.schedule",
        household_id=household_id,
        actor=actor,
        payload={
            "coordination_event_id": "coord-1",
            "responsibility_id": "resp-1",
            "title": "Pickup run",
            "start_at": "2039-01-01T15:00:00Z",
            "end_at": "2039-01-01T16:00:00Z",
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="household.event.reschedule",
        household_id=household_id,
        actor=actor,
        payload={
            "coordination_event_id": "coord-1",
            "responsibility_id": "resp-1",
            "title": "Pickup run",
            "start_at": "2039-01-01T16:00:00Z",
            "end_at": "2039-01-01T17:00:00Z",
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="household.execution.change",
        household_id=household_id,
        actor=actor,
        payload={
            "target_type": "responsibility",
            "target_id": "resp-1",
            "execution_state": "completed",
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="household.conflict.detect",
        household_id=household_id,
        actor=actor,
        payload={
            "conflict_id": "conflict-1",
            "conflict_type": "overlap",
            "severity": "medium",
            "related_entity_ids": ["coord-1"],
            "message": "Pickup overlaps another event",
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="household.conflict.resolve",
        household_id=household_id,
        actor=actor,
        payload={
            "conflict_id": "conflict-1",
            "resolution": "rescheduled",
        },
        source="tests.replay",
    )
    runtime.handle_command(
        command_type="household.event.cancel",
        household_id=household_id,
        actor=actor,
        payload={"coordination_event_id": "coord-1"},
        source="tests.replay",
    )

    events_for_household = _events_for_household(event_log, household_id=household_id)
    emitted_types = [row.type for row in events_for_household]

    for expected_event_type in [
        "HouseholdMemberAdded",
        "HouseholdMemberUpdated",
        "ResponsibilityCreated",
        "ResponsibilityAssigned",
        "ResponsibilityUpdated",
        "EventScheduled",
        "EventRescheduled",
        "ExecutionStateChanged",
        "ConflictDetected",
        "ConflictResolved",
        "EventCancelled",
    ]:
        assert expected_event_type in emitted_types

    projection = runtime.get_projection(household_id, force_replay=True)
    surface_projection = projection.get("family_coordination_surface")
    assert isinstance(surface_projection, dict)
    assert "HouseholdStateProjection" in surface_projection
    assert "TodayViewProjection" in surface_projection
    assert "ConflictProjection" in surface_projection

    household_state_projection = surface_projection["HouseholdStateProjection"]
    assert household_state_projection["partition_id"] == household_id
    assert any(member["member_id"] == "member-1" for member in household_state_projection["members"])
    assert any(
        responsibility["responsibility_id"] == "resp-1"
        for responsibility in household_state_projection["responsibilities"]
    )

    conflict_projection = surface_projection["ConflictProjection"]
    assert conflict_projection["counts"]["resolved"] == 1


@pytest.mark.ci_gate
def test_replay_projects_decision_events_into_task_and_decision_state() -> None:
    t0 = datetime(2040, 1, 1, 8, 0, tzinfo=UTC)
    generated_event_type, generated_payload = _decision_card_generated_payload(
        household_id="household-1",
        decision_card_id="task-decision-1",
        root_cause_key="task:decision-replay:defer",
        title="Decision replay target",
        actor_id="user-decision",
        timestamp=t0 + timedelta(seconds=4),
    )
    events = [
        _event(
            event_id="d1",
            event_type="task.rules_evaluated",
            timestamp=t0,
            payload={"request_id": "req-decision", "rules_passed": True},
        ),
        _event(
            event_id="d2",
            event_type="task.risk_assessed",
            timestamp=t0 + timedelta(seconds=1),
            payload={"request_id": "req-decision", "risk": {"level": "low"}},
        ),
        _event(
            event_id="d3",
            event_type="task.fsm_transitioned",
            timestamp=t0 + timedelta(seconds=2),
            payload={
                "request_id": "req-decision",
                "task_id": "task-decision-1",
                "current_state": "created",
                "transitions": [
                    {"index": 1, "from_state": "received", "to_state": "rules_passed"},
                    {"index": 2, "from_state": "rules_passed", "to_state": "risk_assessed"},
                    {"index": 3, "from_state": "risk_assessed", "to_state": "created"},
                ],
            },
        ),
        _event(
            event_id="d4",
            event_type="task.created",
            timestamp=t0 + timedelta(seconds=3),
            payload={
                "request_id": "req-decision",
                "task": {
                    "task_id": "task-decision-1",
                    "request_id": "req-decision",
                    "title": "Decision replay target",
                    "lifecycle_state": "created",
                },
                "response": {
                    "request_id": "req-decision",
                    "task": {"task_id": "task-decision-1", "title": "Decision replay target"},
                },
            },
        ),
        _event(
            event_id="d5",
            event_type=generated_event_type,
            timestamp=t0 + timedelta(seconds=4),
            payload=generated_payload,
        ),
        _event(
            event_id="d6",
            event_type="DecisionCardSurfaced",
            timestamp=t0 + timedelta(seconds=5),
            payload={
                "decision_card_id": "task-decision-1",
                "actor_id": "user-decision",
                "timestamp": "2040-01-01T08:00:05Z",
            },
        ),
        _event(
            event_id="d7",
            event_type="DecisionCardAcknowledged",
            timestamp=t0 + timedelta(seconds=6),
            payload={
                "decision_card_id": "task-decision-1",
                "actor_id": "user-decision",
                "timestamp": "2040-01-01T08:00:06Z",
            },
        ),
        _event(
            event_id="d8",
            event_type="DecisionDeferred",
            timestamp=t0 + timedelta(seconds=7),
            payload={
                "decision_id": "task-decision-1",
                "actor_id": "user-decision",
                "defer_to_date": "2040-01-05",
                "timestamp": "2040-01-01T08:00:07Z",
            },
        ),
        _event(
            event_id="d9",
            event_type="DecisionCompleted",
            timestamp=t0 + timedelta(seconds=8),
            payload={
                "decision_id": "task-decision-1",
                "actor_id": "user-decision",
                "timestamp": "2040-01-01T08:00:08Z",
            },
        ),
    ]

    replayed = _assert_replay_runs_identical(events=events)
    tasks = replayed["derived_state"]["tasks"]
    decisions = replayed["derived_state"]["decisions"]

    assert tasks["task-decision-1"]["status"] == "completed"
    assert tasks["task-decision-1"]["lifecycle_state"] == "completed"
    assert decisions["task-decision-1"]["state"] == "completed"
    assert decisions["task-decision-1"]["actor_id"] == "user-decision"


@pytest.mark.ci_gate
def test_replay_rejects_duplicate_decision_completed_for_same_decision_id() -> None:
    t0 = datetime(2040, 1, 1, 8, 30, tzinfo=UTC)
    generated_event_type, generated_payload = _decision_card_generated_payload(
        household_id="household-1",
        decision_card_id="decision-dup-1",
        root_cause_key="task:decision-dup-1:complete",
        title="Duplicate complete guard",
        actor_id="user-1",
        timestamp=t0,
    )
    events = [
        _event(
            event_id="dc1",
            event_type=generated_event_type,
            timestamp=t0,
            payload=generated_payload,
        ),
        _event(
            event_id="dc2",
            event_type="DecisionCardSurfaced",
            timestamp=t0 + timedelta(seconds=1),
            payload={
                "decision_card_id": "decision-dup-1",
                "actor_id": "user-1",
                "timestamp": "2040-01-01T08:30:01Z",
            },
        ),
        _event(
            event_id="dc3",
            event_type="DecisionCardAcknowledged",
            timestamp=t0 + timedelta(seconds=2),
            payload={
                "decision_card_id": "decision-dup-1",
                "actor_id": "user-1",
                "timestamp": "2040-01-01T08:30:02Z",
            },
        ),
        _event(
            event_id="dc4",
            event_type="DecisionCardResolved",
            timestamp=t0 + timedelta(seconds=3),
            payload={
                "decision_card_id": "decision-dup-1",
                "actor_id": "user-1",
                "resolution_kind": "completed",
                "timestamp": "2040-01-01T08:30:03Z",
            },
        ),
        _event(
            event_id="dc5",
            event_type="DecisionCardApplied",
            timestamp=t0 + timedelta(seconds=4),
            payload={
                "decision_card_id": "decision-dup-1",
                "actor_id": "user-1",
                "resolution_kind": "completed",
                "timestamp": "2040-01-01T08:30:04Z",
            },
        ),
        _event(
            event_id="dc6",
            event_type="DecisionCompleted",
            timestamp=t0 + timedelta(seconds=5),
            payload={"decision_id": "decision-dup-1", "actor_id": "user-1", "timestamp": "2040-01-01T08:30:05Z"},
        ),
        _event(
            event_id="dc7",
            event_type="DecisionCompleted",
            timestamp=t0 + timedelta(seconds=6),
            payload={"decision_id": "decision-dup-1", "actor_id": "user-1", "timestamp": "2040-01-01T08:30:06Z"},
        ),
    ]

    with pytest.raises(ReplayValidationError, match="Duplicate DecisionCompleted"):
        replay(events)


@pytest.mark.ci_gate
def test_replay_requires_defer_to_date_for_decision_deferred() -> None:
    t0 = datetime(2040, 1, 1, 9, 0, tzinfo=UTC)
    generated_event_type, generated_payload = _decision_card_generated_payload(
        household_id="household-1",
        decision_card_id="decision-1",
        root_cause_key="task:decision-1:defer",
        title="Missing defer date guard",
        actor_id="user-1",
        timestamp=t0,
    )
    events = [
        _event(
            event_id="dd1",
            event_type=generated_event_type,
            timestamp=t0,
            payload=generated_payload,
        ),
        _event(
            event_id="dd2",
            event_type="DecisionCardSurfaced",
            timestamp=t0 + timedelta(seconds=1),
            payload={
                "decision_card_id": "decision-1",
                "actor_id": "user-1",
                "timestamp": "2040-01-01T09:00:01Z",
            },
        ),
        _event(
            event_id="dd3",
            event_type="DecisionCardAcknowledged",
            timestamp=t0 + timedelta(seconds=2),
            payload={
                "decision_card_id": "decision-1",
                "actor_id": "user-1",
                "timestamp": "2040-01-01T09:00:02Z",
            },
        ),
        _event(
            event_id="dd4",
            event_type="DecisionDeferred",
            timestamp=t0 + timedelta(seconds=3),
            payload={"decision_id": "decision-1", "actor_id": "user-1", "timestamp": "2040-01-01T09:00:00Z"},
        )
    ]

    with pytest.raises(ReplayValidationError, match="missing defer_to_date"):
        replay(events)
