from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import app


def _seed_schedule(
    client: TestClient,
    *,
    household_id: str,
    schedule_id: str,
    title: str,
    start_at: str,
    end_at: str,
) -> None:
    response = client.post(
        "/command",
        json={
            "command_type": "schedule.create",
            "household_id": household_id,
            "payload": {
                "schedule_id": schedule_id,
                "title": title,
                "start_at": start_at,
                "end_at": end_at,
            },
        },
    )
    assert response.status_code == 200, response.text


def _ingest_message(
    client: TestClient,
    *,
    household_id: str,
    raw_content: str,
    created_at: str,
    member_id: str,
) -> dict[str, Any]:
    response = client.post(
        "/ingest/message",
        json={
            "household_id": household_id,
            "raw_content": raw_content,
            "source": "manual",
            "created_at": created_at,
            "member_id": member_id,
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body, dict)
    return body


def _get_home(client: TestClient, *, household_id: str, scenario_date: str) -> dict[str, Any]:
    response = client.get(
        "/home",
        params={
            "household_id": household_id,
            "date": scenario_date,
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert isinstance(payload, dict)
    return payload


def _get_tasks(client: TestClient, *, household_id: str) -> list[dict[str, Any]]:
    response = client.get(
        "/tasks",
        params={
            "household_id": household_id,
            "status": "pending",
            "sort_by": "created_at",
            "order": "desc",
            "limit": 50,
            "offset": 0,
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert isinstance(payload, dict)
    tasks = payload.get("tasks")
    assert isinstance(tasks, list)
    return [task for task in tasks if isinstance(task, dict)]


@pytest.mark.ci_gate
@pytest.mark.parametrize(
    "raw_content,created_at,seed_schedules,expected",
    [
        (
            "Soccer practice cancelled Tuesday",
            "2026-05-10T09:00:00Z",
            [
                {
                    "schedule_id": "sched-cancel-1",
                    "title": "Soccer practice Tuesday",
                    "start_at": "2026-05-12T17:00:00Z",
                    "end_at": "2026-05-12T18:00:00Z",
                }
            ],
            {
                "classification": "schedule",
                "promotion_target": "calendar_update",
                "promoted_entity_type": "schedule",
                "decision_generated": True,
            },
        ),
        (
            "Game moved from 5pm to 7pm",
            "2026-05-05T09:00:00Z",
            [
                {
                    "schedule_id": "sched-move-1",
                    "title": "Game",
                    "start_at": "2026-05-05T17:00:00Z",
                    "end_at": "2026-05-05T18:00:00Z",
                }
            ],
            {
                "classification": "schedule",
                "promotion_target": "calendar_update",
                "promoted_entity_type": "schedule",
                "decision_generated": True,
            },
        ),
        (
            "New meeting scheduled at 3pm",
            "2026-05-05T08:00:00Z",
            [
                {
                    "schedule_id": "sched-conflict-1",
                    "title": "School pickup",
                    "start_at": "2026-05-05T15:00:00Z",
                    "end_at": "2026-05-05T16:00:00Z",
                }
            ],
            {
                "classification": "schedule",
                "promotion_target": "calendar",
                "promoted_entity_type": "schedule",
                "decision_generated": True,
            },
        ),
        (
            "Pay $15 for picture day by Friday",
            "2026-05-05T09:00:00Z",
            [],
            {
                "classification": "action",
                "promotion_target": "action",
                "promoted_entity_type": "task",
                "decision_generated": True,
            },
        ),
        (
            "Sign permission slip before Monday",
            "2026-05-05T09:15:00Z",
            [],
            {
                "classification": "action",
                "promotion_target": "action",
                "promoted_entity_type": "task",
                "decision_generated": True,
            },
        ),
        (
            "Team dinner after game",
            "2026-05-05T09:20:00Z",
            [],
            {
                "classification": "fyi",
                "promotion_target": "decision",
                "promoted_entity_type": "decision_card",
                "decision_generated": True,
            },
        ),
        (
            "Something changed for tomorrow",
            "2026-05-05T09:30:00Z",
            [],
            {
                "classification": "fyi",
                "promotion_target": "decision",
                "promoted_entity_type": "decision_card",
                "decision_generated": True,
            },
        ),
        (
            "Weekly newsletter update",
            "2026-05-05T09:45:00Z",
            [],
            {
                "classification": "fyi",
                "promotion_target": "ignore",
                "promoted_entity_type": "",
                "decision_generated": False,
            },
        ),
    ],
)
def test_household_message_promotion_adversarial_matrix(
    raw_content: str,
    created_at: str,
    seed_schedules: list[dict[str, str]],
    expected: dict[str, Any],
) -> None:
    household_id = f"promotion-adv-{uuid4().hex[:8]}"

    with TestClient(app) as client:
        for row in seed_schedules:
            _seed_schedule(
                client,
                household_id=household_id,
                schedule_id=row["schedule_id"],
                title=row["title"],
                start_at=row["start_at"],
                end_at=row["end_at"],
            )

        result = _ingest_message(
            client,
            household_id=household_id,
            raw_content=raw_content,
            created_at=created_at,
            member_id="member-adversarial",
        )

        response = result.get("response")
        assert isinstance(response, dict)

        assert response.get("classification") == expected["classification"]
        assert response.get("promotion_target") == expected["promotion_target"]
        assert response.get("promoted_entity_type") == expected["promoted_entity_type"]
        assert bool(response.get("decision_generated")) is expected["decision_generated"]

        interpretation_type = str(response.get("interpretation_type") or "").strip()
        promotion_reason = str(response.get("promotion_reason") or "").strip()
        interpretation_confidence = response.get("interpretation_confidence")

        assert interpretation_type
        assert promotion_reason
        assert isinstance(interpretation_confidence, float)

        if expected["promotion_target"] != "ignore":
            assert str(response.get("promoted_entity_id") or "").strip()
        if expected["decision_generated"] and expected["promoted_entity_type"] != "decision_card":
            assert str(response.get("secondary_entity_type") or "") == "decision_card"
            assert str(response.get("secondary_entity_id") or "").strip()


@pytest.mark.ci_gate
def test_household_message_promotion_home_and_due_date_impact() -> None:
    household_id = f"promotion-home-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-home-conflict-1",
            title="School pickup",
            start_at="2026-05-05T15:00:00Z",
            end_at="2026-05-05T16:00:00Z",
        )

        conflict_response = _ingest_message(
            client,
            household_id=household_id,
            raw_content="New meeting scheduled at 3pm",
            created_at="2026-05-05T08:00:00Z",
            member_id="member-home",
        )
        deadline_response = _ingest_message(
            client,
            household_id=household_id,
            raw_content="Pay $15 for picture day by Friday",
            created_at="2026-05-05T09:00:00Z",
            member_id="member-home",
        )
        ambiguous_response = _ingest_message(
            client,
            household_id=household_id,
            raw_content="Something changed for tomorrow",
            created_at="2026-05-05T10:00:00Z",
            member_id="member-home",
        )

        conflict_payload = conflict_response.get("response")
        deadline_payload = deadline_response.get("response")
        ambiguous_payload = ambiguous_response.get("response")

        assert isinstance(conflict_payload, dict)
        assert isinstance(deadline_payload, dict)
        assert isinstance(ambiguous_payload, dict)

        assert conflict_payload.get("promotion_target") == "calendar"
        assert bool(conflict_payload.get("decision_generated")) is True
        assert deadline_payload.get("promotion_target") == "action"
        assert ambiguous_payload.get("promotion_target") == "decision"

        home_payload = _get_home(client, household_id=household_id, scenario_date=scenario_date)
        needs_decision = home_payload.get("needs_decision")
        actions = home_payload.get("actions")
        summary = str(home_payload.get("summary") or "")

        assert isinstance(needs_decision, list)
        assert isinstance(actions, list)
        assert any(str(item.get("type") or "") == "promotion_decision" for item in needs_decision if isinstance(item, dict))
        assert any(str(item.get("source") or "") == "task" for item in actions if isinstance(item, dict))
        assert summary.startswith("Digest ")

        tasks = _get_tasks(client, household_id=household_id)
        promoted_task_rows = [
            row for row in tasks if str(row.get("task_id") or "").startswith("task-msg-")
        ]
        assert promoted_task_rows
        assert any(str(row.get("due_at") or "").strip() for row in promoted_task_rows)


@pytest.mark.ci_gate
def test_household_message_upm_cap_does_not_drop_high_priority_events() -> None:
    household_id = f"promotion-cap-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-cap-school-pickup",
            title="School pickup",
            start_at="2026-05-05T15:00:00Z",
            end_at="2026-05-05T16:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-cap-practice",
            title="Practice",
            start_at="2026-05-05T18:00:00Z",
            end_at="2026-05-05T19:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-cap-dinner",
            title="Dinner prep",
            start_at="2026-05-05T17:45:00Z",
            end_at="2026-05-05T18:45:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-cap-conference",
            title="Teacher conference",
            start_at="2026-05-09T09:00:00Z",
            end_at="2026-05-09T10:00:00Z",
        )

        inputs = [
            ("New meeting scheduled at 3pm", "2026-05-05T08:00:00Z"),
            ("Work meeting at 6pm", "2026-05-05T08:10:00Z"),
            ("Teacher conference rescheduled to Friday morning", "2026-05-05T08:20:00Z"),
            ("Something changed for tomorrow", "2026-05-05T08:30:00Z"),
            ("Coach said schedule might shift tomorrow", "2026-05-05T08:40:00Z"),
            ("Rain expected; practice may move indoors tomorrow", "2026-05-05T08:50:00Z"),
            ("PTA bake sale donations due Thursday", "2026-05-05T09:00:00Z"),
        ]

        max_promotion_decisions = 0
        created_decision_count = 0
        high_priority_generated = 0
        high_priority_blocked = 0
        for raw_content, created_at in inputs:
            result = _ingest_message(
                client,
                household_id=household_id,
                raw_content=raw_content,
                created_at=created_at,
                member_id="member-cap",
            )
            response = result.get("response")
            assert isinstance(response, dict)
            assert str(response.get("decision_routing_model") or "") == "upm_unified"
            if bool(response.get("decision_generated")):
                created_decision_count += 1
            if str(response.get("upm_priority_class") or "") in {"high", "critical"}:
                if bool(response.get("decision_generated")):
                    high_priority_generated += 1
                if bool(response.get("decision_blocked")):
                    high_priority_blocked += 1

            home_payload = _get_home(client, household_id=household_id, scenario_date=scenario_date)
            needs_decision = home_payload.get("needs_decision")
            assert isinstance(needs_decision, list)
            promotion_decisions = [
                row
                for row in needs_decision
                if isinstance(row, dict) and str(row.get("type") or "") == "promotion_decision"
            ]
            max_promotion_decisions = max(max_promotion_decisions, len(promotion_decisions))

        assert created_decision_count >= 1
    assert high_priority_generated >= 3
    assert high_priority_blocked == 0
    assert max_promotion_decisions >= 4


@pytest.mark.ci_gate
def test_household_message_conflict_detection_cascade_and_derived() -> None:
    household_id = f"promotion-conflict-{uuid4().hex[:8]}"

    with TestClient(app) as client:
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-conflict-practice",
            title="Practice",
            start_at="2026-05-05T18:00:00Z",
            end_at="2026-05-05T19:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-conflict-dinner",
            title="Dinner prep",
            start_at="2026-05-05T17:45:00Z",
            end_at="2026-05-05T18:45:00Z",
        )

        cascade_result = _ingest_message(
            client,
            household_id=household_id,
            raw_content="Work meeting at 6pm",
            created_at="2026-05-05T08:00:00Z",
            member_id="member-parent",
        )
        cascade_response = cascade_result.get("response")
        assert isinstance(cascade_response, dict)
        assert cascade_response.get("interpretation_type") == "schedule_create"
        assert str(cascade_response.get("conflict_schedule_id") or "").strip()
        assert str(cascade_response.get("conflict_type") or "") in {"cascade", "cross_member", "derived", "direct"}

        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-derived-anchor",
            title="Teacher conference",
            start_at="2026-05-08T09:00:00Z",
            end_at="2026-05-08T10:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-derived-conflict",
            title="Doctor follow-up",
            start_at="2026-05-08T09:30:00Z",
            end_at="2026-05-08T10:15:00Z",
        )

        derived_result = _ingest_message(
            client,
            household_id=household_id,
            raw_content="Teacher conference rescheduled to Friday morning",
            created_at="2026-05-05T08:30:00Z",
            member_id="member-parent",
        )
        derived_response = derived_result.get("response")
        assert isinstance(derived_response, dict)
        assert derived_response.get("interpretation_type") == "time_change"
        assert str(derived_response.get("conflict_schedule_id") or "").strip()
        assert str(derived_response.get("conflict_type") or "") in {"derived", "cascade", "cross_member", "direct"}


@pytest.mark.ci_gate
def test_household_message_recall_zero_missed_critical_events() -> None:
    household_id = f"promotion-recall-{uuid4().hex[:8]}"

    with TestClient(app) as client:
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-recall-pickup",
            title="School pickup",
            start_at="2026-05-05T15:00:00Z",
            end_at="2026-05-05T16:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-recall-practice",
            title="Practice",
            start_at="2026-05-05T18:00:00Z",
            end_at="2026-05-05T19:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-recall-dinner",
            title="Dinner prep",
            start_at="2026-05-05T17:45:00Z",
            end_at="2026-05-05T18:45:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-recall-conference",
            title="Teacher conference",
            start_at="2026-05-08T09:00:00Z",
            end_at="2026-05-08T10:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-recall-doctor",
            title="Doctor follow-up",
            start_at="2026-05-08T09:30:00Z",
            end_at="2026-05-08T10:15:00Z",
        )

        critical_inputs = [
            ("New meeting scheduled at 3pm", "2026-05-05T08:00:00Z", "member-recall"),
            ("Work meeting at 6pm", "2026-05-05T08:10:00Z", "member-recall"),
            ("Practice overlaps with dinner time", "2026-05-05T08:20:00Z", "member-recall"),
            ("Teacher conference rescheduled to Friday morning", "2026-05-05T08:30:00Z", "member-recall"),
        ]

        critical_seen = 0
        missed_critical = 0
        for raw_content, created_at, member_id in critical_inputs:
            result = _ingest_message(
                client,
                household_id=household_id,
                raw_content=raw_content,
                created_at=created_at,
                member_id=member_id,
            )
            response = result.get("response")
            assert isinstance(response, dict)

            if bool(response.get("critical_event_detected")):
                critical_seen += 1
                assert str(response.get("decision_routing_model") or "") == "upm_unified"
                assert str(response.get("upm_priority_class") or "") == "critical"
                if (not bool(response.get("decision_generated"))) or bool(response.get("decision_blocked")):
                    missed_critical += 1

        assert critical_seen >= 3
        assert missed_critical == 0


@pytest.mark.ci_gate
def test_household_message_upm_priority_cap_behavior() -> None:
    household_id = f"promotion-upm-cap-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-dual-pickup",
            title="School pickup",
            start_at="2026-05-05T15:00:00Z",
            end_at="2026-05-05T16:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-dual-practice",
            title="Practice",
            start_at="2026-05-05T18:00:00Z",
            end_at="2026-05-05T19:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-dual-dinner",
            title="Dinner prep",
            start_at="2026-05-05T17:45:00Z",
            end_at="2026-05-05T18:45:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-dual-conference",
            title="Teacher conference",
            start_at="2026-05-08T09:00:00Z",
            end_at="2026-05-08T10:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-dual-doctor",
            title="Doctor follow-up",
            start_at="2026-05-08T09:30:00Z",
            end_at="2026-05-08T10:15:00Z",
        )

        low_priority_inputs = [
            ("Something changed for tomorrow", "2026-05-05T08:00:00Z"),
            ("Coach said schedule might shift tomorrow", "2026-05-05T08:10:00Z"),
            ("Rain expected; practice may move indoors tomorrow", "2026-05-05T08:20:00Z"),
            ("Can we rethink tomorrow afternoon plans?", "2026-05-05T08:30:00Z"),
            ("Team dinner after game", "2026-05-05T08:40:00Z"),
        ]
        critical_inputs = [
            ("New meeting scheduled at 3pm", "2026-05-05T08:50:00Z"),
            ("Work meeting at 6pm", "2026-05-05T09:00:00Z"),
            ("Dinner reservation at 6:30", "2026-05-05T09:10:00Z"),
            ("Teacher conference rescheduled to Friday morning", "2026-05-05T09:20:00Z"),
        ]

        low_priority_created = 0
        low_priority_not_created = 0
        critical_created = 0
        max_promotion_decisions = 0

        for raw_content, created_at in [*low_priority_inputs, *critical_inputs]:
            result = _ingest_message(
                client,
                household_id=household_id,
                raw_content=raw_content,
                created_at=created_at,
                member_id="member-dual",
            )
            response = result.get("response")
            assert isinstance(response, dict)

            assert str(response.get("decision_routing_model") or "") == "upm_unified"
            upm_priority_class = str(response.get("upm_priority_class") or "")
            generated = bool(response.get("decision_generated"))
            blocked = bool(response.get("decision_blocked"))
            if upm_priority_class in {"low", "medium", "noise"}:
                if generated:
                    low_priority_created += 1
                else:
                    low_priority_not_created += 1
            elif upm_priority_class == "critical" and generated:
                critical_created += 1

            for score_key in (
                "upm_decision_score",
                "upm_actionability_score",
                "upm_confidence_score",
                "upm_actionability_threshold",
                "upm_confidence_min",
            ):
                value = response.get(score_key)
                assert isinstance(value, float)
                assert 0.0 <= value <= 1.0

            suppressed_score_delta = response.get("suppressed_score_delta")
            suppression_reason = response.get("suppression_reason")
            alternative_path = response.get("alternative_path")
            assert isinstance(suppressed_score_delta, float)
            assert 0.0 <= suppressed_score_delta <= 1.0
            assert isinstance(suppression_reason, str)
            assert isinstance(alternative_path, str)
            if (not bool(response.get("decision_generated"))) and str(suppression_reason or "").strip():
                assert str(alternative_path or "").strip()

            home_payload = _get_home(client, household_id=household_id, scenario_date=scenario_date)
            needs_decision = home_payload.get("needs_decision")
            assert isinstance(needs_decision, list)
            promotion_decisions = [
                row
                for row in needs_decision
                if isinstance(row, dict) and str(row.get("type") or "") == "promotion_decision"
            ]
            max_promotion_decisions = max(max_promotion_decisions, len(promotion_decisions))

        assert low_priority_created <= 2
        assert low_priority_not_created >= 2
        assert critical_created >= 3
        assert max_promotion_decisions > 3


@pytest.mark.ci_gate
def test_household_message_upm_consistency_repeatability() -> None:
    def _run_sequence(*, household_id: str) -> list[tuple[str, str, bool, bool]]:
        with TestClient(app) as client:
            _seed_schedule(
                client,
                household_id=household_id,
                schedule_id="sched-repeat-pickup",
                title="School pickup",
                start_at="2026-05-05T15:00:00Z",
                end_at="2026-05-05T16:00:00Z",
            )
            _seed_schedule(
                client,
                household_id=household_id,
                schedule_id="sched-repeat-practice",
                title="Practice",
                start_at="2026-05-05T18:00:00Z",
                end_at="2026-05-05T19:00:00Z",
            )
            _seed_schedule(
                client,
                household_id=household_id,
                schedule_id="sched-repeat-dinner",
                title="Dinner prep",
                start_at="2026-05-05T17:45:00Z",
                end_at="2026-05-05T18:45:00Z",
            )

            sequence_inputs = [
                ("Work meeting at 6pm", "2026-05-05T08:00:00Z"),
                ("Practice overlaps with dinner time", "2026-05-05T08:05:00Z"),
                ("Can we rethink tomorrow afternoon plans?", "2026-05-05T08:10:00Z"),
                ("Soccer practice cancelled Tuesday", "2026-05-05T08:15:00Z"),
            ]
            observed: list[tuple[str, str, bool, bool]] = []
            for raw_content, created_at in sequence_inputs:
                result = _ingest_message(
                    client,
                    household_id=household_id,
                    raw_content=raw_content,
                    created_at=created_at,
                    member_id="member-repeat",
                )
                response = result.get("response")
                assert isinstance(response, dict)
                observed.append(
                    (
                        str(response.get("interpretation_type") or ""),
                        str(response.get("upm_priority_class") or ""),
                        bool(response.get("decision_generated")),
                        bool(response.get("decision_blocked")),
                    )
                )
            return observed

    first = _run_sequence(household_id=f"promotion-repeat-a-{uuid4().hex[:8]}")
    second = _run_sequence(household_id=f"promotion-repeat-b-{uuid4().hex[:8]}")
    assert first == second


@pytest.mark.ci_gate
def test_household_message_upm_stress_no_suppression_loss() -> None:
    household_id = f"promotion-upm-stress-{uuid4().hex[:8]}"

    with TestClient(app) as client:
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-stress-pickup",
            title="School pickup",
            start_at="2026-05-05T15:00:00Z",
            end_at="2026-05-05T16:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-stress-practice",
            title="Practice",
            start_at="2026-05-05T18:00:00Z",
            end_at="2026-05-05T19:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-stress-dinner",
            title="Dinner prep",
            start_at="2026-05-05T17:45:00Z",
            end_at="2026-05-05T18:45:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-stress-soccer",
            title="Soccer practice Tuesday",
            start_at="2026-05-12T17:00:00Z",
            end_at="2026-05-12T18:00:00Z",
        )

        templates = [
            "Work meeting at 6pm",
            "Practice overlaps with dinner time",
            "Teacher conference rescheduled to Friday morning",
            "Soccer practice cancelled Tuesday",
            "Can we rethink tomorrow afternoon plans?",
        ]

        suppression_losses = 0
        critical_seen = 0
        created_decisions = 0
        for idx in range(25):
            raw_content = templates[idx % len(templates)]
            minute = idx * 3
            hour = 8 + (minute // 60)
            minute_part = minute % 60
            created_at = f"2026-05-05T{hour:02d}:{minute_part:02d}:00Z"
            result = _ingest_message(
                client,
                household_id=household_id,
                raw_content=raw_content,
                created_at=created_at,
                member_id="member-stress",
            )
            response = result.get("response")
            assert isinstance(response, dict)
            assert str(response.get("decision_routing_model") or "") == "upm_unified"
            if bool(response.get("decision_generated")):
                created_decisions += 1

            for score_key in (
                "upm_decision_score",
                "upm_actionability_score",
                "upm_confidence_score",
                "upm_actionability_threshold",
                "upm_confidence_min",
            ):
                value = response.get(score_key)
                assert isinstance(value, float)
                assert 0.0 <= value <= 1.0

            if str(response.get("upm_priority_class") or "") == "critical":
                critical_seen += 1
                assert bool(response.get("decision_generated"))
                assert not bool(response.get("decision_blocked"))

            if bool(response.get("decision_blocked")) and str(response.get("decision_block_root_cause") or "") == "suppression_overreach":
                suppression_losses += 1

        assert critical_seen >= 8
        assert suppression_losses == 0
        assert created_decisions < 25


@pytest.mark.ci_gate
def test_household_message_conflict_detection_recall_threshold() -> None:
    household_id = f"promotion-conflict-recall-{uuid4().hex[:8]}"

    with TestClient(app) as client:
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-threshold-pickup",
            title="School pickup",
            start_at="2026-05-05T15:00:00Z",
            end_at="2026-05-05T16:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-threshold-practice",
            title="Practice",
            start_at="2026-05-05T18:00:00Z",
            end_at="2026-05-05T19:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-threshold-dinner",
            title="Dinner prep",
            start_at="2026-05-05T17:45:00Z",
            end_at="2026-05-05T18:45:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-threshold-cancel-anchor",
            title="Soccer practice Tuesday",
            start_at="2026-05-12T17:00:00Z",
            end_at="2026-05-12T18:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-threshold-cancel-overlap",
            title="Carpool pickup Tuesday",
            start_at="2026-05-12T17:30:00Z",
            end_at="2026-05-12T18:15:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-threshold-conference",
            title="Teacher conference",
            start_at="2026-05-08T09:00:00Z",
            end_at="2026-05-08T10:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-threshold-doctor",
            title="Doctor follow-up",
            start_at="2026-05-08T09:30:00Z",
            end_at="2026-05-08T10:15:00Z",
        )

        conflict_inputs = [
            ("New meeting scheduled at 3pm", "2026-05-05T08:00:00Z"),
            ("Work meeting at 6pm", "2026-05-05T08:10:00Z"),
            ("Practice overlaps with dinner time", "2026-05-05T08:20:00Z"),
            ("Teacher conference rescheduled to Friday morning", "2026-05-05T08:30:00Z"),
            ("Soccer practice cancelled Tuesday", "2026-05-10T09:00:00Z"),
        ]

        detected_conflicts = 0
        for raw_content, created_at in conflict_inputs:
            result = _ingest_message(
                client,
                household_id=household_id,
                raw_content=raw_content,
                created_at=created_at,
                member_id="member-threshold",
            )
            response = result.get("response")
            assert isinstance(response, dict)
            conflict_schedule_id = str(response.get("conflict_schedule_id") or "").strip()
            conflict_type = str(response.get("conflict_type") or "").strip()
            if conflict_schedule_id:
                detected_conflicts += 1
            assert conflict_type in {"", "direct", "cross_member", "derived", "cascade"}

        recall = detected_conflicts / len(conflict_inputs)
        assert recall >= 0.95


@pytest.mark.ci_gate
def test_household_message_ambiguity_recovery_with_dependency_signal() -> None:
    household_id = f"promotion-ambiguity-recovery-{uuid4().hex[:8]}"

    with TestClient(app) as client:
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-ambiguity-practice",
            title="Practice",
            start_at="2026-05-06T18:00:00Z",
            end_at="2026-05-06T19:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-ambiguity-dinner",
            title="Dinner prep",
            start_at="2026-05-06T17:45:00Z",
            end_at="2026-05-06T18:45:00Z",
        )

        preload_inputs = [
            ("Can we rethink tomorrow afternoon plans?", "2026-05-05T08:00:00Z"),
            ("Team dinner after game", "2026-05-05T08:05:00Z"),
            ("Coach said schedule might shift tomorrow", "2026-05-05T08:10:00Z"),
        ]
        for raw_content, created_at in preload_inputs:
            _ingest_message(
                client,
                household_id=household_id,
                raw_content=raw_content,
                created_at=created_at,
                member_id="member-ambiguity-preload",
            )

        result = _ingest_message(
            client,
            household_id=household_id,
            raw_content="Something changed for tomorrow practice and pickup.",
            created_at="2026-05-05T08:15:00Z",
            member_id="member-ambiguity-target",
        )
        response = result.get("response")
        assert isinstance(response, dict)

        assert str(response.get("decision_routing_model") or "") == "upm_unified"
        decision_generated = bool(response.get("decision_generated"))
        suppression_reason = str(response.get("suppression_reason") or "")
        alternative_path = str(response.get("alternative_path") or "")
        assert decision_generated or suppression_reason == "merged_into_existing"
        if not decision_generated:
            assert alternative_path == "decision.merge_existing"
        assert bool(response.get("decision_blocked")) is False
        assert str(response.get("upm_priority_class") or "") in {"medium", "high", "critical"}
        if suppression_reason == "merged_into_existing":
            assert bool(response.get("upm_requires_decision")) is False
        else:
            assert bool(response.get("upm_requires_decision")) is True

        suppressed_score_delta = response.get("suppressed_score_delta")
        assert isinstance(suppressed_score_delta, float)
        assert isinstance(suppression_reason, str)
        assert isinstance(alternative_path, str)
        if str(suppression_reason or "").strip():
            assert str(alternative_path or "").strip()


@pytest.mark.ci_gate
def test_household_message_phase5_chained_dependency_resolution_visibility() -> None:
    household_id = f"promotion-phase5-chain-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-phase5-chain-a",
            title="School pickup",
            start_at="2026-05-05T15:00:00Z",
            end_at="2026-05-05T16:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-phase5-chain-b",
            title="Dinner prep",
            start_at="2026-05-05T17:45:00Z",
            end_at="2026-05-05T18:45:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-phase5-chain-c",
            title="Practice",
            start_at="2026-05-05T18:00:00Z",
            end_at="2026-05-05T19:00:00Z",
        )

        result = _ingest_message(
            client,
            household_id=household_id,
            raw_content="Work meeting at 6pm",
            created_at="2026-05-05T08:00:00Z",
            member_id="member-phase5-chain",
        )
        response = result.get("response")
        assert isinstance(response, dict)

        assert str(response.get("conflict_schedule_id") or "").strip()
        assert str(response.get("conflict_type") or "") in {"cascade", "cross_member", "derived", "direct"}
        assert bool(response.get("upm_conflict_risk")) or bool(response.get("upm_state_dependency"))
        assert bool(response.get("decision_blocked")) is False

        if not bool(response.get("decision_generated")):
            assert str(response.get("suppression_reason") or "") in {
                "merged_into_existing",
                "collapsed_into_low_priority_decision",
            }
            assert str(response.get("alternative_path") or "") == "decision.merge_existing"
            assert bool(response.get("upm_requires_decision")) is False
            home_payload = _get_home(client, household_id=household_id, scenario_date=scenario_date)
            needs_decision = home_payload.get("needs_decision")
            assert isinstance(needs_decision, list)
            assert any(
                isinstance(item, dict) and str(item.get("type") or "") == "promotion_decision"
                for item in needs_decision
            )


@pytest.mark.ci_gate
def test_household_message_phase5_near_window_collision_boundary() -> None:
    household_id = f"promotion-phase5-window-{uuid4().hex[:8]}"

    with TestClient(app) as client:
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-phase5-window-anchor",
            title="School pickup",
            start_at="2026-05-05T15:00:00Z",
            end_at="2026-05-05T16:00:00Z",
        )

        result = _ingest_message(
            client,
            household_id=household_id,
            raw_content="New meeting scheduled at 4:10pm",
            created_at="2026-05-05T08:00:00Z",
            member_id="member-phase5-window",
        )
        response = result.get("response")
        assert isinstance(response, dict)

        assert str(response.get("conflict_schedule_id") or "").strip()
        assert bool(response.get("upm_conflict_risk"))
        assert bool(response.get("decision_blocked")) is False
        if not bool(response.get("decision_generated")):
            assert str(response.get("suppression_reason") or "") == "merged_into_existing"
            assert str(response.get("alternative_path") or "") == "decision.merge_existing"
            assert bool(response.get("upm_requires_decision")) is False


@pytest.mark.ci_gate
def test_household_message_phase5_duplicate_source_conflict_not_masked() -> None:
    household_id = f"promotion-phase5-duplicate-{uuid4().hex[:8]}"

    with TestClient(app) as client:
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-phase5-duplicate-anchor",
            title="School pickup",
            start_at="2026-05-05T15:00:00Z",
            end_at="2026-05-05T16:00:00Z",
        )

        first = _ingest_message(
            client,
            household_id=household_id,
            raw_content="New meeting scheduled at 3pm",
            created_at="2026-05-05T08:00:00Z",
            member_id="member-phase5-dup-a",
        )
        second = _ingest_message(
            client,
            household_id=household_id,
            raw_content="New meeting scheduled at 3pm",
            created_at="2026-05-05T08:01:00Z",
            member_id="member-phase5-dup-b",
        )

        first_response = first.get("response")
        second_response = second.get("response")
        assert isinstance(first_response, dict)
        assert isinstance(second_response, dict)

        assert str(first_response.get("conflict_schedule_id") or "").strip()
        assert str(second_response.get("conflict_schedule_id") or "").strip()
        assert bool(second_response.get("upm_conflict_risk"))
        assert bool(second_response.get("decision_blocked")) is False
        if not bool(second_response.get("decision_generated")):
            assert str(second_response.get("suppression_reason") or "") == "merged_into_existing"
            assert str(second_response.get("alternative_path") or "") == "decision.merge_existing"
            assert bool(second_response.get("upm_requires_decision")) is False


@pytest.mark.ci_gate
def test_household_message_phase5_merge_compression_paths_have_visible_resolution() -> None:
    household_id = f"promotion-phase5-resolution-{uuid4().hex[:8]}"
    scenario_date = "2026-05-05"

    with TestClient(app) as client:
        inputs = [
            ("Can we rethink tomorrow afternoon plans?", "2026-05-05T08:00:00Z"),
            ("Team dinner after game", "2026-05-05T08:05:00Z"),
            ("Coach said schedule might shift tomorrow", "2026-05-05T08:10:00Z"),
            ("Can we rethink tomorrow afternoon plans?", "2026-05-05T08:15:00Z"),
            ("Team dinner after game", "2026-05-05T08:20:00Z"),
            ("Can we rethink tomorrow afternoon plans?", "2026-05-05T08:25:00Z"),
            ("Coach said schedule might shift tomorrow", "2026-05-05T08:30:00Z"),
            ("Can we rethink tomorrow afternoon plans?", "2026-05-05T08:35:00Z"),
        ]

        suppressed_with_resolution = 0
        for raw_content, created_at in inputs:
            result = _ingest_message(
                client,
                household_id=household_id,
                raw_content=raw_content,
                created_at=created_at,
                member_id="member-phase5-resolution",
            )
            response = result.get("response")
            assert isinstance(response, dict)

            suppression_reason = str(response.get("suppression_reason") or "")
            if suppression_reason in {"merged_into_existing", "collapsed_into_low_priority_decision"}:
                suppressed_with_resolution += 1
                assert bool(response.get("decision_generated")) is False
                assert bool(response.get("decision_blocked")) is False
                assert str(response.get("alternative_path") or "") == "decision.merge_existing"
                assert bool(response.get("upm_requires_decision")) is False

                home_payload = _get_home(client, household_id=household_id, scenario_date=scenario_date)
                needs_decision = home_payload.get("needs_decision")
                assert isinstance(needs_decision, list)
                assert any(
                    isinstance(item, dict) and str(item.get("type") or "") == "promotion_decision"
                    for item in needs_decision
                )

        assert suppressed_with_resolution >= 2


@pytest.mark.ci_gate
def test_household_message_phase5_conflict_never_compressed_without_resolution() -> None:
    household_id = f"promotion-phase5-conflict-compression-{uuid4().hex[:8]}"

    with TestClient(app) as client:
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-phase5-conflict-pickup",
            title="School pickup",
            start_at="2026-05-05T15:00:00Z",
            end_at="2026-05-05T16:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-phase5-conflict-practice",
            title="Practice",
            start_at="2026-05-05T18:00:00Z",
            end_at="2026-05-05T19:00:00Z",
        )
        _seed_schedule(
            client,
            household_id=household_id,
            schedule_id="sched-phase5-conflict-dinner",
            title="Dinner prep",
            start_at="2026-05-05T17:45:00Z",
            end_at="2026-05-05T18:45:00Z",
        )

        inputs = [
            ("New meeting scheduled at 3pm", "2026-05-05T08:00:00Z"),
            ("Work meeting at 6pm", "2026-05-05T08:03:00Z"),
            ("Practice overlaps with dinner time", "2026-05-05T08:06:00Z"),
            ("Work meeting at 6pm", "2026-05-05T08:09:00Z"),
            ("Practice overlaps with dinner time", "2026-05-05T08:12:00Z"),
            ("New meeting scheduled at 3pm", "2026-05-05T08:15:00Z"),
            ("Work meeting at 6pm", "2026-05-05T08:18:00Z"),
        ]

        conflict_seen = 0
        for raw_content, created_at in inputs:
            result = _ingest_message(
                client,
                household_id=household_id,
                raw_content=raw_content,
                created_at=created_at,
                member_id="member-phase5-conflict",
            )
            response = result.get("response")
            assert isinstance(response, dict)

            if bool(response.get("upm_conflict_risk")):
                conflict_seen += 1
                assert str(response.get("suppression_reason") or "") not in {
                    "decision_density_cap_reached",
                    "decision_cap_reached",
                    "low_impact_actionable_noise",
                    "upm_no_decision_required",
                }
                if not bool(response.get("decision_generated")):
                    assert str(response.get("suppression_reason") or "") in {
                        "merged_into_existing",
                        "collapsed_into_low_priority_decision",
                    }
                    assert str(response.get("alternative_path") or "") == "decision.merge_existing"
                    assert bool(response.get("decision_blocked")) is False

        assert conflict_seen >= 5
