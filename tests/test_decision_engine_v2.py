from __future__ import annotations

from datetime import datetime

from apps.api.services.decision_engine import run_decision_engine_v2


def _proposal(
    proposal_id: str,
    *,
    priority: float,
    source_module: str = "task_module",
    proposal_type: str = "task_action",
    time_window: str | None = None,
    duration: int | None = None,
    effort: str | None = None,
    category: str | None = None,
) -> dict:
    description = "test"
    if time_window is not None:
        description = f"reference={proposal_id}; time_window={time_window}"
    row = {
        "id": proposal_id,
        "type": proposal_type,
        "title": proposal_id,
        "description": description,
        "priority": priority,
        "source_module": source_module,
        "normalized_priority": priority,
    }
    if duration is not None:
        row["duration"] = duration
    if effort is not None:
        row["effort"] = effort
    if category is not None:
        row["category"] = category
    return row


def _overlaps(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    start_a = datetime.fromisoformat(a_start.replace("Z", "+00:00"))
    end_a = datetime.fromisoformat(a_end.replace("Z", "+00:00"))
    start_b = datetime.fromisoformat(b_start.replace("Z", "+00:00"))
    end_b = datetime.fromisoformat(b_end.replace("Z", "+00:00"))
    return start_a < end_b and start_b < end_a


def _available_hours_for_day(payload: dict, day: str) -> float:
    start_hour = 8
    end_hour = 21
    blocked = 0.0
    for event in payload.get("calendar_events", []):
        start = str(event["start_time"])
        end = str(event["end_time"])
        if not start.startswith(day) or not end.startswith(day):
            continue
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        blocked += (end_dt - start_dt).total_seconds() / 3600.0
    return float(end_hour - start_hour) - blocked


def test_decision_engine_v2_respects_calendar_conflicts_and_no_overlaps() -> None:
    payload = {
        "calendar_events": [
            {
                "start_time": "2026-04-16T09:00:00",
                "end_time": "2026-04-16T10:00:00",
                "source": "calendar_events",
            },
            {
                "start_time": "2026-04-16T12:00:00",
                "end_time": "2026-04-16T13:00:00",
                "source": "calendar_events",
            },
        ],
        "signals": [
            {
                "id": "s1",
                "type": "events_today",
                "message": "events_today=3",
                "severity": "medium",
                "source_module": "calendar_module",
            },
            {
                "id": "s2",
                "type": "high_priority_events",
                "message": "high_priority_events=2",
                "severity": "high",
                "source_module": "calendar_module",
            },
        ],
        "proposals": [
            _proposal(
                "a",
                priority=9.0,
                duration=1,
                effort="low",
                category="task",
            ),
            _proposal(
                "b",
                priority=8.2,
                duration=1,
                effort="medium",
                category="task",
            ),
            _proposal(
                "c",
                priority=6.5,
                duration=1,
                effort="medium",
                category="task",
            ),
        ],
    }

    result = run_decision_engine_v2(payload)

    scheduled = result["scheduled_actions"]
    unscheduled = result["unscheduled_actions"]

    assert len(scheduled) == 3
    assert len(unscheduled) == 0

    # Every scheduled action has concrete start/end times.
    for row in scheduled:
        assert isinstance(row.get("start_time"), str)
        assert isinstance(row.get("end_time"), str)

    # No scheduled action overlaps any calendar event.
    for row in scheduled:
        for event in payload["calendar_events"]:
            assert not _overlaps(
                str(row["start_time"]),
                str(row["end_time"]),
                str(event["start_time"]),
                str(event["end_time"]),
            )

    # No overlap among scheduled actions.
    for i in range(len(scheduled)):
        for j in range(i + 1, len(scheduled)):
            assert not _overlaps(
                str(scheduled[i]["start_time"]),
                str(scheduled[i]["end_time"]),
                str(scheduled[j]["start_time"]),
                str(scheduled[j]["end_time"]),
            )

    # Sequential placement in scan order for this fixture.
    starts = [str(row["start_time"]) for row in scheduled]
    assert starts == sorted(starts)

def test_decision_engine_v2_is_deterministic_for_same_input() -> None:
    payload = {
        "signals": [
            {
                "id": "s1",
                "type": "events_today",
                "message": "events_today=1",
                "severity": "medium",
                "source_module": "calendar_module",
            },
            {
                "id": "s2",
                "type": "high_priority_events",
                "message": "high_priority_events=1",
                "severity": "high",
                "source_module": "calendar_module",
            },
        ],
        "proposals": [
            _proposal("a", priority=9.0, time_window="2026-04-16T09:00:00->2026-04-16T10:00:00"),
            _proposal("b", priority=6.0, time_window="2026-04-16T12:00:00->2026-04-16T13:00:00"),
            _proposal("c", priority=4.0),
        ],
    }

    first = run_decision_engine_v2(payload)
    second = run_decision_engine_v2(payload)

    assert first == second


def test_decision_engine_v2_duration_gap_fill_and_deferral() -> None:
    payload = {
        "calendar_events": [
            {
                "start_time": "2026-04-16T11:00:00",
                "end_time": "2026-04-16T21:00:00",
                "source": "calendar_events",
            }
        ],
        "signals": [
            {
                "id": "s1",
                "type": "events_today",
                "message": "events_today=1",
                "severity": "medium",
                "source_module": "calendar_module",
            },
            {
                "id": "s2",
                "type": "high_priority_events",
                "message": "high_priority_events=0",
                "severity": "low",
                "source_module": "calendar_module",
            },
        ],
        "proposals": [
            _proposal(
                "long_3u",
                priority=9.2,
                duration=3,
                effort="medium",
                category="task",
                time_window="2026-04-16T08:00:00->2026-04-16T09:00:00",
            ),
            _proposal(
                "mid_2u",
                priority=8.8,
                duration=2,
                effort="medium",
                category="task",
                time_window="2026-04-16T09:00:00->2026-04-16T10:00:00",
            ),
            _proposal(
                "short_1u",
                priority=7.5,
                duration=1,
                effort="low",
                category="task",
                time_window="2026-04-16T11:00:00->2026-04-16T12:00:00",
            ),
        ],
    }

    first = run_decision_engine_v2(payload)
    second = run_decision_engine_v2(payload)

    # Deterministic ordering and content.
    assert first == second

    scheduled = first["scheduled_actions"]
    unscheduled = first["unscheduled_actions"]

    # With 11:00-21:00 blocked, only 08:00-11:00 remains (3 hours).
    # 3-hour task should fit; 2-hour should defer; 1-hour should fill remaining slot.
    scheduled_ids = [row.get("proposal_id") for row in scheduled]
    unscheduled_ids = [row.get("proposal_id") for row in unscheduled]

    assert "long_3u" in scheduled_ids
    assert "mid_2u" in unscheduled_ids
    assert "short_1u" in unscheduled_ids

    reasons = {str(row.get("unscheduled_reason", "")) for row in unscheduled}
    assert "no_available_time_slot" in reasons


def test_decision_engine_v2_short_tasks_fill_gaps() -> None:
    payload = {
        "calendar_events": [
            {
                "start_time": "2026-04-16T08:00:00",
                "end_time": "2026-04-16T10:00:00",
                "source": "calendar_events",
            },
            {
                "start_time": "2026-04-16T11:00:00",
                "end_time": "2026-04-16T13:00:00",
                "source": "calendar_events",
            },
        ],
        "signals": [
            {
                "id": "s1",
                "type": "events_today",
                "message": "events_today=2",
                "severity": "medium",
                "source_module": "calendar_module",
            }
        ],
        "proposals": [
            _proposal("short_a", priority=9.0, duration=1, effort="low", category="task"),
            _proposal("short_b", priority=8.0, duration=1, effort="low", category="task"),
            _proposal("short_c", priority=7.0, duration=1, effort="medium", category="task"),
        ],
    }

    result = run_decision_engine_v2(payload)
    scheduled = result["scheduled_actions"]

    assert len(scheduled) == 3
    starts = [str(row["start_time"]) for row in scheduled]
    # Earliest available slots are 10:00, 13:00, 14:00.
    assert starts[0].endswith("10:00:00")
    assert starts[1].endswith("13:00:00")
    assert starts[2].endswith("14:00:00")


def test_decision_engine_v2_single_path_is_stable_for_optimization_flag() -> None:
    payload = {
        "signals": [
            {
                "id": "s1",
                "type": "events_today",
                "message": "events_today=0",
                "severity": "low",
                "source_module": "calendar_module",
            }
        ],
        "proposals": [
            _proposal("t1", priority=9.0, duration=1, effort="medium", category="task"),
            _proposal("m1", priority=8.8, duration=1, effort="high", category="maintenance"),
            _proposal("t2", priority=8.6, duration=1, effort="medium", category="task"),
            _proposal("m2", priority=8.4, duration=1, effort="high", category="maintenance"),
            _proposal("t3", priority=8.2, duration=1, effort="low", category="task"),
            _proposal("m3", priority=8.0, duration=1, effort="high", category="maintenance"),
        ],
    }

    baseline = run_decision_engine_v2(payload, enable_optimization=False)
    flagged = run_decision_engine_v2(payload, enable_optimization=True)

    assert baseline == flagged
    assert baseline["_internal"]["optimization_applied"] is False
    assert flagged["_internal"]["optimization_applied"] is False


def test_decision_engine_v2_constraints_and_determinism() -> None:
    payload = {
        "calendar_events": [
            {
                "start_time": "2026-04-16T10:00:00",
                "end_time": "2026-04-16T11:00:00",
                "source": "calendar_events",
            }
        ],
        "signals": [
            {
                "id": "s1",
                "type": "events_today",
                "message": "events_today=1",
                "severity": "medium",
                "source_module": "calendar_module",
            }
        ],
        "proposals": [
            _proposal("a1", priority=9.0, duration=1, effort="medium", category="task"),
            _proposal("a2", priority=8.9, duration=1, effort="medium", category="task"),
            _proposal("b1", priority=8.8, duration=1, effort="high", category="maintenance"),
            _proposal("b2", priority=8.7, duration=1, effort="high", category="maintenance"),
        ],
    }

    first = run_decision_engine_v2(payload, enable_optimization=True)
    second = run_decision_engine_v2(payload, enable_optimization=True)
    assert first == second

    scheduled = first["scheduled_actions"]

    # No overlaps among optimized scheduled actions.
    for i in range(len(scheduled)):
        for j in range(i + 1, len(scheduled)):
            assert not _overlaps(
                str(scheduled[i]["start_time"]),
                str(scheduled[i]["end_time"]),
                str(scheduled[j]["start_time"]),
                str(scheduled[j]["end_time"]),
            )

    # No overlaps with calendar events.
    for row in scheduled:
        for event in payload["calendar_events"]:
            assert not _overlaps(
                str(row["start_time"]),
                str(row["end_time"]),
                str(event["start_time"]),
                str(event["end_time"]),
            )


def test_decision_engine_v2_backlog_schema_stability_and_repeatability() -> None:
    payload = {
        "calendar_events": [
            {"start_time": "2026-04-16T11:00:00", "end_time": "2026-04-16T21:00:00", "source": "calendar_events"},
            {"start_time": "2026-04-17T08:00:00", "end_time": "2026-04-17T16:00:00", "source": "calendar_events"},
        ],
        "signals": [
            {"id": "s1", "type": "events_today", "message": "events_today=2", "severity": "medium", "source_module": "calendar_module"},
            {"id": "s2", "type": "high_priority_events", "message": "high_priority_events=1", "severity": "high", "source_module": "calendar_module"},
        ],
        "proposals": [
            _proposal("p1", priority=9.2, duration=3, effort="high", category="maintenance"),
            _proposal("p2", priority=8.8, duration=2, effort="medium", category="task"),
            _proposal("p3", priority=8.2, duration=2, effort="medium", category="task"),
            _proposal("p4", priority=7.8, duration=1, effort="low", category="other"),
            _proposal("p5", priority=7.4, duration=1, effort="low", category="other"),
        ],
    }

    first = run_decision_engine_v2(payload)
    second = run_decision_engine_v2(payload)

    assert first["day_1_schedule"] == second["day_1_schedule"]
    assert first["day_2_schedule"] == second["day_2_schedule"]
    assert first["day_3_schedule"] == second["day_3_schedule"]
    assert first["backlog"] == second["backlog"]
    assert first["day_2_schedule"] == []
    assert first["day_3_schedule"] == []


def test_decision_engine_v2_backlog_matches_unscheduled_and_no_duplication() -> None:
    payload = {
        "calendar_events": [
            {"start_time": "2026-04-16T11:00:00", "end_time": "2026-04-16T21:00:00", "source": "calendar_events"},
        ],
        "signals": [
            {"id": "s1", "type": "events_today", "message": "events_today=1", "severity": "medium", "source_module": "calendar_module"},
        ],
        "proposals": [
            _proposal("a", priority=9.5, duration=3, effort="high", category="maintenance"),
            _proposal("b", priority=8.5, duration=2, effort="medium", category="task"),
            _proposal("c", priority=8.0, duration=2, effort="medium", category="task"),
            _proposal("d", priority=7.5, duration=1, effort="low", category="other"),
        ],
    }

    result = run_decision_engine_v2(payload)

    assert result["backlog"] == result["unscheduled_actions"]

    # Ensure no duplication across day schedules + backlog.
    collected_ids: list[str] = []
    for key in ("day_1_schedule", "day_2_schedule", "day_3_schedule", "backlog"):
        collected_ids.extend(str(row.get("proposal_id")) for row in result[key])
    assert len(collected_ids) == len(set(collected_ids))


def test_decision_engine_v2_capacity_metrics_are_consistent() -> None:
    payload = {
        "calendar_events": [
            {"start_time": "2026-04-16T12:00:00", "end_time": "2026-04-16T21:00:00", "source": "calendar_events"},
            {"start_time": "2026-04-17T08:00:00", "end_time": "2026-04-17T14:00:00", "source": "calendar_events"},
        ],
        "signals": [
            {"id": "s1", "type": "events_today", "message": "events_today=2", "severity": "medium", "source_module": "calendar_module"},
        ],
        "proposals": [
            _proposal("x1", priority=9.0, duration=3, effort="high", category="maintenance"),
            _proposal("x2", priority=8.9, duration=2, effort="medium", category="task"),
            _proposal("x3", priority=8.8, duration=2, effort="medium", category="task"),
            _proposal("x4", priority=8.7, duration=1, effort="low", category="other"),
            _proposal("x5", priority=8.6, duration=1, effort="low", category="other"),
        ],
    }

    result = run_decision_engine_v2(payload)
    metrics = result["_internal"]["daily_load_balancer"]

    assert "day_1" in metrics
    assert "day_2" in metrics
    assert "day_3" in metrics

    for day_key in ("day_1", "day_2", "day_3"):
        day_metrics = metrics[day_key]
        assert float(day_metrics["total_capacity_used"]) >= 0.0
        assert float(day_metrics["remaining_slack"]) >= 0.0
        assert float(day_metrics["overload_penalty"]) >= 0.0

    day_1_date = str(result["scheduled_actions"][0]["start_time"])[:10] if result["scheduled_actions"] else "2026-04-16"
    day_1_used = float(sum(int(row.get("duration_units", 1)) for row in result["scheduled_actions"]))
    assert float(metrics["day_1"]["total_capacity_used"]) == day_1_used

    available_hours = _available_hours_for_day(payload, day_1_date)
    assert day_1_used <= available_hours
    assert float(metrics["day_1"]["overload_penalty"]) == 0.0

    assert float(metrics["day_2"]["total_capacity_used"]) == 0.0
    assert float(metrics["day_3"]["total_capacity_used"]) == 0.0

