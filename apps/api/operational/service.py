from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from apps.api.integration_core.models.household_state import HouseholdState
from apps.api.operational.contracts import ConflictItem, OperationalResponse, PriorityItem, ScheduleActionItem


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    raw = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _priority_level_for_event(event: dict[str, Any]) -> str:
    start = _parse_iso(str(event.get("start", "")))
    if start is None:
        return "medium"
    delta_minutes = int((start - datetime.now(UTC)).total_seconds() // 60)
    if delta_minutes <= 120:
        return "high"
    if delta_minutes <= 480:
        return "medium"
    return "low"


def _build_priorities(top_events: list[dict[str, Any]]) -> list[PriorityItem]:
    items: list[PriorityItem] = []
    for event in top_events[:5]:
        title = str(event.get("title", "Untitled event"))
        level = _priority_level_for_event(event)
        reason = "Time-sensitive household event" if level == "high" else "Scheduled household commitment"
        items.append(PriorityItem(title=title, priority_level=level, reason=reason))
    return items


def _build_schedule_actions(events: list[dict[str, Any]]) -> list[ScheduleActionItem]:
    rows: list[ScheduleActionItem] = []
    for event in events[:8]:
        title = str(event.get("title", "Untitled event"))
        start = str(event.get("start", ""))
        level = _priority_level_for_event(event)
        confidence = 0.9 if level == "high" else 0.75 if level == "medium" else 0.6
        rows.append(
            ScheduleActionItem(
                action=f"Prepare for {title}",
                time=start,
                confidence=confidence,
            )
        )
    return rows


def _build_conflicts(conflicts: list[list[dict[str, Any]]]) -> list[ConflictItem]:
    rows: list[ConflictItem] = []
    for pair in conflicts:
        if len(pair) < 2:
            continue
        first = pair[0]
        second = pair[1]
        rows.append(
            ConflictItem(
                conflict_type="schedule_overlap",
                severity="high",
                description=(
                    f"Overlap detected between '{first.get('title', 'Event A')}' and "
                    f"'{second.get('title', 'Event B')}'."
                ),
            )
        )
    return rows


def build_operational_response(
    *,
    household_id: str,
    state: HouseholdState,
    decision_context: Any,
    mode: str,
) -> OperationalResponse:
    top_events = list(getattr(decision_context, "top_events", []) or [])
    state_events = [event.as_dict() for event in state.calendar_events]
    context_conflicts = list(getattr(decision_context, "conflicts", []) or [])

    if mode == "context":
        priorities = _build_priorities(top_events)
        actions = _build_schedule_actions(state_events)
        notes = [
            f"State has {len(state_events)} calendar events.",
            f"Active priorities: {len(priorities)}.",
            f"Tasks loaded: {len(state.tasks)}.",
        ]
    elif mode == "brief":
        priorities = _build_priorities(top_events)
        actions = _build_schedule_actions(top_events)
        notes = [
            "Final operational brief projected from integration core outputs.",
            "Action items represent immediate household commitments.",
        ]
    else:
        priorities = _build_priorities(top_events)
        actions = _build_schedule_actions(top_events or state_events)
        notes = [
            "Daily operational run executed through integration core orchestration pipeline.",
            f"Generated from {len(state_events)} normalized events.",
        ]

    response = OperationalResponse(
        timestamp=_utc_now_iso(),
        household_id=household_id,
        top_priorities=priorities,
        schedule_actions=actions,
        conflicts=_build_conflicts(context_conflicts),
        system_notes=notes,
    )
    return response
