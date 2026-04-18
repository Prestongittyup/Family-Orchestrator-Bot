from __future__ import annotations

from datetime import UTC, datetime, timedelta

from apps.api.services.calendar_service import get_events_by_household
from modules.core.models.module_output import ModuleOutput, Proposal, Signal


def _parse_iso(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.astimezone(UTC).replace(tzinfo=None)
        return parsed
    except Exception:
        return None


def _priority_hint(priority: int) -> str:
    if priority >= 5:
        return "critical"
    if priority >= 4:
        return "high"
    if priority >= 3:
        return "medium"
    return "low"


def _hint_to_numeric(hint: str) -> int:
    mapping = {
        "critical": 5,
        "high": 4,
        "medium": 3,
        "low": 2,
    }
    return mapping.get(hint, 3)


def _proposal_description(
    event_id: str,
    priority_hint: str,
    time_window: str | None,
) -> str:
    window = time_window if time_window is not None else "none"
    return (
        f"reference={event_id}; "
        f"priority_hint={priority_hint}; "
        f"time_window={window}"
    )


def calendar_module(household_id: str) -> ModuleOutput:
    now = datetime.utcnow()
    today = now.date()
    horizon = now + timedelta(days=7)

    events = get_events_by_household(household_id, include_past=False)
    events_sorted = sorted(
        events,
        key=lambda item: (str(item.get("start_time", "")), str(item.get("event_id", ""))),
    )

    events_today = []
    upcoming_events = []
    high_priority_events = []

    for event in events_sorted:
        start_dt = _parse_iso(str(event.get("start_time", "")))
        end_dt = _parse_iso(str(event.get("end_time", "")))
        priority = int(event.get("priority", 3))

        if start_dt is not None and start_dt.date() == today:
            events_today.append(event)

        if start_dt is not None and now < start_dt <= horizon:
            upcoming_events.append(event)

        if priority >= 4:
            high_priority_events.append(event)

        event["_start_dt"] = start_dt
        event["_end_dt"] = end_dt

    signals = [
        Signal(
            id=f"{household_id}_events_today_signal",
            type="events_today",
            message=f"events_today={len(events_today)}",
            severity="medium" if events_today else "low",
            source_module="calendar_module",
        ),
        Signal(
            id=f"{household_id}_upcoming_events_signal",
            type="upcoming_events",
            message=f"upcoming_events={len(upcoming_events)}",
            severity="medium" if upcoming_events else "low",
            source_module="calendar_module",
        ),
        Signal(
            id=f"{household_id}_high_priority_events_signal",
            type="high_priority_events",
            message=f"high_priority_events={len(high_priority_events)}",
            severity="high" if high_priority_events else "low",
            source_module="calendar_module",
        ),
    ]

    proposals: list[Proposal] = []

    for event in events_sorted:
        event_id = str(event.get("event_id", ""))
        title = str(event.get("title", "Untitled Event"))
        priority = int(event.get("priority", 3))
        hint = _priority_hint(priority)
        start_dt = event.get("_start_dt")
        end_dt = event.get("_end_dt")

        if isinstance(start_dt, datetime) and isinstance(end_dt, datetime):
            window = f"{start_dt.isoformat()}->{end_dt.isoformat()}"
        else:
            window = None

        proposals.append(
            Proposal(
                id=f"prepare_for_event_{event_id}",
                type="prepare_for_event",
                title=f"Prepare for: {title}",
                description=_proposal_description(event_id, hint, window),
                priority=max(2, _hint_to_numeric(hint) - 1),
                source_module="calendar_module",
                duration=2 if priority >= 4 else 1,
                effort="medium" if priority < 4 else "high",
                category="event_prep",
            )
        )

        if window is not None:
            proposals.append(
                Proposal(
                    id=f"leave_buffer_time_{event_id}",
                    type="leave_buffer_time",
                    title=f"Leave buffer before: {title}",
                    description=_proposal_description(event_id, hint, window),
                    priority=_hint_to_numeric(hint),
                    source_module="calendar_module",
                    duration=1,
                    effort="low",
                    category="event_prep",
                )
            )

        if priority >= 4:
            proposals.append(
                Proposal(
                    id=f"prioritize_event_{event_id}",
                    type="prioritize_event",
                    title=f"Prioritize: {title}",
                    description=_proposal_description(event_id, "high", window),
                    priority=5,
                    source_module="calendar_module",
                    duration=1,
                    effort="medium",
                    category="event_prep",
                )
            )

    return ModuleOutput(
        module="calendar_module",
        proposals=proposals,
        signals=signals,
        confidence=0.9 if events_sorted else 0.75,
        metadata={
            "household_id": household_id,
            "source": "calendar_events",
            "event_count": len(events_sorted),
            "events_today_count": len(events_today),
            "upcoming_events_count": len(upcoming_events),
            "high_priority_events_count": len(high_priority_events),
        },
    )

