"""
Calendar Service

Manages calendar events for households and users.

DESIGN:
    • TIL-authoritative scheduling decisions
    • Calendar service executes persistence and event emission
    • No local scheduling computation or conflict resolution
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from uuid import uuid4

from sqlalchemy import text

from apps.api.core.event_bus import get_event_bus
from apps.api.core.database import SessionLocal
from apps.api.core.shared_dependencies import get_til
from apps.api.schemas.event import SystemEvent
from apps.api.services.event_log_service import log_system_event

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat()


def _persist_calendar_event(
    event_id: str,
    household_id: str,
    title: str,
    start_time: str,
    end_time: str,
    priority: int,
    metadata: dict,
) -> None:
    session = SessionLocal()
    try:
        session.execute(
            text(
                """
                INSERT OR REPLACE INTO calendar_events (
                    id,
                    household_id,
                    title,
                    start_time,
                    end_time,
                    priority,
                    metadata,
                    created_at
                ) VALUES (
                    :id,
                    :household_id,
                    :title,
                    :start_time,
                    :end_time,
                    :priority,
                    :metadata,
                    :created_at
                )
                """
            ),
            {
                "id": event_id,
                "household_id": household_id,
                "title": title,
                "start_time": start_time,
                "end_time": end_time,
                "priority": max(1, min(5, int(priority))),
                "metadata": json.dumps(metadata, sort_keys=True),
                "created_at": _utc_now_iso(),
            },
        )
        session.commit()
    finally:
        session.close()


def get_events_by_household(household_id: str, include_past: bool = False) -> list[dict]:
    """
    Fetch structured calendar events for a household.

    Returns normalized dictionaries rather than raw DB rows.
    """
    session = SessionLocal()
    try:
        query = """
            SELECT
                id,
                household_id,
                title,
                start_time,
                end_time,
                priority,
                metadata,
                created_at
            FROM calendar_events
            WHERE household_id = :household_id
        """
        params: dict[str, object] = {"household_id": household_id}

        if include_past is False:
            query += " AND end_time >= :now_iso"
            params["now_iso"] = _utc_now_iso()

        query += " ORDER BY start_time ASC, id ASC"

        rows = session.execute(text(query), params).mappings().all()

        events: list[dict] = []
        for row in rows:
            raw_metadata = row.get("metadata")
            parsed_metadata: dict = {}
            if isinstance(raw_metadata, str) and raw_metadata:
                try:
                    parsed_metadata = json.loads(raw_metadata)
                except Exception:
                    parsed_metadata = {}

            events.append(
                {
                    "event_id": str(row["id"]),
                    "household_id": str(row["household_id"]),
                    "title": str(row["title"]),
                    "start_time": str(row["start_time"]),
                    "end_time": str(row["end_time"]),
                    "priority": int(row["priority"]),
                    "metadata": parsed_metadata,
                    "created_at": str(row["created_at"]),
                }
            )

        return events
    finally:
        session.close()


def schedule_event(
    household_id: str,
    user_id: str,
    title: str,
    description: str | None = None,
    duration_minutes: int = 30,
    start_time: str | None = None,
) -> dict:
    """
    Schedule a calendar event for a user in a household.

    TIL is the sole authority for scheduling decisions.

    Args:
        household_id: Identifier for the household (e.g., "hh-123")
        user_id: Identifier for the user (e.g., "user-456")
        title: Event title (e.g., "Team Meeting")
        description: Optional event description
        duration_minutes: Caller-provided requested duration (hint only)
        start_time: Caller-provided requested start time (hint only)

    Returns:
        dict with event details:
          {
            "event_id": "evt-xxx",
            "household_id": "hh-123",
            "user_id": "user-456",
            "title": "Team Meeting",
            "start_time": "2026-04-14T15:00:00",
                        "duration_minutes": 30,
            "created_at": "2026-04-14T14:30:00",
                        "til_schedule": {
                            "estimated_duration_minutes": 30,
                            "scheduled_start_time": "2026-04-14T15:00:00",
                            "scheduled_end_time": "2026-04-14T15:30:00",
                            "availability_check_passed": True
            }
          }

    BEHAVIOR:
      1. Ask TIL for duration estimate
      2. Ask TIL for a schedule suggestion
      3. Ask TIL for availability at suggested start time
      4. If unavailable, request another TIL suggestion (never reject event)
      5. Persist and emit event using TIL-provided schedule only
    """
    """
    TEMPORAL RULE:
    All scheduling decisions are delegated to TIL.
    This service is execution-only for calendar persistence.
    No local scheduling logic is permitted.
    """
    event_payload = {
        "title": title,
        "description": description,
        "requested_duration_minutes": duration_minutes,
        "requested_start_time": start_time,
    }

    # TIL-authoritative scheduling flow
    til = get_til()

    duration = til.estimate_duration(
        task_type="calendar_event",
        payload=event_payload,
    )

    schedule = til.suggest_time_slot(
        user_id=user_id,
        household_id=household_id,
        duration_minutes=duration,
    )

    is_available = til.check_availability(
        user_id=user_id,
        household_id=household_id,
        requested_time=schedule["start_time"],
    )

    # Never reject event: use the next TIL-provided slot when unavailable.
    if is_available is False:
        schedule = til.suggest_time_slot(
            user_id=user_id,
            household_id=household_id,
            duration_minutes=duration,
        )

    # Create persisted event payload using TIL schedule only.
    event_id = f"evt-{str(uuid4()).replace('-', '')[:12]}"
    now = _utc_now_iso()

    event_metadata = {
        "user_id": user_id,
        "description": description,
        "duration_minutes": duration,
        "til_schedule": {
            "estimated_duration_minutes": duration,
            "scheduled_start_time": schedule["start_time"],
            "scheduled_end_time": schedule["end_time"],
            "availability_check_passed": is_available,
        },
    }

    _persist_calendar_event(
        event_id=event_id,
        household_id=household_id,
        title=title,
        start_time=schedule["start_time"],
        end_time=schedule["end_time"],
        priority=3,
        metadata=event_metadata,
    )

    event = {
        "event_id": event_id,
        "household_id": household_id,
        "user_id": user_id,
        "title": title,
        "description": description,
        "start_time": schedule["start_time"],
        "end_time": schedule["end_time"],
        "duration_minutes": duration,
        "created_at": now,
        "til_schedule": {
            "estimated_duration_minutes": duration,
            "scheduled_start_time": schedule["start_time"],
            "scheduled_end_time": schedule["end_time"],
            "availability_check_passed": is_available,
        },
    }

    calendar_event = SystemEvent(
        household_id=household_id,
        type="calendar_event_scheduled",
        source="calendar_service",
        payload=event,
    )

    log_system_event(calendar_event)
    get_event_bus().publish(calendar_event)

    logger.debug(f"Calendar event created: {event_id} for user {user_id}")
    return event


def create_recurring_event(
    household_id: str,
    user_id: str,
    title: str,
    frequency: str,
    duration_minutes: int = 30,
    description: str | None = None,
) -> dict:
    """
    Create a recurring calendar event.

    TIL is the sole authority for recurring schedule and duration decisions.

    Args:
        household_id: Household identifier
        user_id: User identifier
        title: Event title
        frequency: Recurrence frequency ("daily", "weekly", "monthly")
        duration_minutes: Event duration
        description: Optional description

    Returns:
        dict with recurring event details and TIL schedule metadata
    """
    """
    TEMPORAL RULE:
    All scheduling decisions are delegated to TIL.
    This service is execution-only for calendar persistence.
    No local scheduling logic is permitted.
    """
    event_payload = {
        "title": title,
        "description": description,
        "frequency": frequency,
        "requested_duration_minutes": duration_minutes,
    }

    # TIL-authoritative scheduling flow
    til = get_til()

    duration = til.estimate_duration(
        task_type="calendar_event",
        payload=event_payload,
    )

    schedule = til.suggest_time_slot(
        user_id=user_id,
        household_id=household_id,
        duration_minutes=duration,
    )

    is_available = til.check_availability(
        user_id=user_id,
        household_id=household_id,
        requested_time=schedule["start_time"],
    )

    # Never reject event: use the next TIL-provided slot when unavailable.
    if is_available is False:
        schedule = til.suggest_time_slot(
            user_id=user_id,
            household_id=household_id,
            duration_minutes=duration,
        )

    # Create recurring event using only TIL-provided scheduling values.
    event_id = f"evt-{str(uuid4()).replace('-', '')[:12]}"
    now = _utc_now_iso()

    recurring_metadata = {
        "user_id": user_id,
        "description": description,
        "frequency": frequency,
        "duration_minutes": duration,
        "til_schedule": {
            "estimated_duration_minutes": duration,
            "scheduled_start_time": schedule["start_time"],
            "scheduled_end_time": schedule["end_time"],
            "availability_check_passed": is_available,
        },
    }

    _persist_calendar_event(
        event_id=event_id,
        household_id=household_id,
        title=title,
        start_time=schedule["start_time"],
        end_time=schedule["end_time"],
        priority=4,
        metadata=recurring_metadata,
    )

    recurring_event = {
        "event_id": event_id,
        "household_id": household_id,
        "user_id": user_id,
        "title": title,
        "frequency": frequency,
        "start_time": schedule["start_time"],
        "end_time": schedule["end_time"],
        "duration_minutes": duration,
        "created_at": now,
        "description": description,
        "til_schedule": {
            "estimated_duration_minutes": duration,
            "scheduled_start_time": schedule["start_time"],
            "scheduled_end_time": schedule["end_time"],
            "availability_check_passed": is_available,
        },
    }

    recurring_calendar_event = SystemEvent(
        household_id=household_id,
        type="calendar_recurring_event_created",
        source="calendar_service",
        payload=recurring_event,
    )

    log_system_event(recurring_calendar_event)
    get_event_bus().publish(recurring_calendar_event)

    logger.debug(f"Recurring calendar event created: {event_id}")
    return recurring_event
