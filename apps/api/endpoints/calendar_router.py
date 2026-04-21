"""
Calendar REST API Router
=========================
Full CRUD for household calendar events.

Endpoints:
  GET    /v1/calendar/{household_id}/events           → list events
  POST   /v1/calendar/{household_id}/events           → create event
  GET    /v1/calendar/{household_id}/events/{event_id} → single event
  PATCH  /v1/calendar/{household_id}/events/{event_id} → update event
  DELETE /v1/calendar/{household_id}/events/{event_id} → delete event

All writes emit SystemEvents that propagate through the event bus
to update the UI bootstrap state.  The SSE broadcast (P0-5) picks
these up so connected clients see changes in real-time.
"""
from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from apps.api.services.calendar_service import (
    create_recurring_event,
    delete_event,
    get_event_by_id,
    get_events_by_household,
    schedule_event,
    update_event,
)

router = APIRouter(prefix="/v1/calendar", tags=["calendar"])


# ---------------------------------------------------------------------------
# Request / Response contracts
# ---------------------------------------------------------------------------


class CreateEventRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str
    title: str
    description: str | None = None
    start_time: str | None = None
    duration_minutes: int = Field(default=30, ge=5, le=480)
    recurrence: Literal["none", "daily", "weekly", "monthly"] = "none"


class UpdateEventRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    description: str | None = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/{household_id}/events")
def list_events(household_id: str, include_past: bool = False) -> list[dict]:
    """Return all upcoming calendar events for a household."""
    try:
        return get_events_by_household(household_id, include_past=include_past)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/{household_id}/events", status_code=201)
def create_event(household_id: str, body: CreateEventRequest) -> dict:
    """
    Create a new calendar event.

    Recurring events (recurrence != "none") are created via the recurring
    path which sets the metadata frequency field.
    """
    try:
        if body.recurrence != "none":
            return create_recurring_event(
                household_id=household_id,
                user_id=body.user_id,
                title=body.title,
                frequency=body.recurrence,
                duration_minutes=body.duration_minutes,
                description=body.description,
            )
        return schedule_event(
            household_id=household_id,
            user_id=body.user_id,
            title=body.title,
            description=body.description,
            duration_minutes=body.duration_minutes,
            start_time=body.start_time,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{household_id}/events/{event_id}")
def get_event(household_id: str, event_id: str) -> dict:
    """Fetch a single event by id."""
    try:
        return get_event_by_id(household_id, event_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.patch("/{household_id}/events/{event_id}")
def update_event_endpoint(
    household_id: str, event_id: str, body: UpdateEventRequest
) -> dict:
    """Update mutable fields on an existing event."""
    try:
        return update_event(
            household_id=household_id,
            event_id=event_id,
            title=body.title,
            start_time=body.start_time,
            end_time=body.end_time,
            description=body.description,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.delete("/{household_id}/events/{event_id}", status_code=200)
def delete_event_endpoint(household_id: str, event_id: str) -> dict:
    """Delete an event from the calendar."""
    try:
        return delete_event(household_id=household_id, event_id=event_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
