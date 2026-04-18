"""
household_state.py
------------------
Canonical cross-system state model for the Family Orchestration system.

All surfaces (debug, brief, UI) must derive their output ONLY from
HouseholdState.  No endpoint may fetch provider data directly.

Architectural contract
----------------------
Providers produce raw data → StateBuilder produces truth →
Orchestrator produces HouseholdState → Endpoints project it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

State = Literal["enabled", "disabled", "degraded"]


@dataclass
class IntegrationHealth:
    """Health record for a single named integration."""

    integration: str
    state: State
    reason: Optional[str] = None
    action: Optional[str] = None

    def as_dict(self) -> dict:
        return {
            "integration": self.integration,
            "state": self.state,
            "reason": self.reason,
            "action": self.action,
        }


@dataclass
class CalendarEvent:
    """Normalised, presentation-ready calendar event."""

    event_id: str
    start: str
    end: str
    title: str

    def as_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "title": self.title,
            "start": self.start,
            "end": self.end,
        }


@dataclass
class WindowedCalendar:
    """Three windowed views over the same underlying event set."""

    window_7d: List[CalendarEvent] = field(default_factory=list)
    window_30d: List[CalendarEvent] = field(default_factory=list)
    window_90d: List[CalendarEvent] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "window_7d": [e.as_dict() for e in self.window_7d],
            "window_30d": [e.as_dict() for e in self.window_30d],
            "window_90d": [e.as_dict() for e in self.window_90d],
        }


def _serialize_metadata(value: Any) -> Any:
    if isinstance(value, CalendarEvent):
        return value.as_dict()
    if isinstance(value, IntegrationHealth):
        return value.as_dict()
    if isinstance(value, WindowedCalendar):
        return value.as_dict()
    if isinstance(value, list):
        return [_serialize_metadata(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _serialize_metadata(item) for key, item in value.items()}
    return value


@dataclass
class HouseholdState:
    """
    Single canonical state object.  All endpoints must project from this;
    none may call providers directly.
    """

    user_id: str
    calendar_events: List[CalendarEvent] = field(default_factory=list)
    tasks: List[dict] = field(default_factory=list)
    alerts: List[dict] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def integrations(self) -> List[IntegrationHealth]:
        return list(self.metadata.get("integrations", []))

    @property
    def debug_meta(self) -> Dict[str, Any]:
        raw = self.metadata.get("debug_meta", {})
        return dict(raw) if isinstance(raw, dict) else {}

    @property
    def calendar(self) -> WindowedCalendar:
        return WindowedCalendar(
            window_7d=list(self.metadata.get("calendar_window_7d", [])),
            window_30d=list(self.metadata.get("calendar_window_30d", [])),
            window_90d=list(self.metadata.get("calendar_window_90d", [])),
        )

    def as_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "calendar_events": [event.as_dict() for event in self.calendar_events],
            "tasks": [_serialize_metadata(task) for task in self.tasks],
            "alerts": [_serialize_metadata(alert) for alert in self.alerts],
            "metadata": _serialize_metadata(self.metadata),
            "calendar": self.calendar.as_dict(),
            "integrations": [i.as_dict() for i in self.integrations],
            "debug_meta": self.debug_meta,
        }

    # ------------------------------------------------------------------
    # Projections (read-only derived views — no side-effects)
    # ------------------------------------------------------------------

    def brief(self, *, window: int = 7) -> dict:
        """Compatibility wrapper around the pure BriefBuilder projection."""
        from apps.api.integration_core.brief_builder import BriefBuilder

        brief = BriefBuilder().build(self)
        if window == 30:
            events = self.calendar.window_30d
        elif window == 90:
            events = self.calendar.window_90d
        else:
            events = self.calendar.window_7d

        brief["event_count"] = len(events)
        brief["events"] = [event.as_dict() for event in events]
        brief["integrations"] = [integration.as_dict() for integration in self.integrations]
        return brief

    def debug(self) -> dict:
        """Return a full debug projection including all windows and metadata."""
        return {
            **self.as_dict(),
            "brief_7d": self.brief(window=7),
        }
