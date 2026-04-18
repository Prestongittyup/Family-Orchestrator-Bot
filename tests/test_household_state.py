"""
test_household_state.py
-----------------------
Tests for the HouseholdState model, StateBuilder, and the architectural
contract for state projections.

Acceptance criteria
-------------------
1. disabled OAuth → still returns valid HouseholdState (no crash)
2. event exists in provider → appears in all 3 windows if in range
3. debug == brief source consistency
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from apps.api.integration_core.models.household_state import (
    HouseholdState,
    WindowedCalendar,
)
from apps.api.integration_core.event_windowing import OrchestrationView
from apps.api.integration_core.state_builder import StateBuilder


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _future_iso(days: int, hours: int = 0) -> str:
    dt = datetime.now(UTC) + timedelta(days=days, hours=hours)
    return dt.isoformat().replace("+00:00", "Z")


def _make_event(days_from_now: int, title: str = "Test Event") -> dict[str, Any]:
    start = datetime.now(UTC) + timedelta(days=days_from_now)
    end = start + timedelta(hours=1)
    return {
        "id": f"event-{days_from_now}",
        "title": title,
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
    }


class _MockProvider:
    """Provider that returns a fixed list of events without any OAuth."""

    def __init__(self, events: list[dict]) -> None:
        self._events = events
        self._status = {"status": "enabled", "reason": None}

    def fetch_events(self, *, user_id, max_results=50, **kwargs) -> list[dict]:
        return list(self._events)

    def get_runtime_status(self) -> dict:
        return self._status


class _DisabledProvider(_MockProvider):
    """Provider that returns no events and reports disabled status."""

    def __init__(self) -> None:
        super().__init__(events=[])
        self._status = {
            "status": "disabled",
            "reason": "google_oauth_not_configured",
            "configured": False,
            "missing_fields": ["GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET"],
            "message": "Google OAuth not configured",
        }

    def fetch_events(self, *, user_id, max_results=50, **kwargs) -> list[dict]:
        return []


# ---------------------------------------------------------------------------
# 1. Disabled OAuth → valid HouseholdState (no crash)
# ---------------------------------------------------------------------------

def test_disabled_oauth_returns_valid_household_state():
    provider = _DisabledProvider()
    builder = StateBuilder(provider=provider, user_id="test-user")
    state = builder.build()

    assert isinstance(state, HouseholdState)
    assert state.user_id == "test-user"
    assert state.calendar_events == []
    assert state.tasks == []
    assert state.alerts == []
    assert isinstance(state.metadata, dict)
    assert isinstance(state.calendar, WindowedCalendar)
    assert state.calendar.window_7d == []
    assert state.calendar.window_30d == []
    assert state.calendar.window_90d == []
    assert len(state.integrations) == 1
    health = state.integrations[0]
    assert health.state == "disabled"
    assert health.reason == "google_oauth_not_configured"


def test_disabled_oauth_health_surfaces_action():
    provider = _DisabledProvider()
    builder = StateBuilder(provider=provider, user_id="test-user")
    state = builder.build()
    health = state.integrations[0]
    assert health.action is not None
    assert "GOOGLE_CLIENT_ID" in health.action or "connect" in (health.action or "").lower() or health.action


def test_disabled_oauth_state_serialises_cleanly():
    provider = _DisabledProvider()
    state = StateBuilder(provider=provider, user_id="alice").build()
    d = state.as_dict()
    assert d["user_id"] == "alice"
    assert d["integrations"][0]["state"] == "disabled"
    assert "calendar" in d
    assert "debug_meta" in d


# ---------------------------------------------------------------------------
# 2. Event appears in all 3 windows when in range
# ---------------------------------------------------------------------------

def test_event_in_range_appears_in_all_windows():
    # An event 5 days in the future is within 7d, 30d, and 90d.
    events = [_make_event(5, "5-day event")]
    provider = _MockProvider(events)
    state = StateBuilder(provider=provider, user_id="bob").build()

    assert any(e.title == "5-day event" for e in state.calendar.window_7d)
    assert any(e.title == "5-day event" for e in state.calendar.window_30d)
    assert any(e.title == "5-day event" for e in state.calendar.window_90d)


def test_event_beyond_7d_appears_only_in_30d_and_90d():
    # 15 days from now: out of 7d window, in 30d and 90d.
    events = [_make_event(15, "15-day event")]
    provider = _MockProvider(events)
    state = StateBuilder(provider=provider, user_id="bob").build()

    assert not any(e.title == "15-day event" for e in state.calendar.window_7d)
    assert any(e.title == "15-day event" for e in state.calendar.window_30d)
    assert any(e.title == "15-day event" for e in state.calendar.window_90d)


def test_event_beyond_30d_appears_only_in_90d():
    # 45 days from now: only within 90d window.
    events = [_make_event(45, "45-day event")]
    provider = _MockProvider(events)
    state = StateBuilder(provider=provider, user_id="bob").build()

    assert not any(e.title == "45-day event" for e in state.calendar.window_7d)
    assert not any(e.title == "45-day event" for e in state.calendar.window_30d)
    assert any(e.title == "45-day event" for e in state.calendar.window_90d)


def test_stale_event_excluded_from_all_windows():
    # 26 hours in the past: beyond the 24-hour lookback.
    start = datetime.now(UTC) - timedelta(hours=26)
    end = start + timedelta(hours=1)
    stale_event = {
        "id": "stale",
        "title": "Stale Event",
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
    }
    provider = _MockProvider([stale_event])
    state = StateBuilder(provider=provider, user_id="bob").build()

    for window in (state.calendar.window_7d, state.calendar.window_30d, state.calendar.window_90d):
        assert not any(e.title == "Stale Event" for e in window)


# ---------------------------------------------------------------------------
# 3. Debug and brief share the same underlying state
# ---------------------------------------------------------------------------

def test_debug_and_brief_share_same_event_data():
    events = [_make_event(2, "Shared Event")]
    provider = _MockProvider(events)
    state = StateBuilder(provider=provider, user_id="carol").build()

    brief = state.brief(window=7)
    debug = state.debug()

    # brief events come from window_7d; debug calendar.window_7d is the same
    brief_titles = {e["title"] for e in brief["events"]}
    debug_7d_titles = {e["title"] for e in debug["calendar"]["window_7d"]}
    assert brief_titles == debug_7d_titles


def test_debug_brief_7d_key_matches_brief_projection():
    events = [_make_event(1, "Today+1")]
    provider = _MockProvider(events)
    state = StateBuilder(provider=provider, user_id="carol").build()

    debug = state.debug()
    brief = state.brief(window=7)
    assert debug["brief_7d"]["events"] == brief["events"]


def test_household_state_brief_projection_contains_integration_health():
    provider = _DisabledProvider()
    state = StateBuilder(provider=provider, user_id="dave").build()
    brief = state.brief()
    assert "integrations" in brief
    assert brief["integrations"][0]["state"] == "disabled"


def test_household_state_canonical_fields_are_source_of_truth():
    events = [_make_event(1, "Canonical Event")]
    provider = _MockProvider(events)
    state = StateBuilder(provider=provider, user_id="ellen").build()

    assert len(state.calendar_events) == 1
    assert state.calendar_events[0].title == "Canonical Event"
    assert state.tasks == []
    assert state.alerts == []
    assert "reference_time" in state.metadata
    assert "calendar_window_7d" in state.metadata


def test_state_builder_applies_windowing():
    now = datetime.now(UTC)
    events = [
        {
            "id": "keep-past-buffer",
            "title": "Recently ended",
            "start": (now - timedelta(hours=23)).isoformat().replace("+00:00", "Z"),
            "end": (now - timedelta(hours=22)).isoformat().replace("+00:00", "Z"),
        },
        {
            "id": "keep-short",
            "title": "Within 7 days",
            "start": (now + timedelta(days=6)).isoformat().replace("+00:00", "Z"),
            "end": (now + timedelta(days=6, hours=1)).isoformat().replace("+00:00", "Z"),
        },
        {
            "id": "drop-medium",
            "title": "Outside short window",
            "start": (now + timedelta(days=20)).isoformat().replace("+00:00", "Z"),
            "end": (now + timedelta(days=20, hours=1)).isoformat().replace("+00:00", "Z"),
        },
        {
            "id": "drop-stale",
            "title": "Too old",
            "start": (now - timedelta(hours=26)).isoformat().replace("+00:00", "Z"),
            "end": (now - timedelta(hours=25)).isoformat().replace("+00:00", "Z"),
        },
    ]
    provider = _MockProvider(events)
    state = StateBuilder(
        provider=provider,
        user_id="window-user",
        view=OrchestrationView.SHORT,
    ).build()

    assert [event.title for event in state.calendar_events] == [
        "Recently ended",
        "Within 7 days",
    ]
    assert [event.title for event in state.calendar.window_7d] == [
        "Recently ended",
        "Within 7 days",
    ]
    assert state.metadata["active_view"] == OrchestrationView.SHORT.name

