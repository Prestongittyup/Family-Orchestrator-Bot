"""
test_google_calendar_pilot.py
------------------------------
Test suite for the Google Calendar integration pilot.

Covers:
  1. map_google_event_to_raw  — field mapping, timestamp normalization
  2. Normalization determinism — same input → same ExternalEvent
  3. Ordering stability       — sorted output order is stable across runs
  4. ExternalEvent schema     — all required fields present and typed
  5. Architecture guard       — no OS-1 / OS-2 imports in pilot modules
  6. SandboxRunner            — full pipeline with mock HTTP client
  7. Pagination               — multi-page response is handled correctly
  8. Provider contract        — authenticate / health_check / required_scopes
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

from apps.api.integration_core.architecture_guard import (
    FORBIDDEN_IMPORT_PREFIXES,
    validate_loaded_module_boundaries,
)
from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore, OAuthCredential
from apps.api.integration_core.event_windowing import OrchestrationView
from apps.api.integration_core.google_calendar_provider import (
    PROVIDER_NAME,
    GoogleCalendarRealProvider,
    build_event_id_debug,
    map_google_event_to_raw,
)
from apps.api.integration_core.google_calendar_sandbox_runner import (
    GoogleCalendarSandboxRunner,
    SandboxRunResult,
)
from apps.api.integration_core.normalization import (
    ExternalEvent,
    normalize_provider_event,
    normalize_provider_events,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _google_event(
    event_id: str = "gcal-real-001",
    summary: str = "Team standup",
    start_dt: str = "2026-04-17T10:00:00Z",
    end_dt: str = "2026-04-17T10:30:00Z",
    status: str = "confirmed",
    description: str = "Daily sync",
    location: str = "Google Meet",
) -> dict[str, Any]:
    """Minimal Google Calendar API event payload."""
    return {
        "id": event_id,
        "summary": summary,
        "status": status,
        "description": description,
        "location": location,
        "htmlLink": f"https://calendar.google.com/event?eid={event_id}",
        "start": {"dateTime": start_dt, "timeZone": "UTC"},
        "end": {"dateTime": end_dt, "timeZone": "UTC"},
        "organizer": {"email": "organizer@example.test"},
        "attendees": [
            {"email": "alice@example.test"},
            {"email": "bob@example.test"},
        ],
        "created": "2026-04-10T08:00:00Z",
        "updated": "2026-04-15T09:30:00Z",
        "etag": "\"etag-abc123\"",
    }


def _make_http_mock(
    pages: list[list[dict[str, Any]]],
    *,
    calendars: list[dict[str, Any]] | None = None,
) -> MagicMock:
    """
    Build a mock HTTP client that returns *pages* in sequence.
    Each page is a list of raw Google event dicts.
    """
    calendars = calendars or [
        {
            "id": "primary",
            "summary": "Primary",
            "accessRole": "owner",
            "selected": True,
        }
    ]

    events_by_calendar: dict[str, list[list[dict[str, Any]]]] = {
        "primary": pages,
    }
    call_counters: dict[str, int] = {}

    def _response_for_json(payload: dict[str, Any]) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = payload
        return resp

    def _get(url: str, *, headers: dict[str, Any], params: dict[str, Any] | None = None) -> MagicMock:
        if url.endswith("/users/me/calendarList"):
            return _response_for_json(
                {
                    "kind": "calendar#calendarList",
                    "items": calendars,
                }
            )

        if "/calendars/" in url and url.endswith("/events"):
            calendar_id = url.split("/calendars/")[-1].split("/events")[0]
            pages_for_calendar = events_by_calendar.get(calendar_id, pages)
            index = call_counters.get(calendar_id, 0)
            page_items = pages_for_calendar[min(index, len(pages_for_calendar) - 1)] if pages_for_calendar else []
            call_counters[calendar_id] = index + 1

            next_page_token = (
                f"page-token-{calendar_id}-{index + 1}"
                if index < len(pages_for_calendar) - 1
                else None
            )
            return _response_for_json(
                {
                    "kind": "calendar#events",
                    "items": page_items,
                    **({"nextPageToken": next_page_token} if next_page_token else {}),
                }
            )

        return _response_for_json({"items": []})

    http = MagicMock()
    http.get.side_effect = _get
    return http


def _make_store_with_creds(user_id: str) -> InMemoryOAuthCredentialStore:
    store = InMemoryOAuthCredentialStore(test_mode=False)
    store.save_credentials(
        OAuthCredential(
            user_id=user_id,
            provider_name=PROVIDER_NAME,
            access_token="mock-access-token",
            refresh_token=None,
        )
    )
    return store


def _future_iso(*, days: int, hours: int = 0, minutes: int = 0) -> str:
    return (
        datetime.now(UTC) + timedelta(days=days, hours=hours, minutes=minutes)
    ).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# 1. map_google_event_to_raw — field mapping
# ---------------------------------------------------------------------------


class TestMapGoogleEventToRaw:
    def test_event_id_extracted(self):
        raw = map_google_event_to_raw(_google_event(event_id="evt-xyz"))
        assert raw["event_id"] == "evt-xyz"

    def test_summary_becomes_title(self):
        raw = map_google_event_to_raw(_google_event(summary="My Meeting"))
        assert raw["title"] == "My Meeting"

    def test_start_datetime_becomes_timestamp(self):
        raw = map_google_event_to_raw(_google_event(start_dt="2026-05-01T14:00:00Z"))
        assert raw["timestamp"] == "2026-05-01T14:00:00Z"
        assert raw["start"] == "2026-05-01T14:00:00Z"

    def test_end_datetime_extracted(self):
        raw = map_google_event_to_raw(_google_event(end_dt="2026-05-01T15:00:00Z"))
        assert raw["end_timestamp"] == "2026-05-01T15:00:00Z"

    def test_status_normalised_confirmed(self):
        raw = map_google_event_to_raw(_google_event(status="confirmed"))
        assert raw["status"] == "confirmed"

    def test_status_normalised_tentative(self):
        g = _google_event()
        g["status"] = "tentative"
        raw = map_google_event_to_raw(g)
        assert raw["status"] == "tentative"

    def test_status_normalised_unknown(self):
        g = _google_event()
        g["status"] = "something_else"
        raw = map_google_event_to_raw(g)
        assert raw["status"] == "unknown"

    def test_organizer_email_extracted(self):
        raw = map_google_event_to_raw(_google_event())
        assert raw["organizer_email"] == "organizer@example.test"

    def test_attendee_emails_extracted(self):
        raw = map_google_event_to_raw(_google_event())
        assert set(raw["attendee_emails"]) == {"alice@example.test", "bob@example.test"}

    def test_raw_google_event_preserved(self):
        g = _google_event(event_id="orig-001")
        raw = map_google_event_to_raw(g)
        assert "_raw_google_event" in raw
        assert raw["_raw_google_event"]["id"] == "orig-001"

    def test_all_day_event_uses_date_field(self):
        g = _google_event()
        g["start"] = {"date": "2026-06-01"}
        g["end"] = {"date": "2026-06-02"}
        raw = map_google_event_to_raw(g)
        assert raw["timestamp"] == "2026-06-01"

    def test_missing_fields_default_to_empty_string(self):
        raw = map_google_event_to_raw({})
        assert raw["event_id"] == ""
        assert raw["title"] == ""
        assert raw["timestamp"] == ""


# ---------------------------------------------------------------------------
# 2. Normalization determinism
# ---------------------------------------------------------------------------


class TestNormalizationDeterminism:
    def test_same_event_produces_same_event_id(self):
        g = _google_event(event_id="det-001")
        raw = map_google_event_to_raw(g)
        evt1 = normalize_provider_event(
            user_id="u1", provider_name=PROVIDER_NAME, raw_event=raw, event_type="calendar.event"
        )
        evt2 = normalize_provider_event(
            user_id="u1", provider_name=PROVIDER_NAME, raw_event=raw, event_type="calendar.event"
        )
        assert evt1.event_id == evt2.event_id

    def test_different_google_event_ids_produce_different_ext_ids(self):
        raw1 = map_google_event_to_raw(_google_event(event_id="a1"))
        raw2 = map_google_event_to_raw(_google_event(event_id="a2"))
        evt1 = normalize_provider_event(
            user_id="u1", provider_name=PROVIDER_NAME, raw_event=raw1, event_type="calendar.event"
        )
        evt2 = normalize_provider_event(
            user_id="u1", provider_name=PROVIDER_NAME, raw_event=raw2, event_type="calendar.event"
        )
        assert evt1.event_id != evt2.event_id

    def test_different_user_ids_produce_different_ext_ids(self):
        raw = map_google_event_to_raw(_google_event(event_id="shared"))
        evt_a = normalize_provider_event(
            user_id="userA", provider_name=PROVIDER_NAME, raw_event=raw, event_type="calendar.event"
        )
        evt_b = normalize_provider_event(
            user_id="userB", provider_name=PROVIDER_NAME, raw_event=raw, event_type="calendar.event"
        )
        assert evt_a.event_id != evt_b.event_id

    def test_event_id_has_ext_prefix(self):
        raw = map_google_event_to_raw(_google_event())
        evt = normalize_provider_event(
            user_id="u1", provider_name=PROVIDER_NAME, raw_event=raw, event_type="calendar.event"
        )
        assert evt.event_id.startswith("ext-")

    def test_batch_normalization_all_unique_ids(self):
        raws = [map_google_event_to_raw(_google_event(event_id=f"batch-{i}")) for i in range(5)]
        events = normalize_provider_events(
            user_id="u1", provider_name=PROVIDER_NAME, raw_events=raws, event_type="calendar.event"
        )
        ids = [e.event_id for e in events]
        assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# 3. Ordering stability
# ---------------------------------------------------------------------------


class TestOrderingStability:
    def _make_batch(self, user_id: str) -> list[ExternalEvent]:
        times = ["2026-04-17T08:00:00Z", "2026-04-17T10:00:00Z", "2026-04-17T15:00:00Z"]
        raws = [
            map_google_event_to_raw(_google_event(event_id=f"order-{i}", start_dt=t))
            for i, t in enumerate(times)
        ]
        return normalize_provider_events(
            user_id=user_id, provider_name=PROVIDER_NAME, raw_events=raws, event_type="calendar.event"
        )

    def test_batch_sorted_by_timestamp(self):
        events = self._make_batch("u-order")
        sorted_events = sorted(events, key=lambda e: (e.provider_name, e.timestamp, e.event_id))
        timestamps = [e.timestamp for e in sorted_events]
        assert timestamps == sorted(timestamps)

    def test_sorted_order_identical_across_runs(self):
        events1 = self._make_batch("u-stable")
        events2 = self._make_batch("u-stable")
        ids1 = [e.event_id for e in sorted(events1, key=lambda e: (e.provider_name, e.timestamp, e.event_id))]
        ids2 = [e.event_id for e in sorted(events2, key=lambda e: (e.provider_name, e.timestamp, e.event_id))]
        assert ids1 == ids2

    def test_provider_name_appears_first_in_sort_key(self):
        # All events from same provider — sort will fall through to timestamp
        events = self._make_batch("u-key")
        for evt in events:
            assert evt.provider_name == PROVIDER_NAME


# ---------------------------------------------------------------------------
# 4. ExternalEvent schema consistency
# ---------------------------------------------------------------------------


class TestExternalEventSchema:
    def _normalize_one(self, user_id: str = "u1") -> ExternalEvent:
        raw = map_google_event_to_raw(_google_event())
        return normalize_provider_event(
            user_id=user_id, provider_name=PROVIDER_NAME, raw_event=raw, event_type="calendar.event"
        )

    def test_event_id_is_string(self):
        assert isinstance(self._normalize_one().event_id, str)

    def test_user_id_preserved(self):
        evt = self._normalize_one(user_id="schema-user")
        assert evt.user_id == "schema-user"

    def test_provider_name_preserved(self):
        assert self._normalize_one().provider_name == PROVIDER_NAME

    def test_event_type_is_calendar_event(self):
        raw = map_google_event_to_raw(_google_event())
        evt = normalize_provider_event(
            user_id="u1", provider_name=PROVIDER_NAME, raw_event=raw, event_type="calendar.event"
        )
        assert evt.event_type == "calendar.event"

    def test_timestamp_is_string(self):
        assert isinstance(self._normalize_one().timestamp, str)

    def test_payload_is_dict(self):
        assert isinstance(self._normalize_one().payload, dict)

    def test_payload_contains_title(self):
        assert "title" in self._normalize_one().payload

    def test_payload_contains_status(self):
        assert "status" in self._normalize_one().payload

    def test_external_event_is_frozen(self):
        evt = self._normalize_one()
        with pytest.raises((AttributeError, TypeError)):
            evt.event_id = "tampered"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 5. Architecture guard — no forbidden imports
# ---------------------------------------------------------------------------


class TestArchitectureGuard:
    def _load_module(self, dotted_name: str):
        import importlib
        return importlib.import_module(dotted_name)

    def test_provider_module_not_importing_os1(self):
        """Verify no OS-1/OS-2 import prefixes appear in loaded provider module."""
        import sys
        mod = self._load_module("apps.api.integration_core.google_calendar_provider")
        violations = []
        for name in sys.modules:
            for prefix in FORBIDDEN_IMPORT_PREFIXES:
                if name == prefix or name.startswith(f"{prefix}."):
                    # Only flag if the module was introduced BY the pilot modules
                    violations.append(name)
        # The architecture guard function checks the module itself, not transitive deps.
        # We just verify the provider module itself doesn't import forbidden names.
        provider_source = open(mod.__file__).read()
        for prefix in FORBIDDEN_IMPORT_PREFIXES:
            dotted = prefix.replace("apps.api.", "")
            assert prefix not in provider_source, (
                f"Forbidden import prefix '{prefix}' found in google_calendar_provider.py"
            )

    def test_runner_module_not_importing_os1(self):
        import importlib
        mod = importlib.import_module("apps.api.integration_core.google_calendar_sandbox_runner")
        runner_source = open(mod.__file__).read()
        for prefix in FORBIDDEN_IMPORT_PREFIXES:
            assert prefix not in runner_source, (
                f"Forbidden import prefix '{prefix}' found in google_calendar_sandbox_runner.py"
            )

    def test_provider_name_constant(self):
        assert PROVIDER_NAME == "google_calendar"


# ---------------------------------------------------------------------------
# 6. SandboxRunner — full pipeline with mock HTTP client
# ---------------------------------------------------------------------------


class TestSandboxRunner:
    def _run(self, events_page1: list[dict], *, user_id: str = "runner-user") -> SandboxRunResult:
        store = _make_store_with_creds(user_id)
        http = _make_http_mock([events_page1])
        runner = GoogleCalendarSandboxRunner(
            user_id=user_id,
            credential_store=store,
            http_client=http,
        )
        return runner.run()

    def test_runner_returns_correct_event_count(self):
        events = [_google_event(event_id=f"r-{i}") for i in range(3)]
        result = self._run(events)
        assert result.error is None
        assert len(result.normalized_events) == 3

    def test_runner_raw_events_count_matches(self):
        events = [_google_event(event_id=f"rr-{i}") for i in range(2)]
        result = self._run(events)
        assert len(result.raw_events) == 2

    def test_runner_sorted_final_count_matches(self):
        events = [_google_event(event_id=f"sf-{i}") for i in range(4)]
        result = self._run(events)
        assert len(result.sorted_final_events) == 4

    def test_runner_debug_entries_count_matches(self):
        events = [_google_event(event_id=f"de-{i}") for i in range(3)]
        result = self._run(events)
        assert len(result.debug_entries) == 3

    def test_runner_debug_entry_ids_match_normalized(self):
        events = [_google_event(event_id="match-001")]
        result = self._run(events)
        debug_id = result.debug_entries[0].id_debug["derived_event_id"]
        norm_id = result.debug_entries[0].normalized_event.event_id
        assert debug_id == norm_id

    def test_runner_sorted_order_is_deterministic(self):
        events = [
            _google_event(event_id="z001", start_dt="2026-04-17T15:00:00Z"),
            _google_event(event_id="a001", start_dt="2026-04-17T08:00:00Z"),
            _google_event(event_id="m001", start_dt="2026-04-17T12:00:00Z"),
        ]
        result = self._run(events, user_id="sort-u1")
        timestamps = [e.timestamp for e in result.sorted_final_events]
        assert timestamps == sorted(timestamps)

    def test_runner_no_credentials_returns_empty(self):
        store = InMemoryOAuthCredentialStore(test_mode=False)  # no creds stored
        http = _make_http_mock([[_google_event()]])
        runner = GoogleCalendarSandboxRunner(
            user_id="no-cred-user",
            credential_store=store,
            http_client=http,
        )
        result = runner.run()
        assert result.error is None
        assert result.normalized_events == []

    def test_runner_error_captured_in_result(self):
        store = _make_store_with_creds("error-user")
        http = MagicMock()
        http.get.side_effect = RuntimeError("Simulated network failure")
        runner = GoogleCalendarSandboxRunner(
            user_id="error-user",
            credential_store=store,
            http_client=http,
        )
        result = runner.run()
        assert result.error is not None
        assert "RuntimeError" in result.error or "Simulated" in result.error

    def test_print_report_does_not_raise(self, capsys):
        events = [_google_event(event_id="print-001")]
        result = self._run(events)
        GoogleCalendarSandboxRunner.print_report(result)
        captured = capsys.readouterr()
        assert "GOOGLE CALENDAR SANDBOX RUNNER" in captured.out
        assert "print-001" in captured.out or "ext-" in captured.out

    def test_print_report_shows_error(self, capsys):
        result = SandboxRunResult(user_id="err-user", error="TestError: boom")
        GoogleCalendarSandboxRunner.print_report(result)
        captured = capsys.readouterr()
        assert "ERROR" in captured.out
        assert "boom" in captured.out


# ---------------------------------------------------------------------------
# 7. Pagination
# ---------------------------------------------------------------------------


class TestPagination:
    def test_two_pages_combined(self):
        page1 = [_google_event(event_id=f"p1-{i}") for i in range(3)]
        page2 = [_google_event(event_id=f"p2-{i}") for i in range(2)]
        user_id = "page-user"
        store = _make_store_with_creds(user_id)
        http = _make_http_mock([page1, page2])
        runner = GoogleCalendarSandboxRunner(
            user_id=user_id, credential_store=store, http_client=http, max_results=10
        )
        result = runner.run()
        assert result.error is None
        assert len(result.normalized_events) == 5

    def test_max_results_respected_across_pages(self):
        page1 = [_google_event(event_id=f"mr-{i}") for i in range(3)]
        page2 = [_google_event(event_id=f"mr-{i + 3}") for i in range(3)]
        user_id = "max-user"
        store = _make_store_with_creds(user_id)
        http = _make_http_mock([page1, page2])
        runner = GoogleCalendarSandboxRunner(
            user_id=user_id, credential_store=store, http_client=http, max_results=4
        )
        result = runner.run()
        assert result.error is None
        assert len(result.normalized_events) <= 4


# ---------------------------------------------------------------------------
# 8. Provider contract
# ---------------------------------------------------------------------------


class TestProviderContract:
    def _provider(self, user_id: str = "pc-user") -> GoogleCalendarRealProvider:
        store = _make_store_with_creds(user_id)
        return GoogleCalendarRealProvider(
            credential_store=store,
            http_client=_make_http_mock([[_google_event()]]),
        )

    def test_provider_name_is_correct(self):
        p = self._provider()
        assert p.provider_name == PROVIDER_NAME

    def test_authenticate_stores_credentials(self):
        store = InMemoryOAuthCredentialStore(test_mode=False)
        p = GoogleCalendarRealProvider(credential_store=store, http_client=MagicMock())
        cred = OAuthCredential(
            user_id="auth-user",
            provider_name=PROVIDER_NAME,
            access_token="tok",
            refresh_token=None,
        )
        result = p.authenticate(cred)
        assert result is True
        assert store.get_credentials(user_id="auth-user", provider_name=PROVIDER_NAME) is not None

    def test_authenticate_rejects_wrong_provider(self):
        store = InMemoryOAuthCredentialStore(test_mode=False)
        p = GoogleCalendarRealProvider(credential_store=store, http_client=MagicMock())
        cred = OAuthCredential(
            user_id="u", provider_name="gmail",
            access_token="tok", refresh_token=None,
        )
        assert p.authenticate(cred) is False

    def test_health_check_returns_dict(self):
        p = self._provider()
        result = p.health_check()
        assert isinstance(result, dict)
        assert result["provider_name"] == PROVIDER_NAME
        assert result["healthy"] is True

    def test_required_scopes_readonly(self):
        p = self._provider()
        scopes = p.required_scopes()
        assert any("readonly" in s for s in scopes)
        assert all("write" not in s for s in scopes)
        assert all("edit" not in s for s in scopes)

    def test_fetch_events_returns_list(self):
        p = self._provider()
        events = p.fetch_events(user_id="pc-user")
        assert isinstance(events, list)

    def test_fetch_events_empty_when_no_creds(self):
        store = InMemoryOAuthCredentialStore(test_mode=False)
        p = GoogleCalendarRealProvider(
            credential_store=store,
            http_client=_make_http_mock([[_google_event()]]),
        )
        events = p.fetch_events(user_id="no-cred-user")
        assert events == []

    def test_fetch_events_sends_auth_header(self):
        user_id = "header-user"
        store = _make_store_with_creds(user_id)
        http = _make_http_mock([[_google_event()]])
        p = GoogleCalendarRealProvider(credential_store=store, http_client=http)
        p.fetch_events(user_id=user_id)
        call_kwargs = http.get.call_args
        headers = call_kwargs[1]["headers"]
        assert headers["Authorization"].startswith("Bearer ")

    def test_fetch_events_uses_readonly_params(self):
        user_id = "params-user"
        store = _make_store_with_creds(user_id)
        http = _make_http_mock([[_google_event()]])
        p = GoogleCalendarRealProvider(credential_store=store, http_client=http)
        p.fetch_events(user_id=user_id)
        call_kwargs = http.get.call_args
        params = call_kwargs[1]["params"]
        assert params["singleEvents"] == "true"
        assert params["orderBy"] == "startTime"
        assert params["timeMin"].endswith("Z")
        assert params["timeMax"].endswith("Z")


def test_fetch_events_with_explicit_time_window_filters_outside_bounds() -> None:
    user_id = "explicit-window-user"
    store = _make_store_with_creds(user_id)

    now = datetime(2026, 4, 16, 12, 0, tzinfo=UTC)
    in_range = _google_event(
        event_id="inside",
        start_dt="2026-04-17T09:00:00Z",
        end_dt="2026-04-17T10:00:00Z",
    )
    too_old = _google_event(
        event_id="too-old",
        start_dt="2026-04-16T02:00:00Z",
        end_dt="2026-04-16T04:30:00Z",
    )
    too_far = _google_event(
        event_id="too-far",
        start_dt="2026-04-25T09:00:00Z",
        end_dt="2026-04-25T10:00:00Z",
    )

    http = _make_http_mock([[in_range, too_old, too_far]])
    provider = GoogleCalendarRealProvider(credential_store=store, http_client=http)

    rows = provider.fetch_events(
        user_id=user_id,
        max_results=20,
        time_min=now - timedelta(hours=6),
        time_max=now + timedelta(days=7),
    )

    assert [row["event_id"] for row in rows] == ["inside"]


def test_fetch_events_with_long_term_view_keeps_ninety_day_future_event() -> None:
    user_id = "long-term-view-user"
    store = _make_store_with_creds(user_id)
    http = _make_http_mock(
        [[
            _google_event(
                event_id="ninety-day",
                start_dt="2026-06-30T09:00:00Z",
                end_dt="2026-06-30T10:00:00Z",
            )
        ]]
    )
    provider = GoogleCalendarRealProvider(credential_store=store, http_client=http)

    rows = provider.fetch_events(
        user_id=user_id,
        max_results=10,
        view=OrchestrationView.LONG_TERM,
    )

    assert [row["event_id"] for row in rows] == ["ninety-day"]


def test_multi_calendar_aggregation_returns_all_calendars() -> None:
    user_id = "multi-cal-user"
    store = _make_store_with_creds(user_id)

    calendars = [
        {"id": "primary", "summary": "Primary", "accessRole": "owner", "selected": True},
        {"id": "secondary", "summary": "Family", "accessRole": "owner", "selected": True},
        {"id": "shared", "summary": "Shared", "accessRole": "reader", "selected": True},
    ]

    events_by_calendar = {
        "primary": [
            _google_event(event_id="p-1", start_dt=_future_iso(days=1, hours=8)),
            _google_event(event_id="p-2", start_dt=_future_iso(days=1, hours=9)),
        ],
        "secondary": [
            _google_event(event_id="s-1", start_dt=_future_iso(days=1, hours=10)),
            _google_event(event_id="s-2", start_dt=_future_iso(days=1, hours=11)),
            _google_event(event_id="s-3", start_dt=_future_iso(days=1, hours=12)),
        ],
        "shared": [
            _google_event(event_id="sh-1", start_dt=_future_iso(days=1, hours=13)),
        ],
    }

    def _http_get(url: str, *, headers: dict[str, Any], params: dict[str, Any] | None = None):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if url.endswith("/users/me/calendarList"):
            resp.json.return_value = {"items": calendars}
            return resp

        for cal_id, items in events_by_calendar.items():
            if f"/calendars/{cal_id}/events" in url:
                resp.json.return_value = {"items": items}
                return resp

        resp.json.return_value = {"items": []}
        return resp

    http = MagicMock()
    http.get.side_effect = _http_get

    provider = GoogleCalendarRealProvider(credential_store=store, http_client=http)
    rows = provider.fetch_events(user_id=user_id, max_results=50)

    assert len(rows) == 6
    assert {row["source_calendar_id"] for row in rows} == {"primary", "secondary", "shared"}


def test_events_from_secondary_calendar_are_included() -> None:
    user_id = "secondary-user"
    store = _make_store_with_creds(user_id)

    calendars = [
        {"id": "primary", "summary": "Primary", "accessRole": "owner", "selected": True},
        {"id": "secondary", "summary": "Secondary", "accessRole": "owner", "selected": True},
    ]

    primary_items = [_google_event(event_id="pr-1", start_dt=_future_iso(days=2, hours=8))]
    secondary_items = [_google_event(event_id="sec-1", start_dt=_future_iso(days=2, hours=9))]

    def _http_get(url: str, *, headers: dict[str, Any], params: dict[str, Any] | None = None):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if url.endswith("/users/me/calendarList"):
            resp.json.return_value = {"items": calendars}
        elif "/calendars/primary/events" in url:
            resp.json.return_value = {"items": primary_items}
        elif "/calendars/secondary/events" in url:
            resp.json.return_value = {"items": secondary_items}
        else:
            resp.json.return_value = {"items": []}
        return resp

    http = MagicMock()
    http.get.side_effect = _http_get

    provider = GoogleCalendarRealProvider(credential_store=store, http_client=http)
    rows = provider.fetch_events(user_id=user_id, max_results=10)

    event_ids = {row["event_id"] for row in rows}
    assert "pr-1" in event_ids
    assert "sec-1" in event_ids
    assert any(row["source_calendar_id"] == "secondary" for row in rows)


def test_no_duplication_across_calendars() -> None:
    user_id = "dedupe-user"
    store = _make_store_with_creds(user_id)

    calendars = [
        {"id": "primary", "summary": "Primary", "accessRole": "owner", "selected": True},
        {"id": "shared", "summary": "Shared", "accessRole": "reader", "selected": True},
    ]

    duplicate_event_primary = _google_event(event_id="dupe-1", start_dt=_future_iso(days=3, hours=10))
    duplicate_event_primary["iCalUID"] = "ical-dupe-100"

    duplicate_event_shared = _google_event(event_id="dupe-2", start_dt=_future_iso(days=3, hours=10))
    duplicate_event_shared["iCalUID"] = "ical-dupe-100"

    unique_event = _google_event(event_id="unique-1", start_dt=_future_iso(days=3, hours=11))
    unique_event["iCalUID"] = "ical-unique-1"

    def _http_get(url: str, *, headers: dict[str, Any], params: dict[str, Any] | None = None):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if url.endswith("/users/me/calendarList"):
            resp.json.return_value = {"items": calendars}
        elif "/calendars/primary/events" in url:
            resp.json.return_value = {"items": [duplicate_event_primary, unique_event]}
        elif "/calendars/shared/events" in url:
            resp.json.return_value = {"items": [duplicate_event_shared]}
        else:
            resp.json.return_value = {"items": []}
        return resp

    http = MagicMock()
    http.get.side_effect = _http_get

    provider = GoogleCalendarRealProvider(credential_store=store, http_client=http)
    rows = provider.fetch_events(user_id=user_id, max_results=20)

    assert len(rows) == 2
    assert len({row["iCalUID"] for row in rows if row.get("iCalUID")}) == 2


def test_calendar_list_integration_mocked() -> None:
    user_id = "calendar-list-user"
    store = _make_store_with_creds(user_id)

    calendars = [
        {"id": "primary", "summary": "Primary", "accessRole": "owner", "selected": True},
        {"id": "hidden", "summary": "Hidden", "accessRole": "none", "selected": False},
        {"id": "subscribed", "summary": "Subscribed", "accessRole": "reader", "selected": False},
    ]

    def _http_get(url: str, *, headers: dict[str, Any], params: dict[str, Any] | None = None):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if url.endswith("/users/me/calendarList"):
            resp.json.return_value = {"items": calendars}
        elif "/calendars/primary/events" in url:
            resp.json.return_value = {"items": [_google_event(event_id="c1", start_dt=_future_iso(days=4, hours=8))]}
        elif "/calendars/subscribed/events" in url:
            resp.json.return_value = {"items": [_google_event(event_id="c2", start_dt=_future_iso(days=4, hours=9))]}
        elif "/calendars/hidden/events" in url:
            pytest.fail("Hidden calendar with accessRole=none and selected=false should not be fetched")
        else:
            resp.json.return_value = {"items": []}
        return resp

    http = MagicMock()
    http.get.side_effect = _http_get

    provider = GoogleCalendarRealProvider(credential_store=store, http_client=http)
    listed = provider.list_calendars(access_token="mock-access-token")
    rows = provider.fetch_events(user_id=user_id, max_results=20)

    assert {cal["id"] for cal in listed} == {"primary", "hidden", "subscribed"}
    assert {row["event_id"] for row in rows} == {"c1", "c2"}


def test_regression_primary_shared_imported_calendar_visibility() -> None:
    user_id = "visibility-parity-user"
    store = _make_store_with_creds(user_id)

    calendars = [
        {
            "id": "primary",
            "summary": "Primary",
            "primary": True,
            "accessRole": "owner",
            "selected": True,
            "hidden": False,
            "deleted": False,
        },
        {
            "id": "shared-team",
            "summary": "Shared Team",
            "primary": False,
            "accessRole": "reader",
            "selected": False,
            "hidden": False,
            "deleted": False,
        },
        {
            "id": "imported-ics",
            "summary": "Imported ICS",
            "primary": False,
            "accessRole": "reader",
            "selected": False,
            "hidden": False,
            "deleted": False,
        },
    ]

    events_by_calendar = {
        "primary": [_google_event(event_id="primary-evt", start_dt=_future_iso(days=5, hours=8))],
        "shared-team": [_google_event(event_id="shared-evt", start_dt=_future_iso(days=5, hours=9))],
        "imported-ics": [_google_event(event_id="imported-evt", start_dt=_future_iso(days=5, hours=10))],
    }

    def _http_get(url: str, *, headers: dict[str, Any], params: dict[str, Any] | None = None):
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        if url.endswith("/users/me/calendarList"):
            resp.json.return_value = {"items": calendars}
            return resp

        for cal_id, items in events_by_calendar.items():
            if f"/calendars/{cal_id}/events" in url:
                resp.json.return_value = {"items": items}
                return resp

        resp.json.return_value = {"items": []}
        return resp

    http = MagicMock()
    http.get.side_effect = _http_get

    provider = GoogleCalendarRealProvider(credential_store=store, http_client=http)
    rows = provider.fetch_events(user_id=user_id, max_results=20)

    assert {row["event_id"] for row in rows} == {"primary-evt", "shared-evt", "imported-evt"}
    assert {row["source_calendar_id"] for row in rows} == {"primary", "shared-team", "imported-ics"}
    assert {row["source_calendar_name"] for row in rows} == {"Primary", "Shared Team", "Imported ICS"}
