from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi.testclient import TestClient

from apps.api import main
from apps.api.endpoints import brief_endpoint
from apps.api.endpoints import integrations_router as ir
from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore, OAuthCredential


@dataclass
class _FakeResponse:
    payload: dict[str, Any]
    status_code: int = 200

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP error status: {self.status_code}")

    def json(self) -> dict[str, Any]:
        return dict(self.payload)


class _SimulationHttpClient:
    def __init__(self, events: list[dict[str, Any]]) -> None:
        self._events = events

    def get(self, url: str, *, headers: dict[str, Any], params: dict[str, Any] | None = None) -> _FakeResponse:
        if url.endswith("/users/me/calendarList"):
            return _FakeResponse({"items": [{"id": "primary", "summary": "Primary"}]})
        if "/calendars/primary/events" in url:
            return _FakeResponse({"items": list(self._events)})
        return _FakeResponse({"items": []})

    def post(self, url: str, *, headers: dict[str, Any], data: dict[str, Any] | None = None, timeout: int | None = None) -> _FakeResponse:
        return _FakeResponse(
            {
                "access_token": "simulation-access-token",
                "token_type": "Bearer",
                "expires_in": 3600,
            }
        )


def _to_google_event(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload", {}) if isinstance(event.get("payload"), dict) else {}
    return {
        "id": event.get("event_id", "unknown"),
        "summary": payload.get("title", event.get("title", "Untitled")),
        "status": "cancelled" if event.get("type") == "cancellation" else "confirmed",
        "description": f"type={event.get('type', 'unknown')}; participants={','.join(event.get('participants', []))}",
        "start": {"dateTime": event.get("start_time"), "timeZone": "UTC"},
        "end": {"dateTime": event.get("end_time"), "timeZone": "UTC"},
        "attendees": [
            {"email": p.lower().replace(' ', '.') + "@example.test"}
            for p in event.get("participants", [])
        ],
        "organizer": {"email": "household@example.test"},
    }


def run_ingestion_sequence(
    *,
    timeline_events: list[dict[str, Any]],
    household_id: str,
    user_id: str,
) -> list[dict[str, Any]]:
    store = InMemoryOAuthCredentialStore()
    store.save_credentials(
        OAuthCredential(
            user_id=user_id,
            provider_name="google_calendar",
            access_token="simulation-access-token",
            refresh_token="simulation-refresh-token",
        )
    )

    brief_endpoint._clear_brief_cache()

    active_events: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []

    with TestClient(main.app, raise_server_exceptions=True, follow_redirects=False) as client:
        for idx, timeline_event in enumerate(timeline_events, start=1):
            event_type = str(timeline_event.get("type", "work_event"))
            if event_type == "cancellation":
                target = timeline_event.get("payload", {}).get("cancel_target")
                active_events = [e for e in active_events if e.get("event_id") != target]
            elif event_type == "reschedule":
                target = timeline_event.get("payload", {}).get("cancel_target")
                shift_minutes = int(timeline_event.get("payload", {}).get("shift_minutes", 30))
                for item in active_events:
                    if item.get("event_id") == target:
                        item["start_time"] = timeline_event.get("start_time", item.get("start_time"))
                        item["end_time"] = timeline_event.get("end_time", item.get("end_time"))
                        item.setdefault("payload", {})["shift_minutes"] = shift_minutes
            else:
                active_events.append(dict(timeline_event))

            ingest_payload = {
                "household_id": household_id,
                "type": "task_created",
                "source": "simulation",
                "payload": {
                    "title": timeline_event.get("title", f"Sim Event {idx}"),
                    "event_type": event_type,
                    "timestamp": timeline_event.get("timestamp"),
                },
            }
            event_response = client.post("/event", json=ingest_payload)
            event_response.raise_for_status()

            google_events = [_to_google_event(e) for e in active_events]
            http_client = _SimulationHttpClient(google_events)
            ir._last_debug_snapshot.clear()
            main.app.dependency_overrides[ir.get_credential_store] = lambda: store
            main.app.dependency_overrides[ir.get_http_client] = lambda: http_client
            try:
                brief_response = client.get(f"/brief/{household_id}", params={"user_id": user_id})
                brief_response.raise_for_status()
                brief_payload = brief_response.json()
            finally:
                main.app.dependency_overrides.clear()

            snapshots.append(
                {
                    "step": idx,
                    "event": dict(timeline_event),
                    "active_event_ids": [e.get("event_id") for e in active_events],
                    "brief": brief_payload,
                }
            )

    return snapshots
