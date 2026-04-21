from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi.testclient import TestClient

from apps.api import main
from apps.api.endpoints import brief_endpoint
from apps.api.endpoints import integrations_router as ir
from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore, OAuthCredential
from tests.evaluation.scenario_models import HouseholdScenario


@dataclass
class _FakeResponse:
    payload: dict[str, Any]
    status_code: int = 200

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP error status: {self.status_code}")

    def json(self) -> dict[str, Any]:
        return dict(self.payload)


class _ScenarioHttpClient:
    def __init__(self, events: list[dict[str, Any]]) -> None:
        self._events = list(events)

    def get(self, url: str, *, headers: dict[str, Any], params: dict[str, Any] | None = None) -> _FakeResponse:
        if url.endswith("/users/me/calendarList"):
            return _FakeResponse(
                {
                    "items": [
                        {
                            "id": "primary",
                            "summary": "Primary",
                            "accessRole": "owner",
                            "selected": True,
                        }
                    ]
                }
            )
        if "/calendars/primary/events" in url:
            return _FakeResponse({"items": list(self._events)})
        return _FakeResponse({"items": []})

    def post(self, url: str, *, headers: dict[str, Any], data: dict[str, Any] | None = None, timeout: int | None = None) -> _FakeResponse:
        # Refresh token endpoint stub for completeness; not used in normal test path.
        return _FakeResponse(
            {
                "access_token": "evaluation-access-token",
                "token_type": "Bearer",
                "expires_in": 3600,
            }
        )


def _scenario_to_google_events(scenario: HouseholdScenario) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, event in enumerate(scenario.events, start=1):
        rows.append(
            {
                "id": f"{scenario.scenario_id}-evt-{index:03d}",
                "summary": event.title,
                "status": "confirmed",
                "description": (
                    f"type={event.type}; participants={','.join(event.participants)}; "
                    f"priority_hint={event.priority_hint or 'none'}"
                ),
                "start": {"dateTime": event.start_time, "timeZone": "UTC"},
                "end": {"dateTime": event.end_time, "timeZone": "UTC"},
                "attendees": [{"email": p.lower().replace(' ', '.') + "@example.test"} for p in event.participants],
                "organizer": {"email": "household@example.test"},
            }
        )
    return rows


def run_scenario(scenario: HouseholdScenario) -> dict[str, Any]:
    user_id = f"eval-user-{scenario.scenario_id}"
    household_id = f"eval-household-{scenario.scenario_id}"

    store = InMemoryOAuthCredentialStore()
    store.save_credentials(
        OAuthCredential(
            user_id=user_id,
            provider_name="google_calendar",
            access_token="evaluation-access-token",
            refresh_token="evaluation-refresh-token",
        )
    )

    http_client = _ScenarioHttpClient(_scenario_to_google_events(scenario))

    ir._last_debug_snapshot.clear()
    main.app.dependency_overrides[ir.get_credential_store] = lambda: store
    main.app.dependency_overrides[ir.get_http_client] = lambda: http_client

    brief_endpoint._clear_brief_cache()

    try:
        with TestClient(main.app, raise_server_exceptions=True, follow_redirects=False) as client:
            response = client.get(f"/brief/{household_id}", params={"user_id": user_id})
            response.raise_for_status()
            payload = response.json()
    finally:
        main.app.dependency_overrides.clear()

    return {
        "scenario_id": scenario.scenario_id,
        "brief_output": payload,
    }
