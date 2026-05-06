from __future__ import annotations
import pytest

from datetime import UTC, datetime, timedelta
from typing import Any

from archive.apps.api.integration_core.credentials import InMemoryOAuthCredentialStore, OAuthCredential
from archive.apps.api.integration_core.outlook_calendar_provider import OutlookCalendarProvider

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



class _MockResponse:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http error {self.status_code}")

    def json(self) -> dict[str, Any]:
        return dict(self._payload)


class _MockHttpClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, *, headers: dict[str, str], params: dict[str, Any]) -> _MockResponse:
        self.calls.append({"url": url, "headers": headers, "params": dict(params)})

        if url.endswith("/me/calendars"):
            return _MockResponse(
                {
                    "value": [
                        {"id": "primary", "name": "Primary"},
                        {"id": "team-calendar", "name": "Team Shared"},
                    ]
                }
            )

        if "/me/calendars/primary/calendarView" in url:
            return _MockResponse(
                {
                    "value": [
                        {
                            "id": "evt-1",
                            "subject": "Primary Event",
                            "start": {"dateTime": _future_iso(1)},
                            "end": {"dateTime": _future_iso(1, hours=1)},
                        }
                    ]
                }
            )

        if "/me/calendars/team-calendar/calendarView" in url:
            return _MockResponse(
                {
                    "value": [
                        {
                            "id": "evt-2",
                            "subject": "Shared Event",
                            "start": {"dateTime": _future_iso(2)},
                            "end": {"dateTime": _future_iso(2, hours=1)},
                        }
                    ]
                }
            )

        return _MockResponse({"value": []})


def _future_iso(days: int, *, hours: int = 0) -> str:
    dt = datetime.now(UTC) + timedelta(days=days, hours=hours)
    return dt.isoformat().replace("+00:00", "Z")


@pytest.mark.integration
@pytest.mark.legacy
def test_outlook_provider_fetches_events_from_primary_and_shared_calendars() -> None:
    store = InMemoryOAuthCredentialStore()
    store.save_credentials(
        OAuthCredential(
            user_id="outlook-user",
            provider_name="outlook_calendar",
            access_token="outlook-token",
            refresh_token=None,
        )
    )

    http = _MockHttpClient()
    provider = OutlookCalendarProvider(credential_store=store, http_client=http)

    rows = provider.fetch_events(user_id="outlook-user", max_results=10)

    assert len(rows) == 2
    titles = {row["title"] for row in rows}
    assert titles == {"Primary Event", "Shared Event"}
    source_names = {row["source_calendar_name"] for row in rows}
    assert source_names == {"Primary", "Team Shared"}
    assert provider.get_runtime_status()["status"] == "ok"


@pytest.mark.integration
@pytest.mark.legacy
def test_outlook_provider_returns_disabled_when_not_connected() -> None:
    provider = OutlookCalendarProvider(
        credential_store=InMemoryOAuthCredentialStore(),
        http_client=_MockHttpClient(),
    )

    rows = provider.fetch_events(user_id="missing-user", max_results=10)

    assert rows == []
    assert provider.get_runtime_status()["status"] == "disabled"
    assert provider.get_runtime_status()["reason"] == "outlook_integration_not_connected"