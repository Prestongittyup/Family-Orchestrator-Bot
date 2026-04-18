from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from apps.api.integration_core.credentials import CredentialStore, OAuthCredential
from apps.api.integration_core.event_windowing import OrchestrationView
from apps.api.integration_core.identity import UserIdentity


class Provider(Protocol):
    provider_name: str

    def authenticate(self, credentials: OAuthCredential) -> bool:
        ...

    def fetch_events(
        self,
        *,
        user_id: str,
        max_results: int = 50,
        view: OrchestrationView = OrchestrationView.SHORT_TERM,
        time_min: datetime | None = None,
        time_max: datetime | None = None,
    ) -> list[dict[str, Any]]:
        ...

    def health_check(self) -> dict[str, Any]:
        ...


class IntegrationProvider(Provider, Protocol):
    # Backward-compatible alias surface for previous integration-core tests.
    provider_id: str

    def required_scopes(self) -> tuple[str, ...]:
        ...

    def status(self, *, user: UserIdentity, credential_store: CredentialStore) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class GmailProviderMock:
    credential_store: CredentialStore
    provider_name: str = "gmail"
    provider_id: str = "gmail"

    def required_scopes(self) -> tuple[str, ...]:
        return ("https://www.googleapis.com/auth/gmail.readonly",)

    def authenticate(self, credentials: OAuthCredential) -> bool:
        if not self.credential_store.test_mode:
            raise RuntimeError("GmailProviderMock supports test mode only")
        if credentials.provider_name != self.provider_name:
            return False
        self.credential_store.save_credentials(credentials)
        return True

    def fetch_events(
        self,
        *,
        user_id: str,
        max_results: int = 50,
        view: OrchestrationView = OrchestrationView.SHORT_TERM,
        time_min: datetime | None = None,
        time_max: datetime | None = None,
    ) -> list[dict[str, Any]]:
        if not self.credential_store.test_mode:
            raise RuntimeError("GmailProviderMock supports test mode only")
        token = self.credential_store.get_credentials(user_id=user_id, provider_name=self.provider_name)
        if token is None:
            return []

        rows = [
            {
                "provider": self.provider_name,
                "event_id": f"gmail-msg-{user_id}-001",
                "title": "Mock Gmail Event: Inbox Review",
                "start": "2026-01-01T09:00:00",
            },
            {
                "provider": self.provider_name,
                "event_id": f"gmail-msg-{user_id}-002",
                "title": "Mock Gmail Event: Follow-up",
                "start": "2026-01-01T13:00:00",
            },
        ]
        limit = max(0, int(max_results))
        return rows[:limit]

    def health_check(self) -> dict[str, Any]:
        return {
            "provider_name": self.provider_name,
            "healthy": True,
            "mode": "mock" if self.credential_store.test_mode else "live-unsupported",
        }

    def status(self, *, user: UserIdentity, credential_store: CredentialStore) -> dict[str, Any]:
        token = credential_store.get_credentials(user_id=user.user_id, provider_name=self.provider_name)
        return {
            "provider_id": self.provider_id,
            "connected": token is not None,
            "mode": "mock" if credential_store.test_mode else "live",
            "scopes": list(self.required_scopes()),
        }


@dataclass(frozen=True)
class GoogleCalendarProviderMock:
    credential_store: CredentialStore
    provider_name: str = "google_calendar"
    provider_id: str = "google_calendar"

    def required_scopes(self) -> tuple[str, ...]:
        return ("https://www.googleapis.com/auth/calendar.readonly",)

    def authenticate(self, credentials: OAuthCredential) -> bool:
        if not self.credential_store.test_mode:
            raise RuntimeError("GoogleCalendarProviderMock supports test mode only")
        if credentials.provider_name != self.provider_name:
            return False
        self.credential_store.save_credentials(credentials)
        return True

    def fetch_events(
        self,
        *,
        user_id: str,
        max_results: int = 50,
        view: OrchestrationView = OrchestrationView.SHORT_TERM,
        time_min: datetime | None = None,
        time_max: datetime | None = None,
    ) -> list[dict[str, Any]]:
        if not self.credential_store.test_mode:
            raise RuntimeError("GoogleCalendarProviderMock supports test mode only")
        token = self.credential_store.get_credentials(user_id=user_id, provider_name=self.provider_name)
        if token is None:
            return []

        rows = [
            {
                "provider": self.provider_name,
                "event_id": f"gcal-{user_id}-001",
                "title": "Mock Calendar Event: Standup",
                "start": "2026-01-01T10:00:00",
            },
            {
                "provider": self.provider_name,
                "event_id": f"gcal-{user_id}-002",
                "title": "Mock Calendar Event: Planning",
                "start": "2026-01-01T15:00:00",
            },
        ]
        limit = max(0, int(max_results))
        return rows[:limit]

    def health_check(self) -> dict[str, Any]:
        return {
            "provider_name": self.provider_name,
            "healthy": True,
            "mode": "mock" if self.credential_store.test_mode else "live-unsupported",
        }

    def status(self, *, user: UserIdentity, credential_store: CredentialStore) -> dict[str, Any]:
        token = credential_store.get_credentials(user_id=user.user_id, provider_name=self.provider_name)
        return {
            "provider_id": self.provider_id,
            "connected": token is not None,
            "mode": "mock" if credential_store.test_mode else "live",
            "scopes": list(self.required_scopes()),
        }


# Backward-compatible aliases.
GmailPlaceholderProvider = GmailProviderMock
GoogleCalendarPlaceholderProvider = GoogleCalendarProviderMock
