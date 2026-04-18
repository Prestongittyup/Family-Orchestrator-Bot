from __future__ import annotations

from typing import Callable

from apps.api.integration_core.credentials import CredentialStore
from apps.api.integration_core.providers import (
    GmailProviderMock,
    GoogleCalendarProviderMock,
    IntegrationProvider,
    Provider,
)


ProviderFactory = Callable[[CredentialStore], Provider]


class ProviderRegistry:
    def __init__(self, credential_store: CredentialStore) -> None:
        self._credential_store = credential_store
        self._factories: dict[str, ProviderFactory] = {}

    @property
    def credential_store(self) -> CredentialStore:
        return self._credential_store

    def register_provider(self, provider_name: str, factory: ProviderFactory) -> None:
        self._factories[str(provider_name)] = factory

    def get_provider(self, provider_name: str) -> Provider:
        key = str(provider_name)
        if key not in self._factories:
            raise KeyError(f"provider not registered: {key}")
        return self._factories[key](self._credential_store)

    def list_providers(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories.keys()))

    # Backward-compatible aliases used by existing tests/callers.
    def register(self, provider_id: str, factory: Callable[[CredentialStore], IntegrationProvider]) -> None:
        self.register_provider(provider_id, factory)

    def create(self, provider_id: str) -> IntegrationProvider:
        return self.get_provider(provider_id)  # type: ignore[return-value]

    def list_registered(self) -> tuple[str, ...]:
        return self.list_providers()

    def clear_providers(self) -> None:
        """Remove all registered provider factories (test/reset helper)."""
        self._factories.clear()


def build_default_provider_registry(credential_store: CredentialStore) -> ProviderRegistry:
    registry = ProviderRegistry(credential_store)
    registry.register_provider("gmail", lambda store: GmailProviderMock(credential_store=store))
    registry.register_provider("google_calendar", lambda store: GoogleCalendarProviderMock(credential_store=store))
    return registry
