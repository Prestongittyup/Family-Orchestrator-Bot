from __future__ import annotations

from apps.api.integration_core import (
    IdentityService,
    InMemoryIdentityRepository,
    InMemoryOAuthCredentialStore,
    IntegrationOrchestrator,
    OAuthCredential,
    ProviderRegistry,
)
from apps.api.integration_core.normalization import ExternalEvent, normalize_provider_event, normalize_provider_events
from apps.api.integration_core.providers import GmailProviderMock, GoogleCalendarProviderMock


def _build_registry(store: InMemoryOAuthCredentialStore) -> ProviderRegistry:
    registry = ProviderRegistry(store)
    registry.register_provider("gmail", lambda injected_store: GmailProviderMock(credential_store=injected_store))
    registry.register_provider(
        "google_calendar",
        lambda injected_store: GoogleCalendarProviderMock(credential_store=injected_store),
    )
    return registry


def test_user_creation_and_lookup() -> None:
    service = IdentityService(InMemoryIdentityRepository())
    service.create_household(household_id="hh-suite", name="Suite Household")

    user = service.create_user(
        email="suite-user@example.com",
        display_name="Suite User",
        household_id="hh-suite",
    )
    found = service.get_user(user.user_id)

    assert found is not None
    assert found.user_id == user.user_id
    assert found.household_id == "hh-suite"


def test_credential_storage_lifecycle() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)

    created = OAuthCredential(
        user_id="u-suite",
        provider_name="gmail",
        access_token="access-a",
        refresh_token="refresh-a",
        scopes=("gmail.read",),
    )
    store.save_credentials(created)

    loaded = store.get_credentials(user_id="u-suite", provider_name="gmail")
    assert loaded is not None
    assert loaded.access_token == "access-a"

    overwritten = OAuthCredential(
        user_id="u-suite",
        provider_name="gmail",
        access_token="access-b",
        refresh_token="refresh-b",
        scopes=("gmail.read", "gmail.labels"),
    )
    store.save_credentials(overwritten)
    updated = store.get_credentials(user_id="u-suite", provider_name="gmail")
    assert updated is not None
    assert updated.access_token == "access-b"

    assert store.delete_credentials(user_id="u-suite", provider_name="gmail") is True
    assert store.get_credentials(user_id="u-suite", provider_name="gmail") is None


def test_provider_registration_and_retrieval() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    registry = _build_registry(store)

    assert registry.list_providers() == ("gmail", "google_calendar")

    gmail = registry.get_provider("gmail")
    calendar = registry.get_provider("google_calendar")

    assert gmail.provider_name == "gmail"
    assert calendar.provider_name == "google_calendar"
    assert gmail.health_check()["healthy"] is True
    assert calendar.health_check()["healthy"] is True


def test_orchestrator_multi_provider_aggregation_and_ordering_stability() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    registry = _build_registry(store)
    orchestrator = IntegrationOrchestrator(registry)

    store.save_credentials(
        OAuthCredential(
            user_id="u-agg",
            provider_name="gmail",
            access_token="gmail-token",
            refresh_token="gmail-refresh",
        )
    )
    store.save_credentials(
        OAuthCredential(
            user_id="u-agg",
            provider_name="google_calendar",
            access_token="calendar-token",
            refresh_token="calendar-refresh",
        )
    )

    first = orchestrator.collect_external_events("u-agg")
    second = orchestrator.collect_external_events("u-agg")

    assert first == second
    assert len(first) == 4
    assert {event.source_provider for event in first} == {"gmail", "google_calendar"}

    ordering_keys = [
        (event.source_provider, event.timestamp, event.event_id, event.title)
        for event in first
    ]
    assert ordering_keys == sorted(ordering_keys)


def test_event_normalization_determinism_and_consistency() -> None:
    raw = {
        "id": "evt-1",
        "title": "Provider Event",
        "start": "2026-02-01T10:00:00",
        "meta": {"k": "v"},
    }
    first = normalize_provider_event(
        user_id="u-norm",
        provider_name="gmail",
        raw_event=raw,
        event_type="external_event",
    )
    second = normalize_provider_event(
        user_id="u-norm",
        provider_name="gmail",
        raw_event=raw,
        event_type="external_event",
    )
    assert first.event_id == second.event_id

    gmail_rows = [
        {"id": "g1", "title": "G1", "start": "2026-02-01T11:00:00"},
        {"id": "g2", "title": "G2", "start": "2026-02-01T09:00:00"},
    ]
    cal_rows = [
        {"id": "c1", "title": "C1", "start": "2026-02-01T10:00:00"},
    ]

    normalized = [
        *normalize_provider_events(
            user_id="u-norm",
            provider_name="gmail",
            raw_events=gmail_rows,
            event_type="external_event",
        ),
        *normalize_provider_events(
            user_id="u-norm",
            provider_name="google_calendar",
            raw_events=cal_rows,
            event_type="external_event",
        ),
    ]
    normalized.sort(key=lambda row: (row.timestamp, row.provider_name))

    assert [row.timestamp for row in normalized] == [
        "2026-02-01T09:00:00",
        "2026-02-01T10:00:00",
        "2026-02-01T11:00:00",
    ]
    assert [row.provider_name for row in normalized] == [
        "gmail",
        "google_calendar",
        "gmail",
    ]


def test_end_to_end_user_credentials_orchestrator_normalized_external_events() -> None:
    # create user
    identity_service = IdentityService(InMemoryIdentityRepository())
    identity_service.create_household(household_id="hh-e2e", name="E2E Home")
    user = identity_service.create_user(
        email="e2e@example.com",
        display_name="E2E User",
        household_id="hh-e2e",
    )

    # attach mock Gmail + Calendar credentials
    store = InMemoryOAuthCredentialStore(test_mode=True)
    store.save_credentials(
        OAuthCredential(
            user_id=str(user.user_id),
            provider_name="gmail",
            access_token="gmail-access",
            refresh_token="gmail-refresh",
        )
    )
    store.save_credentials(
        OAuthCredential(
            user_id=str(user.user_id),
            provider_name="google_calendar",
            access_token="calendar-access",
            refresh_token="calendar-refresh",
        )
    )

    # run orchestrator
    registry = _build_registry(store)
    orchestrator = IntegrationOrchestrator(registry)
    collected = orchestrator.collect_external_events(str(user.user_id))

    # validate normalized ExternalEvent output
    normalized: list[ExternalEvent] = [
        normalize_provider_event(
            user_id=str(user.user_id),
            provider_name=row.source_provider,
            raw_event=row.raw_payload,
            event_type="external_event",
        )
        for row in collected
    ]

    assert len(normalized) == 4
    assert all(isinstance(row, ExternalEvent) for row in normalized)
    assert all(row.user_id == str(user.user_id) for row in normalized)
    assert {row.provider_name for row in normalized} == {"gmail", "google_calendar"}
