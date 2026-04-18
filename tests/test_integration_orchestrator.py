from __future__ import annotations

from apps.api.integration_core import (
    InMemoryOAuthCredentialStore,
    IntegrationOrchestrator,
    OAuthCredential,
    build_default_provider_registry,
)


def test_multi_provider_aggregation() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    registry = build_default_provider_registry(store)
    orchestrator = IntegrationOrchestrator(registry)

    store.save_credentials(
        OAuthCredential(
            user_id="u-1",
            provider_name="gmail",
            access_token="gmail-token",
            refresh_token="gmail-refresh",
        )
    )
    store.save_credentials(
        OAuthCredential(
            user_id="u-1",
            provider_name="google_calendar",
            access_token="cal-token",
            refresh_token="cal-refresh",
        )
    )

    events = orchestrator.collect_external_events("u-1")

    assert len(events) == 4
    providers = {event.source_provider for event in events}
    assert providers == {"gmail", "google_calendar"}
    assert all(event.raw_payload for event in events)


def test_empty_credential_handling() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    registry = build_default_provider_registry(store)
    orchestrator = IntegrationOrchestrator(registry)

    events = orchestrator.collect_external_events("no-creds-user")
    assert events == []


def test_deterministic_output_ordering() -> None:
    store = InMemoryOAuthCredentialStore(test_mode=True)
    registry = build_default_provider_registry(store)
    orchestrator = IntegrationOrchestrator(registry)

    store.save_credentials(
        OAuthCredential(
            user_id="u-2",
            provider_name="gmail",
            access_token="gmail-token",
            refresh_token="gmail-refresh",
        )
    )
    store.save_credentials(
        OAuthCredential(
            user_id="u-2",
            provider_name="google_calendar",
            access_token="cal-token",
            refresh_token="cal-refresh",
        )
    )

    first = orchestrator.collect_external_events("u-2")
    second = orchestrator.collect_external_events("u-2")

    assert first == second

    ordering_keys = [
        (event.source_provider, event.timestamp, event.event_id, event.title)
        for event in first
    ]
    assert ordering_keys == sorted(ordering_keys)
