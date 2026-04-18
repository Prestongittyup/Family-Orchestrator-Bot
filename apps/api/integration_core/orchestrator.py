"""
orchestrator.py
---------------
Pure coordinator layer.

Dependency direction:
    Endpoints -> Orchestrator -> StateBuilder -> Providers

Rules:
- No provider imports in this module
- No env-var access in this module
- No external IO in this module
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from apps.api.integration_core.decision_engine import DecisionContext, DecisionEngine
from apps.api.integration_core.event_windowing import OrchestrationView
from apps.api.integration_core.models.household_state import HouseholdState
from apps.api.integration_core.registry import ProviderRegistry
from apps.api.integration_core.state_builder import StateBuilder

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExternalEvent:
    event_id: str
    source_provider: str
    timestamp: str
    title: str
    raw_payload: dict[str, Any]


def _coerce_timestamp(row: dict[str, Any]) -> str:
    for key in ("timestamp", "start", "time", "date"):
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return ""


def _normalize_event(provider_name: str, row: dict[str, Any]) -> ExternalEvent:
    event_id = str(row.get("event_id", row.get("id", "")))
    title = str(row.get("title", row.get("name", "")))
    timestamp = _coerce_timestamp(row)
    return ExternalEvent(
        event_id=event_id,
        source_provider=provider_name,
        timestamp=timestamp,
        title=title,
        raw_payload=dict(row),
    )


class Orchestrator:
    """Pure coordinator that delegates all integration work to StateBuilder."""

    def __init__(
        self,
        state_builder: StateBuilder,
        decision_engine: DecisionEngine | None = None,
    ) -> None:
        self.state_builder = state_builder
        self.decision_engine = decision_engine

    def build_household_state(self, user_id: str) -> HouseholdState | tuple[HouseholdState, DecisionContext]:
        state = self.state_builder.build(user_id)
        log.info(
            "state_built",
            extra={"user_id": user_id, "events": len(state.calendar_events)},
        )

        if self.decision_engine is None:
            return state

        decision_context = self.decision_engine.process(state)
        log.info(
            "decision_generated",
            extra={
                "top_events": len(decision_context.top_events),
                "conflicts": len(decision_context.conflicts),
            },
        )
        return state, decision_context


def create_orchestrator(
    *,
    credential_store: Any,
    http_client: Any = None,
    max_results: int = 50,
    provider_mode: str | None = None,
    decision_engine: DecisionEngine | None = None,
) -> Orchestrator:
    """Create the default orchestrator wired to the canonical StateBuilder."""
    state_builder = StateBuilder(
        credential_store=credential_store,
        http_client=http_client,
        max_results=max_results,
        provider_mode=provider_mode,
    )
    return Orchestrator(state_builder, decision_engine=decision_engine)


class IntegrationOrchestrator:
    """Multi-provider orchestration via ProviderRegistry and StateBuilder."""

    def __init__(self, registry: ProviderRegistry) -> None:
        self._registry = registry

    def collect_external_events(
        self,
        user_id: str,
        *,
        max_results_per_provider: int = 50,
        view: OrchestrationView = OrchestrationView.SHORT_TERM,
        time_min: datetime | None = None,
        time_max: datetime | None = None,
    ) -> list[ExternalEvent]:
        normalized: list[ExternalEvent] = []

        for provider_name in self._registry.list_providers():
            credentials = self._registry.credential_store.get_credentials(
                user_id=user_id,
                provider_name=provider_name,
            )
            if credentials is None:
                continue

            provider = self._registry.get_provider(provider_name)
            builder = StateBuilder(
                provider=provider,
                provider_name=provider_name,
                max_results=max_results_per_provider,
            )
            state = builder.build(user_id)

            for row in state.debug_meta.get("raw_events", []):
                if isinstance(row, dict):
                    normalized.append(_normalize_event(provider_name, row))

        normalized.sort(
            key=lambda event: (
                event.source_provider,
                event.timestamp,
                event.event_id,
                event.title,
            )
        )
        return normalized
