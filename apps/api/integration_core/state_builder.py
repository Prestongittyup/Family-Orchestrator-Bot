"""
state_builder.py
----------------
The ONLY place in the system that calls provider.fetch_events().

Responsibilities
----------------
- Accept user_id + optional provider / credential store
- Fetch raw calendar data through the provider interface
- Build all three windowed views (7d / 30d / 90d)
- Determine integration health (enabled / disabled / degraded)
- Assemble and return a HouseholdState

Contract
--------
- No endpoint calls a provider directly.
- No filtering logic lives in endpoints or the orchestrator.
- StateBuilder is the single source of truth for HouseholdState.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any, Optional

from archive.apps.api.integration_core.event_windowing import (
    ACTIVE_PAST_HOURS,
    OrchestrationView,
    get_time_window,
    filter_events_to_window,
    parse_event_datetime,
    utc_now,
)
from archive.apps.api.integration_core.models.household_state import (
    CalendarEvent,
    HouseholdState,
    IntegrationHealth,
    WindowedCalendar,
)

logger = logging.getLogger(__name__)


_DEFAULT_CALENDAR_PROVIDERS: tuple[str, ...] = (
    "google_calendar",
    "outlook_calendar",
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_calendar_event(row: dict[str, Any]) -> CalendarEvent | None:
    """Convert a raw provider dict to a CalendarEvent, or None if unparseable."""
    start = parse_event_datetime(row.get("timestamp") or row.get("start"))
    end = parse_event_datetime(
        row.get("end_timestamp") or row.get("end") or row.get("timestamp") or row.get("start")
    )
    if start is None or end is None:
        return None

    event_id = str(row.get("event_id") or row.get("id") or "")
    title = str(row.get("title") or row.get("name") or event_id or "Untitled event")
    return CalendarEvent(
        event_id=event_id,
        start=start.isoformat(),
        end=end.isoformat(),
        title=title,
    )


def _window_events(
    events: list[CalendarEvent],
    *,
    now: datetime,
    days: int,
) -> list[CalendarEvent]:
    """Return events whose effective range falls within [now - 24h, now + days]."""
    cutoff_past = now - timedelta(hours=ACTIVE_PAST_HOURS)
    cutoff_future = now + timedelta(days=days)
    return sorted(
        [e for e in events if _event_in_range(e, cutoff_past=cutoff_past, cutoff_future=cutoff_future)],
        key=lambda e: e.start,
    )


def _event_in_range(
    event: CalendarEvent,
    *,
    cutoff_past: datetime,
    cutoff_future: datetime,
) -> bool:
    start = parse_event_datetime(event.start)
    end = parse_event_datetime(event.end)
    if start is None or end is None:
        return False
    # Keep if event ends after the past-cutoff AND starts before future-cutoff
    return end >= cutoff_past and start <= cutoff_future


def _health_from_runtime_status(
    raw: dict[str, Any],
    *,
    integration: str,
) -> IntegrationHealth:
    """Convert a provider runtime_status dict into an IntegrationHealth."""
    status = str(raw.get("status", "ok")).lower()
    reason = raw.get("reason")

    if status == "disabled":
        action: Optional[str] = None
        if integration == "google_calendar" and reason == "google_oauth_not_configured":
            action = "set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET"
        elif integration == "google_calendar" and reason in (
            "no_credentials_stored",
            "token_refresh_failed_no_refresh_token",
            "google_integration_not_connected",
            "google_refresh_token_missing",
        ):
            action = "reconnect via /integrations/google-calendar/connect/{user_id}"
        elif reason in ("outlook_integration_not_connected", "no_credentials_stored"):
            action = "connect integration and retry"
        return IntegrationHealth(
            integration=integration,
            state="disabled",
            reason=reason,
            action=action,
        )
    elif status == "degraded":
        return IntegrationHealth(
            integration=integration,
            state="degraded",
            reason=reason,
            action="check logs or reconnect",
        )
    return IntegrationHealth(
        integration=integration,
        state="enabled",
        reason=None,
        action=None,
    )


def _health_no_provider(*, integration: str = "google_calendar") -> IntegrationHealth:
    if integration == "google_calendar":
        return IntegrationHealth(
            integration="google_calendar",
            state="disabled",
            reason="google_oauth_not_configured",
            action="set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET",
        )
    return IntegrationHealth(
        integration=integration,
        state="disabled",
        reason="integration_not_connected",
        action="connect integration and retry",
    )


# ---------------------------------------------------------------------------
# StateBuilder
# ---------------------------------------------------------------------------

class StateBuilder:
    """
    Builds a HouseholdState from a provider and a user_id.

    Usage::

        builder = StateBuilder(provider=provider, user_id="alice")
        state: HouseholdState = builder.build()
    """

    def __init__(
        self,
        *,
        provider: Any | None = None,
        credential_store: Any | None = None,
        http_client: Any = None,
        user_id: str | None = None,
        provider_name: str = "google_calendar",
        max_results: int = 200,
        provider_mode: str | None = None,
        view: OrchestrationView = OrchestrationView.LONG,
    ) -> None:
        self._provider = provider
        self._credential_store = credential_store
        self._http_client = http_client
        self._user_id = str(user_id) if user_id is not None else None
        self._provider_name = str(provider_name)
        self._max_results = int(max_results)
        self._provider_mode = provider_mode
        self._view = view

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(
        self,
        user_id: str | None = None,
        *,
        view: OrchestrationView | None = None,
    ) -> HouseholdState:
        """
        Full pipeline:
          fetch → normalise → window (7/30/90) → health → assemble
        """
        if user_id is None and self._user_id is None:
            raise ValueError("user_id is required")
        target_user_id = str(user_id if user_id is not None else self._user_id)
        now = utc_now()
        selected_view = view or self._view
        provider_event_counts: dict[str, int] = {}

        if self._provider is not None:
            raw_events, health = self._fetch_with_health(
                provider=self._provider,
                provider_name=self._provider_name,
                user_id=target_user_id,
                now=now,
                view=selected_view,
            )
            integration_health = [health]
            provider_event_counts[self._provider_name] = len(raw_events)
        else:
            raw_events, integration_health, provider_event_counts = self._fetch_multi_provider_events(
                user_id=target_user_id,
                now=now,
                view=selected_view,
            )

        # Normalise to CalendarEvent, dropping unparseable rows
        calendar_events: list[CalendarEvent] = [
            ce for row in raw_events if (ce := _to_calendar_event(row)) is not None
        ]
        calendar_events = self._dedupe_calendar_events(calendar_events)

        # Build three windows from the same event set
        windowed = WindowedCalendar(
            window_7d=_window_events(calendar_events, now=now, days=7),
            window_30d=_window_events(calendar_events, now=now, days=30),
            window_90d=_window_events(calendar_events, now=now, days=90),
        )

        debug_meta = self._build_debug_meta(
            raw_events=raw_events,
            calendar_events=calendar_events,
            windowed=windowed,
            now=now,
            provider_event_counts=provider_event_counts,
        )

        logger.info(
            "[StateBuilder] user=%s raw=%d normalised=%d 7d=%d 30d=%d 90d=%d",
            target_user_id,
            len(raw_events),
            len(calendar_events),
            len(windowed.window_7d),
            len(windowed.window_30d),
            len(windowed.window_90d),
        )

        return HouseholdState(
            user_id=target_user_id,
            calendar_events=calendar_events,
            tasks=[],
            alerts=[],
            metadata={
                "reference_time": now.isoformat(),
                "active_view": selected_view.name,
                "integrations": integration_health,
                "debug_meta": debug_meta,
                "calendar_window_7d": windowed.window_7d,
                "calendar_window_30d": windowed.window_30d,
                "calendar_window_90d": windowed.window_90d,
            },
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_with_health(
        self,
        *,
        provider: Any,
        provider_name: str,
        user_id: str,
        now: datetime,
        view: OrchestrationView,
    ) -> tuple[list[dict[str, Any]], IntegrationHealth]:
        """
        Call provider.fetch_events() and immediately enforce the selected
        time window inside StateBuilder.

        Returns (raw_event_dicts, IntegrationHealth).
        """
        time_min, time_max = get_time_window(view)
        fetch_started = time.perf_counter()
        raw_before_filter_count = 0
        filtered_count = 0
        try:
            raw = provider.fetch_events(
                user_id=user_id,
                max_results=self._max_results,
                view=view,
                time_min=time_min,
                time_max=time_max,
            )
            raw_dicts = [dict(row) for row in raw if isinstance(row, dict)]
            raw_before_filter_count = len(raw_dicts)
            logger.info(
                "events_fetched",
                extra={"user_id": user_id, "count": len(raw_dicts), "view": view.name},
            )
            raw_dicts = filter_events_to_window(
                raw_dicts,
                now=now,
                time_min=time_min,
                time_max=time_max,
            )
            filtered_count = len(raw_dicts)
            logger.info(
                "events_after_filter",
                extra={"user_id": user_id, "count": len(raw_dicts), "view": view.name},
            )
        except Exception as exc:
            logger.warning(
                "[StateBuilder] provider fetch error for user=%s: %s", user_id, exc
            )
            raw_dicts = []
            filtered_count = 0
        finally:
            provider_latency_ms = round((time.perf_counter() - fetch_started) * 1000.0, 3)
            logger.info(
                "state_builder_provider_metrics",
                extra={
                    "user_id": user_id,
                    "provider_name": provider_name,
                    "raw_event_count": raw_before_filter_count,
                    "filtered_event_count": filtered_count,
                    "provider_latency_ms": provider_latency_ms,
                    "view": view.name,
                    "window_type": f"{view.value}d",
                },
            )

        # Determine health from provider if supported
        if hasattr(provider, "get_runtime_status"):
            health = _health_from_runtime_status(
                provider.get_runtime_status(),
                integration=provider_name,
            )
        else:
            health = IntegrationHealth(
                integration=provider_name,
                state="enabled",
                reason=None,
                action=None,
            )

        return raw_dicts, health

    def _configured_calendar_providers(self) -> list[str]:
        configured = os.environ.get(
            "INTEGRATION_CORE_CALENDAR_PROVIDERS",
            ",".join(_DEFAULT_CALENDAR_PROVIDERS),
        )
        names: list[str] = []
        for token in configured.split(","):
            value = token.strip().lower()
            if value and value not in names:
                names.append(value)

        default_provider = self._provider_name.strip().lower()
        if default_provider and default_provider not in names:
            names.insert(0, default_provider)

        return names or ["google_calendar"]

    def _build_provider(self, *, provider_name: str | None = None) -> Any:
        """Provider selection lives here to keep orchestration layer pure."""
        if self._credential_store is None:
            raise ValueError("credential_store is required when provider is not injected")

        selected_provider = str(provider_name or self._provider_name).strip().lower()
        if selected_provider == "google_calendar":
            selected_mode = self._provider_mode
            if selected_mode is None:
                selected_mode = os.environ.get("INTEGRATION_CORE_GOOGLE_PROVIDER_MODE", "real")

            mode = str(selected_mode).lower()
            if mode == "mock":
                from archive.apps.api.integration_core.providers import GoogleCalendarProviderMock

                return GoogleCalendarProviderMock(credential_store=self._credential_store)

            from archive.apps.api.integration_core.google_calendar_provider import GoogleCalendarRealProvider

            return GoogleCalendarRealProvider(
                credential_store=self._credential_store,
                http_client=self._http_client,
            )

        if selected_provider == "outlook_calendar":
            from archive.apps.api.integration_core.outlook_calendar_provider import OutlookCalendarProvider

            return OutlookCalendarProvider(
                credential_store=self._credential_store,
                http_client=self._http_client,
            )

        raise ValueError(f"Unsupported calendar provider '{selected_provider}'")

    @staticmethod
    def _dedupe_raw_events(raw_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        selected: dict[str, dict[str, Any]] = {}
        for row in raw_events:
            provider = str(row.get("source_provider") or row.get("provider") or "")
            event_id = str(row.get("event_id") or row.get("id") or "")
            start = str(row.get("timestamp") or row.get("start") or "")
            source_calendar = str(row.get("source_calendar_id") or "")
            title = str(row.get("title") or row.get("name") or "")
            key = f"{provider}|{source_calendar}|{event_id}|{start}|{title}"

            if key in selected:
                continue
            selected[key] = row

        deduped = list(selected.values())
        deduped.sort(
            key=lambda row: (
                str(row.get("timestamp") or row.get("start") or ""),
                str(row.get("source_provider") or row.get("provider") or ""),
                str(row.get("event_id") or row.get("id") or ""),
            )
        )
        return deduped

    @staticmethod
    def _dedupe_calendar_events(events: list[CalendarEvent]) -> list[CalendarEvent]:
        selected: dict[str, CalendarEvent] = {}
        for event in events:
            key = f"{event.event_id}|{event.start}|{event.end}|{event.title}"
            if key in selected:
                continue
            selected[key] = event

        deduped = list(selected.values())
        deduped.sort(key=lambda row: (row.start, row.event_id, row.title))
        return deduped

    def _fetch_multi_provider_events(
        self,
        *,
        user_id: str,
        now: datetime,
        view: OrchestrationView,
    ) -> tuple[list[dict[str, Any]], list[IntegrationHealth], dict[str, int]]:
        if self._credential_store is None:
            raise ValueError("credential_store is required when provider is not injected")

        configured_names = self._configured_calendar_providers()
        connected_names = [
            name
            for name in configured_names
            if self._credential_store.get_credentials(user_id=user_id, provider_name=name) is not None
        ]

        selected_names = connected_names if connected_names else [self._provider_name.strip().lower()]

        raw_events: list[dict[str, Any]] = []
        health_rows: list[IntegrationHealth] = []
        provider_event_counts: dict[str, int] = {}

        for provider_name in selected_names:
            try:
                provider = self._build_provider(provider_name=provider_name)
            except ValueError:
                logger.warning("Unsupported provider configured for state build: %s", provider_name)
                continue

            rows, health = self._fetch_with_health(
                provider=provider,
                provider_name=provider_name,
                user_id=user_id,
                now=now,
                view=view,
            )
            provider_event_counts[provider_name] = len(rows)
            raw_events.extend(rows)
            health_rows.append(health)

        if not health_rows:
            health_rows = [_health_no_provider(integration=self._provider_name)]

        deduped_events = self._dedupe_raw_events(raw_events)
        return deduped_events, health_rows, provider_event_counts

    @staticmethod
    def _build_debug_meta(
        *,
        raw_events: list[dict[str, Any]],
        calendar_events: list[CalendarEvent],
        windowed: WindowedCalendar,
        now: datetime,
        provider_event_counts: dict[str, int],
    ) -> dict[str, Any]:
        return {
            "fetched_at": now.isoformat(),
            "raw_event_count": len(raw_events),
            "normalised_event_count": len(calendar_events),
            "window_7d_count": len(windowed.window_7d),
            "window_30d_count": len(windowed.window_30d),
            "window_90d_count": len(windowed.window_90d),
            "provider_count": len(provider_event_counts),
            "provider_event_counts": dict(provider_event_counts),
            "raw_events": raw_events,
        }
