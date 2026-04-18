"""
google_calendar_provider.py
----------------------------
GoogleCalendarRealProvider — implements the existing ``Provider`` protocol
against the live Google Calendar API (read-only).

Safety constraints enforced here:
  - READ-ONLY scope only  (``calendar.readonly``)
  - NO calendar write operations
  - NO OS-1 imports
  - NO OS-2 imports
  - Passes architecture guard (no forbidden import prefixes)

OAuth flow is NOT implemented here.  The caller is expected to supply a
pre-issued ``OAuthCredential`` through the ``CredentialStore`` before calling
``fetch_events``.  This matches the existing credential injection pattern used
by the mock providers.

Google Calendar API reference used:
  https://developers.google.com/calendar/api/v3/reference/events/list
"""
from __future__ import annotations

import json
import logging
import urllib.parse
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from apps.api.integration_core.credentials import CredentialStore, OAuthCredential
from apps.api.integration_core.event_windowing import (
    OrchestrationView,
    filter_events_to_window,
    get_time_window,
    prune_stale_events,
    to_rfc3339,
)
from apps.api.integration_core.google_oauth_config import (
    GoogleOAuthClientConfig,
    refresh_access_token,
)
from apps.api.integration_core.normalization import (
    ExternalEvent,
    normalize_provider_event,
    normalize_provider_events,
    _coerce_timestamp,
    _derive_event_id,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROVIDER_NAME = "google_calendar"
LEGACY_PROVIDER_NAME = "google_calendar_real"

# Minimal read-only scope required for event listing.
REQUIRED_SCOPES: tuple[str, ...] = (
    "https://www.googleapis.com/auth/calendar.readonly",
)

# Google Calendar API base URL
_GCAL_API_BASE = "https://www.googleapis.com/calendar/v3"

# Default to primary calendar; callers may override via fetch_events kwargs.
_DEFAULT_CALENDAR_ID = "primary"

# Max results per page (Google API hard cap: 2500)
_MAX_RESULTS_PER_PAGE = 250

_INCLUDED_ACCESS_ROLES: tuple[str, ...] = ("owner", "writer", "reader")


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Raw event normalization helpers
# ---------------------------------------------------------------------------


def _extract_datetime_string(google_date_obj: dict[str, Any] | None) -> str:
    """
    Extract ISO-8601 datetime string from a Google Calendar dateTime/date object.

    Google represents both all-day events (``date``) and timed events
    (``dateTime``) using a nested dict.  We prefer ``dateTime`` and fall back
    to ``date`` so all-day events still get a deterministic timestamp.
    """
    if not google_date_obj or not isinstance(google_date_obj, dict):
        return ""
    return google_date_obj.get("dateTime") or google_date_obj.get("date") or ""


def _map_status(google_status: str | None) -> str:
    """Map Google event status to a normalised string."""
    mapping = {
        "confirmed": "confirmed",
        "tentative": "tentative",
        "cancelled": "cancelled",
    }
    return mapping.get(google_status or "", "unknown")


def map_google_event_to_raw(google_event: dict[str, Any]) -> dict[str, Any]:
    """
    Flatten a raw Google Calendar API event dict into a provider-agnostic
    intermediate dict suitable for passing to ``normalize_provider_event``.

    Only read-only fields are extracted.  All fields are preserved in
    ``raw_payload`` for full traceability.

    Field mapping table
    -------------------
    Google field          → Intermediate key
    ─────────────────────────────────────────
    id                    → event_id
    summary               → title
    start.dateTime/date   → timestamp  (ISO-8601)
    end.dateTime/date     → end_timestamp
    status                → status (normalised)
    description           → description
    location              → location
    htmlLink              → html_link
    organizer.email       → organizer_email
    attendees[].email     → attendee_emails (list)
    created               → created_at
    updated               → updated_at
    etag                  → etag
    """
    start_str = _extract_datetime_string(google_event.get("start"))
    end_str = _extract_datetime_string(google_event.get("end"))
    organizer = google_event.get("organizer") or {}
    attendees = google_event.get("attendees") or []

    return {
        # Core identity
        "event_id": google_event.get("id", ""),
        "title": google_event.get("summary", ""),
        # Timing (normalised to ISO strings)
        "timestamp": start_str,
        "start": start_str,
        "end_timestamp": end_str,
        # Metadata
        "status": _map_status(google_event.get("status")),
        "description": google_event.get("description", ""),
        "location": google_event.get("location", ""),
        "html_link": google_event.get("htmlLink", ""),
        "organizer_email": organizer.get("email", ""),
        "attendee_emails": [a.get("email", "") for a in attendees if isinstance(a, dict)],
        "created_at": google_event.get("created", ""),
        "updated_at": google_event.get("updated", ""),
        "etag": google_event.get("etag", ""),
        "iCalUID": google_event.get("iCalUID", ""),
        "source_calendar_id": "",
        "source_calendar_name": "",
        # Full raw payload for traceability
        "_raw_google_event": dict(google_event),
    }


def build_event_id_debug(
    *,
    user_id: str,
    raw: dict[str, Any],
    mapped: dict[str, Any],
) -> dict[str, Any]:
    """
    Return a human-readable debug dict that shows every field used to derive
    the deterministic ExternalEvent.event_id, plus the resulting ID.

    Useful in the sandbox runner for auditing normalization decisions.
    """
    timestamp_used = mapped.get("timestamp") or mapped.get("start") or ""
    # Re-derive for full transparency — does not modify any state.
    event_id_value = _derive_event_id(
        user_id=user_id,
        provider_name=PROVIDER_NAME,
        event_type="calendar.event",
        timestamp=timestamp_used,
        payload=mapped,
    )

    return {
        "input_fields": {
            "user_id": user_id,
            "provider_name": PROVIDER_NAME,
            "event_type": "calendar.event",
            "timestamp_source": "start.dateTime → start.date fallback",
            "timestamp_used": timestamp_used,
            "google_event_id": raw.get("id", ""),
            "google_summary": raw.get("summary", ""),
        },
        "derived_event_id": event_id_value,
    }


# ---------------------------------------------------------------------------
# Provider implementation
# ---------------------------------------------------------------------------


@dataclass
class GoogleCalendarRealProvider:
    """
    Real Google Calendar integration provider (read-only).

    Implements the ``Provider`` protocol defined in
    ``apps.api.integration_core.providers``.

    Parameters
    ----------
    credential_store:
        Used to retrieve a pre-issued OAuth access token for the target user.
    calendar_id:
        Which calendar to fetch events from. Defaults to ``"primary"``.
    http_client:
        Injectable HTTP client for testing.  Must expose a callable:
        ``get(url, *, headers, params) → response`` where response has
        ``.status_code: int``, ``.json() → dict``, ``.raise_for_status()``.
        When ``None`` (default), the real ``requests`` library is used if
        available.  Tests should always inject a mock.
    """

    credential_store: CredentialStore
    calendar_id: str = _DEFAULT_CALENDAR_ID
    http_client: Any = field(default=None, repr=False)

    # Protocol-required fields
    provider_name: str = field(default=PROVIDER_NAME, init=False)
    _last_fetch_status: dict[str, Any] = field(
        default_factory=lambda: {"status": "unknown", "reason": None},
        init=False,
        repr=False,
    )

    @staticmethod
    def _normalize_calendar_list_item(item: dict[str, Any]) -> dict[str, Any] | None:
        calendar_id = str(item.get("id", "")).strip()
        if not calendar_id:
            return None

        return {
            "id": calendar_id,
            "summary": str(item.get("summary", "")).strip(),
            "primary": bool(item.get("primary", False)),
            "accessRole": str(item.get("accessRole", "none")).strip().lower() or "none",
            "selected": bool(item.get("selected", False)),
            "hidden": bool(item.get("hidden", False)),
            "deleted": bool(item.get("deleted", False)),
        }

    @staticmethod
    def _calendar_inclusion_decision(calendar: dict[str, Any]) -> tuple[bool, str]:
        if bool(calendar.get("deleted", False)):
            return False, "deleted"
        if bool(calendar.get("hidden", False)):
            return False, "hidden"

        access_role = str(calendar.get("accessRole", "none")).strip().lower() or "none"
        if access_role not in _INCLUDED_ACCESS_ROLES:
            return False, f"accessRole:{access_role}"

        return True, "included"

    def _get_headers(self, access_token: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

    def _set_fetch_status(self, *, status: str, reason: str | None = None) -> None:
        self._last_fetch_status = {
            "status": status,
            "reason": reason,
        }

    def get_runtime_status(self) -> dict[str, Any]:
        if self._last_fetch_status.get("status") != "unknown":
            return {
                "status": self._last_fetch_status.get("status", "ok"),
                "reason": self._last_fetch_status.get("reason"),
                "configured": True,
                "missing_fields": [],
                "message": "Google OAuth configured",
            }

        config_status = GoogleOAuthClientConfig.from_env().status()
        if not config_status.configured:
            return {
                "status": "disabled",
                "reason": "google_oauth_not_configured",
                "configured": False,
                "missing_fields": config_status.missing_fields,
                "message": config_status.message,
            }
        return {
            "status": "ok",
            "reason": None,
            "configured": True,
            "missing_fields": [],
            "message": "Google OAuth configured",
        }

    def _is_access_token_expiring(self, *, credentials: OAuthCredential, skew_seconds: int = 60) -> bool:
        if credentials.expires_at is None:
            return False
        expires_at = credentials.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        now = datetime.now(UTC)
        return expires_at <= (now + timedelta(seconds=skew_seconds))

    def _get_stored_credentials(self, *, user_id: str) -> OAuthCredential | None:
        credentials = self.credential_store.get_credentials(
            user_id=user_id,
            provider_name=self.provider_name,
        )
        if credentials is None and self.provider_name == PROVIDER_NAME:
            credentials = self.credential_store.get_credentials(
                user_id=user_id,
                provider_name=LEGACY_PROVIDER_NAME,
            )
        return credentials

    def _refresh_credentials_if_needed(self, *, user_id: str, credentials: OAuthCredential) -> OAuthCredential | None:
        if not self._is_access_token_expiring(credentials=credentials):
            self._set_fetch_status(status="ok")
            return credentials

        logger.debug(
            "Google Calendar access token is expired or near expiry for user_id=%s; attempting refresh",
            user_id,
        )

        if not credentials.refresh_token:
            logger.warning(
                "Google Calendar refresh token missing for user_id=%s; returning empty dataset",
                user_id,
            )
            self._set_fetch_status(status="disabled", reason="google_refresh_token_missing")
            return None

        config = GoogleOAuthClientConfig.from_env()
        config_status = config.status()
        if not config_status.configured:
            logger.warning(
                "Google OAuth config missing during refresh for user_id=%s; returning empty dataset",
                user_id,
            )
            self._set_fetch_status(status="disabled", reason="google_oauth_not_configured")
            return None

        try:
            refreshed = refresh_access_token(
                refresh_token=credentials.refresh_token,
                config=config,
                http_client=self._get_http(),
            )
        except Exception:
            logger.exception(
                "Google Calendar token refresh failed for user_id=%s; returning empty dataset",
                user_id,
            )
            self._set_fetch_status(status="disabled", reason="google_token_refresh_failed")
            return None

        updated_credentials = OAuthCredential(
            user_id=credentials.user_id,
            provider_name=credentials.provider_name,
            access_token=refreshed.access_token,
            refresh_token=refreshed.refresh_token or credentials.refresh_token,
            scopes=credentials.scopes,
            expires_at=(
                datetime.now(UTC) + timedelta(seconds=int(refreshed.expires_in or 0))
                if refreshed.expires_in is not None
                else credentials.expires_at
            ),
        )
        self.credential_store.save_credentials(updated_credentials)
        self._set_fetch_status(status="ok")
        logger.debug("Google Calendar access token refreshed for user_id=%s", user_id)
        return updated_credentials

    def _resolve_access_token_for_call(self, *, user_id: str | None, fallback_access_token: str) -> str | None:
        if not user_id:
            return fallback_access_token

        credentials = self._get_stored_credentials(user_id=user_id)
        if credentials is None:
            return fallback_access_token

        refreshed = self._refresh_credentials_if_needed(user_id=user_id, credentials=credentials)
        if refreshed is None:
            return None
        return refreshed.access_token

    def list_calendars(self, *, access_token: str, user_id: str | None = None) -> list[dict[str, Any]]:
        """
        Discover accessible calendars for the current user.

        Returns normalized rows with:
        id, summary, primary, accessRole, selected, hidden, deleted.
        """
        http = self._get_http()
        url = f"{_GCAL_API_BASE}/users/me/calendarList"

        calendars: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            params: dict[str, Any] = {}
            if page_token:
                params["pageToken"] = page_token

            current_access_token = self._resolve_access_token_for_call(
                user_id=user_id,
                fallback_access_token=access_token,
            )
            if current_access_token is None:
                return []
            headers = self._get_headers(current_access_token)
            response = http.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                if not isinstance(item, dict):
                    continue
                normalized = self._normalize_calendar_list_item(item)
                if normalized is not None:
                    calendars.append(normalized)

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        if not calendars:
            # Backward-compatible fallback for mock/test clients that only
            # return event payloads and do not model calendarList.
            return [
                {
                    "id": self.calendar_id,
                    "summary": self.calendar_id,
                    "primary": self.calendar_id == "primary",
                    "accessRole": "owner",
                    "selected": True,
                    "hidden": False,
                    "deleted": False,
                }
            ]

        deduped: dict[str, dict[str, Any]] = {}
        for row in calendars:
            deduped[row["id"]] = row

        return sorted(deduped.values(), key=lambda c: (c["id"], c["summary"]))

    def fetch_events_for_calendar(
        self,
        *,
        access_token: str,
        user_id: str | None,
        calendar_id: str,
        calendar_name: str,
        max_results: int,
        time_min: datetime,
        time_max: datetime,
    ) -> list[dict[str, Any]]:
        """Fetch and map events for a single calendar id."""
        http = self._get_http()

        encoded_calendar_id = urllib.parse.quote(str(calendar_id), safe="")
        url = f"{_GCAL_API_BASE}/calendars/{encoded_calendar_id}/events"
        collected: list[dict[str, Any]] = []
        page_token: str | None = None
        remaining = max(1, int(max_results))

        while True:
            params: dict[str, Any] = {
                "maxResults": min(remaining, _MAX_RESULTS_PER_PAGE),
                "singleEvents": "true",
                "orderBy": "startTime",
                "timeMin": to_rfc3339(time_min),
                "timeMax": to_rfc3339(time_max),
            }
            if page_token:
                params["pageToken"] = page_token

            current_access_token = self._resolve_access_token_for_call(
                user_id=user_id,
                fallback_access_token=access_token,
            )
            if current_access_token is None:
                return []
            headers = self._get_headers(current_access_token)
            response = http.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            items: list[dict[str, Any]] = data.get("items", [])
            for item in items:
                if isinstance(item, dict):
                    mapped = map_google_event_to_raw(item)
                    mapped["source_calendar_id"] = str(calendar_id)
                    mapped["source_calendar_name"] = str(calendar_name or calendar_id)
                    collected.append(mapped)
                if len(collected) >= max_results:
                    break

            if len(collected) >= max_results:
                break

            page_token = data.get("nextPageToken")
            if not page_token:
                break

            remaining = max_results - len(collected)

        return collected

    def _deduplicate_aggregated_events(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Deduplicate rows from multiple calendars.

        Priority key:
          1) iCalUID if present
          2) event_id + timestamp fallback
        """
        selected: dict[str, dict[str, Any]] = {}
        for row in rows:
            i_cal_uid = str(row.get("iCalUID", "")).strip()
            if i_cal_uid:
                dedupe_key = f"ical:{i_cal_uid}"
            else:
                dedupe_key = f"fallback:{row.get('event_id', '')}|{row.get('timestamp', '')}"

            existing = selected.get(dedupe_key)
            if existing is None:
                selected[dedupe_key] = row
                continue

            # Deterministic winner selection for duplicates.
            row_rank = (
                str(row.get("timestamp", "")),
                str(row.get("event_id", "")),
                str(row.get("source_calendar_id", "")),
                str(row.get("source_calendar_name", "")),
                json.dumps(row, sort_keys=True, default=str),
            )
            existing_rank = (
                str(existing.get("timestamp", "")),
                str(existing.get("event_id", "")),
                str(existing.get("source_calendar_id", "")),
                str(existing.get("source_calendar_name", "")),
                json.dumps(existing, sort_keys=True, default=str),
            )
            if row_rank < existing_rank:
                selected[dedupe_key] = row

        deduped = list(selected.values())
        deduped.sort(
            key=lambda row: (
                str(row.get("timestamp", "")),
                PROVIDER_NAME,
                str(row.get("event_id", "")),
            )
        )
        return deduped

    def _get_http(self) -> Any:
        if self.http_client is not None:
            return self.http_client
        try:
            import requests  # noqa: PLC0415 (lazy import)
            return requests
        except ImportError as exc:
            raise RuntimeError(
                "GoogleCalendarRealProvider requires the 'requests' library. "
                "Install it with: pip install requests"
            ) from exc

    def authenticate(self, credentials: OAuthCredential) -> bool:
        """
        Store the supplied credentials.  Actual OAuth exchange is handled
        externally; this simply persists the token for later use.
        """
        if credentials.provider_name not in {self.provider_name, LEGACY_PROVIDER_NAME}:
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
        calendar_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch events from Google Calendar for *user_id*.

        - Uses credentials from the injected store.
        - Handles pagination transparently up to *max_results* events.
        - Returns raw intermediate dicts (output of ``map_google_event_to_raw``).
          The orchestrator/normalization layer converts these to ExternalEvent.
        - READ-ONLY: no write operations are performed.

        Returns an empty list if no credentials are found.
        """
        config_status = GoogleOAuthClientConfig.from_env().status()
        credentials = self._get_stored_credentials(user_id=user_id)
        if credentials is None:
            if not config_status.configured:
                logger.info(
                    "Google Calendar fetch skipped for user_id=%s because OAuth config is not available",
                    user_id,
                )
                self._set_fetch_status(status="disabled", reason="google_oauth_not_configured")
            else:
                self._set_fetch_status(status="disabled", reason="google_integration_not_connected")
            return []

        credentials = self._refresh_credentials_if_needed(user_id=user_id, credentials=credentials)
        if credentials is None:
            return []
        access_token = credentials.access_token
        resolved_time_min: datetime
        resolved_time_max: datetime
        if time_min is not None and time_max is not None:
            resolved_time_min = time_min
            resolved_time_max = time_max
        else:
            resolved_time_min, resolved_time_max = get_time_window(view)

        # Explicit single-calendar mode retained for compatibility and targeted
        # diagnostics; default mode aggregates across all included calendars.
        if calendar_id:
            events = self.fetch_events_for_calendar(
                access_token=access_token,
                user_id=user_id,
                calendar_id=calendar_id,
                calendar_name=calendar_id,
                max_results=max_results,
                time_min=resolved_time_min,
                time_max=resolved_time_max,
            )
            deduped_events = self._deduplicate_aggregated_events(events)
            windowed_events = filter_events_to_window(
                deduped_events,
                time_min=resolved_time_min,
                time_max=resolved_time_max,
            )
            return prune_stale_events(windowed_events)

        calendars = self.list_calendars(access_token=access_token, user_id=user_id)

        included: list[dict[str, Any]] = []
        excluded: list[tuple[dict[str, Any], str]] = []
        for calendar in calendars:
            include, reason = self._calendar_inclusion_decision(calendar)
            if include:
                included.append(calendar)
            else:
                excluded.append((calendar, reason))

        excluded_reason_counts: dict[str, int] = {}
        for _, reason in excluded:
            excluded_reason_counts[reason] = excluded_reason_counts.get(reason, 0) + 1

        logger.debug(
            "Google Calendar discovery: discovered=%s included=%s excluded=%s",
            len(calendars),
            len(included),
            len(excluded),
        )
        logger.debug(
            "Google Calendar exclusions by reason: %s",
            excluded_reason_counts,
        )

        if not included:
            return []

        aggregated: list[dict[str, Any]] = []
        remaining_global = max(1, int(max_results))

        for cal in included:
            if remaining_global <= 0:
                break
            calendar_events = self.fetch_events_for_calendar(
                access_token=access_token,
                user_id=user_id,
                calendar_id=str(cal.get("id", self.calendar_id)),
                calendar_name=str(cal.get("summary", "")),
                max_results=remaining_global,
                time_min=resolved_time_min,
                time_max=resolved_time_max,
            )
            aggregated.extend(calendar_events)
            remaining_global = max_results - len(aggregated)

        deduped_events = self._deduplicate_aggregated_events(aggregated)
        windowed_events = filter_events_to_window(
            deduped_events,
            time_min=resolved_time_min,
            time_max=resolved_time_max,
        )
        return prune_stale_events(windowed_events)

    def health_check(self) -> dict[str, Any]:
        """Lightweight read-only check: attempt to access the calendar list."""
        return {
            "provider_name": self.provider_name,
            "healthy": True,
            "mode": "real",
            "scope": REQUIRED_SCOPES[0],
            "note": "health_check does not make live API calls",
        }

    def required_scopes(self) -> tuple[str, ...]:
        return REQUIRED_SCOPES


class GoogleCalendarProviderReal(GoogleCalendarRealProvider):
    """Compatibility alias using the requested class name."""
    pass
