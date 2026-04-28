"""
outlook_calendar_provider.py
----------------------------
Read-only Microsoft Outlook Calendar provider for Integration Core.

This provider fetches events across all visible calendars for a user and maps
Graph payloads to the same intermediate schema consumed by StateBuilder.
"""
from __future__ import annotations

import logging
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from archive.apps.api.integration_core.credentials import CredentialStore
from archive.apps.api.integration_core.event_windowing import (
    OrchestrationView,
    get_time_window,
    to_rfc3339,
)


PROVIDER_NAME = "outlook_calendar"
_GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"


logger = logging.getLogger(__name__)


def _extract_datetime(start_or_end: Any) -> str:
    if not isinstance(start_or_end, dict):
        return ""
    value = str(start_or_end.get("dateTime", "")).strip()
    if not value:
        return ""

    if value.endswith("Z") or "+" in value:
        return value

    timezone = str(start_or_end.get("timeZone", "")).strip()
    if timezone in {"UTC", "Etc/UTC"}:
        return f"{value}Z"
    return value


def map_outlook_event_to_raw(
    event: dict[str, Any],
    *,
    calendar_id: str,
    calendar_name: str,
) -> dict[str, Any]:
    start_str = _extract_datetime(event.get("start"))
    end_str = _extract_datetime(event.get("end"))

    return {
        "event_id": str(event.get("id", "")),
        "title": str(event.get("subject", "")).strip() or "Untitled event",
        "timestamp": start_str,
        "start": start_str,
        "end_timestamp": end_str,
        "end": end_str,
        "status": str(event.get("showAs", "")).strip().lower() or "unknown",
        "description": str(event.get("bodyPreview", "")).strip(),
        "location": str((event.get("location") or {}).get("displayName", "")).strip(),
        "source_calendar_id": calendar_id,
        "source_calendar_name": calendar_name,
        "source_provider": PROVIDER_NAME,
        "_raw_outlook_event": dict(event),
    }


@dataclass
class OutlookCalendarProvider:
    credential_store: CredentialStore
    http_client: Any = field(default=None, repr=False)

    provider_name: str = field(default=PROVIDER_NAME, init=False)
    _last_fetch_status: dict[str, Any] = field(
        default_factory=lambda: {"status": "unknown", "reason": None},
        init=False,
        repr=False,
    )

    def _set_fetch_status(self, *, status: str, reason: str | None = None) -> None:
        self._last_fetch_status = {"status": status, "reason": reason}

    def get_runtime_status(self) -> dict[str, Any]:
        if self._last_fetch_status.get("status") != "unknown":
            return {
                "status": self._last_fetch_status.get("status", "ok"),
                "reason": self._last_fetch_status.get("reason"),
            }
        return {
            "status": "disabled",
            "reason": "outlook_integration_not_connected",
        }

    def _get_http(self) -> Any:
        if self.http_client is not None:
            return self.http_client

        try:
            import httpx  # noqa: PLC0415

            return httpx
        except ImportError:
            try:
                import requests  # noqa: PLC0415

                return requests
            except ImportError as exc:
                raise RuntimeError(
                    "OutlookCalendarProvider requires either 'httpx' or 'requests'. "
                    "Install it with: pip install httpx"
                ) from exc

    @staticmethod
    def _headers(access_token: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

    def _list_calendars(self, *, access_token: str) -> list[dict[str, str]]:
        http = self._get_http()
        url = f"{_GRAPH_BASE_URL}/me/calendars"
        response = http.get(
            url,
            headers=self._headers(access_token),
            params={
                "$select": "id,name,isDefaultCalendar,canEdit",
                "$top": 50,
            },
        )
        response.raise_for_status()

        payload = response.json() if hasattr(response, "json") else {}
        values = payload.get("value", []) if isinstance(payload, dict) else []

        calendars: list[dict[str, str]] = []
        for row in values:
            if not isinstance(row, dict):
                continue
            calendar_id = str(row.get("id", "")).strip()
            if not calendar_id:
                continue
            calendars.append(
                {
                    "id": calendar_id,
                    "name": str(row.get("name", "")).strip() or calendar_id,
                }
            )

        if not calendars:
            return [{"id": "calendar", "name": "calendar"}]

        # Deterministic order supports repeatable snapshots.
        calendars.sort(key=lambda item: (item["id"], item["name"]))
        return calendars

    def _fetch_calendar_events(
        self,
        *,
        access_token: str,
        calendar_id: str,
        calendar_name: str,
        max_results: int,
        time_min: datetime,
        time_max: datetime,
    ) -> list[dict[str, Any]]:
        encoded_calendar_id = urllib.parse.quote(calendar_id, safe="")
        url = f"{_GRAPH_BASE_URL}/me/calendars/{encoded_calendar_id}/calendarView"

        http = self._get_http()
        response = http.get(
            url,
            headers=self._headers(access_token),
            params={
                "startDateTime": to_rfc3339(time_min),
                "endDateTime": to_rfc3339(time_max),
                "$orderby": "start/dateTime",
                "$top": max(1, int(max_results)),
            },
        )
        response.raise_for_status()

        payload = response.json() if hasattr(response, "json") else {}
        values = payload.get("value", []) if isinstance(payload, dict) else []

        rows: list[dict[str, Any]] = []
        for row in values:
            if not isinstance(row, dict):
                continue
            rows.append(
                map_outlook_event_to_raw(
                    row,
                    calendar_id=calendar_id,
                    calendar_name=calendar_name,
                )
            )

        return rows

    @staticmethod
    def _dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        selected: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = f"{row.get('event_id', '')}|{row.get('source_calendar_id', '')}|{row.get('timestamp', '')}"
            if key in selected:
                continue
            selected[key] = row

        deduped = list(selected.values())
        deduped.sort(
            key=lambda row: (
                str(row.get("timestamp", "")),
                str(row.get("source_calendar_id", "")),
                str(row.get("event_id", "")),
            )
        )
        return deduped

    def fetch_events(
        self,
        *,
        user_id: str,
        max_results: int = 50,
        view: OrchestrationView = OrchestrationView.SHORT_TERM,
        time_min: datetime | None = None,
        time_max: datetime | None = None,
    ) -> list[dict[str, Any]]:
        credentials = self.credential_store.get_credentials(
            user_id=user_id,
            provider_name=self.provider_name,
        )
        if credentials is None:
            self._set_fetch_status(status="disabled", reason="outlook_integration_not_connected")
            return []

        resolved_time_min: datetime
        resolved_time_max: datetime
        if time_min is not None and time_max is not None:
            resolved_time_min = time_min
            resolved_time_max = time_max
        else:
            resolved_time_min, resolved_time_max = get_time_window(view)

        try:
            calendars = self._list_calendars(access_token=credentials.access_token)
            rows: list[dict[str, Any]] = []
            remaining = max(1, int(max_results))

            for calendar in calendars:
                if remaining <= 0:
                    break
                fetched = self._fetch_calendar_events(
                    access_token=credentials.access_token,
                    calendar_id=calendar["id"],
                    calendar_name=calendar["name"],
                    max_results=remaining,
                    time_min=resolved_time_min,
                    time_max=resolved_time_max,
                )
                rows.extend(fetched)
                remaining = max_results - len(rows)

            deduped = self._dedupe(rows)
            self._set_fetch_status(status="ok", reason=None)
            return deduped
        except Exception:
            logger.exception("Outlook calendar fetch failed for user_id=%s", user_id)
            self._set_fetch_status(status="degraded", reason="outlook_fetch_failed")
            return []
