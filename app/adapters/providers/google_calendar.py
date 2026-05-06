from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import httpx


_GCAL_API_BASE = "https://www.googleapis.com/calendar/v3"
_MAX_RESULTS_PER_PAGE = 250


@dataclass(frozen=True)
class GoogleCalendarEvent:
    event_id: str
    title: str
    start: str | None
    end: str | None
    status: str | None
    timestamp: str | None
    source_calendar_id: str
    source_calendar_name: str
    payload: dict[str, Any]


class GoogleCalendarProvider:
    """Read-only Google Calendar adapter with pagination and event normalization."""

    def __init__(self, *, timeout_seconds: float = 20.0) -> None:
        self._timeout_seconds = timeout_seconds

    def list_calendars(self, *, access_token: str) -> list[dict[str, Any]]:
        calendars: list[dict[str, Any]] = []
        page_token: str | None = None

        while True:
            params: dict[str, Any] = {}
            if page_token:
                params["pageToken"] = page_token

            data = self._request_json(
                url=f"{_GCAL_API_BASE}/users/me/calendarList",
                access_token=access_token,
                params=params,
            )
            if data is None:
                break

            for row in data.get("items", []):
                if not isinstance(row, dict):
                    continue
                normalized = {
                    "id": str(row.get("id") or "").strip(),
                    "summary": str(row.get("summary") or "").strip(),
                    "primary": bool(row.get("primary", False)),
                    "accessRole": str(row.get("accessRole") or "").strip(),
                    "selected": bool(row.get("selected", True)),
                    "hidden": bool(row.get("hidden", False)),
                    "deleted": bool(row.get("deleted", False)),
                }
                if normalized["id"]:
                    calendars.append(normalized)

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        deduped: dict[str, dict[str, Any]] = {}
        for row in calendars:
            deduped[row["id"]] = row
        return sorted(deduped.values(), key=lambda row: (row["id"], row["summary"]))

    def fetch_events(
        self,
        *,
        access_token: str,
        calendar_id: str = "primary",
        calendar_name: str | None = None,
        max_results: int = 50,
        time_min: datetime | None = None,
        time_max: datetime | None = None,
    ) -> list[dict[str, Any]]:
        resolved_time_min = time_min or datetime.now(UTC) - timedelta(days=1)
        resolved_time_max = time_max or datetime.now(UTC) + timedelta(days=30)

        encoded_calendar_id = quote(str(calendar_id), safe="")
        url = f"{_GCAL_API_BASE}/calendars/{encoded_calendar_id}/events"

        collected: list[dict[str, Any]] = []
        page_token: str | None = None
        remaining = max(1, int(max_results))

        while True:
            params: dict[str, Any] = {
                "maxResults": min(remaining, _MAX_RESULTS_PER_PAGE),
                "singleEvents": "true",
                "orderBy": "startTime",
                "timeMin": _to_rfc3339(resolved_time_min),
                "timeMax": _to_rfc3339(resolved_time_max),
            }
            if page_token:
                params["pageToken"] = page_token

            data = self._request_json(url=url, access_token=access_token, params=params)
            if data is None:
                break

            for item in data.get("items", []):
                if not isinstance(item, dict):
                    continue
                mapped = _map_google_event_to_raw(item)
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

        return _deduplicate_events(collected)

    def _request_json(self, *, url: str, access_token: str, params: dict[str, Any]) -> dict[str, Any] | None:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }
        try:
            with httpx.Client(timeout=self._timeout_seconds) as client:
                response = client.get(url, headers=headers, params=params)
                response.raise_for_status()
                payload = response.json()
                if isinstance(payload, dict):
                    return payload
        except Exception:
            return None
        return None


def normalize_provider_events(rows: list[dict[str, Any]]) -> list[GoogleCalendarEvent]:
    normalized: list[GoogleCalendarEvent] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(
            GoogleCalendarEvent(
                event_id=str(row.get("event_id") or ""),
                title=str(row.get("title") or "").strip(),
                start=_extract_date_or_datetime(row.get("start")),
                end=_extract_date_or_datetime(row.get("end")),
                status=str(row.get("status")) if row.get("status") else None,
                timestamp=str(row.get("timestamp")) if row.get("timestamp") else None,
                source_calendar_id=str(row.get("source_calendar_id") or "primary"),
                source_calendar_name=str(row.get("source_calendar_name") or "primary"),
                payload=dict(row),
            )
        )
    return normalized


def _map_google_event_to_raw(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": str(item.get("id") or ""),
        "title": str(item.get("summary") or "").strip(),
        "description": str(item.get("description") or "").strip(),
        "location": str(item.get("location") or "").strip(),
        "status": item.get("status"),
        "start": item.get("start") if isinstance(item.get("start"), dict) else {},
        "end": item.get("end") if isinstance(item.get("end"), dict) else {},
        "timestamp": str(item.get("updated") or ""),
        "iCalUID": str(item.get("iCalUID") or ""),
        "provider": "google_calendar",
    }


def _deduplicate_events(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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

        row_rank = (
            str(row.get("timestamp", "")),
            str(row.get("event_id", "")),
            str(row.get("source_calendar_id", "")),
            str(row.get("source_calendar_name", "")),
        )
        existing_rank = (
            str(existing.get("timestamp", "")),
            str(existing.get("event_id", "")),
            str(existing.get("source_calendar_id", "")),
            str(existing.get("source_calendar_name", "")),
        )
        if row_rank < existing_rank:
            selected[dedupe_key] = row

    deduped = list(selected.values())
    deduped.sort(key=lambda row: (str(row.get("timestamp", "")), str(row.get("event_id", ""))))
    return deduped


def _to_rfc3339(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat().replace("+00:00", "Z")


def _extract_date_or_datetime(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    date_time = value.get("dateTime")
    if isinstance(date_time, str) and date_time.strip():
        return date_time.strip()
    date_only = value.get("date")
    if isinstance(date_only, str) and date_only.strip():
        return date_only.strip()
    return None


__all__ = ["GoogleCalendarEvent", "GoogleCalendarProvider", "normalize_provider_events"]
