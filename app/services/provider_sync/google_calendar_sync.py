from __future__ import annotations

from datetime import datetime

from app.adapters.providers.google_calendar import GoogleCalendarProvider, normalize_provider_events


class GoogleCalendarSyncService:
    """Service-level orchestration for Google Calendar read synchronization."""

    def __init__(self, *, provider: GoogleCalendarProvider | None = None) -> None:
        self._provider = provider or GoogleCalendarProvider()

    def sync_calendar(
        self,
        *,
        user_id: str,
        access_token: str,
        calendar_id: str = "primary",
        max_results: int = 50,
        time_min: datetime | None = None,
        time_max: datetime | None = None,
    ) -> list[dict]:
        del user_id
        events = self._provider.fetch_events(
            access_token=access_token,
            calendar_id=calendar_id,
            calendar_name=calendar_id,
            max_results=max_results,
            time_min=time_min,
            time_max=time_max,
        )
        return [row.payload for row in normalize_provider_events(events)]

    def sync_all_visible_calendars(
        self,
        *,
        user_id: str,
        access_token: str,
        max_results: int = 100,
        time_min: datetime | None = None,
        time_max: datetime | None = None,
    ) -> list[dict]:
        del user_id
        calendars = self._provider.list_calendars(access_token=access_token)
        if not calendars:
            return self.sync_calendar(
                user_id="unknown",
                access_token=access_token,
                calendar_id="primary",
                max_results=max_results,
                time_min=time_min,
                time_max=time_max,
            )

        aggregated: list[dict] = []
        remaining = max(1, int(max_results))

        for calendar in calendars:
            if remaining <= 0:
                break
            if bool(calendar.get("deleted", False)):
                continue
            if bool(calendar.get("hidden", False)):
                continue
            if calendar.get("accessRole") not in {"owner", "writer", "reader"}:
                continue

            calendar_id = str(calendar.get("id") or "").strip()
            if not calendar_id:
                continue

            rows = self._provider.fetch_events(
                access_token=access_token,
                calendar_id=calendar_id,
                calendar_name=str(calendar.get("summary") or calendar_id),
                max_results=remaining,
                time_min=time_min,
                time_max=time_max,
            )
            aggregated.extend(rows)
            remaining = max_results - len(aggregated)

        return [row.payload for row in normalize_provider_events(aggregated)]


__all__ = ["GoogleCalendarSyncService"]
