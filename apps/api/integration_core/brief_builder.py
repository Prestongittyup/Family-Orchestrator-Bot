from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from apps.api.integration_core.decision_engine import DecisionContext
from apps.api.integration_core.event_windowing import parse_event_datetime
from apps.api.integration_core.models.household_state import HouseholdState

log = logging.getLogger(__name__)


class BriefBuilder:
    def build(
        self, state: HouseholdState, decision_context: DecisionContext | None = None
    ) -> dict:
        """
        Convert state + decision context into user-facing brief.
        No IO allowed.
        Pure transformation only.
        Consumes exclusively from HouseholdState and DecisionContext.
        """
        reference_time = self._reference_time(state)
        today = reference_time.date()

        dated_events: list[tuple[datetime, dict]] = []
        for event in state.calendar_events:
            start_dt = parse_event_datetime(event.start)
            if start_dt is None:
                continue
            dated_events.append((start_dt, event.as_dict()))

        dated_events.sort(key=lambda item: (item[0], item[1].get("event_id", "")))
        today_events = [payload for start_dt, payload in dated_events if start_dt.date() == today]
        next_upcoming_event = next(
            (payload for start_dt, payload in dated_events if start_dt >= reference_time),
            None,
        )

        calendar_section = self._extract_calendar_section(
            state, today_events, next_upcoming_event, dated_events
        )

        # Log brief generation with comprehensive state metrics
        log.info(
            "brief_generated",
            extra={
                "user_id": state.user_id,
                "events": len(state.calendar_events),
                "today_events": len(today_events),
                "next_upcoming": next_upcoming_event is not None,
                "tasks": len(state.tasks),
                "alerts": len(state.alerts),
                "conflicts": len(decision_context.conflicts) if decision_context else 0,
            },
        )

        brief_output = {
            "date": today.isoformat(),
            "today_events": today_events,
            "events": today_events,
            "event_count": len(today_events),
            "next_upcoming_event": next_upcoming_event,
            "calendar": calendar_section,
            "summary": {
                "today_event_count": len(today_events),
                "calendar_event_count": len(state.calendar_events),
                "task_count": len(state.tasks),
                "alert_count": len(state.alerts),
                "has_alerts": bool(state.alerts),
                "has_upcoming_event": next_upcoming_event is not None,
            },
        }

        if decision_context is not None:
            brief_output["next_event"] = decision_context.next_event
            brief_output["top_events"] = decision_context.top_events
            brief_output["conflicts"] = decision_context.conflicts

        return brief_output

    @staticmethod
    def _extract_calendar_section(
        state: HouseholdState,
        today_events: list[dict],
        next_upcoming_event: dict | None,
        dated_events: list[tuple[datetime, dict]],
    ) -> dict:
        """Extract calendar view from state exclusively.
        
        Constructs calendar section by pure transformation of state.calendar_events.
        No provider access. No external data sources.
        """
        return {
            "events_today": today_events,
            "upcoming": [payload for _, payload in dated_events if _ > datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)],
            "total_events": len(state.calendar_events),
            "next_event": next_upcoming_event,
        }

    @staticmethod
    def _reference_time(state: HouseholdState) -> datetime:
        raw = state.metadata.get("reference_time")
        if isinstance(raw, str):
            parsed = parse_event_datetime(raw)
            if parsed is not None:
                return parsed
        return datetime.now(UTC)