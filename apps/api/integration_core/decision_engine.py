from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DecisionContext:
    top_events: list[dict[str, Any]]
    next_event: dict[str, Any] | None
    conflicts: list[list[dict[str, Any]]]
    summary: dict[str, Any]


class DecisionEngine:
    """
    Pure decision layer.
    MUST NOT:
    - fetch data
    - mutate state
    - access environment
    """

    def process(self, state: Any) -> DecisionContext:
        events = [self._to_event_dict(e) for e in list(state.calendar_events)]

        now = self._resolve_now(state)

        scored: list[tuple[int, dict[str, Any]]] = []
        rules_triggered_count = 0
        for event in events:
            score = self._score_event(event, now)
            if score > 0:
                rules_triggered_count += 1
            if self._event_has_conflict(event, events):
                score += 30
                rules_triggered_count += 1
            scored.append((score, event))

        scored.sort(key=lambda x: x[0], reverse=True)

        sorted_events = [e for _, e in scored]

        conflicts = self._detect_conflicts(sorted_events)

        next_event = self._get_next_event(sorted_events, now)

        priority_distribution = {
            "high": sum(1 for score, _ in scored if score >= 80),
            "medium": sum(1 for score, _ in scored if 40 <= score < 80),
            "low": sum(1 for score, _ in scored if score < 40),
        }
        log.info(
            "decision_engine_metrics",
            extra={
                "decision_count": len(sorted_events),
                "priority_distribution": priority_distribution,
                "conflict_count": len(conflicts),
                "rules_triggered_count": rules_triggered_count,
            },
        )

        return DecisionContext(
            top_events=sorted_events[:5],
            next_event=next_event,
            conflicts=conflicts,
            summary={
                "total_events": len(events),
                "conflict_count": len(conflicts),
            },
        )

    def decide(self, state: Any) -> DecisionContext:
        """Backward-compatible alias."""
        return self.process(state)

    def _score_event(self, event: dict[str, Any], now: datetime) -> int:
        score = 0

        start = self._parse_time(event.get("start"))

        if not start:
            return score

        delta = (start - now).total_seconds()

        if delta > 0:
            score += 10

        if 0 < delta <= 7200:
            score += 40

        if start.date() == now.date():
            score += 50

        return score

    def _detect_conflicts(self, events: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
        conflicts: list[list[dict[str, Any]]] = []

        for i in range(len(events)):
            for j in range(i + 1, len(events)):
                a = events[i]
                b = events[j]

                if self._overlaps(a, b):
                    conflicts.append([a, b])

        return conflicts

    def _get_next_event(self, events: list[dict[str, Any]], now: datetime) -> dict[str, Any] | None:
        for event in events:
            start = self._parse_time(event.get("start"))
            if start and start > now:
                return event
        return None

    def _event_has_conflict(self, event: dict[str, Any], events: list[dict[str, Any]]) -> bool:
        for other in events:
            if other is event:
                continue
            if self._overlaps(event, other):
                return True
        return False

    def _overlaps(self, a: dict[str, Any], b: dict[str, Any]) -> bool:
        a_start = self._parse_time(a.get("start"))
        a_end = self._parse_time(a.get("end"))
        b_start = self._parse_time(b.get("start"))
        b_end = self._parse_time(b.get("end"))

        if not all([a_start, a_end, b_start, b_end]):
            return False

        return bool(a_start < b_end and b_start < a_end)

    def _parse_time(self, value: Any) -> datetime | None:
        if not value:
            return None
        raw = str(value)
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        return datetime.fromisoformat(raw)

    def _resolve_now(self, state: Any) -> datetime:
        reference = getattr(state, "metadata", {}).get("reference_time")
        parsed = self._parse_time(reference)
        if parsed is not None:
            return parsed
        return datetime.utcnow()

    @staticmethod
    def _to_event_dict(event: Any) -> dict[str, Any]:
        if isinstance(event, dict):
            return dict(event)
        if hasattr(event, "as_dict") and callable(event.as_dict):
            return dict(event.as_dict())
        return {
            "event_id": str(getattr(event, "event_id", "")),
            "title": str(getattr(event, "title", "")),
            "start": str(getattr(event, "start", "")),
            "end": str(getattr(event, "end", "")),
        }
