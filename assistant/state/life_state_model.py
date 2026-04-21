from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from assistant.governance.intent_lock import IntentClassification, IntentType


@dataclass(frozen=True)
class LifeState:
    workload_score: float  # 0-1
    stress_index: float  # 0-1
    routine_stability: float  # 0-1
    recent_focus_distribution: dict[str, int] = field(default_factory=dict)
    active_backlog_size: int = 0


class LifeStateModel:
    """Persistent rolling life-state model used as a weighting signal for routing."""

    def __init__(self, life_state_path: Path | None = None) -> None:
        self.life_state_path = life_state_path or (
            Path(__file__).resolve().parent.parent.parent / "household_state" / "life_state.json"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self, household_id: str) -> LifeState:
        payload = self._read_store()
        data = payload.get("households", {}).get(household_id, {}).get("life_state", {})
        if not data:
            return self._default_state()

        return LifeState(
            workload_score=self._clip01(float(data.get("workload_score", 0.0))),
            stress_index=self._clip01(float(data.get("stress_index", 0.0))),
            routine_stability=self._clip01(float(data.get("routine_stability", 0.5))),
            recent_focus_distribution={
                str(k): int(v) for k, v in dict(data.get("recent_focus_distribution", {})).items()
            },
            active_backlog_size=int(data.get("active_backlog_size", 0)),
        )

    def update_after_run(
        self,
        *,
        household_id: str,
        graph: dict[str, Any],
        classification: IntentClassification | None,
        timestamp: datetime | None = None,
    ) -> LifeState:
        now = timestamp or self._coerce_datetime(graph.get("reference_time"))
        payload = self._read_store()
        house = payload.setdefault("households", {}).setdefault(household_id, {})

        intent_history = list(house.get("intent_history", []))
        if classification is not None:
            intent_history.append(
                {
                    "intent": classification.primary_intent.value,
                    "secondary_intents": [i.value for i in classification.secondary_intents],
                    "confidence": float(classification.confidence),
                    "captured_at": self._iso(now),
                }
            )

        # Keep only recent history for rolling distribution and compact persistence.
        cutoff = now - timedelta(days=14)
        intent_history = [
            item for item in intent_history
            if self._coerce_datetime(item.get("captured_at")) >= cutoff
        ]

        life_state = self._compute_state(graph=graph, intent_history=intent_history, now=now)

        house["intent_history"] = intent_history
        house["life_state"] = asdict(life_state)
        house["updated_at"] = self._iso(now)
        self._write_store(payload)
        return life_state

    def update_after_approval(
        self,
        *,
        household_id: str,
        graph: dict[str, Any],
        timestamp: datetime | None = None,
    ) -> LifeState:
        now = timestamp or self._coerce_datetime(graph.get("reference_time"))
        payload = self._read_store()
        house = payload.setdefault("households", {}).setdefault(household_id, {})
        intent_history = list(house.get("intent_history", []))

        cutoff = now - timedelta(days=14)
        intent_history = [
            item for item in intent_history
            if self._coerce_datetime(item.get("captured_at")) >= cutoff
        ]

        life_state = self._compute_state(graph=graph, intent_history=intent_history, now=now)
        house["intent_history"] = intent_history
        house["life_state"] = asdict(life_state)
        house["updated_at"] = self._iso(now)
        self._write_store(payload)
        return life_state

    # ------------------------------------------------------------------
    # Computation
    # ------------------------------------------------------------------

    def _compute_state(
        self,
        *,
        graph: dict[str, Any],
        intent_history: list[dict[str, Any]],
        now: datetime,
    ) -> LifeState:
        recent_events = self._calendar_events_last_days(graph=graph, now=now, days=7)
        conflicts = self._count_overlaps(recent_events)
        event_density = len(recent_events) / 7.0

        backlog = self._active_backlog_size(graph)
        deferred = self._count_deferred_actions(graph=graph, now=now)

        # Workload grows with calendar density and unresolved backlog.
        workload_score = self._clip01((event_density / 4.0) * 0.6 + (min(backlog, 10) / 10.0) * 0.4)

        # Stress rises with conflicts and workload, plus deferred pressure.
        stress_index = self._clip01(
            (workload_score * 0.5)
            + (min(conflicts, 6) / 6.0) * 0.35
            + (min(deferred, 6) / 6.0) * 0.15
        )

        routine_stability = self._routine_stability(graph=graph, now=now)
        recent_focus_distribution = self._focus_distribution(intent_history=intent_history, now=now)

        return LifeState(
            workload_score=workload_score,
            stress_index=stress_index,
            routine_stability=routine_stability,
            recent_focus_distribution=recent_focus_distribution,
            active_backlog_size=backlog,
        )

    def _active_backlog_size(self, graph: dict[str, Any]) -> int:
        tasks = list(graph.get("tasks", []))
        open_tasks = sum(1 for t in tasks if str(t.get("status", "")).lower() != "completed")

        lifecycle_actions = dict(graph.get("action_lifecycle", {}).get("actions", {}))
        unresolved_actions = sum(
            1
            for payload in lifecycle_actions.values()
            if str(payload.get("current_state", "")) in {"proposed", "pending_approval", "approved", "ignored", "rejected"}
        )
        return int(open_tasks + unresolved_actions)

    def _count_deferred_actions(self, *, graph: dict[str, Any], now: datetime) -> int:
        lifecycle_actions = dict(graph.get("action_lifecycle", {}).get("actions", {}))
        deferred = 0
        for payload in lifecycle_actions.values():
            state = str(payload.get("current_state", ""))
            if state in {"ignored", "rejected"}:
                deferred += 1
                continue

            if state in {"proposed", "pending_approval", "approved"}:
                created_at = payload.get("created_at")
                created = self._coerce_datetime(created_at)
                if created <= (now - timedelta(hours=24)):
                    deferred += 1
        return deferred

    def _routine_stability(self, *, graph: dict[str, Any], now: datetime) -> float:
        records = list(graph.get("behavior_feedback", {}).get("records", []))
        cutoff = now - timedelta(days=14)
        relevant = [
            r for r in records
            if self._coerce_datetime(r.get("timestamp")) >= cutoff
        ]
        if not relevant:
            return 0.5

        approved = sum(1 for r in relevant if str(r.get("status", "")) == "approved")
        ignored = sum(1 for r in relevant if str(r.get("status", "")) == "ignored")
        rejected = sum(1 for r in relevant if str(r.get("status", "")) == "rejected")

        total = max(1, approved + ignored + rejected)
        positive = approved / total
        friction = (ignored + rejected) / total

        return self._clip01(positive * 0.7 + (1.0 - friction) * 0.3)

    def _focus_distribution(self, *, intent_history: list[dict[str, Any]], now: datetime) -> dict[str, int]:
        cutoff = now - timedelta(days=7)
        distribution: dict[str, int] = {t.value: 0 for t in IntentType}
        for item in intent_history:
            captured = self._coerce_datetime(item.get("captured_at"))
            if captured < cutoff:
                continue
            intent = str(item.get("intent", ""))
            if intent in distribution:
                distribution[intent] += 1
        return distribution

    def _calendar_events_last_days(self, *, graph: dict[str, Any], now: datetime, days: int) -> list[dict[str, Any]]:
        events = list(graph.get("calendar_events", []))
        cutoff = now - timedelta(days=days)
        result: list[dict[str, Any]] = []
        for event in events:
            start = self._coerce_datetime(event.get("start"))
            if start >= cutoff and start <= now:
                result.append(event)
        return sorted(result, key=lambda e: str(e.get("start", "")))

    def _count_overlaps(self, events: list[dict[str, Any]]) -> int:
        parsed: list[tuple[datetime, datetime]] = []
        for event in events:
            start = self._coerce_datetime(event.get("start"))
            end = self._coerce_datetime(event.get("end"))
            if end <= start:
                end = start + timedelta(minutes=30)
            parsed.append((start, end))

        parsed.sort(key=lambda item: item[0])
        overlaps = 0
        for idx in range(1, len(parsed)):
            previous_end = parsed[idx - 1][1]
            current_start = parsed[idx][0]
            if current_start < previous_end:
                overlaps += 1
        return overlaps

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _default_state(self) -> LifeState:
        return LifeState(
            workload_score=0.0,
            stress_index=0.0,
            routine_stability=0.5,
            recent_focus_distribution={t.value: 0 for t in IntentType},
            active_backlog_size=0,
        )

    def _read_store(self) -> dict[str, Any]:
        if not self.life_state_path.exists():
            return {"households": {}}
        try:
            return json.loads(self.life_state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"households": {}}

    def _write_store(self, payload: dict[str, Any]) -> None:
        self.life_state_path.parent.mkdir(parents=True, exist_ok=True)
        self.life_state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    @staticmethod
    def _clip01(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    @staticmethod
    def _iso(value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    @staticmethod
    def _coerce_datetime(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value.astimezone(UTC)
        if isinstance(value, str) and value:
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
            except ValueError:
                pass
        return datetime.now(UTC)
