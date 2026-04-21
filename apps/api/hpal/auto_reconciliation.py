from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from apps.api.hpal.orchestration_adapter import OrchestrationAdapter


@dataclass(frozen=True)
class ReconciliationResult:
    status: str
    reason: str


class AutoReconciliationHook:
    """HPAL-side trigger layer that requests plan recomputation via orchestration."""

    def __init__(self, adapter: OrchestrationAdapter) -> None:
        self.adapter = adapter

    def on_task_failure(self, *, family_id: str, plan_id: str, reason: str) -> ReconciliationResult:
        return self._submit_recompute(
            family_id=family_id,
            plan_id=plan_id,
            trigger="task_failure",
            reason=reason,
        )

    def on_schedule_conflict(self, *, family_id: str, plan_id: str, reason: str) -> ReconciliationResult:
        return self._submit_recompute(
            family_id=family_id,
            plan_id=plan_id,
            trigger="schedule_conflict",
            reason=reason,
        )

    def on_availability_change(self, *, family_id: str, plan_id: str, reason: str) -> ReconciliationResult:
        return self._submit_recompute(
            family_id=family_id,
            plan_id=plan_id,
            trigger="availability_change",
            reason=reason,
        )

    def on_event_update(self, *, family_id: str, plan_id: str, reason: str) -> ReconciliationResult:
        return self._submit_recompute(
            family_id=family_id,
            plan_id=plan_id,
            trigger="event_update",
            reason=reason,
        )

    def _submit_recompute(self, *, family_id: str, plan_id: str, trigger: str, reason: str) -> ReconciliationResult:
        graph = self.adapter.load_graph(family_id)
        hpal = graph.setdefault("hpal", {})
        dedupe = hpal.setdefault("reconciliation_dedupe", {})
        key = f"recompute:{plan_id}:{trigger}:{reason.strip().lower()}"
        existing = dedupe.get(key)
        if existing is not None:
            return ReconciliationResult(status="replayed", reason=trigger)

        self.adapter.submit_command(
            family_id=family_id,
            command_type="recompute_plan",
            intent_text=f"Recompute plan {plan_id} because {trigger}: {reason}",
            idempotency_key=key,
            payload={"family_id": family_id, "plan_id": plan_id, "reason": reason},
        )
        graph = self.adapter.load_graph(family_id)
        hpal = graph.setdefault("hpal", {})
        dedupe = hpal.setdefault("reconciliation_dedupe", {})
        dedupe[key] = {
            "plan_id": plan_id,
            "trigger": trigger,
            "reason": reason,
            "recorded_at": datetime.utcnow().isoformat() + "Z",
        }
        self.adapter.save_hpal_state(family_id=family_id, graph=graph)
        return ReconciliationResult(status="accepted", reason=trigger)
