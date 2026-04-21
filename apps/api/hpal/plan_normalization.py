from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from apps.api.hpal.contracts import CreatePlanRequest, PlanModel, TimeWindow, now_iso


@dataclass(frozen=True)
class PlanNormalizationResult:
    plan: PlanModel
    merged: bool
    conflict_revision: bool


class PlanNormalizationEngine:
    def normalize(
        self,
        *,
        family_id: str,
        request: CreatePlanRequest,
        existing_plans: list[dict[str, Any]],
    ) -> PlanNormalizationResult:
        plan_type = self._infer_plan_type(request.title, request.intent_origin)
        merge_key = self._merge_key(family_id, plan_type, request.schedule_window)
        plan_id = f"plan-{self._digest(merge_key)[:12]}"

        active_match = self._find_overlap(plan_type=plan_type, window=request.schedule_window, existing=existing_plans)
        if active_match is None:
            plan = PlanModel(
                plan_id=plan_id,
                family_id=family_id,
                title=request.title,
                intent_origin=request.intent_origin,
                status="active",
                linked_tasks=[],
                schedule_window=request.schedule_window,
                last_recomputed_at=now_iso(),
                revision=1,
                stability_state="adjusting",
            )
            return PlanNormalizationResult(plan=plan, merged=False, conflict_revision=False)

        compatible = self._is_compatible(active_match, request)
        revision = int(active_match.get("revision", 1)) + 1
        plan = PlanModel(
            plan_id=active_match.get("plan_id", plan_id),
            family_id=family_id,
            title=request.title if compatible else f"{request.title} (revised)",
            intent_origin=request.intent_origin,
            status="active",
            linked_tasks=list(active_match.get("linked_tasks", [])),
            schedule_window=request.schedule_window,
            last_recomputed_at=now_iso(),
            revision=revision,
            stability_state="adjusting",
        )
        return PlanNormalizationResult(plan=plan, merged=compatible, conflict_revision=not compatible)

    def revise(
        self,
        *,
        existing_plan: dict[str, Any],
        title: str | None,
        schedule_window: TimeWindow | None,
        intent_origin: str,
    ) -> PlanModel:
        revision = int(existing_plan.get("revision", 1)) + 1
        return PlanModel(
            plan_id=str(existing_plan.get("plan_id", "")),
            family_id=str(existing_plan.get("family_id", "")),
            title=title or str(existing_plan.get("title", "Untitled plan")),
            intent_origin=intent_origin,
            status="active" if str(existing_plan.get("status", "active")) != "paused" else "paused",
            linked_tasks=list(existing_plan.get("linked_tasks", [])),
            schedule_window=schedule_window or TimeWindow.model_validate(existing_plan.get("schedule_window", {})),
            last_recomputed_at=now_iso(),
            revision=revision,
            stability_state="adjusting",
        )

    def _is_compatible(self, existing: dict[str, Any], request: CreatePlanRequest) -> bool:
        existing_origin = str(existing.get("intent_origin", "")).strip().lower()
        request_origin = request.intent_origin.strip().lower()
        return existing_origin == request_origin

    def _find_overlap(
        self,
        *,
        plan_type: str,
        window: TimeWindow,
        existing: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        start = self._parse(window.start)
        end = self._parse(window.end)
        for plan in existing:
            status = str(plan.get("status", "active"))
            if status not in {"active", "paused"}:
                continue
            if str(plan.get("plan_type", "")) != plan_type:
                continue
            current = plan.get("schedule_window", {})
            c_start = self._parse(str(current.get("start", "")))
            c_end = self._parse(str(current.get("end", "")))
            if c_start < end and start < c_end:
                return plan
        return None

    def _merge_key(self, family_id: str, plan_type: str, window: TimeWindow) -> str:
        start = self._parse(window.start).isoformat()
        end = self._parse(window.end).isoformat()
        bucket = f"{start}|{end}"
        return f"{family_id}:{plan_type}:{bucket}"

    def _infer_plan_type(self, title: str, intent_origin: str) -> str:
        text = f"{title} {intent_origin}".lower()
        if any(token in text for token in ("dinner", "breakfast", "meal", "cook")):
            return "meal_plan"
        if any(token in text for token in ("school", "morning")):
            return "school_plan"
        if any(token in text for token in ("weekend", "family")):
            return "weekend_plan"
        if any(token in text for token in ("maintenance", "repair")):
            return "maintenance_plan"
        return "household_plan"

    def _digest(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _parse(self, value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
