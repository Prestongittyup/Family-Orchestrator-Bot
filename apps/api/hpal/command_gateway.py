from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from apps.api.hpal.auto_reconciliation import AutoReconciliationHook
from apps.api.hpal.contracts import (
    CommandResult,
    CreateEventRequest,
    CreateFamilyRequest,
    CreatePlanRequest,
    EventModel,
    FamilyModel,
    LinkEventPlanRequest,
    PersonModel,
    PlanModel,
    RecomputePlanRequest,
    UpdateMemberRequest,
    UpdatePlanRequest,
)
from apps.api.hpal.event_integration import EventIntegrationLayer
from apps.api.hpal.orchestration_adapter import OrchestrationAdapter
from apps.api.hpal.plan_normalization import PlanNormalizationEngine
from apps.api.hpal.projection_builder import ProjectionBuilder
from apps.api.hpal.task_sync import TaskSynchronizationEngine


@dataclass(frozen=True)
class _CommandOutcome:
    result: CommandResult
    request_id: str | None
    action_id: str | None
    graph: dict[str, Any]


class HpalCommandGateway:
    """Deterministic HPAL write gateway for Family/Plan/Task/Event boundaries."""

    def __init__(self, adapter: OrchestrationAdapter | None = None) -> None:
        self.adapter = adapter or OrchestrationAdapter()
        self.plan_engine = PlanNormalizationEngine()
        self.task_sync = TaskSynchronizationEngine()
        self.projections = ProjectionBuilder()
        self.events = EventIntegrationLayer()
        self.reconcile = AutoReconciliationHook(self.adapter)

    def create_family(self, request: CreateFamilyRequest) -> FamilyModel:
        command_outcome = self._submit_once(
            family_id=request.family_id,
            command_type="create_family",
            idempotency_key=f"create_family:{request.family_id}",
            payload=request.model_dump(),
            intent_text=f"Initialize family {request.name}",
        )
        graph = command_outcome.graph
        hpal = self._ensure_hpal(graph)

        hpal["family"] = {
            "family_id": request.family_id,
            "name": request.name,
            "shared_calendar_ref": request.shared_calendar_ref,
            "default_time_zone": request.default_time_zone,
            "household_preferences": request.household_preferences,
            "created_at": self._now_iso(),
        }
        members = hpal.setdefault("members", {})
        for person in request.initial_members:
            self._assert_family_scope(request.family_id, person.person_id, person.preferences.get("family_id"))
            members[person.person_id] = person.model_dump()

        self._persist_derived(graph=graph, family_id=request.family_id)
        return self.projections.build_family(family_id=request.family_id, graph=graph)

    def update_member(self, *, family_id: str, person_id: str, request: UpdateMemberRequest) -> PersonModel:
        baseline = self.adapter.load_graph(family_id)
        hpal0 = self._ensure_hpal(baseline)
        members0 = hpal0.setdefault("members", {})
        if person_id not in members0:
            raise ValueError("person not found")

        command_outcome = self._submit_once(
            family_id=family_id,
            command_type="update_member",
            idempotency_key=f"update_member:{person_id}:{self._hash_payload(request.model_dump())}",
            payload={"family_id": family_id, "person_id": person_id, **request.model_dump()},
            intent_text=f"Update household member {person_id}",
        )
        graph = command_outcome.graph
        hpal = self._ensure_hpal(graph)
        members = hpal.setdefault("members", {})
        payload = dict(members.get(person_id, {}))
        if not payload:
            payload = dict(members0[person_id])

        if request.name is not None:
            payload["name"] = request.name
        if request.role is not None:
            payload["role"] = request.role
        if request.availability_constraints is not None:
            payload["availability_constraints"] = list(request.availability_constraints)
        if request.preferences is not None:
            payload["preferences"] = dict(request.preferences)

        members[person_id] = payload
        self._persist_derived(graph=graph, family_id=family_id)
        return PersonModel.model_validate(payload)

    def create_plan_from_intent(self, *, family_id: str, request: CreatePlanRequest) -> tuple[PlanModel, CommandResult]:
        baseline = self.adapter.load_graph(family_id)
        hpal0 = self._ensure_hpal(baseline)
        for person_id in request.participants:
            if person_id not in hpal0.setdefault("members", {}):
                raise ValueError(f"unknown participant: {person_id}")

        plans0 = hpal0.setdefault("plans", {})
        normalized = self.plan_engine.normalize(
            family_id=family_id,
            request=request,
            existing_plans=list(plans0.values()),
        )

        expected_previous_revision = None
        previous_plan = plans0.get(normalized.plan.plan_id)
        if previous_plan is not None:
            expected_previous_revision = int(previous_plan.get("revision", 1))

        command_outcome = self._submit_once(
            family_id=family_id,
            command_type="create_or_merge_plan",
            idempotency_key=request.idempotency_key,
            payload={"family_id": family_id, **request.model_dump(), "plan_id": normalized.plan.plan_id},
            intent_text=request.intent_origin,
        )

        graph = command_outcome.graph
        hpal = self._ensure_hpal(graph)
        plans = hpal.setdefault("plans", {})
        current_existing = plans.get(normalized.plan.plan_id)
        if expected_previous_revision is not None and current_existing is not None:
            if int(current_existing.get("revision", 1)) != expected_previous_revision:
                raise ValueError("concurrent plan revision collision")

        if normalized.conflict_revision and current_existing is not None:
            self._append_plan_history(hpal=hpal, plan_id=normalized.plan.plan_id, prior=current_existing)

        plan_payload = normalized.plan.model_dump()
        if current_existing is not None and not normalized.conflict_revision:
            plan_payload["revision"] = int(current_existing.get("revision", 1)) + 1
        plan_payload["plan_type"] = self.plan_engine._infer_plan_type(request.title, request.intent_origin)
        plan_payload["participants"] = sorted(request.participants)
        plans[normalized.plan.plan_id] = plan_payload

        if command_outcome.result.status == "accepted":
            self._bind_orchestration_refs(
                hpal=hpal,
                plan_id=normalized.plan.plan_id,
                request_id=command_outcome.request_id,
                action_id=command_outcome.action_id,
            )

        self._persist_derived(graph=graph, family_id=family_id)
        return PlanModel.model_validate(plans[normalized.plan.plan_id]), command_outcome.result

    def update_plan(self, *, family_id: str, plan_id: str, request: UpdatePlanRequest) -> tuple[PlanModel, CommandResult]:
        baseline = self.adapter.load_graph(family_id)
        hpal0 = self._ensure_hpal(baseline)
        plans0 = hpal0.setdefault("plans", {})
        existing0 = plans0.get(plan_id)
        if existing0 is None:
            raise ValueError("plan not found")
        expected_revision = int(existing0.get("revision", 1))

        command_outcome = self._submit_once(
            family_id=family_id,
            command_type="update_plan",
            idempotency_key=request.idempotency_key,
            payload={"family_id": family_id, "plan_id": plan_id, **request.model_dump()},
            intent_text=f"Update plan {plan_id}",
        )

        graph = command_outcome.graph
        hpal = self._ensure_hpal(graph)
        plans = hpal.setdefault("plans", {})
        existing = plans.get(plan_id)
        if existing is None:
            raise ValueError("plan not found")
        if int(existing.get("revision", 1)) != expected_revision:
            raise ValueError("concurrent plan revision collision")

        revised = self.plan_engine.revise(
            existing_plan=existing,
            title=request.title,
            schedule_window=request.schedule_window,
            intent_origin="plan_update",
        )
        if request.status is not None:
            revised = revised.model_copy(update={"status": request.status})

        self._append_plan_history(hpal=hpal, plan_id=plan_id, prior=existing)
        plans[plan_id] = {
            **revised.model_dump(),
            "plan_type": existing.get("plan_type", "household_plan"),
            "participants": existing.get("participants", []),
        }

        self._persist_derived(graph=graph, family_id=family_id)
        return PlanModel.model_validate(plans[plan_id]), command_outcome.result

    def recompute_plan(self, *, family_id: str, plan_id: str, request: RecomputePlanRequest) -> CommandResult:
        baseline = self.adapter.load_graph(family_id)
        hpal0 = self._ensure_hpal(baseline)
        plans0 = hpal0.setdefault("plans", {})
        existing0 = plans0.get(plan_id)
        if existing0 is None:
            raise ValueError("plan not found")
        expected_revision = int(existing0.get("revision", 1))

        command_outcome = self._submit_once(
            family_id=family_id,
            command_type="recompute_plan",
            idempotency_key=request.idempotency_key,
            payload={"family_id": family_id, "plan_id": plan_id, **request.model_dump()},
            intent_text=f"Recompute plan {plan_id}: {request.reason}",
        )

        graph = command_outcome.graph
        hpal = self._ensure_hpal(graph)
        plans = hpal.setdefault("plans", {})
        existing = plans.get(plan_id)
        if existing is None:
            raise ValueError("plan not found")

        if command_outcome.result.status == "accepted" and int(existing.get("revision", 1)) != expected_revision:
            raise ValueError("concurrent plan revision collision")

        updated = dict(existing)
        updated["last_recomputed_at"] = self._now_iso()
        updated["stability_state"] = "adjusting"
        plans[plan_id] = updated

        self._persist_derived(graph=graph, family_id=family_id)
        return command_outcome.result

    def create_event(self, *, family_id: str, request: CreateEventRequest) -> tuple[EventModel, CommandResult]:
        command_outcome = self._submit_once(
            family_id=family_id,
            command_type="create_event",
            idempotency_key=request.idempotency_key,
            payload={"family_id": family_id, **request.model_dump()},
            intent_text=f"Create event {request.title}",
        )
        graph = command_outcome.graph
        event = self.events.create_event(family_id=family_id, request=request, graph=graph)

        self._persist_derived(graph=graph, family_id=family_id)
        return event, command_outcome.result

    def link_event_to_plan(self, *, family_id: str, event_id: str, request: LinkEventPlanRequest) -> tuple[EventModel, CommandResult]:
        baseline = self.adapter.load_graph(family_id)
        hpal0 = self._ensure_hpal(baseline)
        plans0 = hpal0.setdefault("plans", {})
        plan = plans0.get(request.plan_id)
        if plan is None:
            raise ValueError("plan not found")
        plan_revision = int(plan.get("revision", 1))

        command_outcome = self._submit_once(
            family_id=family_id,
            command_type="link_event_to_plan",
            idempotency_key=request.idempotency_key,
            payload={"family_id": family_id, "event_id": event_id, **request.model_dump(), "plan_revision": plan_revision},
            intent_text=f"Link event {event_id} to plan {request.plan_id}",
        )

        graph = command_outcome.graph
        hpal = self._ensure_hpal(graph)
        before = dict(hpal.setdefault("events", {}).get(event_id, {})) if event_id in hpal.setdefault("events", {}) else None
        event = self.events.link_event_to_plan(
            family_id=family_id,
            event_id=event_id,
            request=request,
            plan_revision=plan_revision,
            graph=graph,
        )

        if self.events.should_trigger_recompute(before=before, after=event.model_dump()):
            self.reconcile.on_event_update(family_id=family_id, plan_id=request.plan_id, reason="event update")

        self._persist_derived(graph=graph, family_id=family_id)
        return event, command_outcome.result

    def get_family_state(self, *, family_id: str) -> FamilyModel:
        graph = self.adapter.load_graph(family_id)
        self._ensure_hpal(graph)
        return self.projections.build_family(family_id=family_id, graph=graph)

    def get_household_overview(self, *, family_id: str):
        graph = self.adapter.load_graph(family_id)
        self._ensure_hpal(graph)
        return self.projections.build_overview(family_id=family_id, graph=graph)

    def get_plan_status(self, *, family_id: str, plan_id: str) -> PlanModel:
        graph = self.adapter.load_graph(family_id)
        self._ensure_hpal(graph)
        plans = {plan.plan_id: plan for plan in self.projections.build_plans(graph=graph)}
        if plan_id not in plans:
            raise ValueError("plan not found")
        return plans[plan_id]

    def get_tasks_by_family(self, *, family_id: str) -> list[dict[str, Any]]:
        graph = self.adapter.load_graph(family_id)
        self._ensure_hpal(graph)
        return [task.model_dump() for task in self.projections.build_tasks(graph=graph)]

    def get_plans_by_family(self, *, family_id: str) -> list[dict[str, Any]]:
        graph = self.adapter.load_graph(family_id)
        self._ensure_hpal(graph)
        return [plan.model_dump() for plan in self.projections.build_plans(graph=graph)]

    def get_tasks_by_person(self, *, family_id: str, person_id: str) -> list[dict[str, Any]]:
        graph = self.adapter.load_graph(family_id)
        hpal = self._ensure_hpal(graph)
        if person_id not in hpal.setdefault("members", {}):
            raise ValueError("person not found")
        return [task.model_dump() for task in self.projections.build_tasks(graph=graph, person_id=person_id)]

    def get_calendar_view(self, *, family_id: str) -> list[dict[str, Any]]:
        graph = self.adapter.load_graph(family_id)
        self._ensure_hpal(graph)
        return [event.model_dump() for event in self.projections.build_events(graph=graph)]

    def system_override_task_status(self, *, family_id: str, task_id: str, target_status: str, reason_code: str) -> dict[str, Any]:
        baseline = self.adapter.load_graph(family_id)
        hpal0 = self._ensure_hpal(baseline)
        existing_task = next((row for row in hpal0.get("tasks", []) if row.get("task_id") == task_id), None)
        if existing_task is None:
            raise ValueError("task not found")

        command_outcome = self._submit_once(
            family_id=family_id,
            command_type="system_override_task_status",
            idempotency_key=f"task_status:{task_id}:{target_status}:{reason_code}",
            payload={"family_id": family_id, "task_id": task_id, "target_status": target_status, "reason_code": reason_code},
            intent_text=f"System update task status {task_id} to {target_status}",
        )

        graph = command_outcome.graph
        self._persist_derived(graph=graph, family_id=family_id)

        if target_status == "failed":
            plan_id = str(existing_task.get("plan_id", ""))
            if plan_id:
                self.reconcile.on_task_failure(family_id=family_id, plan_id=plan_id, reason=reason_code)

        tasks = self.get_tasks_by_family(family_id=family_id)
        updated = next((row for row in tasks if row.get("task_id") == task_id), None)
        if updated is None:
            raise ValueError("task not found")
        return updated

    def system_reschedule_task(self, *, family_id: str, task_id: str, due_time: str, reason_code: str) -> dict[str, Any]:
        baseline = self.adapter.load_graph(family_id)
        hpal0 = self._ensure_hpal(baseline)
        existing_task = next((row for row in hpal0.get("tasks", []) if row.get("task_id") == task_id), None)
        if existing_task is None:
            raise ValueError("task not found")

        command_outcome = self._submit_once(
            family_id=family_id,
            command_type="system_reschedule_task",
            idempotency_key=f"task_reschedule:{task_id}:{due_time}:{reason_code}",
            payload={"family_id": family_id, "task_id": task_id, "due_time": due_time, "reason_code": reason_code},
            intent_text=f"System reschedule task {task_id}",
        )

        graph = command_outcome.graph
        self._persist_derived(graph=graph, family_id=family_id)

        plan_id = str(existing_task.get("plan_id", ""))
        if plan_id:
            self.reconcile.on_schedule_conflict(family_id=family_id, plan_id=plan_id, reason=reason_code)

        tasks = self.get_tasks_by_family(family_id=family_id)
        updated = next((row for row in tasks if row.get("task_id") == task_id), None)
        if updated is None:
            raise ValueError("task not found")
        return updated

    def _submit_once(
        self,
        *,
        family_id: str,
        command_type: str,
        idempotency_key: str,
        payload: dict[str, Any],
        intent_text: str,
    ) -> _CommandOutcome:
        self._assert_family_scope(family_id, None, payload.get("family_id"))
        graph = self.adapter.load_graph(family_id)
        hpal = self._ensure_hpal(graph)
        registry = hpal.setdefault("idempotency", {})
        namespace_key = f"{command_type}:{idempotency_key}"
        payload_hash = self._hash_payload(payload)

        existing = registry.get(namespace_key)
        if existing is not None:
            if existing.get("payload_hash") != payload_hash:
                raise ValueError("idempotency key hash mismatch")
            return _CommandOutcome(
                result=CommandResult(
                    command_id=str(existing.get("command_id", "")),
                    status="replayed",
                    submitted_at=str(existing.get("submitted_at", self._now_iso())),
                ),
                request_id=existing.get("request_id"),
                action_id=existing.get("action_id"),
                graph=graph,
            )

        submission = self.adapter.submit_command(
            family_id=family_id,
            command_type=command_type,
            intent_text=intent_text,
            idempotency_key=idempotency_key,
            payload=payload,
        )

        graph = self.adapter.load_graph(family_id)
        hpal = self._ensure_hpal(graph)
        registry = hpal.setdefault("idempotency", {})
        concurrent = registry.get(namespace_key)
        if concurrent is not None and concurrent.get("payload_hash") != payload_hash:
            raise ValueError("idempotency key hash mismatch")
        if concurrent is None:
            registry[namespace_key] = {
                "command_id": submission.command_id,
                "payload_hash": payload_hash,
                "submitted_at": self._now_iso(),
                "request_id": submission.request_id,
                "action_id": submission.action_id,
            }
            expected_version = int(graph.get("state_version", 0))
            graph = self.adapter.save_hpal_state(
                family_id=family_id,
                graph=graph,
                expected_state_version=expected_version,
            )

        return _CommandOutcome(
            result=CommandResult(
                command_id=submission.command_id,
                status="accepted",
                submitted_at=self._now_iso(),
            ),
            request_id=submission.request_id,
            action_id=submission.action_id,
            graph=graph,
        )

    def _ensure_hpal(self, graph: dict[str, Any]) -> dict[str, Any]:
        hpal = graph.setdefault("hpal", {})
        hpal.setdefault("family", {})
        hpal.setdefault("members", {})
        hpal.setdefault("plans", {})
        hpal.setdefault("tasks", [])
        hpal.setdefault("events", {})
        hpal.setdefault("idempotency", {})
        hpal.setdefault("command_log", [])
        hpal.setdefault("plan_action_map", {})
        hpal.setdefault("plan_request_map", {})
        hpal.setdefault("plan_revision_history", {})
        hpal.setdefault("projection_watermark", {})
        hpal.setdefault("reconciliation_dedupe", {})
        return hpal

    def _append_plan_history(self, *, hpal: dict[str, Any], plan_id: str, prior: dict[str, Any]) -> None:
        snapshot = deepcopy(prior)
        entry = {
            "recorded_at": self._now_iso(),
            "revision": int(snapshot.get("revision", 1)),
            "plan": snapshot,
        }
        hpal.setdefault("plan_revision_history", {}).setdefault(plan_id, []).append(entry)

    def _sync_plan_tasks(self, *, graph: dict[str, Any]) -> None:
        hpal = graph.setdefault("hpal", {})
        plan_tasks: dict[str, list[str]] = {}
        for task in list(hpal.get("tasks", [])):
            plan_tasks.setdefault(str(task.get("plan_id", "")), []).append(str(task.get("task_id", "")))
        plans = hpal.setdefault("plans", {})
        for plan_id, payload in plans.items():
            updated = dict(payload)
            updated["linked_tasks"] = sorted(plan_tasks.get(plan_id, []))
            statuses = [t.get("status") for t in hpal.get("tasks", []) if t.get("plan_id") == plan_id]
            if statuses and all(s == "completed" for s in statuses):
                updated["status"] = "completed"
                updated["stability_state"] = "stable"
            elif "stale_projection" in statuses:
                updated["status"] = "active"
                updated["stability_state"] = "blocked"
            elif "failed" in statuses:
                updated["status"] = "failed"
                updated["stability_state"] = "blocked"
            elif statuses:
                updated["status"] = "active"
                updated["stability_state"] = "adjusting"
            plans[plan_id] = updated

    def _bind_orchestration_refs(self, *, hpal: dict[str, Any], plan_id: str, request_id: str | None, action_id: str | None) -> None:
        if request_id:
            hpal.setdefault("plan_request_map", {})[request_id] = plan_id
        if action_id:
            hpal.setdefault("plan_action_map", {})[action_id] = plan_id

    def _persist_derived(self, *, graph: dict[str, Any], family_id: str) -> None:
        self.task_sync.sync(family_id=family_id, graph=graph)
        self._sync_plan_tasks(graph=graph)
        self._update_projection_watermark(graph=graph)
        expected_version = int(graph.get("state_version", 0))
        out = self.adapter.save_hpal_state(
            family_id=family_id,
            graph=graph,
            expected_state_version=expected_version,
        )
        graph.clear()
        graph.update(out)

    def _update_projection_watermark(self, *, graph: dict[str, Any]) -> None:
        hpal = graph.setdefault("hpal", {})
        watermark = hpal.setdefault("projection_watermark", {})
        transition_count = len(graph.get("action_lifecycle", {}).get("transition_log", []))
        event_count = len(graph.get("event_history", []))
        snapshot_hash = self._hash_payload(
            {
                "plans": hpal.get("plans", {}),
                "tasks": hpal.get("tasks", []),
                "events": hpal.get("events", {}),
                "transition_count": transition_count,
                "event_count": event_count,
            }
        )
        watermark["projection_epoch"] = int(watermark.get("projection_epoch", 0)) + 1
        watermark["transition_count"] = transition_count
        watermark["event_count"] = event_count
        watermark["source_state_version"] = int(graph.get("state_version", 0))
        watermark["snapshot_hash"] = snapshot_hash
        watermark["last_projection_at"] = graph.get("updated_at", self._now_iso())

    def _assert_family_scope(self, family_id: str, person_id: str | None, payload_family_id: Any) -> None:
        if not family_id.strip():
            raise ValueError("family_id is required")
        if payload_family_id is not None and str(payload_family_id) != family_id:
            raise ValueError("cross-family mutation is not allowed")
        if person_id is not None and not str(person_id).strip():
            raise ValueError("person_id must be non-empty")

    def _hash_payload(self, payload: dict[str, Any]) -> str:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _now_iso(self) -> str:
        return datetime.utcnow().isoformat() + "Z"
