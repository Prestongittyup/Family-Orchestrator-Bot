from __future__ import annotations

from apps.api.hpal.contracts import (
    EventModel,
    FamilyModel,
    HouseholdOverview,
    PersonModel,
    PlanModel,
    TaskModel,
)


class ProjectionBuilder:
    """Pure HPAL projection transforms from orchestration graph snapshots."""

    def build_family(self, *, family_id: str, graph: dict[str, Any]) -> FamilyModel:
        hpal = graph.get("hpal", {})
        family = dict(hpal.get("family", {}))
        members = [PersonModel.model_validate(row) for row in list(hpal.get("members", {}).values())]
        plans = [PlanModel.model_validate(row) for row in list(hpal.get("plans", {}).values()) if row.get("status") in {"active", "paused"}]
        watermark = dict(hpal.get("projection_watermark", {}))
        transition_count = len(graph.get("action_lifecycle", {}).get("transition_log", []))
        event_count = len(graph.get("event_history", []))
        stale_projection = (
            int(watermark.get("transition_count", -1)) != transition_count
            or int(watermark.get("event_count", -1)) != event_count
        )
        summary = {
            "state_version": int(graph.get("state_version", 0)),
            "pending_actions": sum(1 for item in graph.get("action_lifecycle", {}).get("actions", {}).values() if str(item.get("current_state", "")) in {"pending_approval", "approved", "proposed"}),
            "projection_epoch": int(watermark.get("projection_epoch", 0)),
            "last_projection_at": str(watermark.get("last_projection_at", graph.get("updated_at", ""))),
            "stale_projection": stale_projection,
        }
        return FamilyModel(
            family_id=family_id,
            members=members,
            shared_calendar_ref=str(family.get("shared_calendar_ref", "primary")),
            default_time_zone=str(family.get("default_time_zone", "UTC")),
            household_preferences=dict(family.get("household_preferences", {})),
            active_plans=[plan.plan_id for plan in plans],
            system_state_summary=summary,
        )

    def build_plans(self, *, graph: dict[str, Any]) -> list[PlanModel]:
        return [
            PlanModel.model_validate(row)
            for _pid, row in sorted(dict(graph.get("hpal", {}).get("plans", {})).items())
        ]

    def build_tasks(self, *, graph: dict[str, Any], person_id: str | None = None) -> list[TaskModel]:
        tasks = [TaskModel.model_validate(self._strip_internal(row)) for row in list(graph.get("hpal", {}).get("tasks", []))]
        if person_id is None:
            return tasks
        return [task for task in tasks if task.assigned_to == person_id]

    def build_events(self, *, graph: dict[str, Any]) -> list[EventModel]:
        return [
            EventModel.model_validate(row)
            for _eid, row in sorted(dict(graph.get("hpal", {}).get("events", {})).items())
        ]

    def build_overview(self, *, family_id: str, graph: dict[str, Any]) -> HouseholdOverview:
        family = self.build_family(family_id=family_id, graph=graph)
        tasks = self.build_tasks(graph=graph)
        events = self.build_events(graph=graph)
        return HouseholdOverview(
            family=family,
            today_events=events,
            active_plan_count=len(family.active_plans),
            pending_task_count=sum(1 for t in tasks if t.status in {"pending", "in_progress"}),
            completed_task_count=sum(1 for t in tasks if t.status == "completed"),
        )

    def _strip_internal(self, row: dict[str, Any]) -> dict[str, Any]:
        cleaned = dict(row)
        cleaned.pop("_internal_action_ref", None)
        cleaned.pop("last_synced_at", None)
        return cleaned
