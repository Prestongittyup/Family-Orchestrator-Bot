from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any


class TaskSynchronizationEngine:
    """Maps hidden action lifecycle into user-visible task projections."""

    STATUS_MAP = {
        "pending_approval": "pending",
        "proposed": "pending",
        "approved": "pending",
        "executing": "in_progress",
        "executed": "completed",
        "completed": "completed",
        "failed": "failed",
        "rejected": "failed",
        "ignored": "failed",
        "superseded": "failed",
    }

    def sync(self, *, family_id: str, graph: dict[str, Any]) -> list[dict[str, Any]]:
        hpal = graph.setdefault("hpal", {})
        plan_action_map = hpal.setdefault("plan_action_map", {})
        plan_request_map = hpal.setdefault("plan_request_map", {})
        actions = dict(graph.get("action_lifecycle", {}).get("actions", {}))

        tasks: list[dict[str, Any]] = []
        for action_id, raw in actions.items():
            request_id = str(raw.get("request_id", ""))
            plan_id = plan_action_map.get(action_id) or plan_request_map.get(request_id)
            if not plan_id:
                continue
            status = self.STATUS_MAP.get(str(raw.get("current_state", "")), "stale_projection")
            task_id = f"task-{self._digest(f'{family_id}:{plan_id}:{action_id}')[:12]}"
            tasks.append(
                {
                    "task_id": task_id,
                    "plan_id": plan_id,
                    "assigned_to": "system",
                    "status": status,
                    "due_time": raw.get("scheduled_for"),
                    "auto_generated": True,
                    "priority": "medium",
                    "title": str(raw.get("title", "Task")),
                    "_internal_action_ref": action_id,
                    "last_synced_at": self._now_iso(),
                }
            )

        # deterministic shape: last write wins on task_id, sorted output
        deduped: dict[str, dict[str, Any]] = {task["task_id"]: task for task in tasks}
        final_tasks = [deduped[key] for key in sorted(deduped)]
        hpal["tasks"] = final_tasks
        return final_tasks

    def _digest(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _now_iso(self) -> str:
        return datetime.utcnow().isoformat() + "Z"
