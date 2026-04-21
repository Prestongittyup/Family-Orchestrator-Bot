from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from apps.api.integration_core.models.household_state import HouseholdState
from apps.assistant_core.meal_planner import default_inventory, default_recipe_history


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class HouseholdStateManager:
    _graph_cache: dict[str, dict[str, Any]] = {}

    def __init__(self, graph_path: Path | None = None) -> None:
        self.graph_path = graph_path or (Path(__file__).resolve().parent.parent / "data" / "household_state_graph.json")

    def refresh_graph(
        self,
        *,
        household_id: str,
        state: HouseholdState,
        query: str,
        fitness_goal: str | None,
    ) -> dict[str, Any]:
        graph = self.load_graph(household_id)
        graph.update(
            {
                "household_id": household_id,
                "reference_time": str(state.metadata.get("reference_time", "")) or _utc_now_iso(),
                "calendar_events": sorted(
                    [deepcopy(event.as_dict()) for event in state.calendar_events],
                    key=lambda item: (str(item.get("start", "")), str(item.get("title", "")), str(item.get("event_id", ""))),
                ),
                "tasks": sorted(
                    [deepcopy(task) for task in state.tasks],
                    key=lambda item: (str(item.get("priority", "")), str(item.get("title", "")), str(item.get("id", ""))),
                ),
                "inventory": dict(graph.get("inventory") or default_inventory()),
                "meal_history": list(graph.get("meal_history") or default_recipe_history()),
                "fitness_goals": self._merge_fitness_goals(list(graph.get("fitness_goals", [])), query, fitness_goal),
                "assistant_actions": list(graph.get("assistant_actions", [])),
                "decision_history": list(graph.get("decision_history", [])),
                "event_history": list(graph.get("event_history", [])),
                "responses": dict(graph.get("responses", {})),
                "updated_at": _utc_now_iso(),
            }
        )
        self._write_graph(graph)
        return deepcopy(graph)

    def load_graph(self, household_id: str) -> dict[str, Any]:
        if household_id in self._graph_cache:
            return deepcopy(self._graph_cache[household_id])

        payload = self._read_store()
        graph = deepcopy(payload.get("households", {}).get(household_id, {}))
        if not graph:
            graph = self._empty_graph(household_id)
            self._write_graph(graph)
        self._graph_cache[household_id] = deepcopy(graph)
        return deepcopy(graph)

    def store_decision(self, household_id: str, query: str, response_dump: dict[str, Any]) -> dict[str, Any]:
        graph = self.load_graph(household_id)
        request_id = str(response_dump.get("request_id", ""))
        graph.setdefault("responses", {})[request_id] = deepcopy(response_dump)
        graph.setdefault("assistant_actions", []).append(
            {
                **deepcopy(response_dump.get("recommended_action", {})),
                "request_id": request_id,
            }
        )
        graph.setdefault("decision_history", []).append(
            {
                "request_id": request_id,
                "intent_summary": response_dump.get("intent_summary", ""),
                "recommended_action": deepcopy(response_dump.get("recommended_action", {})),
                "recorded_at": _utc_now_iso(),
            }
        )
        graph.setdefault("event_history", []).append(
            {
                "type": "assistant_query",
                "request_id": request_id,
                "query": query,
                "recorded_at": _utc_now_iso(),
            }
        )
        graph["updated_at"] = _utc_now_iso()
        self._write_graph(graph)
        return deepcopy(graph)

    def get_response(self, household_id: str, request_id: str) -> dict[str, Any] | None:
        graph = self.load_graph(household_id)
        response = graph.get("responses", {}).get(request_id)
        return None if response is None else deepcopy(response)

    def find_household_id_for_request(self, request_id: str) -> str | None:
        payload = self._read_store()
        for household_id, graph in payload.get("households", {}).items():
            if request_id in graph.get("responses", {}):
                return household_id
        return None

    def apply_approval(self, household_id: str, request_id: str, action_ids: list[str]) -> dict[str, Any] | None:
        graph = self.load_graph(household_id)
        response = graph.get("responses", {}).get(request_id)
        if response is None:
            return None

        requested = set(action_ids)
        for action in graph.get("assistant_actions", []):
            if action.get("request_id") == request_id and action.get("action_id") in requested:
                action["approval_status"] = "approved"

        recommended_action = deepcopy(response.get("recommended_action", {}))
        if recommended_action.get("action_id") in requested:
            recommended_action["approval_status"] = "approved"

        grouped_approvals = []
        for group in response.get("grouped_approvals", []):
            updated = deepcopy(group)
            if requested.intersection(set(updated.get("action_ids", []))):
                updated["approval_status"] = "approved"
            grouped_approvals.append(updated)

        response["recommended_action"] = recommended_action
        response["grouped_approvals"] = grouped_approvals
        response["reasoning_trace"] = [
            *list(response.get("reasoning_trace", []))[:4],
            "Approval recorded without executing any downstream side effects.",
        ][:5]
        graph.setdefault("responses", {})[request_id] = deepcopy(response)
        graph.setdefault("event_history", []).append(
            {
                "type": "assistant_approval",
                "request_id": request_id,
                "action_ids": sorted(requested),
                "recorded_at": _utc_now_iso(),
            }
        )
        graph["updated_at"] = _utc_now_iso()
        self._write_graph(graph)
        return deepcopy(response)

    def _empty_graph(self, household_id: str) -> dict[str, Any]:
        return {
            "household_id": household_id,
            "reference_time": _utc_now_iso(),
            "calendar_events": [],
            "assistant_actions": [],
            "inventory": default_inventory(),
            "tasks": [],
            "fitness_goals": [],
            "meal_history": default_recipe_history(),
            "decision_history": [],
            "event_history": [],
            "responses": {},
            "updated_at": _utc_now_iso(),
        }

    def _merge_fitness_goals(self, current_goals: list[str], query: str, fitness_goal: str | None) -> list[str]:
        goals = list(current_goals)
        inferred = fitness_goal or self._infer_fitness_goal(query)
        if inferred and inferred not in goals:
            goals.append(inferred)
        return sorted(goals)

    def _infer_fitness_goal(self, query: str) -> str | None:
        normalized = query.lower()
        if any(token in normalized for token in ("fat loss", "lose weight", "lean")):
            return "fat loss"
        if any(token in normalized for token in ("strength", "stronger", "muscle")):
            return "strength"
        if any(token in normalized for token in ("work out", "working out", "exercise", "fitness")):
            return "consistency"
        return None

    def _read_store(self) -> dict[str, Any]:
        if not self.graph_path.exists():
            return {"households": {}}
        try:
            return json.loads(self.graph_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"households": {}}

    def _write_graph(self, graph: dict[str, Any]) -> None:
        payload = self._read_store()
        payload.setdefault("households", {})[graph["household_id"]] = deepcopy(graph)
        self.graph_path.parent.mkdir(parents=True, exist_ok=True)
        self.graph_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        self._graph_cache[graph["household_id"]] = deepcopy(graph)