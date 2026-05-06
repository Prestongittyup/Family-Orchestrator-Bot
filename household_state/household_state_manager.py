from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
import importlib
from pathlib import Path
from typing import Any


LIFECYCLE_HYDRATION_VIEWS_KEY = "_lifecycle_hydration_views"


@dataclass(frozen=True)
class LifecycleHydrationView:
    raw_payload: dict[str, Any]
    lifecycle_snapshot: dict[str, Any]


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class HouseholdStateManager:
    """Compatibility adapter over the canonical event-log-backed household state graph store."""

    def __init__(self, graph_path: Path | None = None, *, store: Any | None = None) -> None:
        self.graph_path = graph_path
        if store is not None:
            self._store = store
            return

        module = importlib.import_module("household_os.core.household_state_graph")
        store_cls = getattr(module, "HouseholdStateGraphStore")
        self._store = store_cls(graph_path=graph_path)

    def refresh_graph(
        self,
        *,
        household_id: str,
        state: Any,
        query: str,
        fitness_goal: str | None,
    ) -> dict[str, Any]:
        return self._store.refresh_graph(
            household_id=household_id,
            state=state,
            query=query,
            fitness_goal=fitness_goal,
        )

    def load_graph(self, household_id: str) -> dict[str, Any]:
        return self._store.load_graph(household_id)

    def _parse_lifecycle_sections(self, graph: dict[str, Any]) -> dict[str, Any]:
        normalized_graph = deepcopy(graph)

        lifecycle = normalized_graph.get("action_lifecycle", {})
        actions = lifecycle.get("actions", {}) if isinstance(lifecycle, dict) else {}
        action_views: dict[str, LifecycleHydrationView] = {}
        if isinstance(actions, dict):
            for action_id, payload in actions.items():
                if not isinstance(payload, dict):
                    continue
                action_views[str(action_id)] = LifecycleHydrationView(
                    raw_payload=deepcopy(payload),
                    lifecycle_snapshot={
                        "current_state": payload.get("current_state"),
                    },
                )

        transition_log = lifecycle.get("transition_log", []) if isinstance(lifecycle, dict) else []
        transition_views: list[LifecycleHydrationView] = []
        if isinstance(transition_log, list):
            for payload in transition_log:
                if not isinstance(payload, dict):
                    continue
                transition_views.append(
                    LifecycleHydrationView(
                        raw_payload=deepcopy(payload),
                        lifecycle_snapshot={
                            "from_state": payload.get("from_state"),
                            "to_state": payload.get("to_state"),
                        },
                    )
                )

        behavior_feedback = normalized_graph.get("behavior_feedback", {})
        feedback_records = behavior_feedback.get("records", []) if isinstance(behavior_feedback, dict) else []
        feedback_views: list[LifecycleHydrationView] = []
        if isinstance(feedback_records, list):
            for payload in feedback_records:
                if not isinstance(payload, dict):
                    continue
                feedback_views.append(
                    LifecycleHydrationView(
                        raw_payload=deepcopy(payload),
                        lifecycle_snapshot={"status": payload.get("status")},
                    )
                )

        if action_views or transition_views or feedback_views:
            normalized_graph[LIFECYCLE_HYDRATION_VIEWS_KEY] = {
                "actions": action_views,
                "transition_log": transition_views,
                "behavior_feedback": feedback_views,
            }

        return normalized_graph

    def store_decision(self, household_id: str, query: str, response_dump: dict[str, Any]) -> dict[str, Any]:
        graph = self._store.load_graph(household_id)
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
        return self._store.save_graph(graph)

    def get_response(self, household_id: str, request_id: str) -> dict[str, Any] | None:
        graph = self._store.load_graph(household_id)
        response = graph.get("responses", {}).get(request_id)
        return None if response is None else deepcopy(response)

    def find_household_id_for_request(self, request_id: str) -> str | None:
        return self._store.find_household_id_for_request(request_id)

    def apply_approval(self, household_id: str, request_id: str, action_ids: list[str]) -> dict[str, Any] | None:
        graph = self._store.load_graph(household_id)
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

        grouped_payload = deepcopy(response.get("grouped_approval_payload", {}))
        if requested.intersection(set(grouped_payload.get("action_ids", []))):
            grouped_payload["approval_status"] = "approved"

        response["recommended_action"] = recommended_action
        response["grouped_approval_payload"] = grouped_payload
        response["reasoning_trace"] = [
            *list(response.get("reasoning_trace", []))[:4],
            "Approval recorded without executing any downstream side effects.",
        ][:6]
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
        self._store.save_graph(graph)
        return deepcopy(response)
