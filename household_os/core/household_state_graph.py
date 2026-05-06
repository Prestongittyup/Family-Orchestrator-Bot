from __future__ import annotations

import hashlib
import json
import logging
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from app.services.events import CanonicalRouterService, EventLogService
from household_os.connectors import CalendarConnector, GroceryConnector, TaskConnector
from household_os.core.lifecycle_state import (
    LifecycleState,
    assert_lifecycle_state,
    parse_lifecycle_state,
)
from household_os.runtime.event_router import CanonicalEventEnvelope
from household_os.security.trust_boundary_enforcer import enforce_import_boundary, validate_forbidden_call


LIFECYCLE_HYDRATION_KEY = "_lifecycle_hydration"
logger = logging.getLogger(__name__)


enforce_import_boundary("household_os.core.household_state_graph")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class HouseholdStateGraphStore:
    """Canonical event-log-backed household state projection for Household OS."""

    _cache: dict[str, tuple[str, dict[str, Any]]] = {}
    _known_households: set[str] = set()

    def __init__(
        self,
        graph_path: Path | None = None,
        *,
        router_service: CanonicalRouterService | None = None,
        event_log_service: EventLogService | None = None,
        owner_verifier: Callable[[str, str], bool] | None = None,
    ) -> None:
        # Kept for backward constructor compatibility only; file persistence is intentionally removed.
        self.graph_path = graph_path
        self._router = router_service or CanonicalRouterService()
        self._event_log = event_log_service or EventLogService()
        self._owner_verifier = owner_verifier
        self._calendar_connector = CalendarConnector()
        self._task_connector = TaskConnector()
        self._grocery_connector = GroceryConnector()
        self._scope_suffix = self._compute_scope_suffix(graph_path)

    @staticmethod
    def _compute_scope_suffix(graph_path: Path | None) -> str | None:
        if graph_path is None:
            return None
        digest = hashlib.sha1(str(graph_path).encode("utf-8")).hexdigest()
        return digest[:12]

    def _scoped_household_id(self, household_id: str) -> str:
        if not self._scope_suffix:
            return household_id
        return f"{household_id}::scope:{self._scope_suffix}"

    def load_graph(self, household_id: str) -> dict[str, Any]:
        scoped_household_id = self._scoped_household_id(household_id)
        validate_forbidden_call(
            "HouseholdStateGraphStore.load_graph",
            skip_modules={
                "household_os.core.household_state_graph",
                "household_state.household_state_manager",
                "assistant.runtime.assistant_runtime",
                "assistant.daily_loop",
                "archive.apps.api.assistant_runtime_router",
                "apps.api.assistant_runtime_router",
                "archive.apps.assistant_core.assistant_router",
                "anyio",
                "starlette",
                "fastapi",
            },
        )
        rows = self._event_log.get_event_logs(household_id=scoped_household_id, limit=5000)
        latest_event_id = rows[0].event_id if rows else ""
        cached = self._cache.get(scoped_household_id)
        if cached is not None and cached[0] == latest_event_id:
            return deepcopy(cached[1])

        graph = self._empty_graph(household_id)
        graph = self._apply_runtime_projection(graph)

        ordered_rows = sorted(rows, key=lambda row: (row.timestamp, row.event_id))
        for row in ordered_rows:
            if str(row.type) != "household.graph.snapshot":
                continue
            payload = dict(row.payload or {}) if isinstance(row.payload, dict) else {}
            snapshot = payload.get("graph")
            if not isinstance(snapshot, dict):
                continue
            if str(snapshot.get("household_id") or household_id) != household_id:
                continue
            graph = self._coerce_graph(snapshot)

        graph = self._ensure_runtime_sections(graph)
        graph = self._parse_lifecycle_sections(graph)
        graph = self._sanitize_lifecycle_actions(graph)

        self._cache[scoped_household_id] = (latest_event_id, deepcopy(graph))
        self.__class__._known_households.add(household_id)
        return deepcopy(graph)

    def verify_household_owner(self, household_id: str, user_id: str) -> bool:
        """Return True when the user is an active member of the household."""
        if not household_id or not user_id:
            return False

        if self._owner_verifier is None:
            return False
        try:
            return bool(self._owner_verifier(household_id, user_id))
        except Exception:
            logger.warning(
                "verify_household_owner failed for household_id=%s user_id=%s",
                household_id,
                user_id,
                exc_info=True,
            )
            return False

    def refresh_graph(
        self,
        *,
        household_id: str,
        state: Any,
        query: str,
        fitness_goal: str | None,
        constraints: list[str] | None = None,
    ) -> dict[str, Any]:
        graph = self.load_graph(household_id)

        metadata = dict(getattr(state, "metadata", {}) or {})

        calendar_events = sorted(
            self._calendar_connector.read_events(state),
            key=lambda item: (str(item.get("start", "")), str(item.get("title", "")), str(item.get("event_id", ""))),
        )
        tasks = sorted(
            self._task_connector.read_tasks(state),
            key=lambda item: (str(item.get("priority", "")), str(item.get("title", "")), str(item.get("id", ""))),
        )

        fitness_routines = list(graph.get("fitness_routines", []))
        inferred_routine = self._infer_fitness_routine(query, fitness_goal)
        if inferred_routine and inferred_routine not in fitness_routines:
            fitness_routines.append(inferred_routine)

        canonical_graph = {
            "household_id": household_id,
            "reference_time": str(metadata.get("reference_time", "")) or _utc_now_iso(),
            "calendar_events": calendar_events,
            "tasks": tasks,
            "meal_history": list(graph.get("meal_history") or _default_recipe_history()),
            "grocery_inventory": dict(graph.get("grocery_inventory") or _default_inventory()),
            "fitness_routines": sorted(fitness_routines),
            "household_constraints": sorted(set((constraints or []) + list(graph.get("household_constraints", [])))),
            "approval_actions": list(graph.get("approval_actions", [])),
            "responses": dict(graph.get("responses", {})),
            "event_history": list(graph.get("event_history", [])),
            "action_lifecycle": deepcopy(graph.get("action_lifecycle", {})),
            "execution_log": list(graph.get("execution_log", [])),
            "behavior_feedback": deepcopy(graph.get("behavior_feedback", {})),
            "runtime": deepcopy(graph.get("runtime", {})),
            "state_version": int(graph.get("state_version", 0)) + 1,
            "updated_at": _utc_now_iso(),
        }
        canonical_graph = self._ensure_runtime_sections(canonical_graph)
        canonical_graph = self._parse_lifecycle_sections(canonical_graph)
        canonical_graph = self._sanitize_lifecycle_actions(canonical_graph)

        canonical_graph["event_history"].append(
            {
                "event_type": "query_received",
                "query": query,
                "recorded_at": _utc_now_iso(),
            }
        )

        self._persist_graph(canonical_graph, source="household_os.state_projection", reason="refresh")
        return deepcopy(canonical_graph)

    def store_response(self, household_id: str, response: dict[str, Any]) -> dict[str, Any]:
        graph = self.load_graph(household_id)
        request_id = str(response.get("request_id", ""))

        graph.setdefault("responses", {})[request_id] = deepcopy(response)
        graph.setdefault("approval_actions", []).append(
            {
                "request_id": request_id,
                "action_id": str(response.get("recommended_action", {}).get("action_id", "")),
                "approval_status": str(response.get("recommended_action", {}).get("approval_status", "pending")),
            }
        )
        graph.setdefault("event_history", []).append(
            {
                "event_type": "response_emitted",
                "request_id": request_id,
                "recorded_at": _utc_now_iso(),
            }
        )
        graph["updated_at"] = _utc_now_iso()
        self._persist_graph(graph, source="household_os.state_projection", reason="store_response")
        return deepcopy(graph)

    def get_response(self, household_id: str, request_id: str) -> dict[str, Any] | None:
        graph = self.load_graph(household_id)
        payload = graph.get("responses", {}).get(request_id)
        return None if payload is None else deepcopy(payload)

    def find_household_id_for_request(self, request_id: str) -> str | None:
        for household_id in sorted(self._known_households):
            graph = self.load_graph(household_id)
            if request_id in graph.get("responses", {}):
                return household_id
        return None

    def apply_approval(self, household_id: str, request_id: str, action_ids: list[str]) -> dict[str, Any] | None:
        graph = self.load_graph(household_id)
        payload = graph.get("responses", {}).get(request_id)
        if payload is None:
            return None

        requested = set(action_ids)
        recommended = dict(payload.get("recommended_action", {}))
        if recommended.get("action_id") in requested:
            recommended["approval_status"] = "approved"

        approval_payload = dict(payload.get("grouped_approval_payload", {}))
        if requested.intersection(set(approval_payload.get("action_ids", []))):
            approval_payload["approval_status"] = "approved"

        payload["recommended_action"] = recommended
        payload["grouped_approval_payload"] = approval_payload
        payload["reasoning_trace"] = [*list(payload.get("reasoning_trace", []))[:5], "Approval captured by Household OS without automatic execution."][:6]

        graph.setdefault("responses", {})[request_id] = deepcopy(payload)
        for action in graph.get("approval_actions", []):
            if action.get("request_id") == request_id and action.get("action_id") in requested:
                action["approval_status"] = "approved"

        graph.setdefault("event_history", []).append(
            {
                "event_type": "approval_recorded",
                "request_id": request_id,
                "action_ids": sorted(requested),
                "recorded_at": _utc_now_iso(),
            }
        )
        graph["updated_at"] = _utc_now_iso()
        self._persist_graph(graph, source="household_os.state_projection", reason="apply_approval")
        return deepcopy(payload)

    def _empty_graph(self, household_id: str) -> dict[str, Any]:
        return {
            "household_id": household_id,
            "reference_time": _utc_now_iso(),
            "calendar_events": [],
            "tasks": [],
            "meal_history": _default_recipe_history(),
            "grocery_inventory": _default_inventory(),
            "fitness_routines": [],
            "household_constraints": [],
            "approval_actions": [],
            "responses": {},
            "event_history": [],
            "action_lifecycle": {
                "actions": {},
                "transition_log": [],
            },
            "execution_log": [],
            "behavior_feedback": {
                "records": [],
            },
            "runtime": {
                "processed_trigger_ids": [],
                "last_time_tick": {},
                "last_processed_state_version": 0,
                "daily_cycle": {
                    "pending_follow_up_queries": [],
                    "last_morning_run": None,
                    "last_evening_run": None,
                },
            },
            "state_version": 1,
            "updated_at": _utc_now_iso(),
        }

    def save_graph(self, graph: dict[str, Any]) -> dict[str, Any]:
        validate_forbidden_call(
            "HouseholdStateGraphStore.save_graph",
            skip_modules={
                "household_os.core.household_state_graph",
                "household_state.household_state_manager",
                "assistant.runtime.assistant_runtime",
                "assistant.daily_loop",
                "archive.apps.api.assistant_runtime_router",
                "apps.api.assistant_runtime_router",
                "archive.apps.assistant_core.assistant_router",
                "anyio",
                "starlette",
                "fastapi",
            },
        )
        graph = self._ensure_runtime_sections(deepcopy(graph))
        self._assert_lifecycle_sections(graph)
        graph["updated_at"] = _utc_now_iso()
        self._persist_graph(graph, source="household_os.state_projection", reason="save_graph")
        return deepcopy(graph)

    def _infer_fitness_routine(self, query: str, fitness_goal: str | None) -> str | None:
        normalized = query.lower()
        if fitness_goal:
            return fitness_goal
        if any(token in normalized for token in ("work out", "working out", "exercise", "fitness", "training")):
            return "consistency"
        return None

    def _coerce_graph(self, graph: dict[str, Any]) -> dict[str, Any]:
        normalized_graph = deepcopy(graph)
        normalized_graph.setdefault("household_id", str(graph.get("household_id") or ""))
        normalized_graph.setdefault("reference_time", _utc_now_iso())
        normalized_graph.setdefault("updated_at", _utc_now_iso())
        return self._sanitize_lifecycle_actions(self._ensure_runtime_sections(normalized_graph))

    def _sanitize_lifecycle_actions(self, graph: dict[str, Any]) -> dict[str, Any]:
        lifecycle = graph.get("action_lifecycle")
        if not isinstance(lifecycle, dict):
            return graph

        actions = lifecycle.get("actions")
        if not isinstance(actions, dict):
            return graph

        required_fields = {
            "action_id",
            "request_id",
            "title",
            "description",
            "domain",
            "execution_handler",
            "current_state",
            "approval_required",
            "trigger_id",
            "trigger_type",
            "created_at",
            "updated_at",
            "transitions",
        }

        sanitized_actions: dict[str, dict[str, Any]] = {}
        for action_id, payload in actions.items():
            if not isinstance(payload, dict):
                continue
            if any(field not in payload for field in required_fields):
                continue
            sanitized_actions[action_id] = payload

        lifecycle["actions"] = sanitized_actions

        transition_log = lifecycle.get("transition_log")
        if isinstance(transition_log, list):
            lifecycle["transition_log"] = [
                entry
                for entry in transition_log
                if not isinstance(entry, dict)
                or not entry.get("action_id")
                or str(entry.get("action_id")) in sanitized_actions
            ]
        return graph

    def _apply_runtime_projection(self, graph: dict[str, Any]) -> dict[str, Any]:
        from app.services.commands.runtime import get_command_runtime_service

        projection = get_command_runtime_service().get_projection(str(graph.get("household_id") or ""))
        responses = deepcopy(projection.get("responses", {})) if isinstance(projection.get("responses"), dict) else {}
        actions = deepcopy(projection.get("actions", {})) if isinstance(projection.get("actions"), dict) else {}
        events = deepcopy(projection.get("events", [])) if isinstance(projection.get("events"), list) else []

        approval_actions: list[dict[str, Any]] = []
        lifecycle_actions: dict[str, dict[str, Any]] = {}
        transition_log: list[dict[str, Any]] = []

        for action_id, payload in actions.items():
            if not isinstance(payload, dict):
                continue
            status = str(payload.get("approval_status") or "pending")
            request_id = str(payload.get("request_id") or "")
            approval_actions.append(
                {
                    "request_id": request_id,
                    "action_id": str(action_id),
                    "approval_status": status,
                }
            )

        graph["responses"] = responses
        graph["approval_actions"] = approval_actions
        graph["calendar_events"] = [dict(item) for item in events if isinstance(item, dict)]
        graph["event_history"] = [
            {
                "event_type": "projection.event_replayed",
                "event_id": str(event_row.get("event_id") or ""),
                "recorded_at": _utc_now_iso(),
            }
            for event_row in graph["calendar_events"]
        ]
        graph["action_lifecycle"] = {
            "actions": lifecycle_actions,
            "transition_log": transition_log,
        }
        graph["state_version"] = int(max(int(graph.get("state_version", 0)), int(projection.get("state_version", 0))))
        return self._ensure_runtime_sections(graph)

    def _persist_graph(self, graph: dict[str, Any], *, source: str, reason: str) -> None:
        persisted_graph = self._strip_lifecycle_hydration(self._ensure_runtime_sections(deepcopy(graph)))
        self._assert_lifecycle_sections(persisted_graph)
        household_id = str(persisted_graph.get("household_id") or "")
        if not household_id:
            raise ValueError("household_id is required")
        scoped_household_id = self._scoped_household_id(household_id)

        checksum = hashlib.sha256(
            json.dumps(persisted_graph, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()
        envelope = CanonicalEventEnvelope(
            event_id=str(uuid4()),
            event_type="household.graph.snapshot",
            user_id="system",
            household_id=scoped_household_id,
            source=source,
            payload={
                "graph": deepcopy(persisted_graph),
                "reason": reason,
                "checksum": checksum,
                "saved_at": _utc_now_iso(),
            },
            version=1,
            severity="info",
            idempotency_key=f"household.graph.snapshot:{scoped_household_id}:{checksum}",
            actor_type="system_worker",
            timestamp=datetime.now(UTC),
        )
        self._router.route(envelope, persist=True, dispatch=False)
        self.__class__._cache[scoped_household_id] = (envelope.event_id, self._parse_lifecycle_sections(persisted_graph))
        self.__class__._known_households.add(household_id)

    def _ensure_runtime_sections(self, graph: dict[str, Any]) -> dict[str, Any]:
        lifecycle = graph.setdefault("action_lifecycle", {})
        lifecycle.setdefault("actions", {})
        lifecycle.setdefault("transition_log", [])

        runtime = graph.setdefault("runtime", {})
        runtime.setdefault("processed_trigger_ids", [])
        runtime.setdefault("last_time_tick", {})
        runtime.setdefault("last_processed_state_version", 0)

        daily_cycle = runtime.setdefault("daily_cycle", {})
        daily_cycle.setdefault("pending_follow_up_queries", [])
        daily_cycle.setdefault("last_morning_run", None)
        daily_cycle.setdefault("last_evening_run", None)

        graph.setdefault("execution_log", [])
        behavior_feedback = graph.setdefault("behavior_feedback", {})
        behavior_feedback.setdefault("records", [])
        return graph

    def _parse_lifecycle_sections(self, graph: dict[str, Any]) -> dict[str, Any]:
        normalized_graph = deepcopy(graph)

        lifecycle = normalized_graph.get("action_lifecycle", {})
        actions = lifecycle.get("actions", {}) if isinstance(lifecycle, dict) else {}
        action_snapshots: dict[str, dict[str, Any]] = {}
        if isinstance(actions, dict):
            for action_id, payload in actions.items():
                if not isinstance(payload, dict):
                    continue
                action_snapshots[action_id] = {
                    "current_state": payload.get("current_state"),
                }

        transition_log = lifecycle.get("transition_log", []) if isinstance(lifecycle, dict) else []
        transition_snapshots: list[dict[str, Any]] = []
        if isinstance(transition_log, list):
            for item in transition_log:
                if not isinstance(item, dict):
                    continue
                transition_snapshots.append(
                    {
                        "from_state": item.get("from_state"),
                        "to_state": item.get("to_state"),
                    }
                )

        feedback_records = normalized_graph.get("behavior_feedback", {}).get("records", [])
        feedback_snapshots: list[dict[str, Any]] = []
        if isinstance(feedback_records, list):
            for record in feedback_records:
                if not isinstance(record, dict):
                    continue
                feedback_snapshots.append({"status": record.get("status")})

        if action_snapshots or transition_snapshots or feedback_snapshots:
            normalized_graph[LIFECYCLE_HYDRATION_KEY] = {
                "action_lifecycle": {
                    "actions": action_snapshots,
                    "transition_log": transition_snapshots,
                },
                "behavior_feedback": feedback_snapshots,
            }
        return normalized_graph

    def _validated_lifecycle_state(self, value: Any, *, field_name: str) -> LifecycleState:
        parsed_state = parse_lifecycle_state(value)
        assert_lifecycle_state(parsed_state)
        return parsed_state

    def _strip_lifecycle_hydration(self, graph: dict[str, Any]) -> dict[str, Any]:
        stripped_graph = deepcopy(graph)
        stripped_graph.pop(LIFECYCLE_HYDRATION_KEY, None)
        return stripped_graph

    def _assert_lifecycle_sections(self, graph: dict[str, Any]) -> None:
        lifecycle = graph.get("action_lifecycle", {})
        actions = lifecycle.get("actions", {}) if isinstance(lifecycle, dict) else {}
        if isinstance(actions, dict):
            for action_id, payload in actions.items():
                if not isinstance(payload, dict):
                    continue
                state = payload.get("current_state")
                if state is not None:
                    validated_state = self._validated_lifecycle_state(
                        state,
                        field_name=f"Action {action_id} current_state",
                    )
                else:
                    validated_state = None

                transitions = payload.get("transitions", [])
                if isinstance(transitions, list) and transitions:
                    latest = transitions[-1]
                    if isinstance(latest, dict):
                        latest_to_state = latest.get("to_state")
                        if latest_to_state is not None and validated_state is not None:
                            validated_latest = self._validated_lifecycle_state(
                                latest_to_state,
                                field_name=f"Action {action_id} latest transition to_state",
                            )
                        else:
                            validated_latest = None
                        if validated_latest is not None and validated_latest != validated_state:
                            raise ValueError(
                                f"Action {action_id} current_state must match latest transition to_state"
                            )

        transition_log = lifecycle.get("transition_log", []) if isinstance(lifecycle, dict) else []
        if isinstance(transition_log, list):
            for entry in transition_log:
                if not isinstance(entry, dict):
                    continue
                from_state = entry.get("from_state")
                if from_state is not None:
                    self._validated_lifecycle_state(
                        from_state,
                        field_name="transition_log.from_state",
                    )
                to_state = entry.get("to_state")
                if to_state is not None:
                    self._validated_lifecycle_state(
                        to_state,
                        field_name="transition_log.to_state",
                    )

        if isinstance(actions, dict) and isinstance(transition_log, list):
            latest_log_state_by_action: dict[str, LifecycleState] = {}
            for entry in transition_log:
                if not isinstance(entry, dict):
                    continue
                action_id = entry.get("action_id")
                to_state = entry.get("to_state")
                if isinstance(action_id, str) and to_state is not None:
                    latest_log_state_by_action[action_id] = self._validated_lifecycle_state(
                        to_state,
                        field_name=f"transition_log[{action_id}].to_state",
                    )

            for action_id, payload in actions.items():
                if not isinstance(payload, dict):
                    continue
                logged_state = latest_log_state_by_action.get(action_id)
                if logged_state is None:
                    continue
                current_state = payload.get("current_state")
                if current_state is None:
                    continue
                validated_current = self._validated_lifecycle_state(
                    current_state,
                    field_name=f"Action {action_id} current_state",
                )
                if validated_current != logged_state:
                    raise ValueError(
                        f"Action {action_id} current_state diverges from transition_log latest to_state"
                    )

        feedback_records = graph.get("behavior_feedback", {}).get("records", [])
        if isinstance(feedback_records, list):
            for record in feedback_records:
                if not isinstance(record, dict):
                    continue
                status = record.get("status")
                if status is not None:
                    self._validated_lifecycle_state(
                        status,
                        field_name="behavior_feedback.status",
                    )

def _default_inventory() -> dict[str, int]:
    return {
        "salmon": 1,
        "brown rice": 2,
        "broccoli": 1,
        "olive oil": 2,
        "chicken": 2,
        "quinoa": 1,
        "spinach": 2,
        "black beans": 2,
        "tortillas": 1,
        "avocado": 2,
        "eggs": 8,
        "sweet potato": 3,
        "lentils": 1,
        "carrots": 4,
        "onion": 2,
        "diced tomatoes": 2,
        "garlic": 2,
        "turkey": 1,
        "whole wheat pasta": 1,
        "zucchini": 2,
        "tofu": 1,
        "rice noodles": 1,
        "soy sauce": 1,
        "chickpeas": 2,
        "coconut milk": 1,
        "curry paste": 1,
        "shrimp": 1,
        "peas": 1,
        "greek yogurt": 2,
        "oats": 1,
        "berries": 2,
        "chia seeds": 1,
        "chicken sausage": 1,
        "potatoes": 4,
        "cheddar cheese": 1,
    }


def _default_recipe_history() -> list[dict[str, str]]:
    return [
        {"recipe_name": "Salmon Rice Plate", "served_on": "2026-04-12"},
        {"recipe_name": "Egg and Sweet Potato Skillet", "served_on": "2026-04-16"},
        {"recipe_name": "Chicken Quinoa Bowl", "served_on": "2026-04-08"},
    ]
