from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import re

import pytest

from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.core.lifecycle_state import LifecycleState, normalize_state
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.state_reducer import StateReductionError, reduce_state
from household_state.household_state_manager import HouseholdStateManager, LifecycleHydrationView
from tests.lifecycle_test_utils import assert_no_lifecycle_mutation


def test_normalize_state_is_single_authority() -> None:
    assert normalize_state(LifecycleState.APPROVED.value) == LifecycleState.APPROVED
    with pytest.raises(ValueError):
        normalize_state("executed")


def test_graph_store_parse_is_read_through_only(tmp_path: Path) -> None:
    store = HouseholdStateGraphStore(graph_path=tmp_path / "graph.json")
    source_graph = {
        "household_id": "immutability-household",
        "action_lifecycle": {
            "actions": {
                "a1": {
                    "action_id": "a1",
                    "current_state": LifecycleState.APPROVED.value,
                    "transitions": [{"from_state": LifecycleState.PROPOSED.value, "to_state": LifecycleState.APPROVED.value}],
                }
            },
            "transition_log": [{"action_id": "a1", "from_state": LifecycleState.PROPOSED.value, "to_state": LifecycleState.APPROVED.value}],
        },
        "behavior_feedback": {"records": [{"status": LifecycleState.APPROVED.value}]},
    }
    original = deepcopy(source_graph)

    parsed = store._parse_lifecycle_sections(source_graph)

    # Input graph remains unchanged (no in-place rewrite).
    assert source_graph == original
    assert_no_lifecycle_mutation(original, parsed)
    assert parsed["action_lifecycle"]["actions"]["a1"]["current_state"] == LifecycleState.APPROVED.value
    assert parsed["action_lifecycle"]["transition_log"][0]["to_state"] == LifecycleState.APPROVED.value
    assert parsed["behavior_feedback"]["records"][0]["status"] == LifecycleState.APPROVED.value
    assert parsed["_lifecycle_hydration"]["action_lifecycle"]["actions"]["a1"]["current_state"] == LifecycleState.APPROVED.value
    assert parsed["_lifecycle_hydration"]["action_lifecycle"]["transition_log"][0]["to_state"] == LifecycleState.APPROVED.value
    assert parsed["_lifecycle_hydration"]["behavior_feedback"][0]["status"] == LifecycleState.APPROVED.value


def test_legacy_manager_parse_is_read_through_only(tmp_path: Path) -> None:
    manager = HouseholdStateManager(graph_path=tmp_path / "legacy_graph.json")
    source_graph = {
        "household_id": "legacy-household",
        "action_lifecycle": {
            "actions": {
                "a1": {
                    "action_id": "a1",
                    "current_state": LifecycleState.REJECTED.value,
                    "transitions": [{"from_state": LifecycleState.PROPOSED.value, "to_state": LifecycleState.REJECTED.value}],
                }
            }
        },
    }
    original = deepcopy(source_graph)

    parsed = manager._parse_lifecycle_sections(source_graph)

    assert source_graph == original
    assert_no_lifecycle_mutation(original, parsed)
    assert parsed["action_lifecycle"]["actions"]["a1"]["current_state"] == LifecycleState.REJECTED.value
    view = parsed["_lifecycle_hydration_views"]["actions"]["a1"]
    assert isinstance(view, LifecycleHydrationView)
    assert view.raw_payload["current_state"] == LifecycleState.REJECTED.value
    assert view.lifecycle_snapshot["current_state"] == LifecycleState.REJECTED.value


def test_persistence_guard_rejects_state_rewrite_without_matching_transition(tmp_path: Path) -> None:
    store = HouseholdStateGraphStore(graph_path=tmp_path / "guard_graph.json")
    graph = store.load_graph("guard-household")

    graph.setdefault("action_lifecycle", {}).setdefault("actions", {})["a1"] = {
        "action_id": "a1",
        "request_id": "req-1",
        "title": "Guarded action",
        "current_state": LifecycleState.REJECTED,
        "transitions": [{"from_state": LifecycleState.PROPOSED, "to_state": LifecycleState.APPROVED}],
    }
    graph["action_lifecycle"]["transition_log"] = [
        {
            "action_id": "a1",
            "from_state": LifecycleState.PROPOSED,
            "to_state": LifecycleState.APPROVED,
        }
    ]

    with pytest.raises(ValueError):
        store.save_graph(graph)


def test_reducer_uses_fsm_transition_rules() -> None:
    invalid_stream = [
        DomainEvent.create(
            aggregate_id="immutability-reducer-1",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            payload={"state": LifecycleState.PROPOSED},
        ),
        DomainEvent.create(
            aggregate_id="immutability-reducer-1",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_COMMITTED"],
            payload={"state": LifecycleState.COMMITTED},
        ),
    ]

    with pytest.raises(StateReductionError):
        reduce_state(invalid_stream)


def test_no_in_place_lifecycle_rewrites_in_persistence_modules() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    targets = [
        repo_root / "household_os" / "core" / "household_state_graph.py",
        repo_root / "household_state" / "household_state_manager.py",
    ]

    forbidden_patterns = [
        r"\bpayload\[\"current_state\"\]\s*=",
        r"\bpayload\[\"status\"\]\s*=",
        r"\btransition_log\[index\]\s*=",
        r"\bfeedback_records\[index\]\s*=",
        r"\benforce_boundary_state\(",
    ]

    for target in targets:
        text = target.read_text(encoding="utf-8")
        for pattern in forbidden_patterns:
            assert re.search(pattern, text) is None, f"Forbidden lifecycle rewrite pattern in {target}: {pattern}"
