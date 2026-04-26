from __future__ import annotations

from pathlib import Path

from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.core.lifecycle_state import LifecycleState


def test_state_round_trip(tmp_path) -> None:
    store = HouseholdStateGraphStore(graph_path=Path(tmp_path) / "state_graph.json")

    graph = store.load_graph("roundtrip-integrity-household")
    graph.setdefault("action_lifecycle", {}).setdefault("actions", {})["roundtrip-action-001"] = {
        "action_id": "roundtrip-action-001",
        "request_id": "req-roundtrip-integrity",
        "title": "Roundtrip integrity",
        "current_state": LifecycleState.COMMITTED,
    }

    saved = store.save_graph(graph)
    store._cache.pop(saved["household_id"], None)
    loaded = store.load_graph(saved["household_id"])

    loaded_state = loaded["action_lifecycle"]["actions"]["roundtrip-action-001"]["current_state"]
    assert loaded_state == LifecycleState.COMMITTED.value
    assert loaded["_lifecycle_hydration"]["action_lifecycle"]["actions"]["roundtrip-action-001"]["current_state"] == LifecycleState.COMMITTED.value
