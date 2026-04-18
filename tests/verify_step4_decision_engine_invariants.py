from __future__ import annotations

import copy

from apps.api.services.decision_engine import activate_decision_layer
from modules.core.services.orchestrator_lite import run_orchestrator


def test_does_not_mutate_input_layers():
    baseline = run_orchestrator("test-household-001")
    before_by_module = copy.deepcopy(baseline["by_module"])
    before_semantic = copy.deepcopy(baseline["semantic_layer"])

    out = activate_decision_layer(baseline)

    assert baseline["by_module"] == before_by_module
    assert baseline["semantic_layer"] == before_semantic
    assert out["by_module"] == before_by_module
    assert out["semantic_layer"] == before_semantic


def test_decision_layer_shape():
    out = activate_decision_layer(run_orchestrator("test-household-001"))

    assert "decision_layer" in out
    assert set(out["decision_layer"].keys()) == {"decisions"}

    for item in out["decision_layer"]["decisions"]:
        assert set(item.keys()) == {
            "proposal_id",
            "decision_type",
            "reason",
            "source_module",
            "confidence",
        }
        assert item["decision_type"] in {"notification", "suggestion", "interrupt"}
        assert isinstance(item["confidence"], float)
