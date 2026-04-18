from __future__ import annotations

import copy

from modules.core.models.module_output import ModuleOutput, Proposal, Signal
from modules.core.services.module_runner import run_all_modules
from modules.core.services.orchestrator_lite import (
    apply_semantic_layer,
    merge_module_outputs,
    run_orchestrator,
)


def run_orchestrator_with_duplicates() -> dict:
    outputs = [
        ModuleOutput(
            module="task_module",
            proposals=[
                Proposal(
                    id="dup-task-1",
                    type="task_action",
                    title="Fix sink",
                    description="Task copy",
                    priority=3,
                    source_module="task_module",
                )
            ],
            signals=[],
            confidence=0.9,
            metadata={"scenario": "duplicates"},
        ),
        ModuleOutput(
            module="calendar_module",
            proposals=[
                Proposal(
                    id="dup-cal-1",
                    type="task_action",
                    title="  fix   sink!! ",
                    description="Calendar copy",
                    priority=2,
                    source_module="calendar_module",
                )
            ],
            signals=[],
            confidence=0.9,
            metadata={"scenario": "duplicates"},
        ),
        ModuleOutput(
            module="meal_module",
            proposals=[
                Proposal(
                    id="meal-unique-1",
                    type="meal_plan",
                    title="Cook rice",
                    description="Unique proposal",
                    priority=2,
                    source_module="meal_module",
                )
            ],
            signals=[],
            confidence=0.9,
            metadata={"scenario": "duplicates"},
        ),
    ]
    return apply_semantic_layer(merge_module_outputs(outputs))


def run_orchestrator_with_conflicts() -> dict:
    outputs = [
        ModuleOutput(
            module="calendar_module",
            proposals=[],
            signals=[
                Signal(
                    id="sig-cal-1",
                    type="schedule_conflict",
                    message="Schedule conflict detected at 4:00 PM.",
                    severity="high",
                    source_module="calendar_module",
                )
            ],
            confidence=0.9,
            metadata={"scenario": "conflicts"},
        ),
        ModuleOutput(
            module="task_module",
            proposals=[],
            signals=[
                Signal(
                    id="sig-task-1",
                    type="task_overdue",
                    message="Task overdue at 4:00 PM.",
                    severity="medium",
                    source_module="task_module",
                )
            ],
            confidence=0.9,
            metadata={"scenario": "conflicts"},
        ),
        ModuleOutput(
            module="meal_module",
            proposals=[],
            signals=[
                Signal(
                    id="sig-meal-1",
                    type="meal_info",
                    message="Meal prep reminder for tonight.",
                    severity="low",
                    source_module="meal_module",
                )
            ],
            confidence=0.9,
            metadata={"scenario": "conflicts"},
        ),
    ]
    return apply_semantic_layer(merge_module_outputs(outputs))


def snapshot_by_module() -> dict:
    outputs = run_all_modules()
    merged = merge_module_outputs(outputs)
    return copy.deepcopy(merged["by_module"])


# TEST 1 — DUPLICATE DETECTION
def test_duplicate_detection():
    outputs = run_orchestrator_with_duplicates()

    clusters = outputs["semantic_layer"]["duplicate_clusters"]

    assert len(clusters) > 0
    assert all("proposals" in c for c in clusters)


# TEST 2 — SIGNAL CORRELATION
def test_signal_correlation_links():
    outputs = run_orchestrator_with_conflicts()

    correlations = outputs["semantic_layer"]["signal_correlations"]

    assert any("correlation_id" in s for s in correlations)


# TEST 3 — PRIORITY NORMALIZATION
def test_priority_normalization():
    outputs = run_orchestrator("test-household-001")

    assert "semantic_layer" in outputs
    for p in outputs["semantic_layer"]["ordering_index"]:
        assert "normalized_priority" in p
        assert isinstance(p["normalized_priority"], float)


# TEST 4 — STABLE ORDERING
def test_ordering_determinism():
    a = run_orchestrator("test-household-001")
    b = run_orchestrator("test-household-001")

    assert [p["id"] for p in a["semantic_layer"]["ordering_index"]] == [
        p["id"] for p in b["semantic_layer"]["ordering_index"]
    ]


# TEST 5 — IMMUTABILITY GUARANTEE
def test_by_module_unchanged():
    before = snapshot_by_module()
    run_orchestrator("test-household-001")
    after = snapshot_by_module()

    assert before == after


# HARD PASS / FAIL CRITERIA (STEP 3 GATE)
def test_step3_hard_gate_criteria():
    # 1) Structural Integrity: by_module unchanged + ModuleOutput unchanged
    before = snapshot_by_module()
    output = run_orchestrator("test-household-001")
    after = snapshot_by_module()
    assert before == after
    assert all(isinstance(v, ModuleOutput) for v in output["by_module"].values())

    # 2) Semantic Layer Exists
    assert "semantic_layer" in output

    # 3) Duplicate Clustering Works (synthetic)
    dup = run_orchestrator_with_duplicates()
    assert len(dup["semantic_layer"]["duplicate_clusters"]) >= 1

    # 4) Signal Correlation Exists (synthetic)
    conf = run_orchestrator_with_conflicts()
    assert len(conf["semantic_layer"]["signal_correlations"]) >= 1

    # 5) Deterministic Ordering
    a = run_orchestrator("test-household-001")
    b = run_orchestrator("test-household-001")
    assert [p["id"] for p in a["semantic_layer"]["ordering_index"]] == [
        p["id"] for p in b["semantic_layer"]["ordering_index"]
    ]
