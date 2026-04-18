from __future__ import annotations

import pytest

from modules.core.models.module_output import ModuleOutput, Proposal, Signal
from modules.core.services.orchestrator_lite import (
    DuplicateModuleNameError,
    merge_module_outputs,
    run_orchestrator,
)


def test_merge_structure_and_counts():
    merged = run_orchestrator("test-household-001")

    assert set(merged.keys()) == {
        "proposals",
        "signals",
        "by_module",
        "metadata",
        "semantic_layer",
    }
    assert isinstance(merged["proposals"], list)
    assert isinstance(merged["signals"], list)
    assert isinstance(merged["by_module"], dict)
    assert isinstance(merged["metadata"], dict)
    assert merged["metadata"]["module_count"] == 3


def test_preserves_all_records():
    outputs = [
        ModuleOutput(
            module="a",
            proposals=[
                Proposal(
                    id="p1",
                    type="t",
                    title="A",
                    description="A",
                    priority=1,
                    source_module="a",
                )
            ],
            signals=[
                Signal(
                    id="s1",
                    type="st",
                    message="M",
                    severity="low",
                    source_module="a",
                )
            ],
            confidence=1.0,
            metadata={},
        ),
        ModuleOutput(
            module="b",
            proposals=[
                Proposal(
                    id="p2",
                    type="t",
                    title="B",
                    description="B",
                    priority=2,
                    source_module="b",
                )
            ],
            signals=[],
            confidence=1.0,
            metadata={},
        ),
    ]

    merged = merge_module_outputs(outputs)

    assert len(merged["proposals"]) == 2
    assert len(merged["signals"]) == 1
    assert set(merged["by_module"].keys()) == {"a", "b"}


def test_duplicate_module_names_raise_error():
    outputs = [
        ModuleOutput(module="dup", proposals=[], signals=[], confidence=0.5, metadata={}),
        ModuleOutput(module="dup", proposals=[], signals=[], confidence=0.6, metadata={}),
    ]

    with pytest.raises(DuplicateModuleNameError):
        merge_module_outputs(outputs)


def test_does_not_mutate_input():
    outputs = [
        ModuleOutput(module="x", proposals=[], signals=[], confidence=0.3, metadata={}),
        ModuleOutput(module="y", proposals=[], signals=[], confidence=0.4, metadata={}),
    ]

    before = list(outputs)
    merge_module_outputs(outputs)
    after = list(outputs)

    assert before == after
