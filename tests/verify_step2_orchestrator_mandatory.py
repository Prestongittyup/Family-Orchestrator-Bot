from __future__ import annotations

import copy

import pytest

from modules.calendar.services.calendar_module import calendar_module
from modules.core.models.module_output import ModuleOutput
from modules.core.services.orchestrator_lite import (
    DuplicateModuleNameError,
    merge_module_outputs,
    run_orchestrator,
)
from modules.core.services.module_runner import run_all_modules
from modules.meals.services.meal_module import meal_module
from modules.tasks.services.task_module import task_module


def test_1_merge_correctness():
    outputs = run_all_modules()
    merged = merge_module_outputs(outputs)

    expected_proposals = sum(len(output.proposals) for output in outputs)
    expected_signals = sum(len(output.signals) for output in outputs)

    assert len(merged["proposals"]) == expected_proposals
    assert len(merged["signals"]) == expected_signals

    # No missing items, no duplication for current deterministic stubs.
    proposal_ids = [proposal.id for proposal in merged["proposals"]]
    signal_ids = [signal.id for signal in merged["signals"]]
    assert len(proposal_ids) == len(set(proposal_ids))
    assert len(signal_ids) == len(set(signal_ids))


def test_2_module_traceability():
    merged = run_orchestrator("test-household-001")

    expected_keys = {"task_module", "calendar_module", "meal_module"}
    assert set(merged["by_module"].keys()) == expected_keys

    for key, value in merged["by_module"].items():
        assert isinstance(value, ModuleOutput)
        assert value.module == key


def test_3_input_immutability():
    outputs = run_all_modules("test-household-001")
    before = copy.deepcopy(outputs)

    merge_module_outputs(outputs)

    assert outputs == before


def test_4_determinism():
    first = run_orchestrator("test-household-001")
    second = run_orchestrator("test-household-001")

    assert first == second


def test_5_duplicate_module_guard():
    with pytest.raises(DuplicateModuleNameError):
        merge_module_outputs([task_module(), task_module()])


# Explicit guard that all by_module values keep full ModuleOutput shape in dict form.
def test_by_module_structure_matches_module_output_contract():
    merged = run_orchestrator("test-household-001")

    for output in merged["by_module"].values():
        as_dict = output.to_dict()
        assert set(as_dict.keys()) == {
            "module",
            "proposals",
            "signals",
            "confidence",
            "metadata",
        }


# Ensure required modules are callable and deterministic on their own.
def test_individual_modules_are_deterministic():
    assert task_module() == task_module()
    assert calendar_module() == calendar_module()
    assert meal_module() == meal_module()
