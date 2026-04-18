from __future__ import annotations

from modules.core.models.module_output import ModuleOutput, Proposal, Signal
from modules.core.services.module_runner import run_all_modules_as_dict


REQUIRED_TOP_LEVEL_KEYS = {"module", "proposals", "signals", "confidence", "metadata"}
REQUIRED_PROPOSAL_KEYS = {"id", "type", "title", "description", "priority", "source_module"}
REQUIRED_SIGNAL_KEYS = {"id", "type", "message", "severity", "source_module"}
VALID_SEVERITY = {"low", "medium", "high"}


def _assert_top_level_contract(item: dict) -> None:
    assert set(item.keys()) == REQUIRED_TOP_LEVEL_KEYS
    assert isinstance(item["module"], str)
    assert isinstance(item["proposals"], list)
    assert isinstance(item["signals"], list)
    assert isinstance(item["confidence"], (int, float))
    assert isinstance(item["metadata"], dict)


def _assert_proposals_and_signals_integrity(item: dict) -> None:
    for proposal in item["proposals"]:
        assert set(proposal.keys()) == REQUIRED_PROPOSAL_KEYS
        assert isinstance(proposal["id"], str)
        assert isinstance(proposal["type"], str)
        assert isinstance(proposal["title"], str)
        assert isinstance(proposal["description"], str)
        assert isinstance(proposal["priority"], int)
        assert 1 <= proposal["priority"] <= 5
        assert isinstance(proposal["source_module"], str)

    for signal in item["signals"]:
        assert set(signal.keys()) == REQUIRED_SIGNAL_KEYS
        assert isinstance(signal["id"], str)
        assert isinstance(signal["type"], str)
        assert isinstance(signal["message"], str)
        assert signal["severity"] in VALID_SEVERITY
        assert isinstance(signal["source_module"], str)


def test_1_structure_validation():
    outputs = run_all_modules_as_dict()

    for item in outputs:
        _assert_top_level_contract(item)


def test_2_contract_consistency():
    outputs = run_all_modules_as_dict()

    assert isinstance(outputs, list)
    assert len(outputs) == 3

    baseline_keys = set(outputs[0].keys())
    for item in outputs:
        assert set(item.keys()) == baseline_keys
        _assert_top_level_contract(item)


def test_3_proposal_signal_integrity():
    outputs = run_all_modules_as_dict()

    for item in outputs:
        _assert_proposals_and_signals_integrity(item)


def test_4_determinism():
    first_run = run_all_modules_as_dict()
    second_run = run_all_modules_as_dict()

    assert first_run == second_run


def test_contract_types_defined():
    # Hard-fail guard for undefined types.
    assert isinstance(ModuleOutput, type)
    assert isinstance(Proposal, type)
    assert isinstance(Signal, type)
