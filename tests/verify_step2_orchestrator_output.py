from __future__ import annotations

from modules.core.services.orchestrator_lite import run_orchestrator_as_dict


def test_orchestrator_output_is_deterministic():
    first = run_orchestrator_as_dict()
    second = run_orchestrator_as_dict()

    assert first == second
