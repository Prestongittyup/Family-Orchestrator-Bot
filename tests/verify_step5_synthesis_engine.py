from __future__ import annotations

import copy

from apps.api.services.decision_engine import activate_decision_layer
from apps.api.services.synthesis_engine import build_daily_brief
from modules.core.services.orchestrator_lite import run_orchestrator


def test_brief_structure_validity():
    orchestrator_output = run_orchestrator("household-test")
    activated = activate_decision_layer(orchestrator_output)

    brief = build_daily_brief(
        household_id="household-test",
        decision_layer=activated["decision_layer"],
        orchestrator_output=orchestrator_output,
    )

    assert set(brief.keys()) == {
        "household_id",
        "date",
        "schedule",
        "personal_agendas",
        "suggestions",
        "financial",
        "meals",
        "interrupts",
        "meta",
    }
    assert isinstance(brief["household_id"], str)
    assert isinstance(brief["date"], str)
    assert isinstance(brief["schedule"], list)
    assert isinstance(brief["personal_agendas"], dict)
    assert isinstance(brief["suggestions"], list)
    assert isinstance(brief["financial"], dict)
    assert isinstance(brief["meals"], dict)
    assert isinstance(brief["interrupts"], list)
    assert set(brief["meta"].keys()) == {"decision_count", "interrupt_count", "suggestion_count"}


def test_domain_mapping_correctness():
    orchestrator_output = run_orchestrator("household-test")
    activated = activate_decision_layer(orchestrator_output)

    brief = build_daily_brief(
        household_id="household-test",
        decision_layer=activated["decision_layer"],
        orchestrator_output=orchestrator_output,
    )

    assert all(item["source_module"] == "task_module" for item in brief["personal_agendas"]["tasks"])
    assert all(item["source_module"] == "calendar_module" for item in brief["schedule"])
    assert all(item["source_module"] == "meal_module" for item in brief["meals"]["items"])


def test_deterministic_output():
    orchestrator_output = run_orchestrator("household-test")
    activated = activate_decision_layer(orchestrator_output)

    a = build_daily_brief("household-test", activated["decision_layer"], orchestrator_output)
    b = build_daily_brief("household-test", activated["decision_layer"], orchestrator_output)

    assert a == b


def test_no_input_mutation():
    orchestrator_output = run_orchestrator("household-test")
    activated = activate_decision_layer(orchestrator_output)

    before_orchestrator = copy.deepcopy(orchestrator_output)
    before_decision = copy.deepcopy(activated["decision_layer"])

    _ = build_daily_brief(
        household_id="household-test",
        decision_layer=activated["decision_layer"],
        orchestrator_output=orchestrator_output,
    )

    assert orchestrator_output == before_orchestrator
    assert activated["decision_layer"] == before_decision
