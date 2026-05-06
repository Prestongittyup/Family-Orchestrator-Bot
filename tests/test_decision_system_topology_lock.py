from __future__ import annotations

from pathlib import Path

import pytest

from scripts.decision_card_shadow_detector import REPO_ROOT, collect_decision_shadow_violations


DECISION_CARD_TOKENS = (
    "decision.card.",
    "DecisionCardGenerated",
    "DecisionCardSurfaced",
    "DecisionCardAcknowledged",
    "DecisionCardResolved",
    "DecisionCardApplied",
)


@pytest.mark.ci_gate
def test_decision_topology_lock_requires_single_authority_path() -> None:
    runtime_path = REPO_ROOT / "app" / "services" / "commands" / "runtime.py"
    replay_path = REPO_ROOT / "core" / "replay" / "event_replay_engine.py"
    registry_path = REPO_ROOT / "decision_card_system" / "registry.py"
    shim_path = REPO_ROOT / "core" / "replay" / "decision_card_registry.py"
    contract_py_path = REPO_ROOT / "decision_card_system" / "decision_card_contract.py"
    contract_ts_path = REPO_ROOT / "hpal-frontend" / "src" / "runtime" / "decision_card_contract.ts"
    rules_path = REPO_ROOT / "app" / "services" / "rules_engine" / "command_rules.py"
    assistant_router_path = REPO_ROOT / "apps" / "api" / "assistant_runtime_router.py"
    frontend_contract_path = REPO_ROOT / "hpal-frontend" / "src" / "runtime" / "interactionContract.ts"
    workflow_path = REPO_ROOT / ".github" / "workflows" / "governance-gate.yml"

    runtime_source = runtime_path.read_text(encoding="utf-8")
    replay_source = replay_path.read_text(encoding="utf-8")
    registry_source = registry_path.read_text(encoding="utf-8")
    shim_source = shim_path.read_text(encoding="utf-8")
    contract_py_source = contract_py_path.read_text(encoding="utf-8")
    contract_ts_source = contract_ts_path.read_text(encoding="utf-8")
    rules_source = rules_path.read_text(encoding="utf-8")
    assistant_router_source = assistant_router_path.read_text(encoding="utf-8")
    frontend_contract_source = frontend_contract_path.read_text(encoding="utf-8")
    workflow_source = workflow_path.read_text(encoding="utf-8")

    assert "createDecisionCard(" in registry_source
    assert "def reduce_decision_card_projection(" in registry_source
    assert "from decision_card_system.registry import *" in shim_source
    assert "def createDecisionCard(" not in shim_source
    assert "def reduce_decision_card_projection(" not in shim_source

    assert "from decision_card_system.registry import" in runtime_source
    assert "createDecisionCard" in runtime_source
    assert "reduce_decision_card_projection" in runtime_source
    assert "_require_decision_card_authority" in runtime_source

    assert "from decision_card_system.registry import" in replay_source
    assert "reduce_decision_card_projection" in replay_source
    assert "from decision_card_system.registry import" in rules_source
    assert "DECISION_CARD_CANONICAL_ORIGIN_API" in rules_source

    assert "class DecisionCardRecord" in contract_py_source
    assert "def " not in contract_py_source
    assert "interface DecisionCardRecord" in contract_ts_source
    assert "function " not in contract_ts_source

    for token in DECISION_CARD_TOKENS:
        assert token not in assistant_router_source
        assert token not in frontend_contract_source

    assert "python scripts/decision_card_shadow_detector.py" in workflow_source
    assert "python scripts/decision_card_dependency_graph_guard.py" in workflow_source


@pytest.mark.ci_gate
def test_decision_topology_lock_forbids_parallel_shadow_paths() -> None:
    violations = collect_decision_shadow_violations(REPO_ROOT)
    rendered = "\n".join(violation.render(REPO_ROOT) for violation in violations)
    assert not violations, f"Decision system topology drift detected:\n{rendered}"
