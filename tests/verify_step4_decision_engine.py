from __future__ import annotations

from dataclasses import dataclass, field

from modules.core.services.orchestrator_lite import run_orchestrator
from apps.api.services.decision_engine import activate_decision_layer
from safety.execution_gate import ExecutionStatus, RiskLevel


def test_classification_correctness():
    orchestrator_output = run_orchestrator("test-household-001")
    activated = activate_decision_layer(orchestrator_output)

    decisions = activated["decision_layer"]["decisions"]
    assert len(decisions) == len(orchestrator_output["semantic_layer"]["ordering_index"])

    by_id = {decision["proposal_id"]: decision for decision in decisions}

    # task-proposal-1 has normalized_priority 4.0 -> suggestion
    assert by_id["task-proposal-1"]["decision_type"] == "suggestion"

    # calendar-proposal-1 is linked with high severity schedule_conflict -> interrupt
    assert by_id["calendar-proposal-1"]["decision_type"] == "interrupt"

    # meal-proposal-1 has low normalized priority and no correlated signals -> notification
    assert by_id["meal-proposal-1"]["decision_type"] == "notification"


@dataclass
class _FakeGateDecision:
    status: ExecutionStatus
    risk_level: RiskLevel = RiskLevel.HIGH
    reasons: list[str] = field(default_factory=lambda: ["forced_reject"])


class _RejectingGate:
    def evaluate(self, **_: object) -> _FakeGateDecision:
        return _FakeGateDecision(status=ExecutionStatus.REJECT)


def test_safety_gate_override_behavior():
    orchestrator_output = run_orchestrator("test-household-001")
    activated = activate_decision_layer(orchestrator_output, gate=_RejectingGate())

    decisions = activated["decision_layer"]["decisions"]
    assert decisions

    for decision in decisions:
        assert decision["decision_type"] == "notification"
        assert decision["reason"] == "safety_gate_downgrade"


def test_deterministic_output_ordering():
    orchestrator_output = run_orchestrator("test-household-001")

    first = activate_decision_layer(orchestrator_output)
    second = activate_decision_layer(orchestrator_output)

    first_ids = [decision["proposal_id"] for decision in first["decision_layer"]["decisions"]]
    second_ids = [decision["proposal_id"] for decision in second["decision_layer"]["decisions"]]

    assert first_ids == second_ids
    assert first["decision_layer"]["decisions"] == second["decision_layer"]["decisions"]
