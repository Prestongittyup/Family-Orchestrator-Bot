from __future__ import annotations

from typing import Any, Mapping, Sequence


ValidationPlanArtifact = dict[str, Any]

DEFAULT_VALIDATION_PLAN_VERSION = "validation-plan-v1"

_FEATURE_INTAKE_TEST = "tests/governance/test_feature_" "intake_contract.py"

_DEFAULT_VALIDATION_STEPS: tuple[dict[str, Any], ...] = (
    {
        "order": 1,
        "name": "read-model-boundary-stability",
        "args": [
            "tests/test_read_model_boundary_stability.py",
            "tests/test_read_model_contract_parity.py",
            "-q",
        ],
        "stop_on_failure": True,
        "required": True,
    },
    {
        "order": 2,
        "name": "trajectory-integration",
        "args": ["tests/test_household_trajectory_integration.py", "-q"],
        "stop_on_failure": True,
        "required": True,
    },
    {
        "order": 3,
        "name": "feedback-reconciliation",
        "args": ["tests/test_household_feedback_reconciliation.py", "-q"],
        "stop_on_failure": True,
        "required": True,
    },
    {
        "order": 4,
        "name": "decision-alignment",
        "args": ["tests/test_household_decision_alignment.py", "-q"],
        "stop_on_failure": True,
        "required": True,
    },
    {
        "order": 5,
        "name": "insight-compression-correctness",
        "args": ["tests/test_household_insight_surface.py", "-q"],
        "stop_on_failure": True,
        "required": True,
    },
    {
        "order": 6,
        "name": "architecture-suite",
        "args": ["tests/system/test_architecture_suite.py", "-q"],
        "stop_on_failure": True,
        "required": True,
    },
    {
        "order": 7,
        "name": "regression-suite",
        "args": [
            "tests/test_hard_freeze_regression.py",
            "tests/test_governance_gates.py",
            "tests/test_layer_redundancy_guard.py",
            "tests/test_ui_canonical_wiring_guard.py",
            "tests/test_sprint0_sprint1_minimal_execution.py",
            "tests/test_task_creation_command_runtime.py",
            "tests/replay/test_event_replay_engine.py",
            "-q",
        ],
        "stop_on_failure": True,
        "required": True,
    },
)


def _normalized_step(step: Mapping[str, Any]) -> dict[str, Any]:
    raw_args = step.get("args")
    args: list[str] = []
    if isinstance(raw_args, list):
        args = [str(arg).strip() for arg in raw_args if str(arg).strip()]

    return {
        "order": int(step.get("order") or 0),
        "name": str(step.get("name") or "").strip(),
        "args": args,
        "stop_on_failure": bool(step.get("stop_on_failure", True)),
        "required": bool(step.get("required", True)),
    }


def build_validation_plan_artifact(
    *,
    steps: Sequence[Mapping[str, Any]],
    household_id: str | None,
    projection_version: str | None,
    date: str | None,
) -> ValidationPlanArtifact:
    normalized_steps = [_normalized_step(step) for step in steps]

    ordered_steps = sorted(
        normalized_steps,
        key=lambda row: (
            int(row.get("order") or 0),
            str(row.get("name") or ""),
            "\x1f".join(str(arg) for arg in row.get("args") or []),
        ),
    )

    return {
        "household_id": household_id,
        "projection_version": projection_version,
        "date": date,
        "steps": ordered_steps,
    }


def build_default_validation_plan(
    *,
    household_id: str | None = None,
    projection_version: str | None = None,
    date: str | None = None,
) -> ValidationPlanArtifact:
    return build_validation_plan_artifact(
        steps=_DEFAULT_VALIDATION_STEPS,
        household_id=household_id,
        projection_version=projection_version,
        date=date,
    )


DEFAULT_VALIDATION_PLAN: ValidationPlanArtifact = build_default_validation_plan()
