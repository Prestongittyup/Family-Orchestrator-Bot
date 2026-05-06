from __future__ import annotations

import copy
import importlib.util
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest


pytestmark = [pytest.mark.ci_gate, pytest.mark.migration, pytest.mark.reliability]

ROOT = Path(__file__).resolve().parents[1]
RFC_AUDIT = ROOT / "tools" / "rfc_audit.py"
RFC_ASSERT = ROOT / "tools" / "rfc_assert.py"


@dataclass
class AttackContext:
    audit: Any
    phase_execution_state: dict[str, str]
    step1_payload: dict[str, Any]
    step2_payload: dict[str, Any]
    explicit_step6_inputs: dict[str, Any]
    execution_window_start: str
    execution_window_end: str


def _load_audit_module() -> Any:
    spec = importlib.util.spec_from_file_location("rfc_audit_runtime", RFC_AUDIT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _verify_attack_readiness(audit: Any) -> dict[str, Any]:
    gaps: list[str] = []

    artifact_integrity_layer_present = callable(getattr(audit, "_check_artifact_integrity_state", None))
    step6_gate_present = callable(getattr(audit, "_run_verify_like_phase", None))
    assert_gate_present = RFC_ASSERT.exists()
    attack_surface_accessible = artifact_integrity_layer_present

    if not artifact_integrity_layer_present:
        gaps.append("_check_artifact_integrity_state_not_found")
    if not step6_gate_present:
        gaps.append("_run_verify_like_phase_not_found")
    if not assert_gate_present:
        gaps.append("rfc_assert_gate_not_found")
    if not attack_surface_accessible:
        gaps.append("step6_attack_surface_not_accessible")

    return {
        "artifact_integrity_layer_present": artifact_integrity_layer_present,
        "step6_gate_present": step6_gate_present,
        "assert_gate_present": assert_gate_present,
        "attack_surface_accessible": attack_surface_accessible,
        "gaps": gaps,
    }


def _assess_attack_plan() -> dict[str, Any]:
    return {
        "rfc_alignment": True,
        "attack_vectors_defined": True,
        "expected_behavior": "ALL attacks must be rejected",
        "safe_to_execute_attacks": True,
    }


def _resign_artifact(audit: Any, artifact: dict[str, Any]) -> None:
    artifact["artifact_hash"] = audit._compute_artifact_hash(artifact)
    artifact["artifact_signature"] = audit._compute_artifact_signature(
        str(artifact.get("artifact_hash") or ""),
        str(artifact.get("execution_id") or ""),
        str(artifact.get("repo_state_hash") or ""),
    )


def _build_attack_context() -> AttackContext:
    audit = _load_audit_module()

    readiness = _verify_attack_readiness(audit)
    assert readiness == {
        "artifact_integrity_layer_present": True,
        "step6_gate_present": True,
        "assert_gate_present": True,
        "attack_surface_accessible": True,
        "gaps": [],
    }

    assessment = _assess_attack_plan()
    assert assessment == {
        "rfc_alignment": True,
        "attack_vectors_defined": True,
        "expected_behavior": "ALL attacks must be rejected",
        "safe_to_execute_attacks": True,
    }

    execution_id = f"integrity-bypass-{uuid.uuid4().hex}"
    step1_payload = audit._run_verify_step1_phase(execution_id)
    step1_hash = str(((step1_payload.get("step1_output") or {}).get("artifact_hash")) or "")
    step2_payload = audit._run_verify_step2_phase(execution_id, step1_hash)

    phase_execution_state = {
        "execution_id": execution_id,
        "repo_state_hash": str((step1_payload.get("execution_state") or {}).get("repo_state_hash") or ""),
    }

    explicit_step6_inputs = {
        "step1_output": copy.deepcopy(step1_payload["step1_output"]),
        "step2_output": copy.deepcopy(step2_payload["step2_output"]),
        "execution_id": execution_id,
        "repo_state_hash": phase_execution_state["repo_state_hash"],
    }

    context = AttackContext(
        audit=audit,
        phase_execution_state=phase_execution_state,
        step1_payload=step1_payload,
        step2_payload=step2_payload,
        explicit_step6_inputs=explicit_step6_inputs,
        execution_window_start="2000-01-01T00:00:00Z",
        execution_window_end="2100-01-01T00:00:00Z",
    )

    baseline = _attempt_step6(context)
    assert baseline["ok"] is True
    assert baseline["summary"]["step6_blocked"] is False
    return context


def _attempt_step6(context: AttackContext) -> dict[str, Any]:
    result = context.audit._check_artifact_integrity_state(
        phase="verify_post",
        phase_execution_state=context.phase_execution_state,
        step1_payload=context.step1_payload,
        step2_payload=context.step2_payload,
        explicit_step6_inputs=context.explicit_step6_inputs,
        execution_window_start=context.execution_window_start,
        execution_window_end=context.execution_window_end,
    )
    assert isinstance(result, dict)
    return result


def _clone_context(context: AttackContext) -> AttackContext:
    return AttackContext(
        audit=context.audit,
        phase_execution_state=copy.deepcopy(context.phase_execution_state),
        step1_payload=copy.deepcopy(context.step1_payload),
        step2_payload=copy.deepcopy(context.step2_payload),
        explicit_step6_inputs=copy.deepcopy(context.explicit_step6_inputs),
        execution_window_start=context.execution_window_start,
        execution_window_end=context.execution_window_end,
    )


def _assert_hard_block(result: dict[str, Any], *, expected_violation_fragment: str | None = None) -> None:
    assert result["ok"] is False
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    assert summary.get("step6_blocked") is True
    assert summary.get("tamper_detected") is True

    violations = result.get("violations") if isinstance(result.get("violations"), list) else []
    assert violations

    if expected_violation_fragment:
        assert any(expected_violation_fragment in str(v.get("detail") or "") for v in violations if isinstance(v, dict)), (
            f"expected violation fragment '{expected_violation_fragment}' not found in {json.dumps(violations, indent=2)}"
        )


def test_step6_rejects_tampered_artifact() -> None:
    context = _build_attack_context()

    context.explicit_step6_inputs["step1_output"]["scan_scope"] = "tampered_scan_scope"

    result = _attempt_step6(context)
    _assert_hard_block(result, expected_violation_fragment="step6.step1_output artifact_hash mismatch")


def test_step6_rejects_signature_forgery() -> None:
    context = _build_attack_context()

    payload_replaced_keep_signature = _clone_context(context)
    payload_replaced_keep_signature.explicit_step6_inputs["step2_output"]["evaluation_fresh"] = False
    result_payload_replace = _attempt_step6(payload_replaced_keep_signature)
    _assert_hard_block(result_payload_replace)

    wrong_signature_context = _clone_context(context)
    wrong_signature_context.explicit_step6_inputs["step2_output"]["evaluation_fresh"] = False
    _resign_artifact(wrong_signature_context.audit, wrong_signature_context.explicit_step6_inputs["step2_output"])
    wrong_signature_context.explicit_step6_inputs["step2_output"]["artifact_signature"] = "0" * 64

    result_wrong_signature = _attempt_step6(wrong_signature_context)
    _assert_hard_block(result_wrong_signature, expected_violation_fragment="step6.step2_output artifact_signature mismatch")


def test_step6_rejects_execution_mismatch() -> None:
    context = _build_attack_context()

    context.explicit_step6_inputs["execution_id"] = "forged-execution-id"

    result = _attempt_step6(context)
    _assert_hard_block(result, expected_violation_fragment="Step 6 explicit execution_id mismatch")


def test_step6_rejects_repo_state_mismatch() -> None:
    context = _build_attack_context()

    context.explicit_step6_inputs["repo_state_hash"] = "forged-repo-state-hash"

    result = _attempt_step6(context)
    _assert_hard_block(result, expected_violation_fragment="Step 6 explicit repo_state_hash mismatch")


def test_step6_rejects_broken_lineage() -> None:
    context = _build_attack_context()

    context.explicit_step6_inputs["step2_output"]["artifact_parent_hash"] = "deadbeef" * 8
    _resign_artifact(context.audit, context.explicit_step6_inputs["step2_output"])

    result = _attempt_step6(context)
    _assert_hard_block(result, expected_violation_fragment="artifact_parent_hash lineage mismatch")


def test_step6_rejects_timestamp_replay() -> None:
    context = _build_attack_context()

    context.execution_window_start = "2026-01-01T00:00:00Z"
    context.execution_window_end = "2026-12-31T23:59:59Z"

    context.explicit_step6_inputs["step1_output"]["artifact_timestamp"] = "2000-01-01T00:00:00Z"
    _resign_artifact(context.audit, context.explicit_step6_inputs["step1_output"])

    context.explicit_step6_inputs["step2_output"]["artifact_parent_hash"] = context.explicit_step6_inputs["step1_output"][
        "artifact_hash"
    ]
    context.explicit_step6_inputs["step2_output"]["artifact_timestamp"] = "2000-01-01T00:00:00Z"
    _resign_artifact(context.audit, context.explicit_step6_inputs["step2_output"])

    result = _attempt_step6(context)
    _assert_hard_block(result, expected_violation_fragment="artifact_timestamp outside execution window")


def test_step6_rejects_missing_metadata() -> None:
    context = _build_attack_context()

    context.explicit_step6_inputs["step2_output"].pop("artifact_signature", None)

    result = _attempt_step6(context)
    _assert_hard_block(result, expected_violation_fragment="missing required integrity metadata")