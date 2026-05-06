from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest


pytestmark = [pytest.mark.ci_gate, pytest.mark.migration, pytest.mark.reliability]

ROOT = Path(__file__).resolve().parent
RFC_AUDIT = ROOT / "tools" / "rfc_audit.py"
RFC_ASSERT = ROOT / "tools" / "rfc_assert.py"


def _artifact_hash_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    normalized.pop("artifact_hash", None)
    normalized.pop("artifact_signature", None)
    return normalized


def _compute_artifact_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        _artifact_hash_payload(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _compute_artifact_signature(artifact_hash: str, execution_id: str, repo_state_hash: str) -> str:
    signed_payload = f"{artifact_hash}{execution_id}{repo_state_hash}"
    return hashlib.sha256(signed_payload.encode("utf-8")).hexdigest()


def _recompute_top_level_integrity(payload: dict[str, Any]) -> None:
    payload["artifact_hash"] = _compute_artifact_hash(payload)
    payload["artifact_signature"] = _compute_artifact_signature(
        str(payload.get("artifact_hash") or ""),
        str(payload.get("execution_id") or ""),
        str(payload.get("repo_state_hash") or ""),
    )


def _run_audit(phase: str, output_path: Path, execution_id: str) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(RFC_AUDIT),
        "--phase",
        phase,
        "--execution-id",
        execution_id,
        "--output",
        str(output_path),
    ]
    proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    return json.loads(output_path.read_text(encoding="utf-8"))


def _run_assert(report_path: Path) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, str(RFC_ASSERT), str(report_path)]
    return subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False)


def _load_assert_summary(proc: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    try:
        return json.loads(proc.stdout)
    except Exception:
        return {
            "status": "FAIL",
            "blockers": [proc.stdout + proc.stderr],
        }


def test_artifact_hash_integrity(tmp_path: Path) -> None:
    report = _run_audit(
        "verify_pre",
        tmp_path / "verify_pre.json",
        execution_id=f"artifact-hash-{uuid.uuid4().hex}",
    )

    assert report["artifact_hash"] == _compute_artifact_hash(report)
    assert report["artifact_signature"] == _compute_artifact_signature(
        report["artifact_hash"],
        report["execution_id"],
        report["repo_state_hash"],
    )

    step1 = report["verification_cycle"]["step1"]
    step2 = report["verification_cycle"]["step2"]
    assert step1["artifact_hash"] == _compute_artifact_hash(step1)
    assert step2["artifact_hash"] == _compute_artifact_hash(step2)
    assert step2["artifact_parent_hash"] == step1["artifact_hash"]


def test_artifact_tamper_detection(tmp_path: Path) -> None:
    report = _run_audit(
        "verify_pre",
        tmp_path / "verify_pre.json",
        execution_id=f"artifact-tamper-{uuid.uuid4().hex}",
    )

    tampered = copy.deepcopy(report)
    tampered["verification_cycle"]["step1"]["scan_scope"] = "tampered_scope"
    tampered_path = tmp_path / "verify_pre_tampered.json"
    tampered_path.write_text(json.dumps(tampered, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    proc = _run_assert(tampered_path)
    summary = _load_assert_summary(proc)

    assert proc.returncode != 0
    assert any("artifact_hash mismatch" in blocker for blocker in summary.get("blockers", []))


def test_artifact_lineage_validation(tmp_path: Path) -> None:
    report = _run_audit(
        "verify_post",
        tmp_path / "verify_post.json",
        execution_id=f"artifact-lineage-{uuid.uuid4().hex}",
    )

    step6_inputs = report["step6_explicit_inputs"]
    assert step6_inputs["step2_output"]["artifact_parent_hash"] == step6_inputs["step1_output"]["artifact_hash"]
    assert report["artifact_lineage_valid"] is True
    assert report["step6_blocked"] is False


def test_step6_rejects_modified_artifacts(tmp_path: Path) -> None:
    report = _run_audit(
        "verify_post",
        tmp_path / "verify_post.json",
        execution_id=f"step6-reject-{uuid.uuid4().hex}",
    )

    tampered = copy.deepcopy(report)
    tampered["step6_explicit_inputs"]["step2_output"]["artifact_parent_hash"] = "0" * 64
    _recompute_top_level_integrity(tampered)

    tampered_path = tmp_path / "verify_post_step6_tampered.json"
    tampered_path.write_text(json.dumps(tampered, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    proc = _run_assert(tampered_path)
    summary = _load_assert_summary(proc)

    assert proc.returncode != 0
    assert any("artifact_parent_hash lineage mismatch" in blocker for blocker in summary.get("blockers", []))


def test_execution_binding_enforced(tmp_path: Path) -> None:
    report = _run_audit(
        "verify_post",
        tmp_path / "verify_post.json",
        execution_id=f"execution-bind-{uuid.uuid4().hex}",
    )

    tampered = copy.deepcopy(report)
    tampered["step6_explicit_inputs"]["execution_id"] = "forged-execution-id"
    _recompute_top_level_integrity(tampered)

    tampered_path = tmp_path / "verify_post_execution_binding_tampered.json"
    tampered_path.write_text(json.dumps(tampered, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    proc = _run_assert(tampered_path)
    summary = _load_assert_summary(proc)

    assert proc.returncode != 0
    assert any("step6_explicit_inputs.execution_id mismatch" in blocker for blocker in summary.get("blockers", []))


def test_repo_state_binding_enforced(tmp_path: Path) -> None:
    report = _run_audit(
        "verify_post",
        tmp_path / "verify_post.json",
        execution_id=f"repo-bind-{uuid.uuid4().hex}",
    )

    tampered = copy.deepcopy(report)
    tampered["step6_explicit_inputs"]["repo_state_hash"] = "forged-repo-state"
    _recompute_top_level_integrity(tampered)

    tampered_path = tmp_path / "verify_post_repo_binding_tampered.json"
    tampered_path.write_text(json.dumps(tampered, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    proc = _run_assert(tampered_path)
    summary = _load_assert_summary(proc)

    assert proc.returncode != 0
    assert any("step6_explicit_inputs.repo_state_hash mismatch" in blocker for blocker in summary.get("blockers", []))
