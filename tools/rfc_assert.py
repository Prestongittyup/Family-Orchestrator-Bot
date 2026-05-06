from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


BASE_REQUIRED_CHECKS = {
    "gateway_bypass": "gateway bypass detected",
    "event_sourcing": "event sourcing broken",
    "fsm_mutation_authority": "FSM is not sole mutation authority",
    "illegal_runtime_mutation": "illegal runtime mutation detected",
    "post_refactor_canonical_health_gate": "post-refactor canonical verification gate failed",
    "execution_isolation_state": "execution isolation state check failed",
    "ephemeral_verification_state": "ephemeral verification state check failed",
    "artifact_integrity_state": "artifact integrity state check failed",
}

VERIFY_POST_REQUIRED_CHECKS = {
    "post_refactor_reverification_consistency": "post-refactor re-verification consistency failed",
}

INTEGRITY_EXTENSION_FLAGS = (
    "artifact_integrity_enforced",
    "artifact_hashing_enabled",
    "artifact_lineage_tracked",
    "execution_binding_enforced",
    "repo_state_binding_enforced",
    "tamper_detection_active",
)

VERIFY_POST_INTEGRITY_FLAGS = (
    "artifact_hash_valid",
    "artifact_signature_valid",
    "artifact_lineage_valid",
    "execution_binding_valid",
    "repo_binding_valid",
    "tamper_detected",
    "step6_blocked",
)

ARTIFACT_INTEGRITY_REQUIRED_FIELDS = (
    "execution_id",
    "repo_state_hash",
    "artifact_hash",
    "artifact_timestamp",
    "artifact_parent_hash",
    "artifact_signature",
)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"unable to parse report JSON: {exc}") from exc


def _first_violation_detail(check_payload: dict[str, Any]) -> str:
    violations = check_payload.get("violations") if isinstance(check_payload, dict) else None
    if not isinstance(violations, list) or not violations:
        return ""

    first = violations[0]
    if not isinstance(first, dict):
        return str(first)

    module = str(first.get("affected_module") or "unknown")
    violation_type = str(first.get("violation_type") or "unknown")
    detail = str(first.get("detail") or "")
    if detail:
        return f"{violation_type} in {module}: {detail}"
    return f"{violation_type} in {module}"


def _artifact_payload_for_hash(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    normalized.pop("artifact_hash", None)
    normalized.pop("artifact_signature", None)
    return normalized


def _compute_artifact_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        _artifact_payload_for_hash(payload),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _compute_artifact_signature(artifact_hash: str, execution_id: str, repo_state_hash: str) -> str:
    signed_payload = f"{artifact_hash}{execution_id}{repo_state_hash}"
    return hashlib.sha256(signed_payload.encode("utf-8")).hexdigest()


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None

    normalized = value.strip()
    if not normalized:
        return None

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _validate_artifact(
    artifact: Any,
    *,
    label: str,
    expected_execution_id: str,
    expected_repo_state_hash: str,
    expected_parent_hash: str | None,
    allow_null_parent: bool,
) -> list[str]:
    blockers: list[str] = []

    if not isinstance(artifact, dict):
        return [f"{label} missing or invalid artifact payload"]

    missing_fields = [field for field in ARTIFACT_INTEGRITY_REQUIRED_FIELDS if field not in artifact]
    if missing_fields:
        blockers.append(f"{label} missing required integrity metadata: {sorted(missing_fields)}")

    artifact_execution_id = str(artifact.get("execution_id") or "").strip()
    artifact_repo_state_hash = str(artifact.get("repo_state_hash") or "").strip()
    artifact_timestamp = str(artifact.get("artifact_timestamp") or "").strip()
    artifact_parent_hash = artifact.get("artifact_parent_hash")
    provided_hash = str(artifact.get("artifact_hash") or "").strip()
    provided_signature = str(artifact.get("artifact_signature") or "").strip()

    if artifact_execution_id != expected_execution_id:
        blockers.append(f"{label} execution_id mismatch")

    if artifact_repo_state_hash != expected_repo_state_hash:
        blockers.append(f"{label} repo_state_hash mismatch")

    computed_hash = _compute_artifact_hash(artifact)
    if not provided_hash or provided_hash != computed_hash:
        blockers.append(f"{label} artifact_hash mismatch")

    expected_signature = _compute_artifact_signature(
        provided_hash or computed_hash,
        artifact_execution_id,
        artifact_repo_state_hash,
    )
    if not provided_signature or provided_signature != expected_signature:
        blockers.append(f"{label} artifact_signature mismatch")

    if allow_null_parent:
        if artifact_parent_hash not in {None, ""}:
            blockers.append(f"{label} expected null artifact_parent_hash")
    else:
        if str(artifact_parent_hash or "") != str(expected_parent_hash or ""):
            blockers.append(f"{label} artifact_parent_hash lineage mismatch")

    if _parse_iso_timestamp(artifact_timestamp) is None:
        blockers.append(f"{label} artifact_timestamp invalid")

    return blockers


def _evaluate(report: dict[str, Any]) -> tuple[list[str], dict[str, Any]]:
    blockers: list[str] = []
    phase = str(report.get("phase") or "unknown")

    if bool(report.get("rfc_compliance")) is False:
        blockers.append("rfc_compliance == false")

    checks = report.get("checks")
    if not isinstance(checks, dict):
        checks = {}

    if phase in {"verify_pre", "verify_post", "implement"}:
        execution_state = report.get("execution_state") if isinstance(report.get("execution_state"), dict) else {}
        execution_id = str(execution_state.get("execution_id") or "").strip()
        repo_state_hash = str(execution_state.get("repo_state_hash") or "").strip()
        scan_timestamp = str(execution_state.get("scan_timestamp") or "").strip()

        if not execution_id:
            blockers.append("missing execution_state.execution_id")
        if not repo_state_hash:
            blockers.append("missing execution_state.repo_state_hash")
        if not scan_timestamp:
            blockers.append("missing execution_state.scan_timestamp")

        isolation_flags = (
            "execution_isolation_enforced",
            "no_shared_runtime_state",
            "no_cross_phase_memory",
            "clean_execution_required",
        )
        for flag in isolation_flags:
            if flag not in report:
                blockers.append(f"missing {flag}")
                continue
            if report.get(flag) is not True:
                blockers.append(f"{flag} must be true")

        for flag in INTEGRITY_EXTENSION_FLAGS:
            if flag not in report:
                blockers.append(f"missing {flag}")
                continue
            if report.get(flag) is not True:
                blockers.append(f"{flag} must be true")

        verification_cycle = report.get("verification_cycle") if isinstance(report.get("verification_cycle"), dict) else {}
        cycle_step2 = verification_cycle.get("step2") if isinstance(verification_cycle.get("step2"), dict) else {}
        cycle_step2_hash = str(cycle_step2.get("artifact_hash") or "").strip()

        blockers.extend(
            _validate_artifact(
                report,
                label=f"{phase}_report",
                expected_execution_id=execution_id,
                expected_repo_state_hash=repo_state_hash,
                expected_parent_hash=cycle_step2_hash or None,
                allow_null_parent=not bool(cycle_step2_hash),
            )
        )

        if phase == "verify_post":
            explicit_inputs = report.get("step6_explicit_inputs")
            expected_keys = {"step1_output", "step2_output", "execution_id", "repo_state_hash"}
            if not isinstance(explicit_inputs, dict):
                blockers.append("missing verify_post.step6_explicit_inputs")
            else:
                explicit_keys = set(explicit_inputs.keys())
                if explicit_keys != expected_keys:
                    blockers.append(
                        "verify_post.step6_explicit_inputs keys mismatch: "
                        f"expected {sorted(expected_keys)} got {sorted(explicit_keys)}"
                    )

                explicit_execution_id = str(explicit_inputs.get("execution_id") or "").strip()
                explicit_repo_state_hash = str(explicit_inputs.get("repo_state_hash") or "").strip()
                if execution_id and explicit_execution_id != execution_id:
                    blockers.append("verify_post.step6_explicit_inputs.execution_id mismatch")
                if repo_state_hash and explicit_repo_state_hash != repo_state_hash:
                    blockers.append("verify_post.step6_explicit_inputs.repo_state_hash mismatch")

                step1_artifact = explicit_inputs.get("step1_output")
                step2_artifact = explicit_inputs.get("step2_output")

                blockers.extend(
                    _validate_artifact(
                        step1_artifact,
                        label="verify_post.step6_explicit_inputs.step1_output",
                        expected_execution_id=execution_id,
                        expected_repo_state_hash=repo_state_hash,
                        expected_parent_hash=None,
                        allow_null_parent=True,
                    )
                )

                step1_hash = ""
                if isinstance(step1_artifact, dict):
                    step1_hash = str(step1_artifact.get("artifact_hash") or "").strip()

                blockers.extend(
                    _validate_artifact(
                        step2_artifact,
                        label="verify_post.step6_explicit_inputs.step2_output",
                        expected_execution_id=execution_id,
                        expected_repo_state_hash=repo_state_hash,
                        expected_parent_hash=step1_hash or None,
                        allow_null_parent=False,
                    )
                )

            for flag in VERIFY_POST_INTEGRITY_FLAGS:
                if flag not in report:
                    blockers.append(f"missing verify_post.{flag}")

            if bool(report.get("rfc_compliance", False)):
                if report.get("tamper_detected") is not False:
                    blockers.append("verify_post.tamper_detected must be false for compliant report")
                if report.get("step6_blocked") is not False:
                    blockers.append("verify_post.step6_blocked must be false for compliant report")

    required_checks = dict(BASE_REQUIRED_CHECKS)
    if phase == "verify_post":
        required_checks.update(VERIFY_POST_REQUIRED_CHECKS)

    for check_name, failure_label in required_checks.items():
        payload = checks.get(check_name)
        if not isinstance(payload, dict):
            blockers.append(f"missing required check: {check_name}")
            continue
        if bool(payload.get("ok", False)) is False:
            detail = _first_violation_detail(payload)
            if detail:
                blockers.append(f"{failure_label} ({detail})")
            else:
                blockers.append(failure_label)

    status = "PASS" if not blockers else "FAIL"
    summary = {
        "phase": phase,
        "status": status,
        "blockers": blockers,
    }
    return blockers, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hard RFC-001 assertion gate")
    parser.add_argument("report_json", help="Path to phase JSON report")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report_path = Path(args.report_json)

    if not report_path.exists():
        print(
            json.dumps(
                {
                    "phase": "unknown",
                    "status": "FAIL",
                    "blockers": [f"report file not found: {report_path.as_posix()}"],
                },
                indent=2,
            )
        )
        return 1

    try:
        report = _load_json(report_path)
    except RuntimeError as exc:
        print(
            json.dumps(
                {
                    "phase": "unknown",
                    "status": "FAIL",
                    "blockers": [str(exc)],
                },
                indent=2,
            )
        )
        return 1

    blockers, summary = _evaluate(report)
    print(json.dumps(summary, indent=2))
    return 1 if blockers else 0


if __name__ == "__main__":
    raise SystemExit(main())
