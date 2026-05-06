from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

import hashlib
from typing import Any, Mapping


ExecutionPlanArtifact = dict[str, Any]


def _action_id(action: Mapping[str, Any], index: int) -> str:
    resolved = str(action.get("priority_id") or "").strip()
    if resolved:
        return resolved
    return f"action-{index:06d}"


def _job_mapping(action_type: str) -> tuple[str, str, int]:
    if action_type == "escalate":
        return ("notify", "simple_retry", 3)
    if action_type == "notify":
        return ("notify", "none", 1)
    if action_type == "remind":
        return ("schedule", "none", 1)
    if action_type == "schedule":
        return ("schedule", "none", 1)
    raise ValueError(f"Unsupported action_type for execution planning: {action_type}")


def _job_id(household_id: str, action_id: str, job_type: str) -> str:
    payload = f"{household_id}{action_id}{job_type}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _idempotency_key(
    action: Mapping[str, Any],
    *,
    household_id: str,
    action_id: str,
    job_type: str,
    date: str,
) -> str:
    constraints = action.get("constraints")
    if isinstance(constraints, Mapping):
        resolved = str(constraints.get("idempotency_key") or "").strip()
        if resolved:
            return resolved

    fallback_payload = f"{household_id}{action_id}{job_type}{date}".encode("utf-8")
    return hashlib.sha256(fallback_payload).hexdigest()[:16]


def _payload(action: Mapping[str, Any], *, action_id: str) -> dict[str, Any]:
    trigger = action.get("trigger")
    timestamp: str | None = None
    if isinstance(trigger, Mapping):
        resolved_timestamp = str(trigger.get("timestamp") or "").strip()
        timestamp = resolved_timestamp or None

    message_raw = str(action.get("reason_code") or "").strip()
    message = message_raw or None

    return {
        "target": action_id,
        "message": message,
        "timestamp": timestamp,
    }


def build_execution_plan_artifact(actions: dict[str, Any]) -> ExecutionPlanArtifact:
    household_id = str(actions.get("household_id") or "").strip()
    resolved_date = str(actions.get("date") or "").strip()
    raw_actions = actions.get("actions")

    indexed_plans: list[tuple[int, str, dict[str, Any]]] = []
    if isinstance(raw_actions, list):
        for index, action in enumerate(raw_actions):
            if not isinstance(action, Mapping):
                raise ValueError("ActionArtifact actions must contain mapping rows")

            source_action_type = str(action.get("action_type") or "").strip()
            job_type, retry_policy, max_attempts = _job_mapping(source_action_type)
            action_id = _action_id(action, index)
            job_id = _job_id(household_id, action_id, job_type)

            indexed_plans.append(
                (
                    index,
                    job_id,
                    {
                        "action_id": action_id,
                        "job_id": job_id,
                        "job_type": job_type,
                        "payload": _payload(action, action_id=action_id),
                        "execution_constraints": {
                            "idempotency_key": _idempotency_key(
                                action,
                                household_id=household_id,
                                action_id=action_id,
                                job_type=job_type,
                                date=resolved_date,
                            ),
                            "retry_policy": retry_policy,
                            "max_attempts": max_attempts,
                        },
                        "source_action_type": source_action_type,
                    },
                )
            )

    execution_plans = [
        row
        for _index, _job_id_value, row in sorted(
            indexed_plans,
            key=lambda item: (item[0], item[1]),
        )
    ]

    return {
        "household_id": household_id,
        "date": resolved_date,
        "execution_plans": execution_plans,
    }