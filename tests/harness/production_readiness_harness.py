from __future__ import annotations

import copy
import hashlib
import json
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from fastapi.testclient import TestClient

from app.main import app as household_surface_app
from app.services.commands import get_command_runtime_service
from app.services.events.event_log_service import EventLogService
from core.replay.event_replay_engine import project_state
from tests.harness.household_contract_harness import (
    assert_home_structure_valid,
    create_test_client,
    reset_projection_state,
)


SURFACE_NAMES = (
    "home",
    "loop",
    "decision",
)

_ALLOWED_HOME_DIFF_PATHS = frozenset(
    {
        "summary",
        "calendar",
        "actions",
        "needs_decision",
    }
)


@dataclass(frozen=True)
class RequestLogRecord:
    name: str
    method: str
    path: str
    status_code: int
    duration_ms: float
    response_hash: str
    normalized_hash: str
    request_payload: dict[str, Any]
    response_payload: Any


def log_debug(event: str, **details: Any) -> None:
    record = {
        "event": event,
        "details": details,
    }
    print(json.dumps(record, indent=2, sort_keys=True, default=str))


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def projection_version_token(projection: Mapping[str, Any]) -> str:
    last_event_id = str(projection.get("last_event_id") or "")
    state_version = str(projection.get("state_version") or "0")
    checksum = str(projection.get("checksum") or "")
    return f"{last_event_id}:{state_version}:{checksum}"


def call_get_surface(
    client: TestClient,
    *,
    name: str,
    household_id: str,
    scenario_date: str,
) -> RequestLogRecord:
    path, params = _surface_path_and_params(name=name, household_id=household_id, scenario_date=scenario_date)

    started = time.perf_counter()
    response = client.get(path, params=params)
    duration_ms = round((time.perf_counter() - started) * 1000.0, 3)

    payload: Any
    try:
        payload = response.json()
    except Exception:
        payload = {"raw_text": response.text}

    normalized_payload = normalize_surface_payload(name, payload)
    record = RequestLogRecord(
        name=name,
        method="GET",
        path=path,
        status_code=response.status_code,
        duration_ms=duration_ms,
        response_hash=stable_hash(payload),
        normalized_hash=stable_hash(normalized_payload),
        request_payload=dict(params),
        response_payload=payload,
    )
    log_request_record(record)
    return record


def call_post_decision_complete(
    client: TestClient,
    *,
    household_id: str,
    decision_id: str,
) -> RequestLogRecord:
    body = {"household_id": household_id, "decision_id": decision_id}
    started = time.perf_counter()
    response = client.post("/decision/complete", json=body)
    duration_ms = round((time.perf_counter() - started) * 1000.0, 3)

    payload: Any
    try:
        payload = response.json()
    except Exception:
        payload = {"raw_text": response.text}

    record = RequestLogRecord(
        name="decision_complete",
        method="POST",
        path="/decision/complete",
        status_code=response.status_code,
        duration_ms=duration_ms,
        response_hash=stable_hash(payload),
        normalized_hash=stable_hash(payload),
        request_payload=body,
        response_payload=payload,
    )
    log_request_record(record)
    return record


def fetch_surface_bundle(
    client: TestClient,
    *,
    household_id: str,
    scenario_date: str,
) -> tuple[dict[str, RequestLogRecord], str, str]:
    runtime = get_command_runtime_service()
    projection_before = runtime.get_projection(household_id, force_replay=True)
    token_before = projection_version_token(projection_before)

    records = {
        name: call_get_surface(
            client,
            name=name,
            household_id=household_id,
            scenario_date=scenario_date,
        )
        for name in SURFACE_NAMES
    }

    projection_after = runtime.get_projection(household_id, force_replay=True)
    token_after = projection_version_token(projection_after)

    log_debug(
        "bundle_projection_tokens",
        household_id=household_id,
        scenario_date=scenario_date,
        token_before=token_before,
        token_after=token_after,
    )
    return records, token_before, token_after


def validate_phase1_contract(records: Mapping[str, RequestLogRecord], *, household_id: str, scenario_date: str) -> None:
    home_payload = _require_payload(records, "home")
    loop_payload = _require_payload(records, "loop")
    decision_payload = _require_payload(records, "decision")

    assert_home_structure_valid(home_payload)

    _assert_required_root_keys(loop_payload, {"household_id", "date", "attention", "actions", "execution_plans", "completion", "drift"}, "loop")
    _assert_required_root_keys(decision_payload, {"household_id", "date", "decision", "context"}, "decision")

    for name, payload in (
        ("loop", loop_payload),
        ("decision", decision_payload),
    ):
        assert str(payload.get("household_id") or "").strip() == household_id, f"{name}.household_id mismatch"

    assert str(loop_payload.get("date") or "") == scenario_date
    assert str(decision_payload.get("date") or "") == scenario_date


def validate_cross_surface_consistency(
    records: Mapping[str, RequestLogRecord],
    *,
    household_id: str,
    scenario_date: str,
    projection_token_before: str,
    projection_token_after: str,
) -> None:
    assert projection_token_before == projection_token_after, (
        "Projection token changed during surface bundle fetch. "
        f"before={projection_token_before}, after={projection_token_after}"
    )

    validate_phase1_contract(records, household_id=household_id, scenario_date=scenario_date)

    home_payload = _require_payload(records, "home")
    decision_payload = _require_payload(records, "decision")

    decision = decision_payload.get("decision")
    assert isinstance(decision, Mapping)

    summary = str(home_payload.get("summary") or "")
    assert summary, "home.summary must be non-empty"

    projection_keys = {}
    for name, record in records.items():
        payload = record.response_payload
        if isinstance(payload, Mapping) and "projection_version" in payload:
            projection_keys[name] = str(payload.get("projection_version") or "")

    if projection_keys:
        unique_values = sorted(set(projection_keys.values()))
        log_debug("explicit_projection_versions", versions=projection_keys)
        assert len(unique_values) == 1, f"Surface projection_version mismatch: {projection_keys}"
    else:
        log_debug(
            "implicit_projection_version_gate",
            projection_token=projection_token_before,
            note="surface payloads do not expose projection_version; runtime projection token gate enforced",
        )


def assert_surface_deterministic(
    left_record: RequestLogRecord,
    right_record: RequestLogRecord,
    *,
    allowed_diff_paths: Iterable[str] | None = None,
) -> None:
    assert left_record.status_code == 200, f"{left_record.name} left status was {left_record.status_code}"
    assert right_record.status_code == 200, f"{right_record.name} right status was {right_record.status_code}"

    left_payload = normalize_surface_payload(left_record.name, _require_mapping_payload(left_record.response_payload, left_record.name))
    right_payload = normalize_surface_payload(right_record.name, _require_mapping_payload(right_record.response_payload, right_record.name))

    diff_entries = deep_diff_entries(left_payload, right_payload)
    allowed = set(allowed_diff_paths or [])

    unexpected_diffs = [entry for entry in diff_entries if not is_allowed_path(entry["path"], allowed)]

    log_debug(
        "surface_diff_report",
        surface=left_record.name,
        allowed_paths=sorted(allowed),
        unexpected_diff_count=len(unexpected_diffs),
        unexpected_diffs=unexpected_diffs,
    )

    assert not unexpected_diffs, f"Unexpected diffs for {left_record.name}: {json.dumps(unexpected_diffs, indent=2, default=str)}"


def report_hash_divergence(records: Sequence[RequestLogRecord], *, label: str) -> dict[str, list[int]]:
    grouped: dict[str, list[int]] = {}
    for index, record in enumerate(records):
        grouped.setdefault(record.normalized_hash, []).append(index)

    if len(grouped) > 1:
        log_debug(
            "hash_divergence_report",
            label=label,
            divergence={hash_value: indices for hash_value, indices in grouped.items()},
        )
    else:
        log_debug(
            "hash_divergence_report",
            label=label,
            divergence="none",
            shared_hash=next(iter(grouped.keys())),
        )
    return grouped


def structure_signature(payload: Any) -> Any:
    if isinstance(payload, Mapping):
        return {
            str(key): structure_signature(payload[key])
            for key in sorted(payload.keys(), key=lambda item: str(item))
        }
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        if not payload:
            return []
        return [structure_signature(payload[0])]
    return type(payload).__name__


def rebuild_projection_from_event_log(household_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows = EventLogService().get_event_logs(household_id=household_id, limit=5000)
    ordered_rows = sorted(rows, key=lambda row: (row.timestamp, row.event_id))

    replay_events: list[dict[str, Any]] = []
    for row in ordered_rows:
        replay_events.append(
            {
                "event_id": str(row.event_id),
                "event_type": str(row.type),
                "timestamp": row.timestamp,
                "household_id": str(getattr(row, "household_id", "") or household_id),
                "payload": dict(row.payload or {}) if isinstance(row.payload, dict) else {},
                "source": str(getattr(row, "source", "runtime.action_pipeline") or "runtime.action_pipeline"),
            }
        )

    projection = project_state(replay_events)
    log_debug(
        "replay_projection_rebuild",
        household_id=household_id,
        event_count=len(replay_events),
        last_event_id=replay_events[-1]["event_id"] if replay_events else "",
        replay_projection_version=projection_version_token(projection),
    )
    return projection, replay_events


def deep_diff_entries(left: Any, right: Any, *, path: str = "") -> list[dict[str, Any]]:
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        entries: list[dict[str, Any]] = []
        keys = sorted(set(left.keys()).union(right.keys()), key=lambda item: str(item))
        for key in keys:
            key_path = f"{path}.{key}" if path else str(key)
            if key not in left:
                entries.append({"path": key_path, "left": "<missing>", "right": to_plain_data(right[key])})
                continue
            if key not in right:
                entries.append({"path": key_path, "left": to_plain_data(left[key]), "right": "<missing>"})
                continue
            entries.extend(deep_diff_entries(left[key], right[key], path=key_path))
        return entries

    if _is_sequence(left) and _is_sequence(right):
        entries: list[dict[str, Any]] = []
        left_list = list(left)
        right_list = list(right)

        if len(left_list) != len(right_list):
            entries.append({"path": f"{path}.length" if path else "<root>.length", "left": len(left_list), "right": len(right_list)})

        for index in range(min(len(left_list), len(right_list))):
            index_path = f"{path}[{index}]" if path else f"[{index}]"
            entries.extend(deep_diff_entries(left_list[index], right_list[index], path=index_path))

        for index in range(min(len(left_list), len(right_list)), len(left_list)):
            index_path = f"{path}[{index}]" if path else f"[{index}]"
            entries.append({"path": index_path, "left": to_plain_data(left_list[index]), "right": "<missing>"})

        for index in range(min(len(left_list), len(right_list)), len(right_list)):
            index_path = f"{path}[{index}]" if path else f"[{index}]"
            entries.append({"path": index_path, "left": "<missing>", "right": to_plain_data(right_list[index])})

        return entries

    if left != right:
        return [{"path": path or "<root>", "left": to_plain_data(left), "right": to_plain_data(right)}]

    return []


def normalize_surface_payload(surface_name: str, payload: Mapping[str, Any] | Any) -> Any:
    _ = surface_name
    return copy.deepcopy(to_plain_data(payload))


def to_plain_data(payload: Any) -> Any:
    if isinstance(payload, Mapping):
        return {str(key): to_plain_data(value) for key, value in payload.items()}
    if _is_sequence(payload):
        return [to_plain_data(item) for item in payload]
    return payload


def log_request_record(record: RequestLogRecord) -> None:
    log_debug(
        "request_response",
        name=record.name,
        method=record.method,
        path=record.path,
        status_code=record.status_code,
        duration_ms=record.duration_ms,
        response_hash=record.response_hash,
        normalized_hash=record.normalized_hash,
        request_payload=record.request_payload,
        response_payload=record.response_payload,
    )


def is_allowed_path(path: str, allowed_paths: set[str]) -> bool:
    if not allowed_paths:
        return False
    for allowed in allowed_paths:
        if path == allowed:
            return True
        if path.startswith(f"{allowed}."):
            return True
        if path.startswith(f"{allowed}["):
            return True
    return False


def require_surface_success(record: RequestLogRecord) -> None:
    assert record.status_code == 200, (
        f"{record.name} request failed with {record.status_code}. "
        f"payload={json.dumps(record.response_payload, indent=2, default=str)}"
    )


def allowed_home_diff_paths() -> set[str]:
    return set(_ALLOWED_HOME_DIFF_PATHS)


def _assert_required_root_keys(payload: Mapping[str, Any], expected: set[str], label: str) -> None:
    missing = sorted(expected.difference(payload.keys()))
    assert not missing, f"{label} payload missing keys: {missing}"


def _surface_path_and_params(*, name: str, household_id: str, scenario_date: str) -> tuple[str, dict[str, Any]]:
    if name == "home":
        return f"/household/{household_id}/home", {"date": scenario_date}
    if name == "loop":
        return f"/household/{household_id}/loop", {"date": scenario_date}
    if name == "decision":
        return f"/household/{household_id}/decision", {"date": scenario_date}
    raise AssertionError(f"Unknown surface name: {name}")


def _require_payload(records: Mapping[str, RequestLogRecord], name: str) -> Mapping[str, Any]:
    record = records[name]
    require_surface_success(record)
    return _require_mapping_payload(record.response_payload, name)


def _require_mapping_payload(payload: Any, name: str) -> Mapping[str, Any]:
    assert isinstance(payload, Mapping), f"{name} payload must be an object"
    return payload


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


__all__ = [
    "RequestLogRecord",
    "SURFACE_NAMES",
    "allowed_home_diff_paths",
    "assert_surface_deterministic",
    "call_get_surface",
    "call_post_decision_complete",
    "create_test_client",
    "deep_diff_entries",
    "fetch_surface_bundle",
    "household_surface_app",
    "log_debug",
    "normalize_surface_payload",
    "projection_version_token",
    "rebuild_projection_from_event_log",
    "report_hash_divergence",
    "require_surface_success",
    "reset_projection_state",
    "stable_hash",
    "structure_signature",
    "to_plain_data",
    "validate_cross_surface_consistency",
    "validate_phase1_contract",
]
