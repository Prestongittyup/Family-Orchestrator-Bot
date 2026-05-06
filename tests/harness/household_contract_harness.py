from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, Iterable

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.adapters.db.models.event_log import EventLog
from app.adapters.db.session_factory import Base, SessionLocal, engine
from app.api.tasks import reset_home_summary_tracker


_REQUIRED_HOME_KEYS = frozenset({"needs_decision", "actions", "calendar", "summary"})

_VOLATILE_HOME_PATHS = frozenset()


def create_test_client(app: FastAPI) -> TestClient:
    return TestClient(app, raise_server_exceptions=True, follow_redirects=False)


def reset_projection_state(household_id: str) -> None:
    resolved_household_id = str(household_id).strip()
    if not resolved_household_id:
        raise AssertionError("household_id is required")

    Base.metadata.create_all(bind=engine)
    session = SessionLocal()
    try:
        session.query(EventLog).filter(EventLog.household_id == resolved_household_id).delete(synchronize_session=False)
        session.commit()
    finally:
        session.close()

    reset_home_summary_tracker(household_id=resolved_household_id)


def get_home(client: TestClient, household_id: str, *, date: str | None = None, days: int | None = None) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if date is not None:
        params["date"] = date
    if days is not None:
        params["days"] = days

    response = client.get(f"/household/{household_id}/home", params=params)
    assert response.status_code == 200, f"GET /household/{{id}}/home failed: {response.status_code} {response.text}"
    payload = response.json()
    assert isinstance(payload, dict), "Expected home payload to be a JSON object"
    return dict(payload)


def post_decision_complete(client: TestClient, household_id: str, decision_id: str) -> dict[str, Any]:
    response = client.post(
        "/decision/complete",
        json={"household_id": household_id, "decision_id": decision_id},
    )
    assert response.status_code == 200, f"POST /decision/complete failed: {response.status_code} {response.text}"
    payload = response.json()
    assert isinstance(payload, dict), "Expected command response to be a JSON object"
    return dict(payload)


def post_decision_defer(client: TestClient, household_id: str, decision_id: str, defer_to_date: str) -> dict[str, Any]:
    response = client.post(
        "/decision/defer",
        json={
            "household_id": household_id,
            "decision_id": decision_id,
            "defer_to_date": defer_to_date,
        },
    )
    assert response.status_code == 200, f"POST /decision/defer failed: {response.status_code} {response.text}"
    payload = response.json()
    assert isinstance(payload, dict), "Expected command response to be a JSON object"
    return dict(payload)


def post_decision_ignore(client: TestClient, household_id: str, decision_id: str) -> dict[str, Any]:
    response = client.post(
        "/decision/ignore",
        json={"household_id": household_id, "decision_id": decision_id},
    )
    assert response.status_code == 200, f"POST /decision/ignore failed: {response.status_code} {response.text}"
    payload = response.json()
    assert isinstance(payload, dict), "Expected command response to be a JSON object"
    return dict(payload)


def assert_home_structure_valid(payload: Mapping[str, Any]) -> None:
    missing_root = sorted(_REQUIRED_HOME_KEYS.difference(payload.keys()))
    assert not missing_root, f"Home payload missing required keys: {missing_root}"

    summary = payload.get("summary")
    assert isinstance(summary, str), "home.summary must be a string"
    assert summary.strip(), "home.summary must be non-empty"

    needs_decision = payload.get("needs_decision")
    assert isinstance(needs_decision, list), "home.needs_decision must be a list"
    for index, item in enumerate(needs_decision):
        assert isinstance(item, Mapping), f"home.needs_decision[{index}] must be an object"
        assert str(item.get("id") or "").strip(), f"home.needs_decision[{index}].id must be non-empty"
        assert str(item.get("type") or "").strip(), f"home.needs_decision[{index}].type must be non-empty"
        assert str(item.get("question") or "").strip(), f"home.needs_decision[{index}].question must be non-empty"
        options = item.get("options")
        assert isinstance(options, list) and options, f"home.needs_decision[{index}].options must be non-empty list"

    actions = payload.get("actions")
    assert isinstance(actions, list), "home.actions must be a list"
    for index, item in enumerate(actions):
        assert isinstance(item, Mapping), f"home.actions[{index}] must be an object"
        assert str(item.get("id") or "").strip(), f"home.actions[{index}].id must be non-empty"
        assert str(item.get("title") or "").strip(), f"home.actions[{index}].title must be non-empty"
        assert str(item.get("source") or "").strip(), f"home.actions[{index}].source must be non-empty"

    calendar = payload.get("calendar")
    assert isinstance(calendar, list), "home.calendar must be a list"
    for index, item in enumerate(calendar):
        assert isinstance(item, Mapping), f"home.calendar[{index}] must be an object"
        assert str(item.get("id") or "").strip(), f"home.calendar[{index}].id must be non-empty"
        assert str(item.get("title") or "").strip(), f"home.calendar[{index}].title must be non-empty"
        start_at = str(item.get("start") or "").strip()
        end_at = str(item.get("end") or "").strip()
        assert start_at, f"home.calendar[{index}].start must be non-empty"
        assert end_at, f"home.calendar[{index}].end must be non-empty"
        try:
            datetime.fromisoformat(start_at.replace("Z", "+00:00"))
            datetime.fromisoformat(end_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise AssertionError(
                f"home.calendar[{index}] timestamps must be ISO-8601"
            ) from exc


def assert_projection_deterministic(left: Mapping[str, Any], right: Mapping[str, Any]) -> None:
    left_normalized = _normalize_home_payload(left)
    right_normalized = _normalize_home_payload(right)

    diff_paths = sorted(_collect_diff_paths(left_normalized, right_normalized, current_path=""))
    assert left_normalized == right_normalized, f"Home payloads are not deterministic. Diff paths: {diff_paths}"


def assert_only_expected_diff(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    allowed_diff_keys: Iterable[str],
) -> set[str]:
    allowed_paths = {path.strip() for path in allowed_diff_keys if str(path).strip()}
    assert allowed_paths, "allowed_diff_keys must not be empty"

    diff_paths = _collect_diff_paths(before, after, current_path="")
    unexpected_paths = sorted(
        path
        for path in diff_paths
        if not _is_allowed_diff_path(path, allowed_paths)
    )
    assert not unexpected_paths, f"Unexpected home diff paths: {unexpected_paths}"
    return diff_paths


def _normalize_home_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(dict(payload))


def _collect_diff_paths(left: Any, right: Any, *, current_path: str) -> set[str]:
    resolved_path = current_path or "<root>"

    if isinstance(left, Mapping) and isinstance(right, Mapping):
        diff_paths: set[str] = set()
        keys = sorted(set(left.keys()).union(right.keys()))
        for key in keys:
            key_path = f"{current_path}.{key}" if current_path else str(key)
            if key not in left or key not in right:
                diff_paths.add(key_path)
                continue
            diff_paths.update(_collect_diff_paths(left[key], right[key], current_path=key_path))
        return diff_paths

    if _is_sequence(left) and _is_sequence(right):
        diff_paths = set()
        left_list = list(left)
        right_list = list(right)

        if len(left_list) != len(right_list):
            diff_paths.add(f"{resolved_path}.length")

        for index in range(min(len(left_list), len(right_list))):
            index_path = f"{resolved_path}[{index}]"
            diff_paths.update(_collect_diff_paths(left_list[index], right_list[index], current_path=index_path))

        for index in range(min(len(left_list), len(right_list)), len(left_list)):
            diff_paths.add(f"{resolved_path}[{index}]")
        for index in range(min(len(left_list), len(right_list)), len(right_list)):
            diff_paths.add(f"{resolved_path}[{index}]")

        return diff_paths

    if left != right:
        return {resolved_path}
    return set()


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _is_allowed_diff_path(path: str, allowed_paths: set[str]) -> bool:
    if path in _VOLATILE_HOME_PATHS:
        return True

    for allowed in allowed_paths:
        if path == allowed:
            return True
        if path.startswith(f"{allowed}."):
            return True
        if path.startswith(f"{allowed}["):
            return True
    return False


__all__ = [
    "create_test_client",
    "reset_projection_state",
    "get_home",
    "post_decision_complete",
    "post_decision_defer",
    "post_decision_ignore",
    "assert_home_structure_valid",
    "assert_projection_deterministic",
    "assert_only_expected_diff",
]
