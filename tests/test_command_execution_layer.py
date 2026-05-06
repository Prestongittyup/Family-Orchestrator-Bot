from __future__ import annotations

import hashlib
from typing import Any
from uuid import uuid4

from app.api import command as command_api
from app.adapters.db.models.event_log import EventLog
from app.adapters.db.session_factory import SessionLocal
from app.main import app as household_surface_app
from app.services.commands import get_command_runtime_service
from tests.harness.household_contract_harness import create_test_client, get_home, reset_projection_state


def _create_task(*, household_id: str, task_id: str) -> None:
    with create_test_client(household_surface_app) as client:
        response = client.post(
            "/command",
            json={
                "command_type": "task.create",
                "household_id": household_id,
                "payload": {
                    "task_id": task_id,
                    "title": "Command execution task",
                    "priority": "medium",
                    "due_at": "2036-01-03T10:00:00Z",
                },
            },
        )
    assert response.status_code == 200, response.text


def _strict_command_payload(*, household_id: str, target_id: str) -> dict[str, str | dict[str, str]]:
    return {
        "household_id": household_id,
        "command_type": "complete",
        "target_type": "action",
        "target_id": target_id,
        "metadata": {
            "source": "command-center",
            "timestamp": "2036-01-03T11:30:00Z",
        },
    }


def _event_rows(*, household_id: str) -> list[EventLog]:
    session = SessionLocal()
    try:
        rows = (
            session.query(EventLog)
            .filter(EventLog.household_id == household_id)
            .order_by(EventLog.timestamp.asc(), EventLog.event_id.asc())
            .all()
        )
        for row in rows:
            session.expunge(row)
        return rows
    finally:
        session.close()


def _rows_by_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows_by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_id = str(row.get("id") or "").strip()
        if row_id:
            rows_by_id[row_id] = dict(row)
    return rows_by_id


def _category_rows(home_payload: dict[str, Any], category: str) -> list[dict[str, Any]]:
    payload = home_payload.get(category)
    if not isinstance(payload, list):
        return []
    return [dict(row) for row in payload if isinstance(row, dict)]


def _delta_entries_as_set(delta_entries: list[dict[str, Any]]) -> set[tuple[str, str]]:
    return {
        (str(row.get("category") or ""), str(row.get("id") or ""))
        for row in delta_entries
        if str(row.get("category") or "").strip() and str(row.get("id") or "").strip()
    }


def _expected_home_delta(before_home: dict[str, Any], after_home: dict[str, Any]) -> tuple[set[tuple[str, str]], set[tuple[str, str]], set[tuple[str, str]]]:
    added: set[tuple[str, str]] = set()
    removed: set[tuple[str, str]] = set()
    updated: set[tuple[str, str]] = set()

    for category in ("needs_decision", "actions", "calendar"):
        before_rows = _rows_by_id(_category_rows(before_home, category))
        after_rows = _rows_by_id(_category_rows(after_home, category))

        for row_id in set(after_rows).difference(before_rows):
            added.add((category, row_id))

        for row_id in set(before_rows).difference(after_rows):
            removed.add((category, row_id))

        for row_id in set(before_rows).intersection(after_rows):
            if before_rows[row_id] != after_rows[row_id]:
                updated.add((category, row_id))

    return added, removed, updated


def test_strict_command_duplicate_is_replay_safe_and_single_emission() -> None:
    household_id = f"strict-cmd-dup-{uuid4().hex[:8]}"
    task_id = f"task-{uuid4().hex[:8]}"
    reset_projection_state(household_id)
    _create_task(household_id=household_id, task_id=task_id)

    payload = _strict_command_payload(household_id=household_id, target_id=task_id)

    with create_test_client(household_surface_app) as client:
        first = client.post("/command", json=payload)
        second = client.post("/command", json=payload)

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text

    first_body = first.json()
    second_body = second.json()

    command_id = hashlib.sha256(f"{household_id}:{task_id}:complete".encode("utf-8")).hexdigest()

    assert first_body["status"] == "applied"
    assert first_body["command_id"] == command_id
    assert first_body["target_type"] == "action"
    assert first_body["target_id"] == task_id
    assert isinstance(first_body["events_emitted"], list)
    assert first_body["events_emitted"]
    assert first_body["home_delta"]["changed"] is True

    assert second_body["status"] == "duplicate"
    assert second_body["command_id"] == command_id
    assert second_body["events_emitted"] == []
    assert second_body["home_delta"]["changed"] is False
    assert second_body["projection_fingerprint_before"] == first_body["projection_fingerprint_after"]
    assert second_body["projection_fingerprint_after"] == first_body["projection_fingerprint_after"]

    rows = _event_rows(household_id=household_id)
    event_types = [row.type for row in rows]

    assert event_types.count("TaskCompleted") == 1
    assert sum(1 for row in rows if row.idempotency_key == command_id) == 1


def test_strict_command_mutation_is_visible_in_home_projection() -> None:
    household_id = f"strict-cmd-home-{uuid4().hex[:8]}"
    task_id = f"task-{uuid4().hex[:8]}"
    reset_projection_state(household_id)
    _create_task(household_id=household_id, task_id=task_id)

    with create_test_client(household_surface_app) as client:
        before = get_home(client, household_id)
        before_action_ids = {str(item.get("id") or "") for item in before.get("actions") or []}
        assert task_id in before_action_ids

        execute_response = client.post(
            "/command",
            json=_strict_command_payload(household_id=household_id, target_id=task_id),
        )
        assert execute_response.status_code == 200, execute_response.text
        execute_body = execute_response.json()

        after = get_home(client, household_id)

    after_action_ids = {str(item.get("id") or "") for item in after.get("actions") or []}
    assert task_id not in after_action_ids

    added_expected, removed_expected, updated_expected = _expected_home_delta(before, after)
    delta = execute_body["home_delta"]

    assert _delta_entries_as_set(delta["added"]) == added_expected
    assert _delta_entries_as_set(delta["removed"]) == removed_expected
    assert _delta_entries_as_set(delta["updated"]) == updated_expected
    assert delta["changed"] is True
    assert execute_body["projection_fingerprint_before"] != execute_body["projection_fingerprint_after"]


def test_strict_command_replay_yields_identical_home_output() -> None:
    household_id = f"strict-cmd-replay-{uuid4().hex[:8]}"
    task_id = f"task-{uuid4().hex[:8]}"
    reset_projection_state(household_id)
    _create_task(household_id=household_id, task_id=task_id)

    with create_test_client(household_surface_app) as client:
        execute_response = client.post(
            "/command",
            json=_strict_command_payload(household_id=household_id, target_id=task_id),
        )
        assert execute_response.status_code == 200, execute_response.text
        execute_body = execute_response.json()
        assert execute_body["status"] == "applied"

        home_after = get_home(client, household_id)
        runtime = get_command_runtime_service()
        replay_projection = runtime.get_projection(household_id, force_replay=True)
        replay_fingerprint = command_api.compute_projection_fingerprint(replay_projection)
        home_after_replay = get_home(client, household_id)

        duplicate_response = client.post(
            "/command",
            json=_strict_command_payload(household_id=household_id, target_id=task_id),
        )

    assert duplicate_response.status_code == 200, duplicate_response.text
    duplicate_body = duplicate_response.json()

    assert home_after == home_after_replay
    assert execute_body["projection_fingerprint_after"] == replay_fingerprint
    assert duplicate_body["status"] == "duplicate"
    assert duplicate_body["events_emitted"] == []
    assert duplicate_body["projection_fingerprint_before"] == execute_body["projection_fingerprint_after"]
    assert duplicate_body["projection_fingerprint_after"] == execute_body["projection_fingerprint_after"]


def test_strict_command_invalid_target_rejected_without_event_emission() -> None:
    household_id = f"strict-cmd-invalid-{uuid4().hex[:8]}"
    reset_projection_state(household_id)

    with create_test_client(household_surface_app) as client:
        response = client.post(
            "/command",
            json=_strict_command_payload(household_id=household_id, target_id="missing-task-id"),
        )

    assert response.status_code == 422
    assert "target_id not found for target_type=action" in response.text
    assert _event_rows(household_id=household_id) == []
