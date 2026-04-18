from __future__ import annotations

from fastapi.testclient import TestClient

from app import app, manual_items
from planning_session import clear_planning_sessions


def setup_function() -> None:
    manual_items.clear()
    clear_planning_sessions()


def teardown_function() -> None:
    manual_items.clear()
    clear_planning_sessions()


def test_session_add_then_refresh_returns_expected_payload() -> None:
    client = TestClient(app)

    add_resp = client.post(
        "/session/add",
        json={
            "household_id": "hh-session-1",
            "title": "Call provider",
            "type": "task",
            "time": "14:30",
        },
    )
    assert add_resp.status_code == 200
    assert add_resp.json()["count"] == 1

    refresh_resp = client.get("/session/refresh?household_id=hh-session-1")
    assert refresh_resp.status_code == 200
    payload = refresh_resp.json()

    assert "brief" in payload
    assert "rendered" in payload
    assert "diff" in payload
    assert isinstance(payload["rendered"], str)
    assert payload["rendered"].strip() != ""

    scheduled = payload["brief"].get("scheduled_actions", [])
    unscheduled = payload["brief"].get("unscheduled_actions", [])
    all_titles = [row.get("title") for row in scheduled + unscheduled]
    assert "Call provider" in all_titles


def test_repeated_refresh_without_changes_has_zero_diff() -> None:
    client = TestClient(app)

    client.post(
        "/session/add",
        json={
            "household_id": "hh-session-2",
            "title": "Read notes",
            "type": "task",
        },
    )

    first = client.get("/session/refresh?household_id=hh-session-2").json()
    second = client.get("/session/refresh?household_id=hh-session-2").json()

    assert first["diff"]["summary"]["added_count"] >= 1
    assert second["diff"]["summary"] == {
        "added_count": 0,
        "removed_count": 0,
        "changed_count": 0,
    }


def test_manual_input_changes_appear_in_diff_only_once() -> None:
    client = TestClient(app)

    client.post(
        "/session/add",
        json={
            "household_id": "hh-session-3",
            "title": "Task A",
            "type": "task",
        },
    )
    client.get("/session/refresh?household_id=hh-session-3")

    client.post(
        "/session/add",
        json={
            "household_id": "hh-session-3",
            "title": "Task B",
            "type": "task",
            "time": "evening",
        },
    )

    changed_once = client.get("/session/refresh?household_id=hh-session-3").json()
    changed_twice = client.get("/session/refresh?household_id=hh-session-3").json()

    assert changed_once["diff"]["summary"]["added_count"] >= 1
    assert changed_twice["diff"]["summary"] == {
        "added_count": 0,
        "removed_count": 0,
        "changed_count": 0,
    }