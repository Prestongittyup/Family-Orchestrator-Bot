from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from apps.api import main
from apps.api.endpoints import ui_bootstrap_router
from apps.api.product_surface.bootstrap_service import UIBootstrapService
from apps.api.product_surface.chat_gateway_service import ChatGatewayService
from apps.api.product_surface.patch_service import UIPatchService


@dataclass(frozen=True)
class _Value:
    value: str


@dataclass(frozen=True)
class _FakeExplanation:
    explanation_id: str
    entity_type: _Value
    entity_id: str
    explanation_text: str
    timestamp: datetime


class _FakeGateway:
    def get_family_state(self, *, family_id: str):
        class _Family:
            def __init__(self, fid: str) -> None:
                self.family_id = fid
                self.default_time_zone = "UTC"
                self.members = [
                    type("M", (), {"name": "Alex"})(),
                    type("M", (), {"name": "Morgan"})(),
                ]
                self.system_state_summary = {
                    "state_version": 7,
                    "pending_actions": 2,
                    "projection_epoch": 10,
                    "last_projection_at": "2026-04-20T10:00:00Z",
                    "stale_projection": False,
                }

        return _Family(family_id)

    def get_plans_by_family(self, *, family_id: str):
        return [
            {
                "plan_id": "plan-a",
                "title": "Morning logistics",
                "status": "active",
                "revision": 3,
                "linked_tasks": ["task-a", "task-b"],
            }
        ]

    def get_tasks_by_family(self, *, family_id: str):
        return [
            {
                "task_id": "task-b",
                "title": "Prepare lunches",
                "plan_id": "plan-a",
                "assigned_to": "Alex",
                "status": "in_progress",
                "priority": "high",
                "due_time": "2026-04-20T11:30:00Z",
            },
            {
                "task_id": "task-a",
                "title": "School drop-off",
                "plan_id": "plan-a",
                "assigned_to": "Morgan",
                "status": "pending",
                "priority": "high",
                "due_time": "2026-04-20T08:30:00Z",
            },
        ]

    def get_calendar_view(self, *, family_id: str):
        return [
            {
                "event_id": "evt-a",
                "title": "Dentist appointment",
                "time_window": {
                    "start": "2026-04-20T14:00:00Z",
                    "end": "2026-04-20T14:30:00Z",
                },
                "participants": ["Alex"],
            }
        ]


class _FakeXAIStore:
    def get_recent(self, *, family_id: str, limit: int = 20):
        return [
            _FakeExplanation(
                explanation_id="xai-1",
                entity_type=_Value("task"),
                entity_id="task-a",
                explanation_text="Task was prioritized due to time constraints.",
                timestamp=datetime(2026, 4, 20, 10, 0, tzinfo=UTC),
            )
        ]


def _install_services(monkeypatch):
    bootstrap_service = UIBootstrapService(
        hpal_gateway=_FakeGateway(),
        xai_store=_FakeXAIStore(),
    )
    patch_service = UIPatchService()
    chat_service = ChatGatewayService(
        bootstrap_service=bootstrap_service,
        patch_service=patch_service,
    )
    monkeypatch.setattr(ui_bootstrap_router, "_bootstrap_service", bootstrap_service)
    monkeypatch.setattr(ui_bootstrap_router, "_chat_service", chat_service)
    return bootstrap_service, patch_service, chat_service


def test_snapshot_determinism(monkeypatch) -> None:
    _install_services(monkeypatch)
    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)

    r1 = client.get("/v1/ui/bootstrap", params={"family_id": "family-1"})
    r2 = client.get("/v1/ui/bootstrap", params={"family_id": "family-1"})

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json() == r2.json()

    payload = r1.json()
    assert isinstance(payload["snapshot_version"], int)
    assert isinstance(payload["source_watermark"], str)


def test_patch_replay_consistency(monkeypatch) -> None:
    bootstrap_service, patch_service, _chat_service = _install_services(monkeypatch)
    current = bootstrap_service.get_state(family_id="family-1")
    patches = patch_service.generate_patches(previous=None, current=current)

    index_once = patch_service.apply_patches(index={}, patches=patches)
    index_twice = patch_service.apply_patches(index=index_once, patches=patches)

    assert index_once == index_twice


def test_chat_response_structure_validation(monkeypatch) -> None:
    _install_services(monkeypatch)
    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)

    res = client.post(
        "/v1/ui/message",
        json={
            "family_id": "family-1",
            "message": "Please help me coordinate today.",
            "session_id": "session-1",
        },
    )

    assert res.status_code == 200
    payload = res.json()

    assert isinstance(payload["assistant_message"], str)
    assert isinstance(payload["requires_confirmation"], bool)
    assert isinstance(payload["action_cards"], list)
    assert isinstance(payload["ui_patch"], list)
    assert isinstance(payload["explanation_summary"], list)
    assert payload["action_cards"]

    first_card = payload["action_cards"][0]
    assert set(first_card.keys()) == {
        "id",
        "type",
        "title",
        "description",
        "related_entity",
        "required_action_payload",
        "risk_level",
    }

    first_patch = payload["ui_patch"][0]
    assert set(first_patch.keys()) == {
        "entity_type",
        "entity_id",
        "change_type",
        "payload",
        "version",
        "source_timestamp",
    }


def test_no_internal_leakage_validation(monkeypatch) -> None:
    _install_services(monkeypatch)
    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)

    bootstrap = client.get("/v1/ui/bootstrap", params={"family_id": "family-1"})
    chat = client.post(
        "/v1/ui/message",
        json={
            "family_id": "family-1",
            "message": "I need help tonight.",
            "session_id": "session-2",
        },
    )

    assert bootstrap.status_code == 200
    assert chat.status_code == 200

    combined = (bootstrap.text + "\n" + chat.text).lower()
    forbidden = ["col", "dag", "lease", "policy", "orchestration", "intent"]

    for term in forbidden:
        assert re.search(rf"\\b{re.escape(term)}\\b", combined) is None
