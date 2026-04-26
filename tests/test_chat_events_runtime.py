from __future__ import annotations

import pytest

from apps.api.schemas.event import SystemEvent
from apps.api.product_surface import chat_gateway_service


class _StubState:
    def __init__(self) -> None:
        self.explanation_digest = []


class _StubBootstrapService:
    def get_state(self, *, family_id: str):
        del family_id
        return _StubState()


class _StubPatchService:
    def generate_patches(self, *, previous, current):
        del previous, current
        return []


class _CaptureRouter:
    def __init__(self) -> None:
        self.calls = 0
        self.events: list[SystemEvent] = []

    def emit(self, event: SystemEvent) -> None:
        self.calls += 1
        self.events.append(event)


def test_success_emits_chat_message_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _CaptureRouter()
    monkeypatch.setattr(chat_gateway_service, "router", capture)
    monkeypatch.setattr(chat_gateway_service, "schedule_event", lambda **kwargs: None)

    service = chat_gateway_service.ChatGatewayService(
        bootstrap_service=_StubBootstrapService(),
        patch_service=_StubPatchService(),
    )

    response = service.execute_action(
        family_id="hh-1",
        session_id="sess-1",
        action_card_id="card-1",
        payload={"user_id": "user-1", "title": "Plan dinner"},
    )

    assert response.assistant_message == "Action executed."
    assert capture.calls == 1
    assert capture.events[-1].type == "chat_message_sent"


def test_failure_emits_chat_message_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(**kwargs):
        del kwargs
        raise RuntimeError("calendar downstream unavailable")

    capture = _CaptureRouter()
    monkeypatch.setattr(chat_gateway_service, "router", capture)
    monkeypatch.setattr(chat_gateway_service, "schedule_event", _boom)

    service = chat_gateway_service.ChatGatewayService(
        bootstrap_service=_StubBootstrapService(),
        patch_service=_StubPatchService(),
    )

    with pytest.raises(RuntimeError):
        service.execute_action(
            family_id="hh-1",
            session_id="sess-1",
            action_card_id="card-2",
            payload={"user_id": "user-1", "title": "Plan dinner"},
        )

    assert capture.calls == 1
    assert capture.events[-1].type == "chat_message_failed"
    assert capture.events[-1].payload.get("reason") == "internal_error"
