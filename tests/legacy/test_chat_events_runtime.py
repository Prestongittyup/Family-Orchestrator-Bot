from __future__ import annotations

import pytest

from archive.apps.api.schemas.event import SystemEvent
from archive.apps.api.product_surface import chat_gateway_service

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



class _StubTask:
    def __init__(self, task_id: str, title: str) -> None:
        self.task_id = task_id
        self.title = title


class _StubTaskBoard:
    def __init__(self, pending: list[_StubTask] | None = None, in_progress: list[_StubTask] | None = None) -> None:
        self.pending = pending or []
        self.in_progress = in_progress or []
        self.completed: list[_StubTask] = []
        self.failed: list[_StubTask] = []


class _StubState:
    def __init__(self, task_board: _StubTaskBoard | None = None) -> None:
        self.explanation_digest = []
        self.task_board = task_board or _StubTaskBoard()


class _StubNotification:
    def __init__(self, *, notification_id: str, title: str, message: str, level: str = "warning") -> None:
        self.notification_id = notification_id
        self.title = title
        self.message = message
        self.level = level


class _StubBootstrapService:
    def __init__(self, state: _StubState | None = None) -> None:
        self._state = state or _StubState()

    def get_state(self, *, family_id: str):
        del family_id
        return self._state


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


class _StubOrchestrator:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def handle_request(self, request):
        self.calls.append(request)
        action_type = getattr(getattr(request, "action_type", None), "value", "")
        if action_type == "READ_SENSITIVE_STATE":
            return {
                "action_lifecycle": {
                    "actions": {
                        "req-1-primary": {
                            "request_id": "req-1",
                        }
                    }
                }
            }
        return {}


class _StubHpalGateway:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def system_override_task_status(self, *, family_id: str, task_id: str, target_status: str, reason_code: str):
        self.calls.append(
            {
                "family_id": family_id,
                "task_id": task_id,
                "target_status": target_status,
                "reason_code": reason_code,
            }
        )
        return {
            "task_id": task_id,
            "status": target_status,
        }


@pytest.mark.system
@pytest.mark.legacy
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


@pytest.mark.system
@pytest.mark.legacy
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


@pytest.mark.system
@pytest.mark.legacy
def test_action_ids_confirm_executes_orchestrator_approval(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _CaptureRouter()
    orchestrator = _StubOrchestrator()

    def _unexpected_schedule(**kwargs):
        del kwargs
        raise AssertionError("schedule_event should not run for action_ids payload")

    monkeypatch.setattr(chat_gateway_service, "router", capture)
    monkeypatch.setattr(chat_gateway_service, "schedule_event", _unexpected_schedule)

    service = chat_gateway_service.ChatGatewayService(
        orchestrator=orchestrator,
        bootstrap_service=_StubBootstrapService(),
        patch_service=_StubPatchService(),
    )

    response = service.execute_action(
        family_id="hh-1",
        session_id="sess-1",
        action_card_id="card:req-1-primary:confirm",
        payload={"action_ids": ["req-1-primary"]},
    )

    action_types = [getattr(getattr(call, "action_type", None), "value", "") for call in orchestrator.calls]

    assert response.assistant_message == "Action approved and executed."
    assert "READ_SENSITIVE_STATE" in action_types
    assert "APPROVE" in action_types
    assert capture.calls == 1
    assert capture.events[-1].type == "chat_message_sent"


@pytest.mark.system
@pytest.mark.legacy
def test_process_message_create_task_executes_direct_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _CaptureRouter()
    orchestrator = _StubOrchestrator()
    created: list[tuple[str, str]] = []

    class _CreatedTask:
        def __init__(self) -> None:
            self.id = "task-123"

    def _fake_create_task(*, household_id: str, title: str):
        created.append((household_id, title))
        return _CreatedTask()

    monkeypatch.setattr(chat_gateway_service, "router", capture)
    monkeypatch.setattr(chat_gateway_service, "create_task", _fake_create_task)

    service = chat_gateway_service.ChatGatewayService(
        orchestrator=orchestrator,
        bootstrap_service=_StubBootstrapService(),
        patch_service=_StubPatchService(),
    )

    response = service.process_message(
        family_id="hh-1",
        message="create task call pediatrician",
        session_id="user-1:main-ui-session",
    )

    assert "Created task: call pediatrician" in response.assistant_message
    assert created == [("hh-1", "call pediatrician")]
    assert orchestrator.calls == []
    assert capture.calls == 1
    assert capture.events[-1].type == "chat_message_sent"


@pytest.mark.system
@pytest.mark.legacy
def test_process_message_schedule_event_executes_direct_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _CaptureRouter()
    orchestrator = _StubOrchestrator()
    scheduled: list[dict[str, str]] = []

    def _fake_schedule_event(**kwargs):
        scheduled.append({key: str(value) for key, value in kwargs.items()})
        return {"event_id": "evt-123"}

    monkeypatch.setattr(chat_gateway_service, "router", capture)
    monkeypatch.setattr(chat_gateway_service, "schedule_event", _fake_schedule_event)

    service = chat_gateway_service.ChatGatewayService(
        orchestrator=orchestrator,
        bootstrap_service=_StubBootstrapService(),
        patch_service=_StubPatchService(),
    )

    response = service.process_message(
        family_id="hh-1",
        message="schedule event dentist appointment",
        session_id="user-1:main-ui-session",
    )

    assert "Scheduled calendar event: dentist appointment" in response.assistant_message
    assert scheduled and scheduled[0]["household_id"] == "hh-1"
    assert scheduled[0]["user_id"] == "user-1"
    assert orchestrator.calls == []
    assert capture.calls == 1


@pytest.mark.system
@pytest.mark.legacy
def test_process_message_sync_inbox_executes_direct_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _CaptureRouter()
    orchestrator = _StubOrchestrator()
    sync_calls: list[dict[str, object]] = []

    def _fake_sync_google_email(**kwargs):
        sync_calls.append(dict(kwargs))
        return {
            "processed_count": 14,
            "ignored_count": 2,
            "failed_count": 1,
        }

    monkeypatch.setattr(chat_gateway_service, "router", capture)
    monkeypatch.setattr(chat_gateway_service, "sync_google_email", _fake_sync_google_email)
    monkeypatch.setattr(chat_gateway_service, "get_oauth_config", lambda: object())
    monkeypatch.setattr(chat_gateway_service, "get_credential_store", lambda: object())
    monkeypatch.setattr(chat_gateway_service, "get_http_client", lambda: object())

    service = chat_gateway_service.ChatGatewayService(
        orchestrator=orchestrator,
        bootstrap_service=_StubBootstrapService(),
        patch_service=_StubPatchService(),
    )

    response = service.process_message(
        family_id="hh-1",
        message="sync inbox 20",
        session_id="user-42:main-ui-session",
    )

    assert "Inbox sync complete: processed 14, ignored 2, failed 1" in response.assistant_message
    assert sync_calls and sync_calls[0]["user_id"] == "user-42"
    assert sync_calls[0]["max_results"] == 20
    assert orchestrator.calls == []
    assert capture.calls == 1


@pytest.mark.system
@pytest.mark.legacy
def test_process_message_complete_task_executes_direct_capability(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _CaptureRouter()
    orchestrator = _StubOrchestrator()
    hpal_gateway = _StubHpalGateway()
    state = _StubState(task_board=_StubTaskBoard(pending=[_StubTask(task_id="task-abc", title="Call pediatrician")]))

    monkeypatch.setattr(chat_gateway_service, "router", capture)

    service = chat_gateway_service.ChatGatewayService(
        orchestrator=orchestrator,
        bootstrap_service=_StubBootstrapService(state=state),
        patch_service=_StubPatchService(),
        hpal_gateway=hpal_gateway,
    )

    response = service.process_message(
        family_id="hh-1",
        message="complete task call pediatrician",
        session_id="user-1:main-ui-session",
    )

    assert "Completed task: Call pediatrician." == response.assistant_message
    assert hpal_gateway.calls == [
        {
            "family_id": "hh-1",
            "task_id": "task-abc",
            "target_status": "completed",
            "reason_code": "assistant_chat_completion",
        }
    ]
    assert orchestrator.calls == []
    assert capture.calls == 1


@pytest.mark.system
@pytest.mark.legacy
def test_process_message_inbox_prompt_returns_contextual_intelligence(monkeypatch: pytest.MonkeyPatch) -> None:
    capture = _CaptureRouter()
    orchestrator = _StubOrchestrator()
    state = _StubState()
    state.notifications = [
        _StubNotification(
            notification_id="notif:email_summary:mail-1",
            title="Email: School reminder",
            message="Field trip form due soon (2 action items, 1 calendar candidate).",
            level="warning",
        ),
        _StubNotification(
            notification_id="notif:email_summary:mail-2",
            title="Email: Dentist billing",
            message="Invoice received and payment follow-up needed (1 action item).",
            level="critical",
        ),
    ]

    monkeypatch.setattr(chat_gateway_service, "router", capture)

    service = chat_gateway_service.ChatGatewayService(
        orchestrator=orchestrator,
        bootstrap_service=_StubBootstrapService(state=state),
        patch_service=_StubPatchService(),
    )

    response = service.process_message(
        family_id="hh-1",
        message="Please prioritize my inbox and draft follow-up replies",
        session_id="user-1:main-ui-session",
    )

    assert "Inbox intelligence:" in response.assistant_message
    assert "2 with explicit action signals." in response.assistant_message
    assert "Top priorities:" in response.assistant_message
    assert "Next step:" in response.assistant_message
    assert orchestrator.calls == []
    assert capture.calls == 1


@pytest.mark.system
@pytest.mark.legacy
def test_extract_signal_count_parses_action_and_calendar_counts() -> None:
    message = "Field trip form due soon (2 action items, 1 calendar candidate)."

    assert chat_gateway_service.ChatGatewayService._extract_signal_count(message, "action item") == 2
    assert chat_gateway_service.ChatGatewayService._extract_signal_count(message, "calendar candidate") == 1