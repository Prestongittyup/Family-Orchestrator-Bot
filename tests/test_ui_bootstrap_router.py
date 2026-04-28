from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime

from fastapi.testclient import TestClient

from archive.apps.api import main
from archive.apps.api.endpoints import ui_bootstrap_router
from archive.apps.api.integration_core.credentials import OAuthCredential
from archive.apps.api.integration_core.models.household_state import CalendarEvent, HouseholdState
from archive.apps.api.product_surface import bootstrap_service as bootstrap_module
from archive.apps.api.product_surface.bootstrap_service import UIBootstrapService
from archive.apps.api.product_surface.chat_gateway_service import ChatGatewayService
from archive.apps.api.product_surface.patch_service import UIPatchService


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


def test_bootstrap_includes_calendar_db_events_when_projection_empty(monkeypatch) -> None:
    class _NoProjectedEventsGateway(_FakeGateway):
        def get_calendar_view(self, *, family_id: str):
            return []

    monkeypatch.setattr(
        bootstrap_module,
        "get_events_by_household",
        lambda household_id, include_past=False: [
            {
                "event_id": "evt-db-1",
                "household_id": household_id,
                "title": "Piano lesson",
                "start_time": "2026-04-20T16:00:00Z",
                "end_time": "2026-04-20T16:30:00Z",
                "priority": 3,
                "metadata": {"user_id": "Alex"},
                "created_at": "2026-04-20T10:05:00Z",
            }
        ],
    )

    service = UIBootstrapService(
        hpal_gateway=_NoProjectedEventsGateway(),
        xai_store=_FakeXAIStore(),
    )

    state = service.get_state(family_id="family-1")

    assert [event.event_id for event in state.calendar.events] == ["evt-db-1"]
    assert state.calendar.events[0].participants == ["Alex"]


def test_bootstrap_includes_runtime_graph_events_and_tasks(monkeypatch) -> None:
    class _RuntimeAdapter:
        def load_graph(self, family_id: str):
            return {
                "household_id": family_id,
                "calendar_events": [
                    {
                        "event_id": "runtime-evt-1",
                        "title": "Workout session",
                        "start": "2026-04-20T18:00:00Z",
                        "end": "2026-04-20T18:45:00Z",
                    }
                ],
                "tasks": [
                    {
                        "id": "runtime-task-1",
                        "title": "Pack gym bag",
                        "status": "pending",
                    }
                ],
            }

    class _RuntimeOnlyGateway(_FakeGateway):
        def __init__(self) -> None:
            self.adapter = _RuntimeAdapter()

        def get_tasks_by_family(self, *, family_id: str):
            return []

        def get_calendar_view(self, *, family_id: str):
            return []

    monkeypatch.setattr(bootstrap_module, "get_events_by_household", lambda household_id, include_past=False: [])

    service = UIBootstrapService(
        hpal_gateway=_RuntimeOnlyGateway(),
        xai_store=_FakeXAIStore(),
    )

    state = service.get_state(family_id="family-1")

    event_ids = [event.event_id for event in state.calendar.events]
    pending_task_ids = [task.task_id for task in state.task_board.pending]

    assert "runtime-evt-1" in event_ids
    assert "runtime-task-1" in pending_task_ids


def test_bootstrap_includes_pantry_and_weekly_recipe_suggestions(monkeypatch) -> None:
    class _RuntimeAdapter:
        def load_graph(self, family_id: str):
            return {
                "household_id": family_id,
                "inventory": {
                    "salmon": 0,
                    "spinach": 1,
                    "eggs": 6,
                },
                "meal_history": [
                    {"recipe_name": "Salmon Rice Plate", "served_on": "2026-04-18"},
                ],
            }

    class _RuntimePantryGateway(_FakeGateway):
        def __init__(self) -> None:
            self.adapter = _RuntimeAdapter()

    service = UIBootstrapService(
        hpal_gateway=_RuntimePantryGateway(),
        xai_store=_FakeXAIStore(),
    )

    state = service.get_state(family_id="family-1")

    assert state.pantry.low_stock_count >= 1
    assert any(item.name == "salmon" and item.status == "out_of_stock" for item in state.pantry.inventory_items)
    assert len(state.pantry.weekly_recipe_suggestions) == 7
    assert state.pantry.weekly_recipe_suggestions[0].recipe_name
    recipe_names = [suggestion.recipe_name for suggestion in state.pantry.weekly_recipe_suggestions]
    assert len(set(recipe_names)) == len(recipe_names)
    assert all(recipe_names[idx] != recipe_names[idx - 1] for idx in range(1, len(recipe_names)))
    first_suggestion = state.pantry.weekly_recipe_suggestions[0]
    assert first_suggestion.servings >= 1
    assert first_suggestion.recipe_source
    assert first_suggestion.recipe_url
    assert first_suggestion.ingredient_requirements


def test_resolve_recipe_url_falls_back_to_search_for_invalid_candidate() -> None:
    resolved = bootstrap_module._resolve_recipe_url(
        recipe_name="Miso Salmon Bowl",
        source_name="AllRecipes",
        candidate_url="",
    )

    assert resolved.startswith("https://www.google.com/search?q=")
    assert "Miso+Salmon+Bowl" in resolved


def test_resolve_recipe_url_falls_back_when_health_check_fails(monkeypatch) -> None:
    monkeypatch.setattr(bootstrap_module, "_recipe_link_healthcheck_enabled", lambda: True)
    monkeypatch.setattr(bootstrap_module, "_is_recipe_url_reachable", lambda url: False)
    with bootstrap_module._RECIPE_URL_CACHE_LOCK:
        bootstrap_module._RECIPE_URL_CACHE.clear()

    resolved = bootstrap_module._resolve_recipe_url(
        recipe_name="Turkey Primavera",
        source_name="Food Network",
        candidate_url="https://www.foodnetwork.com/recipes/food-network-kitchen/turkey-primavera-3364396",
    )

    assert resolved.startswith("https://www.google.com/search?q=")
    assert "Turkey+Primavera" in resolved


def test_bootstrap_includes_integration_events_when_user_is_connected(monkeypatch) -> None:
    class _NoProjectedEventsGateway(_FakeGateway):
        def get_calendar_view(self, *, family_id: str):
            return []

    class _CredentialStore:
        def get_credentials(self, *, user_id: str, provider_name: str):
            if user_id == "user-123" and provider_name == "google_calendar":
                return OAuthCredential(
                    user_id=user_id,
                    provider_name=provider_name,
                    access_token="token",
                    refresh_token="refresh",
                    scopes=(),
                    expires_at=None,
                )
            return None

    class _FakeOrchestrator:
        def build_household_state(self, user_id: str):
            return HouseholdState(
                user_id=user_id,
                calendar_events=[
                    CalendarEvent(
                        event_id="gcal-evt-1",
                        title="Imported from Google",
                        start="2026-04-20T15:00:00Z",
                        end="2026-04-20T15:30:00Z",
                    )
                ],
                tasks=[],
                alerts=[],
                metadata={},
            )

    monkeypatch.setattr(bootstrap_module, "get_events_by_household", lambda household_id, include_past=False: [])
    monkeypatch.setattr(
        bootstrap_module,
        "create_orchestrator",
        lambda **kwargs: _FakeOrchestrator(),
    )

    service = UIBootstrapService(
        hpal_gateway=_NoProjectedEventsGateway(),
        xai_store=_FakeXAIStore(),
    )

    state = service.get_state(
        family_id="family-1",
        user_id="user-123",
        credential_store=_CredentialStore(),
        http_client=None,
    )

    event_ids = [event.event_id for event in state.calendar.events]

    assert "gcal-evt-1" in event_ids
    imported = next(event for event in state.calendar.events if event.event_id == "gcal-evt-1")
    assert imported.participants == ["user-123"]


def test_bootstrap_includes_email_summary_notifications(monkeypatch) -> None:
    class _EventLogRow:
        def __init__(self) -> None:
            self.id = "event-email-1"
            self.payload = {
                "email_id": "mail-123",
                "parsed_fields": {
                    "subject": "School reminder",
                    "summary": "High priority email from school office.",
                    "importance_bucket": "high",
                    "action_items": [{"title": "Sign permission slip"}],
                    "calendar_candidates": [{"title": "Field trip"}],
                },
            }

    monkeypatch.setattr(
        bootstrap_module,
        "get_event_logs",
        lambda household_id, event_type=None, limit=100: [_EventLogRow()] if event_type == "email_parsed" else [],
    )

    service = UIBootstrapService(
        hpal_gateway=_FakeGateway(),
        xai_store=_FakeXAIStore(),
    )

    state = service.get_state(family_id="family-1")
    email_notifications = [
        notification
        for notification in state.notifications
        if notification.notification_id.startswith("notif:email_summary:")
    ]

    assert email_notifications
    notification = email_notifications[0]
    assert notification.title == "Email: School reminder"
    assert "High priority email from school office." in notification.message
    assert "1 action item" in notification.message
    assert "1 calendar candidate" in notification.message
    assert notification.level == "warning"


def test_bootstrap_includes_email_summary_notifications_from_email_received(monkeypatch) -> None:
    class _EventLogRow:
        def __init__(self) -> None:
            self.id = "event-email-received-1"
            self.payload = {
                "email_id": "mail-456",
                "subject": "PTA follow-up",
                "summary": "Medium priority follow-up from school.",
                "importance_bucket": "medium",
                "action_items": [{"title": "Reply to PTA"}],
                "calendar_candidates": [{"title": "PTA meeting"}],
            }

    monkeypatch.setattr(
        bootstrap_module,
        "get_event_logs",
        lambda household_id, event_type=None, limit=100: (
            [] if event_type == "email_parsed" else [_EventLogRow()] if event_type == "email_received" else []
        ),
    )

    service = UIBootstrapService(
        hpal_gateway=_FakeGateway(),
        xai_store=_FakeXAIStore(),
    )

    state = service.get_state(family_id="family-1")
    email_notifications = [
        notification
        for notification in state.notifications
        if notification.notification_id.startswith("notif:email_summary:")
    ]

    assert email_notifications
    notification = email_notifications[0]
    assert notification.title == "Email: PTA follow-up"
    assert "Medium priority follow-up from school." in notification.message
    assert "1 action item" in notification.message
    assert "1 calendar candidate" in notification.message
    assert notification.level == "info"


def test_bootstrap_refreshes_cached_state_when_email_summary_changes(monkeypatch) -> None:
    class _EventLogRow:
        def __init__(self, *, email_id: str, subject: str, summary: str) -> None:
            self.id = f"event-{email_id}"
            self.payload = {
                "email_id": email_id,
                "parsed_fields": {
                    "subject": subject,
                    "summary": summary,
                    "importance_bucket": "high",
                    "action_items": [{"title": "Follow up"}],
                    "calendar_candidates": [],
                },
            }

    current_subject = {"value": "School reminder"}

    def _fake_get_event_logs(household_id, event_type=None, limit=100):
        if event_type != "email_parsed":
            return []
        return [
            _EventLogRow(
                email_id="mail-cache-1",
                subject=current_subject["value"],
                summary=f"Summary for {current_subject['value']}",
            )
        ]

    monkeypatch.setattr(bootstrap_module, "get_event_logs", _fake_get_event_logs)

    service = UIBootstrapService(
        hpal_gateway=_FakeGateway(),
        xai_store=_FakeXAIStore(),
    )

    state_before = service.get_state(family_id="family-1")
    before_titles = [
        notification.title
        for notification in state_before.notifications
        if notification.notification_id.startswith("notif:email_summary:")
    ]
    assert before_titles == ["Email: School reminder"]

    current_subject["value"] = "Updated school reminder"
    state_after = service.get_state(family_id="family-1")
    after_titles = [
        notification.title
        for notification in state_after.notifications
        if notification.notification_id.startswith("notif:email_summary:")
    ]

    assert after_titles == ["Email: Updated school reminder"]
    assert before_titles != after_titles


def test_bootstrap_excludes_junk_email_notifications(monkeypatch) -> None:
    class _EventLogRow:
        def __init__(self) -> None:
            self.id = "event-email-junk-1"
            self.payload = {
                "email_id": "mail-junk-1",
                "parsed_fields": {
                    "subject": "Special offer",
                    "summary": "Promotional email",
                    "importance_bucket": "low",
                    "is_junk": True,
                    "triage_decision": "junk",
                },
            }

    monkeypatch.setattr(
        bootstrap_module,
        "get_event_logs",
        lambda household_id, event_type=None, limit=100: [_EventLogRow()] if event_type == "email_parsed" else [],
    )

    service = UIBootstrapService(
        hpal_gateway=_FakeGateway(),
        xai_store=_FakeXAIStore(),
    )

    state = service.get_state(family_id="family-1")
    email_notifications = [
        notification
        for notification in state.notifications
        if notification.notification_id.startswith("notif:email_summary:")
    ]

    assert email_notifications == []


def test_email_detail_endpoint_returns_combined_email_view(monkeypatch) -> None:
    _install_services(monkeypatch)

    class _ParsedRow:
        def __init__(self) -> None:
            self.id = "evt-parsed-1"
            self.payload = {
                "email_id": "mail-123",
                "parsed_fields": {
                    "subject": "School reminder",
                    "sender": "teacher@school.edu",
                    "recipient": "family@example.test",
                    "provider": "gmail",
                    "received_at": "2026-04-21T08:30:00Z",
                    "summary": "High priority school reminder.",
                    "importance_score": 0.81,
                    "importance_bucket": "high",
                    "triage_decision": "task",
                    "action_items": [{"title": "Sign permission slip"}],
                    "calendar_candidates": [{"title": "Field trip", "time_hint": "2026-04-28"}],
                    "informational_items": [{"title": "Bus departs at 8:30 AM"}],
                },
            }

    class _ReceivedRow:
        def __init__(self) -> None:
            self.id = "evt-received-1"
            self.payload = {
                "email_id": "mail-123",
                "subject": "School reminder",
                "sender": "teacher@school.edu",
                "recipient": "family@example.test",
                "provider": "gmail",
                "received_at": "2026-04-21T08:30:00Z",
                "body": "Please sign and return the permission slip before Friday.",
            }

    monkeypatch.setattr(
        bootstrap_module,
        "get_event_logs",
        lambda household_id, event_type=None, limit=100: (
            [_ParsedRow()]
            if event_type == "email_parsed"
            else [_ReceivedRow()]
            if event_type == "email_received"
            else []
        ),
    )

    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)
    response = None
    for _ in range(6):
        candidate = client.get(
            "/v1/ui/email/detail",
            params={"family_id": "family-1", "email_id": "mail-123"},
        )
        response = candidate
        if candidate.status_code != 429:
            break

    assert response is not None

    assert response.status_code == 200
    payload = response.json()
    assert payload["email_id"] == "mail-123"
    assert payload["subject"] == "School reminder"
    assert payload["triage_decision"] == "task"
    assert payload["action_items"][0]["title"] == "Sign permission slip"
    assert "permission slip" in payload["body"]


def test_email_detail_endpoint_returns_404_when_email_not_found(monkeypatch) -> None:
    _install_services(monkeypatch)
    monkeypatch.setattr(bootstrap_module, "get_event_logs", lambda household_id, event_type=None, limit=100: [])

    client = TestClient(main.app, raise_server_exceptions=True, follow_redirects=False)
    response = None
    for _ in range(6):
        candidate = client.get(
            "/v1/ui/email/detail",
            params={"family_id": "family-1", "email_id": "missing-email"},
        )
        response = candidate
        if candidate.status_code != 429:
            break

    assert response is not None

    assert response.status_code == 404
    assert response.json() == {"detail": "email_not_found"}
