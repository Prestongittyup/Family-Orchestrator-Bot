from __future__ import annotations

from datetime import UTC, datetime

from apps.api.product_surface.contracts import (
    ActionCard,
    CalendarState,
    ChatResponse,
    FamilySummary,
    Notification,
    PlanSummary,
    SystemHealthSnapshot,
    TaskBoardState,
    TaskSummary,
    TodayOverview,
    UIBootstrapState,
    UIPatch,
    XAIExplanationSummary,
)
from apps.api.product_surface.frontend_runtime import (
    ActionExecutionBinder,
    ActionExecutionResult,
    FrontendRuntimeEngine,
    SyncStrategySpec,
)


def _snapshot(version: int = 100, watermark: str = "10:7:2026-04-20T10:00:00Z") -> UIBootstrapState:
    return UIBootstrapState(
        snapshot_version=version,
        source_watermark=watermark,
        family=FamilySummary(
            family_id="family-1",
            member_count=2,
            member_names=["Alex", "Morgan"],
            default_time_zone="UTC",
        ),
        today_overview=TodayOverview(
            date="2026-04-20",
            open_task_count=1,
            scheduled_event_count=1,
            active_plan_count=1,
            notification_count=0,
        ),
        active_plans=[
            PlanSummary(
                plan_id="plan-1",
                title="Morning",
                status="active",
                revision=1,
                linked_task_count=1,
            )
        ],
        task_board=TaskBoardState(
            pending=[
                TaskSummary(
                    task_id="task-1",
                    title="School drop-off",
                    plan_id="plan-1",
                    assigned_to="Alex",
                    status="pending",
                    priority="high",
                    due_time="2026-04-20T08:30:00Z",
                )
            ],
            in_progress=[],
            completed=[],
            failed=[],
        ),
        calendar=CalendarState(
            window_start="2026-04-19T00:00:00Z",
            window_end="2026-05-20T00:00:00Z",
            events=[],
        ),
        notifications=[],
        explanation_digest=[
            XAIExplanationSummary(
                explanation_id="xai-1",
                entity_type="task",
                entity_id="task-1",
                summary="Task prioritized.",
                timestamp="2026-04-20T10:00:00Z",
            )
        ],
        system_health=SystemHealthSnapshot(
            status="healthy",
            pending_actions=0,
            stale_projection=False,
            state_version=7,
            last_updated="2026-04-20T10:00:00Z",
        ),
    )


def _patch(entity_type: str, entity_id: str, change_type: str, payload: dict, version: int, ts: str) -> UIPatch:
    return UIPatch(
        entity_type=entity_type,
        entity_id=entity_id,
        change_type=change_type,
        payload=payload,
        version=version,
        source_timestamp=datetime.fromisoformat(ts.replace("Z", "+00:00")),
    )


def test_deterministic_patch_replay() -> None:
    engine = FrontendRuntimeEngine()
    state = engine.initialize(snapshot=_snapshot())

    p1 = _patch(
        "task",
        "task-1",
        "update",
        {
            "task_id": "task-1",
            "title": "School drop-off",
            "plan_id": "plan-1",
            "assigned_to": "Alex",
            "status": "in_progress",
            "priority": "high",
            "due_time": "2026-04-20T08:30:00Z",
        },
        101,
        "2026-04-20T10:00:01Z",
    )
    p2 = _patch(
        "notification",
        "notif-1",
        "create",
        {
            "notification_id": "notif-1",
            "title": "Heads up",
            "message": "Task started",
            "level": "info",
            "related_entity": "task-1",
        },
        102,
        "2026-04-20T10:00:02Z",
    )

    out1 = engine.apply_patches(state=state, patches=[p2, p1, p1])
    out2 = engine.apply_patches(state=state, patches=[p1, p2])

    assert out1.materialized_index == out2.materialized_index
    assert len(out1.applied_patches) == 2

    replay = engine.apply_patches(state=out1, patches=[p1, p2])
    assert replay == out1


def test_snapshot_reconstruction_equivalence() -> None:
    engine = FrontendRuntimeEngine()
    base = _snapshot(version=200)
    state = engine.initialize(snapshot=base)

    replace_task = _patch(
        "task",
        "task-1",
        "replace",
        {
            "task_id": "task-1",
            "title": "School drop-off",
            "plan_id": "plan-1",
            "assigned_to": "Alex",
            "status": "completed",
            "priority": "high",
            "due_time": "2026-04-20T08:30:00Z",
        },
        201,
        "2026-04-20T10:02:00Z",
    )
    create_event = _patch(
        "event",
        "evt-1",
        "create",
        {
            "event_id": "evt-1",
            "title": "Dentist",
            "start": "2026-04-20T14:00:00Z",
            "end": "2026-04-20T14:30:00Z",
            "participants": ["Alex"],
        },
        202,
        "2026-04-20T10:03:00Z",
    )

    applied = engine.apply_patches(state=state, patches=[replace_task, create_event])
    rebuilt = engine.reconstruct_materialized(snapshot=base, patches=[replace_task, create_event])

    assert applied.materialized_index == rebuilt


def test_chat_session_consistency_under_retry() -> None:
    engine = FrontendRuntimeEngine()
    state = engine.initialize(snapshot=_snapshot())

    patch = _patch(
        "notification",
        "chat-notif-1",
        "create",
        {
            "notification_id": "chat-notif-1",
            "title": "Confirm",
            "message": "Please confirm",
            "level": "warning",
            "related_entity": "task-1",
        },
        101,
        "2026-04-20T10:00:03Z",
    )

    response = ChatResponse(
        assistant_message="Please confirm this change.",
        action_cards=[
            ActionCard(
                id="card-1",
                type="confirm",
                title="Confirm",
                description="Confirm task update",
                related_entity="task-1",
                required_action_payload={"task_id": "task-1"},
                risk_level="medium",
            )
        ],
        ui_patch=[patch],
        requires_confirmation=True,
        explanation_summary=[],
    )

    s1 = engine.apply_chat_response(state=state, session_id="session-1", response=response)
    s2 = engine.apply_chat_response(state=s1, session_id="session-1", response=response)

    session = s2.chat_sessions["session-1"]
    assert len(session.message_history) == 1
    assert session.awaiting_confirmation is True
    assert len(s2.applied_patches) == 1


def test_action_execution_idempotency() -> None:
    binder = ActionExecutionBinder()
    card = ActionCard(
        id="card-approve-1",
        type="approve",
        title="Approve",
        description="Approve task",
        related_entity="task-1",
        required_action_payload={"task_id": "task-1"},
        risk_level="low",
    )
    req = binder.build_request(
        family_id="family-1",
        session_id="session-1",
        action_card=card,
        endpoint="/v1/ui/action/approve",
    )

    calls = {"count": 0}

    def _api_call(_request):
        calls["count"] += 1
        return ActionExecutionResult(status="succeeded", response_payload={"ok": True})

    r1 = binder.execute(request=req, call_api=_api_call)
    r2 = binder.execute(request=req, call_api=_api_call)

    assert calls["count"] == 1
    assert r1 == r2


def test_failure_recovery_uses_backend_snapshot_authority() -> None:
    engine = FrontendRuntimeEngine()
    sync = SyncStrategySpec()
    state = engine.initialize(snapshot=_snapshot())

    missing_version = _patch(
        "task",
        "task-1",
        "update",
        {
            "task_id": "task-1",
            "title": "School drop-off",
            "plan_id": "plan-1",
            "assigned_to": "Alex",
            "status": "in_progress",
            "priority": "high",
            "due_time": "2026-04-20T08:30:00Z",
        },
        105,
        "2026-04-20T10:00:04Z",
    )
    desynced = engine.apply_patches(state=state, patches=[missing_version])
    assert desynced.sync_status == "desynced"

    authoritative = _snapshot(version=300, watermark="11:8:2026-04-20T10:05:00Z")
    reconciled = sync.reconcile(runtime=engine, state=desynced, backend_snapshot=authoritative)

    assert reconciled.sync_status == "synced"
    assert reconciled.snapshot == authoritative
    assert reconciled.applied_patches == []
