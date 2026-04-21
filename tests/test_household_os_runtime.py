from __future__ import annotations

from apps.assistant_core.planning_engine import _fallback_household_state
from household_os.core.decision_engine import HouseholdOSDecisionEngine
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.runtime.action_pipeline import ActionPipeline
from household_os.runtime.daily_cycle import HouseholdDailyCycle
from household_os.runtime.orchestrator import HouseholdOSOrchestrator
from household_os.runtime.trigger_detector import RuntimeTrigger, TriggerDetector


def _runtime_store(tmp_path):
    return HouseholdStateGraphStore(graph_path=tmp_path / "household_os_runtime_graph.json")


def test_trigger_detection(tmp_path):
    store = _runtime_store(tmp_path)
    state = _fallback_household_state("runtime-trigger-household")
    graph = store.refresh_graph(
        household_id="runtime-trigger-household",
        state=state,
        query="bootstrap",
        fitness_goal=None,
    )

    triggers = TriggerDetector().detect(
        household_id="runtime-trigger-household",
        graph=graph,
        user_input="I need to start working out",
        now="2026-04-20T06:30:00Z",
    )

    trigger_types = {trigger.trigger_type for trigger in triggers}
    assert trigger_types == {"USER_INPUT", "TIME_TICK", "STATE_CHANGE"}
    assert all(trigger.trigger_id.startswith("trg-") for trigger in triggers)
    assert all(trigger.household_id == "runtime-trigger-household" for trigger in triggers)


def test_action_lifecycle_flow(tmp_path):
    store = _runtime_store(tmp_path)
    state = _fallback_household_state("runtime-lifecycle-household")
    graph = store.refresh_graph(
        household_id="runtime-lifecycle-household",
        state=state,
        query="I need to start working out",
        fitness_goal="consistency",
    )

    response = HouseholdOSDecisionEngine().run(
        household_id="runtime-lifecycle-household",
        query="I need to start workout consistency",
        graph=graph,
        request_id="runtime-life-001",
    )
    trigger = RuntimeTrigger(
        trigger_id="trg-runtime-life",
        trigger_type="USER_INPUT",
        household_id="runtime-lifecycle-household",
        detected_at="2026-04-20T06:30:00Z",
        detail="User input",
        metadata={"query": "I need to start working out"},
    )

    pipeline = ActionPipeline()
    action = pipeline.register_proposed_action(
        graph=graph,
        trigger=trigger,
        response=response,
        now="2026-04-20T06:30:00Z",
    )
    assert action.current_state == "pending_approval"
    assert [transition.to_state for transition in action.transitions] == ["proposed", "pending_approval"]

    approved = pipeline.approve_actions(
        graph=graph,
        request_id=response.request_id,
        action_ids=[action.action_id],
        now="2026-04-20T06:31:00Z",
    )
    assert len(approved) == 1
    assert approved[0].current_state == "approved"

    executed = pipeline.execute_approved_actions(graph=graph, now="2026-04-20T06:32:00Z")
    assert len(executed) == 1
    assert executed[0].current_state == "executed"
    assert executed[0].execution_result["handler"] == "calendar_update"
    assert any(event.get("event_id") == f"runtime-{action.action_id}" for event in graph["calendar_events"])


def test_orchestrator_tick(tmp_path):
    store = _runtime_store(tmp_path)
    orchestrator = HouseholdOSOrchestrator(state_store=store)
    state = _fallback_household_state("runtime-tick-household")

    result = orchestrator.tick(
        household_id="runtime-tick-household",
        state=state,
        user_input="I need to start working out",
        fitness_goal="consistency",
        now="2026-04-20T06:30:00Z",
    )

    assert result.processed_trigger is not None
    assert result.processed_trigger.trigger_type == "USER_INPUT"
    assert result.response is not None
    assert result.response.recommended_action.action_id == result.action_record.action_id
    assert result.action_record.current_state == "pending_approval"

    persisted = store.load_graph("runtime-tick-household")
    assert result.response.request_id in persisted["responses"]
    assert result.action_record.action_id in persisted["action_lifecycle"]["actions"]


def test_daily_cycle(tmp_path):
    store = _runtime_store(tmp_path)
    orchestrator = HouseholdOSOrchestrator(state_store=store)
    daily_cycle = HouseholdDailyCycle(orchestrator)
    state = _fallback_household_state("runtime-daily-household")

    proposed = orchestrator.tick(
        household_id="runtime-daily-household",
        state=state,
        user_input="I need to start working out",
        fitness_goal="consistency",
        now="2026-04-20T06:30:00Z",
    )
    approval = orchestrator.approve_and_execute(
        household_id="runtime-daily-household",
        request_id=proposed.response.request_id,
        action_ids=[proposed.action_record.action_id],
        now="2026-04-20T06:35:00Z",
    )
    evening = daily_cycle.run_evening(
        household_id="runtime-daily-household",
        now="2026-04-20T19:00:00Z",
    )
    morning = daily_cycle.run_morning(
        household_id="runtime-daily-household",
        now="2026-04-21T06:30:00Z",
    )

    print("\nFull lifecycle demonstration:")
    print(f"1. Proposed action: {proposed.action_record.title} [{proposed.action_record.current_state}]")
    print(f"2. Approval request: {proposed.response.grouped_approval_payload.group_id} -> {proposed.response.recommended_action.approval_status}")
    print(f"3. Approved action: {approval.approved_actions[0].action_id} [{approval.approved_actions[0].current_state}]")
    print(f"4. Executed calendar update: {approval.executed_actions[0].execution_result['event_id']} via {approval.executed_actions[0].execution_result['handler']}")
    print(f"5. Follow-up adjustment next day: {morning.tick.response.recommended_action.title}")

    assert approval.response is not None
    assert approval.response.recommended_action.approval_status == "approved"
    assert len(approval.executed_actions) == 1
    assert approval.executed_actions[0].current_state == "executed"
    assert approval.executed_actions[0].execution_result["handler"] == "calendar_update"
    assert len(evening.queued_follow_ups) == 1
    assert morning.tick.processed_trigger is not None
    assert morning.tick.processed_trigger.trigger_type == "TIME_TICK"
    assert morning.tick.response is not None
    assert "workout" in evening.queued_follow_ups[0]["query"].lower()
    assert morning.tick.response.recommended_action.title.startswith("Start")