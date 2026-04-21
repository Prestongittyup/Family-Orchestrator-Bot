from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from assistant.governance.intent_lock import IntentClassification, IntentType
from assistant.governance.intent_router import IntentRouter, RoutingCase
from assistant.state.life_state_model import LifeState, LifeStateModel


def _dt(days_ago: int = 0, hours: int = 9) -> datetime:
    base = datetime(2026, 4, 20, hours, 0, tzinfo=UTC)
    return base - timedelta(days=days_ago)


def _event(start: datetime, duration_minutes: int = 60) -> dict[str, str]:
    end = start + timedelta(minutes=duration_minutes)
    return {
        "title": "Event",
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
    }


def _graph_with_calendar(events: list[dict[str, str]]) -> dict:
    return {
        "reference_time": _dt(0, 12).isoformat().replace("+00:00", "Z"),
        "calendar_events": events,
        "tasks": [],
        "action_lifecycle": {"actions": {}},
        "behavior_feedback": {"records": []},
    }


def test_workload_score_increases_with_dense_calendar_state(tmp_path: Path) -> None:
    model = LifeStateModel(life_state_path=tmp_path / "life_state.json")

    sparse_events = [_event(_dt(days_ago=1, hours=9))]
    dense_events = [
        _event(_dt(days_ago=d, hours=h))
        for d in range(0, 7)
        for h in (8, 10, 12, 14, 16)
    ]

    sparse_graph = _graph_with_calendar(sparse_events)
    dense_graph = _graph_with_calendar(dense_events)

    sparse_state = model.update_after_run(
        household_id="h1",
        graph=sparse_graph,
        classification=None,
        timestamp=_dt(0, 12),
    )
    dense_state = model.update_after_run(
        household_id="h1",
        graph=dense_graph,
        classification=None,
        timestamp=_dt(0, 12),
    )

    assert dense_state.workload_score > sparse_state.workload_score


def test_stress_index_rises_with_overlapping_events(tmp_path: Path) -> None:
    model = LifeStateModel(life_state_path=tmp_path / "life_state.json")

    no_overlap = [
        _event(_dt(days_ago=1, hours=9), 60),
        _event(_dt(days_ago=1, hours=11), 60),
    ]
    overlap = [
        _event(_dt(days_ago=1, hours=9), 120),
        _event(_dt(days_ago=1, hours=10), 90),
    ]

    no_overlap_state = model.update_after_run(
        household_id="h2",
        graph=_graph_with_calendar(no_overlap),
        classification=None,
        timestamp=_dt(0, 12),
    )
    overlap_state = model.update_after_run(
        household_id="h2",
        graph=_graph_with_calendar(overlap),
        classification=None,
        timestamp=_dt(0, 12),
    )

    assert overlap_state.stress_index > no_overlap_state.stress_index


def test_routing_preference_shifts_under_high_workload() -> None:
    classification = IntentClassification(
        primary_intent=IntentType.MEAL,
        confidence=0.55,  # medium confidence
        secondary_intents=[IntentType.FITNESS, IntentType.DAILY_FOCUS],
        ambiguity_flag=True,
        all_scores={t.value: 0.0 for t in IntentType},
        matched_keywords=["meal"],
    )

    low_workload = LifeState(
        workload_score=0.2,
        stress_index=0.2,
        routine_stability=0.8,
        recent_focus_distribution={t.value: 0 for t in IntentType},
        active_backlog_size=1,
    )
    high_workload = LifeState(
        workload_score=0.9,
        stress_index=0.2,
        routine_stability=0.8,
        recent_focus_distribution={t.value: 0 for t in IntentType},
        active_backlog_size=9,
    )

    low_route = IntentRouter.route(classification, life_state=low_workload)
    high_route = IntentRouter.route(classification, life_state=high_workload)

    assert low_route.routing_case == RoutingCase.MEDIUM_CONFIDENCE
    assert high_route.routing_case == RoutingCase.MEDIUM_CONFIDENCE

    # Under high workload, DAILY_FOCUS should be preferred as the second domain (general).
    assert "meal" in high_route.allowed_domains
    assert "general" in high_route.allowed_domains

    # Under low workload, FITNESS remains the second choice (fitness).
    assert "meal" in low_route.allowed_domains
    assert "fitness" in low_route.allowed_domains


def test_intent_lock_still_prevents_cross_domain_leakage() -> None:
    classification = IntentClassification(
        primary_intent=IntentType.FITNESS,
        confidence=0.9,
        secondary_intents=[IntentType.DAILY_FOCUS],
        ambiguity_flag=False,
        all_scores={t.value: 0.0 for t in IntentType},
        matched_keywords=["fitness"],
    )

    heavy_life_state = LifeState(
        workload_score=0.95,
        stress_index=0.9,
        routine_stability=0.2,
        recent_focus_distribution={t.value: 0 for t in IntentType},
        active_backlog_size=12,
    )

    route = IntentRouter.route(classification, life_state=heavy_life_state)

    # LifeState must never bypass intent lock constraints for high-confidence routes.
    assert route.routing_case == RoutingCase.HIGH_CONFIDENCE
    assert route.allowed_domains == ["fitness"]
    assert "calendar" not in route.allowed_domains
    assert "meal" not in route.allowed_domains


def test_life_state_never_changes_primary_intent_classification() -> None:
    classification = IntentClassification(
        primary_intent=IntentType.MEDICAL,
        confidence=0.65,
        secondary_intents=[IntentType.DAILY_FOCUS, IntentType.FITNESS],
        ambiguity_flag=True,
        all_scores={t.value: 0.0 for t in IntentType},
        matched_keywords=["doctor"],
    )
    heavy_life_state = LifeState(
        workload_score=0.95,
        stress_index=0.9,
        routine_stability=0.2,
        recent_focus_distribution={t.value: 0 for t in IntentType},
        active_backlog_size=12,
    )

    route = IntentRouter.route(classification, life_state=heavy_life_state)

    # LifeState can reorder secondaries, but must never mutate classification intent.
    assert route.classification.primary_intent == IntentType.MEDICAL


def test_persistence_updates_after_run_and_approval(tmp_path: Path) -> None:
    model = LifeStateModel(life_state_path=tmp_path / "life_state.json")

    graph = {
        "reference_time": _dt(0, 12).isoformat().replace("+00:00", "Z"),
        "calendar_events": [_event(_dt(1, 9))],
        "tasks": [{"title": "A", "status": "open"}],
        "action_lifecycle": {"actions": {}},
        "behavior_feedback": {"records": []},
    }

    classification = IntentClassification(
        primary_intent=IntentType.DAILY_FOCUS,
        confidence=0.8,
        secondary_intents=[],
        ambiguity_flag=False,
        all_scores={t.value: 0.0 for t in IntentType},
        matched_keywords=["focus"],
    )

    run_state = model.update_after_run(
        household_id="h3",
        graph=graph,
        classification=classification,
        timestamp=_dt(0, 12),
    )
    approve_state = model.update_after_approval(
        household_id="h3",
        graph=graph,
        timestamp=_dt(0, 12),
    )

    loaded = model.load("h3")
    assert loaded.active_backlog_size == approve_state.active_backlog_size
    assert loaded.workload_score == approve_state.workload_score
    assert run_state.recent_focus_distribution[IntentType.DAILY_FOCUS.value] >= 1
