from __future__ import annotations

import re

from apps.assistant_core.planning_engine import _fallback_household_state
from household_os.core.decision_engine import HouseholdOSDecisionEngine
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.presentation.recommendation_builder import RecommendationBuilder


def _build_enriched(message: str = "I need to start working out"):
    household_id = "recommendation-builder-household"
    state = _fallback_household_state(household_id)
    store = HouseholdStateGraphStore()
    graph = store.refresh_graph(
        household_id=household_id,
        state=state,
        query=message,
        fitness_goal="consistency",
    )
    response = HouseholdOSDecisionEngine().run(
        household_id=household_id,
        query=message,
        graph=graph,
        request_id="recommendation-builder-001",
    )
    return RecommendationBuilder().build(response=response, graph=graph)


def test_recommendation_is_actionable():
    enriched = _build_enriched()

    assert any(verb in enriched.recommendation for verb in ("Schedule", "Create", "Adjust", "Book"))
    assert len(enriched.why) in {2, 3}
    assert any(reason.startswith("Schedule state:") for reason in enriched.why)
    assert any(reason.startswith("Goal or gap:") for reason in enriched.why)
    assert any(reason.startswith("Recent behavior:") for reason in enriched.why)


def test_recommendation_contains_time():
    enriched = _build_enriched()

    assert re.search(r"\b\d{4}-\d{2}-\d{2}\b", enriched.recommendation)
    assert re.search(r"\b\d{2}:\d{2}\b", enriched.recommendation)


def test_no_vague_language():
    enriched = _build_enriched()
    all_text = " ".join([enriched.recommendation, enriched.impact, *enriched.why]).lower()

    assert "start routine" not in all_text
    assert "consider doing" not in all_text
    assert "you may want to" not in all_text
