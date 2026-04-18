from __future__ import annotations

from copy import deepcopy

from brief_endpoint import map_manual_to_brief
from apps.api.ingestion.adapters.manual_priority import (
    partition_actions_by_visibility,
    score_manual_item,
    visibility_block_for_action,
)


def _base_brief() -> dict:
    return {
        "scheduled_actions": [],
        "unscheduled_actions": [],
        "priorities": [],
        "warnings": [],
        "risks": [],
        "summary": "",
    }


def test_score_is_deterministic_for_same_input() -> None:
    title = "Fix urgent bug in billing"
    start_time = "2026-04-16T09:00:00"
    expected = score_manual_item(title, start_time)

    for _ in range(10):
        assert score_manual_item(title, start_time) == expected


def test_keyword_scoring_rules_are_correct() -> None:
    # default
    assert score_manual_item("Read notes", None) == 1.0

    # +3 group
    assert score_manual_item("Fix issue", None) == 4.0
    assert score_manual_item("Bug report", None) == 4.0
    assert score_manual_item("Error handling", None) == 4.0

    # +3 group
    assert score_manual_item("Pay rent", None) == 4.0
    assert score_manual_item("Bill due", None) == 4.0
    assert score_manual_item("Urgent task", None) == 4.0

    # +2 group
    assert score_manual_item("Team meeting", None) == 3.0
    assert score_manual_item("Call mom", None) == 3.0

    # +1 group
    assert score_manual_item("Cook dinner", None) == 2.0
    assert score_manual_item("Buy food", None) == 2.0


def test_time_adjustments_are_applied() -> None:
    # morning +1
    assert score_manual_item("Read notes", "2026-04-16T09:00:00") == 2.0
    # evening +0.5
    assert score_manual_item("Read notes", "2026-04-16T18:30:00") == 1.5
    # afternoon +0
    assert score_manual_item("Read notes", "2026-04-16T14:00:00") == 1.0


def test_manual_item_priority_score_is_attached_in_adapter_mapping() -> None:
    items = [
        {"title": "Fix bug", "time": "morning"},
        {"title": "Call plumber"},
    ]

    output = map_manual_to_brief(deepcopy(_base_brief()), items)

    scheduled = output["scheduled_actions"]
    unscheduled = output["unscheduled_actions"]

    assert len(scheduled) == 1
    assert len(unscheduled) == 1
    assert "priority_score" in scheduled[0]
    assert "priority_score" in unscheduled[0]


def test_ordering_stability_within_same_time_block() -> None:
    items = [
        {"title": "Cook dinner", "time": "evening"},      # 2.5
        {"title": "Call mom", "time": "evening"},         # 3.5
        {"title": "Fix error", "time": "evening"},        # 4.5
        {"title": "Read book", "time": "evening"},        # 1.5
    ]

    first = map_manual_to_brief(deepcopy(_base_brief()), items)
    second = map_manual_to_brief(deepcopy(_base_brief()), items)

    first_titles = [a["title"] for a in first["scheduled_actions"]]
    second_titles = [a["title"] for a in second["scheduled_actions"]]

    assert first_titles == second_titles
    assert first_titles == ["Fix error", "Call mom", "Cook dinner", "Read book"]


def test_renderer_inputs_remain_compatible() -> None:
    items = [
        {"title": "Meeting prep", "time": "14:30"},
        {"title": "Pay bill"},
    ]
    output = map_manual_to_brief(deepcopy(_base_brief()), items)

    # Renderer depends on start_time and title; ensure both remain present where expected.
    assert output["scheduled_actions"][0]["title"] == "Meeting prep"
    assert "start_time" in output["scheduled_actions"][0]
    assert output["unscheduled_actions"][0]["title"] == "Pay bill"


def test_low_priority_actions_filtered_to_unscheduled_by_threshold() -> None:
    actions = [
        {
            "title": "Low morning",
            "start_time": "2026-04-16T09:00:00",
            "priority_score": 1.0,
        },
        {
            "title": "High morning",
            "start_time": "2026-04-16T09:30:00",
            "priority_score": 2.0,
        },
    ]

    scheduled, unscheduled = partition_actions_by_visibility(actions)

    assert [a["title"] for a in scheduled] == ["High morning"]
    assert [a["title"] for a in unscheduled] == ["Low morning"]


def test_visibility_filter_is_deterministic_for_same_input() -> None:
    actions = [
        {
            "title": "Low morning",
            "start_time": "2026-04-16T09:00:00",
            "priority_score": 1.0,
        },
        {
            "title": "High morning",
            "start_time": "2026-04-16T09:30:00",
            "priority_score": 2.0,
        },
        {
            "title": "Evening normal",
            "start_time": "2026-04-16T18:30:00",
            "priority_score": 1.0,
        },
    ]

    first_s, first_u = partition_actions_by_visibility(actions)
    second_s, second_u = partition_actions_by_visibility(actions)

    assert [a["title"] for a in first_s] == [a["title"] for a in second_s]
    assert [a["title"] for a in first_u] == [a["title"] for a in second_u]


def test_visibility_block_default_unscheduled_for_missing_time() -> None:
    action = {
        "title": "No time task",
        "priority_score": 1.0,
    }
    assert visibility_block_for_action(action) == "unscheduled"


def test_manual_mapping_does_not_change_top_level_schema_shape() -> None:
    base = _base_brief()
    before_keys = set(base.keys())
    output = map_manual_to_brief(deepcopy(base), [{"title": "Read notes", "time": "14:30"}])
    after_keys = set(output.keys())

    assert before_keys == after_keys