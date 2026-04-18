from __future__ import annotations

from datetime import datetime
from typing import Any


VISIBILITY_THRESHOLD_BY_BLOCK = {
    "morning": 1.5,
    "afternoon": 1.0,
    "evening": 1.0,
    "unscheduled": 1.0,
}


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _time_block_from_iso(start_time: str | None) -> str | None:
    if not start_time:
        return None
    try:
        parsed = datetime.fromisoformat(str(start_time).replace("Z", "+00:00"))
    except ValueError:
        return None

    hour = parsed.hour
    minute = parsed.minute

    if (hour > 8 or (hour == 8 and minute >= 0)) and hour < 12:
        return "morning"
    if hour >= 12 and hour < 17:
        return "afternoon"
    if hour >= 17 and (hour < 21 or (hour == 21 and minute == 0)):
        return "evening"
    return None


def score_manual_item(title: str, start_time: str | None = None) -> float:
    text = (title or "").strip().lower()
    score = 1.0

    # Keyword scoring groups (rule-based, deterministic)
    if _contains_any(text, ("fix", "bug", "error")):
        score += 3.0
    if _contains_any(text, ("pay", "bill", "urgent")):
        score += 3.0
    if _contains_any(text, ("meeting", "call")):
        score += 2.0
    if _contains_any(text, ("cook", "dinner", "food")):
        score += 1.0

    # Time adjustment (based on normalized start_time block)
    block = _time_block_from_iso(start_time)
    if block == "morning":
        score += 1.0
    elif block == "evening":
        score += 0.5

    return score


def sort_manual_actions(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _block_rank(action: dict[str, Any]) -> int:
        block = _time_block_from_iso(action.get("start_time"))
        if block == "morning":
            return 0
        if block == "afternoon":
            return 1
        if block == "evening":
            return 2
        return 3

    indexed = list(enumerate(actions))
    indexed.sort(
        key=lambda pair: (
            _block_rank(pair[1]),
            -float(pair[1].get("priority_score", 1.0)),
            str(pair[1].get("title", "")).strip().lower(),
            str(pair[1].get("raw_time_input", "")).strip().lower(),
            pair[0],
        )
    )
    return [action for _, action in indexed]


def visibility_block_for_action(action: dict[str, Any]) -> str:
    block = _time_block_from_iso(action.get("start_time"))
    if block in {"morning", "afternoon", "evening"}:
        return block
    return "unscheduled"


def passes_visibility_threshold(action: dict[str, Any]) -> bool:
    block = visibility_block_for_action(action)
    threshold = float(VISIBILITY_THRESHOLD_BY_BLOCK[block])
    score = float(action.get("priority_score", 1.0))
    return score >= threshold


def partition_actions_by_visibility(
    actions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    scheduled_actions: list[dict[str, Any]] = []
    unscheduled_actions: list[dict[str, Any]] = []

    # Filtering is applied after deterministic sorting.
    for action in sort_manual_actions(actions):
        if action.get("start_time") and passes_visibility_threshold(action):
            scheduled_actions.append(action)
        else:
            unscheduled_actions.append(action)

    return scheduled_actions, unscheduled_actions