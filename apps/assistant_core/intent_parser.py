from __future__ import annotations

import re

from apps.assistant_core.contracts import AssistantIntent


_TIME_PATTERNS = (
    r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b",
    r"\b(?:today|tomorrow|tonight|this week|next week|weekend)\b",
    r"\b\d{1,2}(?::\d{2})?\s?(?:am|pm)\b",
    r"\b(?:morning|afternoon|evening|midday|lunch)\b",
)

_ENTITY_PATTERNS = {
    "doctor": ("doctor", "dentist", "pediatrician", "checkup", "clinic"),
    "work": ("work", "office", "meeting", "standup"),
    "school": ("school", "pickup", "drop-off", "teacher"),
    "meal": ("meal", "dinner", "lunch", "breakfast", "cook", "recipe"),
    "fitness": ("workout", "work out", "working out", "exercise", "gym", "run", "strength", "cardio"),
    "household": ("household", "family", "kids", "chores", "coordination"),
}


def _detect_intent_type(normalized: str) -> str:
    if any(token in normalized for token in ("doctor", "dentist", "appointment", "meeting", "school")):
        return "appointment"
    if any(token in normalized for token in ("meal", "dinner", "lunch", "breakfast", "cook", "recipe", "grocery")):
        return "meal"
    if any(token in normalized for token in ("fitness", "workout", "work out", "working out", "exercise", "gym", "run", "strength", "muscle", "fat loss")):
        return "fitness"
    return "general"


def _extract_entities(normalized: str) -> list[str]:
    matches: list[str] = []
    for label, variants in _ENTITY_PATTERNS.items():
        if any(variant in normalized for variant in variants):
            matches.append(label)
    return matches or ["household"]


def _extract_time_constraints(normalized: str) -> list[str]:
    found: list[str] = []
    for pattern in _TIME_PATTERNS:
        found.extend(re.findall(pattern, normalized, flags=re.IGNORECASE))
    unique = []
    for item in found:
        lowered = item.lower()
        if lowered not in unique:
            unique.append(lowered)
    return unique


def _detect_priority(normalized: str) -> str:
    if any(token in normalized for token in ("urgent", "asap", "critical", "immediately")):
        return "high"
    if any(token in normalized for token in ("soon", "important", "priority", "need to")):
        return "medium"
    return "low"


def _extract_context_flags(normalized: str) -> list[str]:
    flags: list[str] = []
    if any(token in normalized for token in ("after school", "pickup", "drop-off", "kids", "family")):
        flags.append("family_schedule")
    if any(token in normalized for token in ("budget", "inventory", "groceries")):
        flags.append("budget_sensitive")
    if any(token in normalized for token in ("work", "office", "meeting")):
        flags.append("workday_constraint")
    if any(token in normalized for token in ("strength", "muscle", "fat loss", "cardio")):
        flags.append("fitness_goal_present")
    return flags


def parse_intent(query: str) -> AssistantIntent:
    normalized = " ".join(query.strip().lower().split())
    return AssistantIntent(
        intent_type=_detect_intent_type(normalized),
        entities=_extract_entities(normalized),
        time_constraints=_extract_time_constraints(normalized),
        priority=_detect_priority(normalized),
        context_flags=_extract_context_flags(normalized),
    )