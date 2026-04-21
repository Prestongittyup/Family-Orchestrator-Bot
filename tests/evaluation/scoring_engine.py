from __future__ import annotations

from datetime import datetime
from typing import Any


def _clamp_score(value: float) -> int:
    value = max(0.0, min(10.0, value))
    return int(round(value))


def _parse_iso(value: str) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _overlap(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    a_s = _parse_iso(a_start)
    a_e = _parse_iso(a_end)
    b_s = _parse_iso(b_start)
    b_e = _parse_iso(b_end)
    if not all([a_s, a_e, b_s, b_e]):
        return False
    return bool(a_s < b_e and b_s < a_e)


def _extract_titles(brief: dict[str, Any]) -> tuple[list[str], list[str], str | None]:
    top_titles = [
        str(row.get("title", "")).strip()
        for row in brief.get("top_events", [])
        if isinstance(row, dict)
    ]

    all_titles: list[str] = []
    for row in brief.get("today_events", []):
        if isinstance(row, dict):
            all_titles.append(str(row.get("title", "")).strip())

    calendar = brief.get("calendar", {}) if isinstance(brief.get("calendar"), dict) else {}
    for row in calendar.get("upcoming", []):
        if isinstance(row, dict):
            all_titles.append(str(row.get("title", "")).strip())

    next_upcoming = brief.get("next_upcoming_event", {})
    next_title = None
    if isinstance(next_upcoming, dict):
        candidate = str(next_upcoming.get("title", "")).strip()
        next_title = candidate or None

    return top_titles, [t for t in all_titles if t], next_title


def _scenario_has_conflict(scenario_events: list[dict[str, Any]]) -> bool:
    for i in range(len(scenario_events)):
        for j in range(i + 1, len(scenario_events)):
            a = scenario_events[i]
            b = scenario_events[j]
            if _overlap(
                str(a.get("start_time", "")),
                str(a.get("end_time", "")),
                str(b.get("start_time", "")),
                str(b.get("end_time", "")),
            ):
                return True
    return False


def evaluate_brief(
    brief_output: dict[str, Any],
    expected_signals: dict[str, Any],
    expected_outcomes: dict[str, Any],
) -> dict[str, Any]:
    wrapped_brief = brief_output.get("brief") if isinstance(brief_output, dict) else None
    brief = wrapped_brief if isinstance(wrapped_brief, dict) else brief_output

    issues: list[str] = []
    top_titles, all_titles, next_title = _extract_titles(brief)

    must_surface = [str(x) for x in expected_signals.get("must_surface", [])]
    required_titles = [str(x) for x in expected_signals.get("required_titles", [])]

    if must_surface:
        surfaced = sum(1 for title in must_surface if title in top_titles or title == next_title)
        priority_score = _clamp_score((surfaced / len(must_surface)) * 10.0)
        if surfaced < len(must_surface):
            missing = [title for title in must_surface if title not in top_titles and title != next_title]
            issues.append(f"missing priority signals: {', '.join(missing)}")
    else:
        priority_score = 10

    if all_titles:
        meaningful = sum(1 for title in all_titles if title and title.lower() not in {"untitled event", ""})
        relevance_score = _clamp_score((meaningful / len(all_titles)) * 10.0)
    else:
        relevance_score = 7
        issues.append("no events surfaced in brief")

    if required_titles:
        covered = sum(1 for title in required_titles if title in all_titles or title in top_titles)
        completeness_score = _clamp_score((covered / len(required_titles)) * 10.0)
        if covered < len(required_titles):
            missing = [title for title in required_titles if title not in all_titles and title not in top_titles]
            issues.append(f"missing expected events: {', '.join(missing)}")
    else:
        completeness_score = 10

    required_structure = {
        "date": str,
        "today_events": list,
        "event_count": int,
        "calendar": dict,
        "summary": dict,
    }
    structure_hits = 0
    for key, expected_type in required_structure.items():
        value = brief.get(key)
        if isinstance(value, expected_type):
            structure_hits += 1
        else:
            issues.append(f"clarity structure mismatch for key '{key}'")
    clarity_score = _clamp_score((structure_hits / len(required_structure)) * 10.0)

    expected_top = str(expected_outcomes.get("top_priority", "")).strip()
    if expected_top:
        actual_top = top_titles[0] if top_titles else next_title
        priority_correctness = 10 if actual_top == expected_top else 0
        if actual_top != expected_top:
            issues.append(f"top priority mismatch: expected '{expected_top}' got '{actual_top or 'none'}'")
    else:
        priority_correctness = 10

    scenario_events = expected_outcomes.get("scenario_events", [])
    inferred_conflict = _scenario_has_conflict(scenario_events) if isinstance(scenario_events, list) else False
    must_flag_conflict = bool(expected_outcomes.get("must_flag_conflict", False) or inferred_conflict)

    conflict_rows = brief.get("conflicts", [])
    has_conflicts_in_brief = isinstance(conflict_rows, list) and len(conflict_rows) > 0

    if must_flag_conflict:
        conflict_handling_score = 10 if has_conflicts_in_brief else 0
        if not has_conflicts_in_brief:
            issues.append("expected conflict not surfaced")
    else:
        conflict_handling_score = 10 if not has_conflicts_in_brief else 6

    must_include = [str(x) for x in expected_outcomes.get("must_include", [])]
    if must_include:
        included = sum(1 for title in must_include if title in all_titles or title in top_titles)
        omission_score = _clamp_score((included / len(must_include)) * 10.0)
        if included < len(must_include):
            missing = [title for title in must_include if title not in all_titles and title not in top_titles]
            issues.append(f"omission detected for required items: {', '.join(missing)}")
    else:
        omission_score = 10

    must_not_include_noise = bool(expected_outcomes.get("must_not_include_noise", False))
    allowed_titles = set(required_titles + must_surface + must_include)
    if must_not_include_noise and allowed_titles:
        noise = [title for title in all_titles if title not in allowed_titles]
        noise_ratio = (len(noise) / len(all_titles)) if all_titles else 0.0
        noise_penalty = _clamp_score((1.0 - noise_ratio) * 10.0)
        if noise:
            issues.append(f"noise items surfaced: {', '.join(noise)}")
    else:
        noise_penalty = 10

    return {
        "priority_score": priority_score,
        "relevance_score": relevance_score,
        "completeness_score": completeness_score,
        "clarity_score": clarity_score,
        "priority_correctness": priority_correctness,
        "conflict_handling_score": conflict_handling_score,
        "omission_score": omission_score,
        "noise_penalty": noise_penalty,
        "issues": issues,
    }
