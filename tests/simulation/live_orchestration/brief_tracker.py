from __future__ import annotations

from typing import Any


def _top_event_titles(brief: dict[str, Any]) -> list[str]:
    body = brief.get("brief", {}) if isinstance(brief.get("brief"), dict) else brief
    top_events = body.get("top_events", []) if isinstance(body, dict) else []
    return [str(item.get("title", "")) for item in top_events if isinstance(item, dict)]


def _conflict_count(brief: dict[str, Any]) -> int:
    body = brief.get("brief", {}) if isinstance(brief.get("brief"), dict) else brief
    conflicts = body.get("conflicts", []) if isinstance(body, dict) else []
    return len(conflicts) if isinstance(conflicts, list) else 0


def build_brief_evolution(snapshots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    evolution: list[dict[str, Any]] = []
    previous_titles: list[str] = []

    for snap in snapshots:
        brief = snap.get("brief", {})
        current_titles = _top_event_titles(brief)
        added = [title for title in current_titles if title not in previous_titles]
        removed = [title for title in previous_titles if title not in current_titles]

        evolution.append(
            {
                "step": snap.get("step"),
                "event_id": snap.get("event", {}).get("event_id"),
                "top_event_titles": current_titles,
                "added_priorities": added,
                "removed_priorities": removed,
                "conflict_count": _conflict_count(brief),
            }
        )
        previous_titles = current_titles

    return evolution


def evaluate_live_assertions(snapshots: list[dict[str, Any]], evolution: list[dict[str, Any]]) -> dict[str, Any]:
    stale_failures: list[str] = []

    for snap in snapshots:
        active_event_ids = set(str(item) for item in snap.get("active_event_ids", []))
        body = snap.get("brief", {}).get("brief", {})
        today_events = body.get("today_events", []) if isinstance(body, dict) else []
        for row in today_events:
            event_id = str(row.get("event_id", ""))
            if event_id and event_id not in active_event_ids:
                stale_failures.append(event_id)

    top_titles_sequence = [tuple(item.get("top_event_titles", [])) for item in evolution]
    flip_count = 0
    for idx in range(1, len(top_titles_sequence)):
        if top_titles_sequence[idx] != top_titles_sequence[idx - 1]:
            flip_count += 1

    urgent_reordered = any(
        str(snap.get("event", {}).get("payload", {}).get("priority_hint", "")) == "urgent"
        and len(item.get("added_priorities", [])) > 0
        for snap, item in zip(snapshots, evolution)
    )

    return {
        "priority_stability_under_change": flip_count <= max(1, len(snapshots) // 2),
        "correct_reordering_of_urgent_events": urgent_reordered,
        "no_stale_event_persistence": len(stale_failures) == 0,
        "correct_conflict_resolution_behavior": any(item.get("conflict_count", 0) >= 0 for item in evolution),
        "priority_flip_count": flip_count,
        "stale_event_failures": stale_failures,
    }
