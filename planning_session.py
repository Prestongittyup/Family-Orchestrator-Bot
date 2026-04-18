from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from brief_endpoint import run_brief_pipeline
from brief_invariants_v1 import project_brief_to_v1
from brief_renderer_v1 import render_brief_v1


_IGNORED_DIFF_KEYS = {
    "proposal_id",
    "start_time",
    "end_time",
    "generated_at",
    "date",
}


@dataclass
class PlanningSessionState:
    household_id: str
    manual_inputs: list[dict[str, Any]] = field(default_factory=list)
    last_brief: dict[str, Any] | None = None
    last_rendered: str | None = None


_SESSIONS: dict[str, PlanningSessionState] = {}


def clear_planning_sessions() -> None:
    _SESSIONS.clear()


def get_or_create_session(household_id: str) -> PlanningSessionState:
    hid = str(household_id or "hh-001").strip() or "hh-001"
    session = _SESSIONS.get(hid)
    if session is None:
        session = PlanningSessionState(household_id=hid)
        _SESSIONS[hid] = session
    return session


def add_session_manual_input(household_id: str, item: dict[str, Any]) -> PlanningSessionState:
    session = get_or_create_session(household_id)
    if isinstance(item, dict):
        session.manual_inputs.append(dict(item))
    return session


def _canonical_action(action: dict[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in action.items()
        if str(key) not in _IGNORED_DIFF_KEYS
    }


def _sorted_actions(brief: dict[str, Any], section: str) -> list[dict[str, Any]]:
    rows = brief.get(section, []) if isinstance(brief, dict) else []
    actions = [row for row in rows if isinstance(row, dict)]
    canonical_rows = [_canonical_action(row) for row in actions]
    return sorted(
        canonical_rows,
        key=lambda row: (
            str(row.get("title", "")).strip().lower(),
            str(sorted(row.items())),
        ),
    )


def compute_brief_diff(
    old_brief: dict[str, Any] | None,
    new_brief: dict[str, Any],
) -> dict[str, Any]:
    old_payload = old_brief if isinstance(old_brief, dict) else {}
    new_payload = new_brief if isinstance(new_brief, dict) else {}

    added: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    changed: list[dict[str, Any]] = []

    for section in ("scheduled_actions", "unscheduled_actions"):
        old_rows = _sorted_actions(old_payload, section)
        new_rows = _sorted_actions(new_payload, section)

        old_by_title: dict[str, list[dict[str, Any]]] = {}
        new_by_title: dict[str, list[dict[str, Any]]] = {}

        for row in old_rows:
            title = str(row.get("title", "")).strip()
            old_by_title.setdefault(title, []).append(row)

        for row in new_rows:
            title = str(row.get("title", "")).strip()
            new_by_title.setdefault(title, []).append(row)

        all_titles = sorted(set(old_by_title.keys()) | set(new_by_title.keys()), key=lambda t: t.lower())

        for title in all_titles:
            old_list = old_by_title.get(title, [])
            new_list = new_by_title.get(title, [])
            overlap = min(len(old_list), len(new_list))

            for idx in range(overlap):
                if old_list[idx] != new_list[idx]:
                    changed.append(
                        {
                            "section": section,
                            "title": title,
                            "before": old_list[idx],
                            "after": new_list[idx],
                        }
                    )

            for idx in range(overlap, len(new_list)):
                added.append(
                    {
                        "section": section,
                        "title": title,
                        "item": new_list[idx],
                    }
                )

            for idx in range(overlap, len(old_list)):
                removed.append(
                    {
                        "section": section,
                        "title": title,
                        "item": old_list[idx],
                    }
                )

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "summary": {
            "added_count": len(added),
            "removed_count": len(removed),
            "changed_count": len(changed),
        },
    }


def refresh_planning_session(household_id: str) -> dict[str, Any]:
    session = get_or_create_session(household_id)
    brief = run_brief_pipeline(manual_items=session.manual_inputs)
    brief_v1 = project_brief_to_v1(brief)
    rendered = render_brief_v1(brief_v1)

    diff = compute_brief_diff(session.last_brief, brief)

    session.last_brief = brief
    session.last_rendered = rendered

    return {
        "household_id": session.household_id,
        "brief": brief,
        "rendered": rendered,
        "diff": diff,
    }