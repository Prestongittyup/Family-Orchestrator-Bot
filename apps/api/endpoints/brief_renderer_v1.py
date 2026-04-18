from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from apps.api.endpoints.brief_contract_v1 import BriefV1


RenderFormat = Literal["text", "markdown"]


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_local_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone()
    return parsed


def _format_local_time(value: Any) -> str:
    parsed = _parse_local_datetime(value)
    if parsed is None:
        return ""
    return parsed.strftime("%I:%M %p").lstrip("0")


def _time_block_from_action(action: dict[str, Any]) -> str:
    parsed = _parse_local_datetime(action.get("start_time"))
    if parsed is not None:
        hour = parsed.hour
        if hour < 12:
            return "morning"
        if hour < 17:
            return "afternoon"
        return "evening"

    bucket = _coerce_str(action.get("time_bucket")).lower()
    if bucket in {"morning", "afternoon", "evening"}:
        return bucket
    return "morning"


def _action_line(action: dict[str, Any]) -> str:
    title = _coerce_str(action.get("title")) or "Untitled action"
    start = _format_local_time(action.get("start_time"))
    end = _format_local_time(action.get("end_time"))

    if start and end:
        return f"- {start}-{end} | {title}"
    if start:
        return f"- {start} | {title}"
    return f"- {title}"


def _unscheduled_line(action: dict[str, Any]) -> str:
    title = _coerce_str(action.get("title")) or "Untitled action"
    reason = _coerce_str(action.get("reason")) or "deferred"
    return f"- {title} ({reason})"


def _priority_line(priority: dict[str, Any]) -> str:
    title = _coerce_str(priority.get("title")) or "Untitled"
    score = priority.get("normalized_priority", priority.get("score"))
    try:
        score_text = f"{float(score):.2f}"
    except (TypeError, ValueError):
        score_text = "0.00"
    return f"- {title} (score {score_text})"


def _signal_line(signal: Any) -> str:
    if isinstance(signal, dict):
        message = _coerce_str(signal.get("message")) or _coerce_str(signal.get("code")) or "item"
        severity = _coerce_str(signal.get("severity"))
        if severity:
            return f"- {message} [{severity}]"
        return f"- {message}"
    return f"- {_coerce_str(signal) or 'item'}"


def _render_text(brief_v1: BriefV1) -> str:
    grouped: dict[str, list[dict[str, Any]]] = {"morning": [], "afternoon": [], "evening": []}
    for action in brief_v1["scheduled_actions"]:
        grouped[_time_block_from_action(action)].append(action)

    lines: list[str] = []
    lines.append("Today's Plan")
    lines.append("Morning")
    lines.extend(_action_line(action) for action in grouped["morning"])
    if not grouped["morning"]:
        lines.append("- None")

    lines.append("Afternoon")
    lines.extend(_action_line(action) for action in grouped["afternoon"])
    if not grouped["afternoon"]:
        lines.append("- None")

    lines.append("Evening")
    lines.extend(_action_line(action) for action in grouped["evening"])
    if not grouped["evening"]:
        lines.append("- None")

    lines.append("")
    lines.append("Unscheduled / Deferred")
    if brief_v1["unscheduled_actions"]:
        lines.extend(_unscheduled_line(action) for action in brief_v1["unscheduled_actions"])
    else:
        lines.append("- None")

    lines.append("")
    lines.append("Priorities")
    if brief_v1["priorities"]:
        lines.extend(_priority_line(row) for row in brief_v1["priorities"])
    else:
        lines.append("- None")

    lines.append("")
    lines.append("Warnings")
    if brief_v1["warnings"]:
        lines.extend(_signal_line(row) for row in brief_v1["warnings"])
    else:
        lines.append("- None")

    lines.append("")
    lines.append("Risks")
    if brief_v1["risks"]:
        lines.extend(_signal_line(row) for row in brief_v1["risks"])
    else:
        lines.append("- None")

    lines.append("")
    lines.append(f"Summary: {brief_v1['summary']}")

    return "\n".join(lines)


def _render_markdown(brief_v1: BriefV1) -> str:
    grouped: dict[str, list[dict[str, Any]]] = {"morning": [], "afternoon": [], "evening": []}
    for action in brief_v1["scheduled_actions"]:
        grouped[_time_block_from_action(action)].append(action)

    lines: list[str] = []
    lines.append("## Today's Plan")
    lines.append("### Morning")
    lines.extend(_action_line(action) for action in grouped["morning"])
    if not grouped["morning"]:
        lines.append("- None")

    lines.append("### Afternoon")
    lines.extend(_action_line(action) for action in grouped["afternoon"])
    if not grouped["afternoon"]:
        lines.append("- None")

    lines.append("### Evening")
    lines.extend(_action_line(action) for action in grouped["evening"])
    if not grouped["evening"]:
        lines.append("- None")

    lines.append("\n## Unscheduled / Deferred")
    if brief_v1["unscheduled_actions"]:
        lines.extend(_unscheduled_line(action) for action in brief_v1["unscheduled_actions"])
    else:
        lines.append("- None")

    lines.append("\n## Priorities")
    if brief_v1["priorities"]:
        lines.extend(_priority_line(row) for row in brief_v1["priorities"])
    else:
        lines.append("- None")

    lines.append("\n## Warnings")
    if brief_v1["warnings"]:
        lines.extend(_signal_line(row) for row in brief_v1["warnings"])
    else:
        lines.append("- None")

    lines.append("\n## Risks")
    if brief_v1["risks"]:
        lines.extend(_signal_line(row) for row in brief_v1["risks"])
    else:
        lines.append("- None")

    lines.append(f"\n**Summary:** {brief_v1['summary']}")
    return "\n".join(lines)


def render_brief_v1(brief_v1: BriefV1, *, output_format: RenderFormat = "text") -> str:
    if output_format == "markdown":
        return _render_markdown(brief_v1)
    return _render_text(brief_v1)
