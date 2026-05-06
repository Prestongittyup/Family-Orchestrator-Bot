from __future__ import annotations

import json
import re
from datetime import date
from typing import Any


_AMBIGUITY_SIGNALS = (
    "asap",
    "urgent",
    "deadline",
    "approve",
    "confirmation",
    "confirm",
    "review",
    "payment",
    "invoice",
    "reminder",
    "help",
    "?",
)


def should_refine_email_priority(*, score: int, subject: str, body: str) -> bool:
    if score >= 12:
        return True
    if score < 6:
        return False
    return is_ambiguous(subject=subject, body=body)


def is_ambiguous(*, subject: str, body: str) -> bool:
    merged = f"{subject}\n{body}".lower()
    return any(signal in merged for signal in _AMBIGUITY_SIGNALS)


def parse_refinement_json(text: str) -> dict[str, Any] | None:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = _unwrap_markdown_json(stripped)

    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", stripped, flags=re.DOTALL)
        if not match:
            return None
        try:
            parsed = json.loads(match.group(0))
        except json.JSONDecodeError:
            return None

    if not isinstance(parsed, dict):
        return None
    return parsed


def normalize_refined_email_priority(raw: dict[str, Any], *, subject: str, body: str) -> dict[str, Any]:
    priority = _normalize_priority(raw.get("priority"))
    needs_attention = bool(raw.get("needs_attention", False))
    reason = _compact(str(raw.get("reason", "")), 180)
    state_summary = _normalize_state_summary(raw.get("state_summary"))

    hard_negative = _is_hard_negative(subject=subject, body=body)
    actions = _normalize_actions(raw.get("actions"), subject=subject)

    if hard_negative:
        priority = "low"
        needs_attention = False
        actions = []
        if not reason:
            reason = "Hard-negative promotional or unsubscribe signal."
        if not state_summary:
            state_summary = "Promotional or low-signal thread; no action required."

    if priority == "low" and not actions:
        needs_attention = False

    triage_decision = "task" if (needs_attention or actions) else "informational"
    is_junk = hard_negative and not actions
    if is_junk:
        triage_decision = "junk"

    confidence = _clamp_float(raw.get("confidence", 0.0), minimum=0.0, maximum=1.0)

    return {
        "priority": priority,
        "priority_label": priority,
        "needs_attention": needs_attention,
        "actions": actions,
        "state_summary": state_summary,
        "reason": reason,
        "confidence": confidence,
        "score": _score_for_priority(priority),
        "importance_bucket": _importance_bucket_for_priority(priority),
        "importance_score": _importance_score_for_priority(priority),
        "triage_decision": triage_decision,
        "is_junk": is_junk,
        "llm_refined": True,
    }


def _normalize_actions(value: Any, *, subject: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    actions: list[dict[str, Any]] = []
    for row in value:
        if not isinstance(row, dict):
            continue
        action_type = str(row.get("type", "")).strip().lower()
        if action_type not in {"reply", "task"}:
            continue

        title = str(row.get("title") or "").strip()
        if not title:
            title = f"Reply re: {subject}" if action_type == "reply" else subject.strip() or "Follow up"

        urgency_raw = str(row.get("urgency") or "normal").strip().lower()
        urgency = "high" if urgency_raw == "high" else "normal"

        due = _normalize_due_date(row.get("due"))
        actions.append(
            {
                "type": action_type,
                "title": title[:120],
                "urgency": urgency,
                "due": due,
            }
        )

    return actions[:4]


def _normalize_due_date(value: Any) -> str | None:
    if value in (None, "", "null"):
        return None
    candidate = str(value).strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", candidate):
        return None
    try:
        date.fromisoformat(candidate)
    except ValueError:
        return None
    return candidate


def _normalize_priority(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"high", "medium", "low"}:
        return normalized
    return "medium"


def _normalize_state_summary(value: Any) -> str:
    text = _compact(str(value or ""), 280)
    if not text:
        return "Latest thread state captured."

    sentences = [segment.strip() for segment in re.split(r"(?<=[.!?])\s+", text) if segment.strip()]
    if not sentences:
        return "Latest thread state captured."
    return " ".join(sentences[:2])


def _is_hard_negative(*, subject: str, body: str) -> bool:
    merged = f"{subject}\n{body}".lower()
    return "unsubscribe" in merged or "newsletter" in merged or "marketing" in merged or "promo" in merged


def _score_for_priority(priority: str) -> int:
    if priority == "high":
        return 15
    if priority == "medium":
        return 8
    return 3


def _importance_bucket_for_priority(priority: str) -> str:
    if priority == "high":
        return "high"
    if priority == "medium":
        return "medium"
    return "low"


def _importance_score_for_priority(priority: str) -> float:
    if priority == "high":
        return 0.75
    if priority == "medium":
        return 0.55
    return 0.35


def _compact(text: str, limit: int) -> str:
    collapsed = " ".join((text or "").split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: max(limit - 3, 0)].rstrip() + "..."


def _clamp_float(value: Any, *, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = minimum
    return max(minimum, min(maximum, parsed))


def _unwrap_markdown_json(text: str) -> str:
    lines = [line for line in text.splitlines() if not line.strip().startswith("```")]
    return "\n".join(lines).strip()


__all__ = ["is_ambiguous", "normalize_refined_email_priority", "parse_refinement_json", "should_refine_email_priority"]
