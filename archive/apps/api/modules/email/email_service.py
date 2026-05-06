from __future__ import annotations

from datetime import datetime
import logging
from typing import Any

from archive.apps.api.modules.email.rule_engine import evaluate_email_rules
from archive.apps.api.schemas.events.email_events import EmailReceivedEvent
from archive.apps.api.services.calendar_service import schedule_event
from archive.apps.api.services.task_service import create_task, update_task_metadata
from archive.apps.api.services.shared_dependencies import get_til

logger = logging.getLogger(__name__)


def _priority_from_importance_bucket(bucket: str | None) -> str | None:
    if bucket is None:
        return None
    normalized = bucket.strip().lower()
    if normalized in {"critical", "high"}:
        return "high"
    if normalized == "low":
        return "low"
    if normalized == "medium":
        return "medium"
    return None


def _action_titles(action_items: list[dict[str, Any]] | None, fallback_title: str) -> list[str]:
    titles: list[str] = []
    for item in action_items or []:
        title = str(item.get("title", "")).strip()
        if not title:
            continue
        if title in titles:
            continue
        titles.append(title)

    if titles:
        return titles
    return [fallback_title]


def _normalized_actions(data: EmailReceivedEvent) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []

    for row in data.actions or []:
        if not isinstance(row, dict):
            continue
        action_type = str(row.get("type") or "").strip().lower()
        if action_type not in {"reply", "task"}:
            continue
        title = str(row.get("title") or "").strip()
        if not title:
            continue
        urgency_raw = str(row.get("urgency") or "normal").strip().lower()
        urgency = "high" if urgency_raw == "high" else "normal"
        due = str(row.get("due") or "").strip() or None
        normalized.append(
            {
                "type": action_type,
                "title": title,
                "urgency": urgency,
                "due": due,
            }
        )

    if normalized:
        return normalized

    fallback_titles = _action_titles(data.action_items, data.subject)
    return [
        {
            "type": "task",
            "title": title,
            "urgency": "normal",
            "due": None,
        }
        for title in fallback_titles
    ]


def _start_hint_from_due_hint(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if text == "":
        return None
    if "T" in text:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text

        if parsed.tzinfo is not None:
            local_value = parsed.astimezone().replace(tzinfo=None, microsecond=0)
            return local_value.isoformat(timespec="seconds")
        return parsed.replace(microsecond=0).isoformat(timespec="seconds")
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return f"{text}T09:00:00"
    return None


def handle_email_received(household_id: str, data: EmailReceivedEvent) -> dict:
    triage_decision = str(data.triage_decision or "task").strip().lower() or "task"
    if bool(data.is_junk) or triage_decision == "junk":
        return {
            "status": "email_ignored_junk",
            "task_id": None,
            "task_title": None,
            "action_item_count": 0,
            "importance_bucket": data.importance_bucket,
            "priority": "low",
            "calendar_event_id": None,
            "triage_decision": "junk",
        }

    needs_attention = bool(data.needs_attention) if data.needs_attention is not None else triage_decision == "task"
    actions = _normalized_actions(data)

    if not needs_attention and triage_decision == "informational":
        return {
            "status": "email_informational",
            "task_id": None,
            "task_title": None,
            "action_item_count": 0,
            "importance_bucket": data.importance_bucket,
            "priority": _priority_from_importance_bucket(data.importance_bucket) or "low",
            "calendar_event_id": None,
            "triage_decision": "informational",
        }

    # SHADOW MODE: Observe TIL estimates for email event
    til = get_til()
    
    # Estimate task duration for email-received events
    til_duration = til.estimate_duration(
        task_type="email_received",
        payload=data.model_dump() if hasattr(data, "model_dump") else {}
    )
    
    # Suggest optimal time slot based on estimated duration
    til_suggestion = til.suggest_time_slot(
        user_id="system",
        household_id=household_id,
        duration_minutes=til_duration
    )
    
    # Log TIL observations (shadow mode: not used for control flow)
    logger.info(
        f"Email received TIL observation: household={household_id} "
        f"subject={data.subject} "
        f"estimated_duration={til_duration}min "
        f"suggested_time={til_suggestion['start_time']}"
    )

    if not actions:
        actions = [
            {
                "type": "task",
                "title": data.subject,
                "urgency": "normal",
                "due": None,
            }
        ]

    action_titles = [str(action.get("title") or "").strip() for action in actions if str(action.get("title") or "").strip()]
    task_title = action_titles[0] if action_titles else data.subject
    if triage_decision == "informational" and needs_attention:
        task_title = f"Review email: {data.subject}".strip() or task_title

    task = create_task(household_id, task_title)

    rules = evaluate_email_rules(data)
    final_priority = rules["priority"]

    if final_priority == "medium":
        inferred_priority = _priority_from_importance_bucket(data.importance_bucket)
        if inferred_priority is not None:
            final_priority = inferred_priority
    if triage_decision == "informational" and final_priority == "medium":
        final_priority = "low"

    tags = rules["tags"]
    if triage_decision == "informational" and "informational" not in tags:
        tags.append("informational")
    metadata_segments: list[str] = []

    if data.state_summary or data.summary:
        metadata_segments.append(f"Summary: {data.state_summary or data.summary}")
    if data.reason:
        metadata_segments.append(f"Reason: {data.reason}")
    if data.category:
        metadata_segments.append(data.category)
    if len(action_titles) > 1:
        metadata_segments.append(f"Additional actions: {len(action_titles) - 1}")
    if data.calendar_candidates:
        metadata_segments.append(f"Calendar candidates: {len(data.calendar_candidates)}")
    if triage_decision:
        metadata_segments.append(f"Triage: {triage_decision}")
    if data.informational_items:
        metadata_segments.append(f"Informational cues: {len(data.informational_items)}")

    if tags:
        tags_text = ", ".join(tags)
        metadata_segments.append(f"Tags: {tags_text}")

    metadata_category = " | ".join(metadata_segments) if metadata_segments else None

    if final_priority != "medium" or metadata_category is not None:
        update_task_metadata(task.id, final_priority, metadata_category)

    calendar_event_id: str | None = None
    top_candidate = (data.calendar_candidates or [None])[0]
    if isinstance(top_candidate, dict):
        raw_confidence = top_candidate.get("confidence", 0.0)
        try:
            confidence = float(raw_confidence)
        except (TypeError, ValueError):
            confidence = 0.0

        if confidence >= 0.65:
            candidate_title = str(top_candidate.get("title", "")).strip() or action_titles[0]
            due_hint = top_candidate.get("time_hint")
            start_hint = _start_hint_from_due_hint(str(due_hint) if due_hint is not None else None)
            try:
                scheduled = schedule_event(
                    household_id=household_id,
                    user_id="system",
                    title=candidate_title,
                    description=f"Auto-derived from email: {data.subject}",
                    start_time=start_hint,
                )
                if isinstance(scheduled, dict):
                    event_id = scheduled.get("event_id")
                    if event_id is not None:
                        calendar_event_id = str(event_id)
            except Exception as exc:
                logger.warning(
                    "Email calendar candidate scheduling failed: household=%s subject=%s error=%s",
                    household_id,
                    data.subject,
                    exc,
                )

    return {
        "status": "email_processed",
        "task_id": task.id,
        "task_title": task_title,
        "action_item_count": len(actions),
        "importance_bucket": data.importance_bucket,
        "priority": final_priority,
        "calendar_event_id": calendar_event_id,
        "triage_decision": triage_decision,
    }
