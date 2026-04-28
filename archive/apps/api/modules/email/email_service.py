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

    action_titles = _action_titles(data.action_items, data.subject)
    task_title = action_titles[0]
    if triage_decision == "informational":
        task_title = f"Review email: {data.subject}".strip() or action_titles[0]

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

    if data.summary:
        metadata_segments.append(f"Summary: {data.summary}")
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
        "action_item_count": len(action_titles),
        "importance_bucket": data.importance_bucket,
        "priority": final_priority,
        "calendar_event_id": calendar_event_id,
        "triage_decision": triage_decision,
    }
