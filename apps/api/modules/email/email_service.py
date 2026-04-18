from __future__ import annotations

import logging

from apps.api.modules.email.rule_engine import evaluate_email_rules
from apps.api.schemas.events.email_events import EmailReceivedEvent
from apps.api.services.task_service import create_task, update_task_metadata
from apps.api.services.shared_dependencies import get_til

logger = logging.getLogger(__name__)


def handle_email_received(household_id: str, data: EmailReceivedEvent) -> dict:
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
    
    # Original task creation and priority logic (unmodified)
    task = create_task(
        household_id,
        data.subject,
        max_retries=data.max_retries if data.max_retries is not None else 3,
        force_fail=bool(data.force_fail),
    )
    rules = evaluate_email_rules(data)
    final_priority = rules["priority"]
    tags = rules["tags"]
    metadata_category = data.category

    if tags:
        tags_text = ", ".join(tags)
        if metadata_category is None:
            metadata_category = f"Tags: {tags_text}"
        else:
            metadata_category = f"{metadata_category} | Tags: {tags_text}"

    if final_priority != "medium" or metadata_category is not None:
        update_task_metadata(task.id, final_priority, metadata_category)

    return {
        "status": "email_processed",
        "task_title": data.subject,
        "priority": final_priority,
    }
