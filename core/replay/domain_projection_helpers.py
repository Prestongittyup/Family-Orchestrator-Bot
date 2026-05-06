from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from datetime import datetime
from typing import Any, Mapping


def apply_task_created_projection(
    *,
    payload: Mapping[str, Any],
    recorded_at: str,
    tasks: Mapping[str, Mapping[str, Any]],
    actions: Mapping[str, Mapping[str, Any]],
    responses: Mapping[str, Mapping[str, Any]],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, Any] | None,
]:
    next_tasks = {
        str(task_id): dict(task_payload)
        for task_id, task_payload in tasks.items()
        if isinstance(task_payload, Mapping)
    }
    next_actions = {
        str(action_id): dict(action_payload)
        for action_id, action_payload in actions.items()
        if isinstance(action_payload, Mapping)
    }
    next_responses = {
        str(response_id): dict(response_payload)
        for response_id, response_payload in responses.items()
        if isinstance(response_payload, Mapping)
    }

    task_payload = payload.get("task")
    if not isinstance(task_payload, Mapping):
        return next_tasks, next_actions, next_responses, None

    normalized_task = dict(task_payload)
    if not str(normalized_task.get("created_at") or "").strip():
        normalized_task["created_at"] = recorded_at
    normalized_task.setdefault("completed_at", None)
    if not str(normalized_task.get("status") or "").strip():
        normalized_task["status"] = "pending"

    task_id = str(normalized_task.get("task_id") or "").strip()
    request_id = str(payload.get("request_id") or normalized_task.get("request_id") or "").strip()
    lifecycle_state = str(normalized_task.get("lifecycle_state") or "created")
    approval_status = "pending" if lifecycle_state == "pending_approval" else "committed"

    if task_id:
        next_tasks = {**next_tasks, task_id: dict(normalized_task)}
        next_actions = {
            **next_actions,
            task_id: {
                "request_id": request_id,
                "approval_status": approval_status,
                "approval_required": lifecycle_state == "pending_approval",
                "title": str(normalized_task.get("title") or ""),
            },
        }

    if not request_id:
        return next_tasks, next_actions, next_responses, None

    response_payload = payload.get("response")
    if isinstance(response_payload, Mapping):
        response_row = dict(response_payload)
    else:
        response_row = {
            "request_id": request_id,
            "task": dict(normalized_task),
        }

    next_responses = {**next_responses, request_id: response_row}
    return next_tasks, next_actions, next_responses, response_row


def apply_task_completed_projection(
    *,
    payload: Mapping[str, Any],
    recorded_at: str,
    tasks: Mapping[str, Mapping[str, Any]],
    actions: Mapping[str, Mapping[str, Any]],
    responses: Mapping[str, Mapping[str, Any]],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, Any] | None,
]:
    next_tasks = {
        str(task_key): dict(task_payload)
        for task_key, task_payload in tasks.items()
        if isinstance(task_payload, Mapping)
    }
    next_actions = {
        str(action_id): dict(action_payload)
        for action_id, action_payload in actions.items()
        if isinstance(action_payload, Mapping)
    }
    next_responses = {
        str(response_id): dict(response_payload)
        for response_id, response_payload in responses.items()
        if isinstance(response_payload, Mapping)
    }

    task_id = str(payload.get("task_id") or "").strip()
    request_id = str(payload.get("request_id") or "").strip()
    completed_at = str(payload.get("completed_at") or "").strip() or recorded_at

    task_payload = payload.get("task")
    if task_id and isinstance(next_tasks.get(task_id), Mapping):
        task = dict(next_tasks[task_id])
    elif isinstance(task_payload, Mapping):
        task = dict(task_payload)
        task_id = str(task.get("task_id") or task_id).strip()
    else:
        task = {}

    if task_id and task:
        task["task_id"] = task_id
        task["status"] = "completed"
        task["completed_at"] = completed_at
        task["lifecycle_state"] = "completed"
        if not str(task.get("created_at") or "").strip():
            task["created_at"] = recorded_at
        next_tasks = {**next_tasks, task_id: dict(task)}

        existing_action = next_actions.get(task_id)
        if isinstance(existing_action, Mapping):
            action_row = {
                **dict(existing_action),
                "approval_status": "committed",
            }
        else:
            action_row = {
                "request_id": request_id,
                "approval_status": "committed",
                "approval_required": False,
                "title": str(task.get("title") or ""),
            }
        next_actions = {**next_actions, task_id: action_row}

    response_payload = payload.get("response")
    if request_id and isinstance(response_payload, Mapping):
        response_row = dict(response_payload)
        next_responses = {**next_responses, request_id: response_row}
        return next_tasks, next_actions, next_responses, response_row
    return next_tasks, next_actions, next_responses, None


def apply_schedule_created_projection(
    *,
    payload: Mapping[str, Any],
    recorded_at: str,
    schedules: Mapping[str, Mapping[str, Any]],
    responses: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    next_schedules = {
        str(schedule_id): dict(schedule_payload)
        for schedule_id, schedule_payload in schedules.items()
        if isinstance(schedule_payload, Mapping)
    }
    next_responses = {
        str(response_id): dict(response_payload)
        for response_id, response_payload in responses.items()
        if isinstance(response_payload, Mapping)
    }

    schedule_payload = payload.get("schedule")
    if not isinstance(schedule_payload, Mapping):
        return next_schedules, next_responses

    normalized_schedule = dict(schedule_payload)
    if not str(normalized_schedule.get("created_at") or "").strip():
        normalized_schedule["created_at"] = recorded_at
    normalized_schedule.setdefault("cancelled_at", None)
    if not str(normalized_schedule.get("status") or "").strip():
        normalized_schedule["status"] = "scheduled"

    schedule_id = str(normalized_schedule.get("schedule_id") or "").strip()
    if schedule_id:
        next_schedules = {**next_schedules, schedule_id: dict(normalized_schedule)}

    request_id = str(payload.get("request_id") or normalized_schedule.get("request_id") or "").strip()
    response_payload = payload.get("response")
    if request_id and isinstance(response_payload, Mapping):
        next_responses = {**next_responses, request_id: dict(response_payload)}

    return next_schedules, next_responses


def apply_schedule_cancelled_projection(
    *,
    payload: Mapping[str, Any],
    recorded_at: str,
    schedules: Mapping[str, Mapping[str, Any]],
    responses: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    next_schedules = {
        str(schedule_id): dict(schedule_payload)
        for schedule_id, schedule_payload in schedules.items()
        if isinstance(schedule_payload, Mapping)
    }
    next_responses = {
        str(response_id): dict(response_payload)
        for response_id, response_payload in responses.items()
        if isinstance(response_payload, Mapping)
    }

    schedule_id = str(payload.get("schedule_id") or "").strip()
    request_id = str(payload.get("request_id") or "").strip()
    cancelled_at = str(payload.get("cancelled_at") or "").strip() or recorded_at

    schedule_payload = payload.get("schedule")
    if schedule_id and isinstance(next_schedules.get(schedule_id), Mapping):
        schedule = dict(next_schedules[schedule_id])
    elif isinstance(schedule_payload, Mapping):
        schedule = dict(schedule_payload)
        schedule_id = str(schedule.get("schedule_id") or schedule_id).strip()
    else:
        schedule = {}

    if schedule_id and schedule:
        schedule["schedule_id"] = schedule_id
        schedule["status"] = "cancelled"
        schedule["cancelled_at"] = cancelled_at
        if not str(schedule.get("created_at") or "").strip():
            schedule["created_at"] = recorded_at
        next_schedules = {**next_schedules, schedule_id: dict(schedule)}

    response_payload = payload.get("response")
    if request_id and isinstance(response_payload, Mapping):
        next_responses = {**next_responses, request_id: dict(response_payload)}

    return next_schedules, next_responses


def apply_reminder_created_projection(
    *,
    payload: Mapping[str, Any],
    recorded_at: str,
    reminders: Mapping[str, Mapping[str, Any]],
    responses: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    next_reminders = {
        str(reminder_id): dict(reminder_payload)
        for reminder_id, reminder_payload in reminders.items()
        if isinstance(reminder_payload, Mapping)
    }
    next_responses = {
        str(response_id): dict(response_payload)
        for response_id, response_payload in responses.items()
        if isinstance(response_payload, Mapping)
    }

    reminder_payload = payload.get("reminder")
    if not isinstance(reminder_payload, Mapping):
        return next_reminders, next_responses

    normalized_reminder = dict(reminder_payload)
    if not str(normalized_reminder.get("created_at") or "").strip():
        normalized_reminder["created_at"] = recorded_at
    normalized_reminder.setdefault("triggered_at", None)
    if not str(normalized_reminder.get("status") or "").strip():
        normalized_reminder["status"] = "active"

    reminder_id = str(normalized_reminder.get("reminder_id") or "").strip()
    if reminder_id:
        next_reminders = {**next_reminders, reminder_id: dict(normalized_reminder)}

    request_id = str(payload.get("request_id") or normalized_reminder.get("request_id") or "").strip()
    response_payload = payload.get("response")
    if request_id and isinstance(response_payload, Mapping):
        next_responses = {**next_responses, request_id: dict(response_payload)}

    return next_reminders, next_responses


def apply_reminder_cancelled_projection(
    *,
    payload: Mapping[str, Any],
    reminders: Mapping[str, Mapping[str, Any]],
    responses: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    next_reminders = {
        str(reminder_id): dict(reminder_payload)
        for reminder_id, reminder_payload in reminders.items()
        if isinstance(reminder_payload, Mapping)
    }
    next_responses = {
        str(response_id): dict(response_payload)
        for response_id, response_payload in responses.items()
        if isinstance(response_payload, Mapping)
    }

    reminder_id = str(payload.get("reminder_id") or "").strip()
    request_id = str(payload.get("request_id") or "").strip()

    reminder_payload = payload.get("reminder")
    if reminder_id and isinstance(next_reminders.get(reminder_id), Mapping):
        reminder = dict(next_reminders[reminder_id])
    elif isinstance(reminder_payload, Mapping):
        reminder = dict(reminder_payload)
        reminder_id = str(reminder.get("reminder_id") or reminder_id).strip()
    else:
        reminder = {}

    if reminder_id and reminder:
        reminder["reminder_id"] = reminder_id
        reminder["status"] = "cancelled"
        next_reminders = {**next_reminders, reminder_id: dict(reminder)}

    response_payload = payload.get("response")
    if request_id and isinstance(response_payload, Mapping):
        next_responses = {**next_responses, request_id: dict(response_payload)}

    return next_reminders, next_responses


def apply_reminder_triggered_projection(
    *,
    payload: Mapping[str, Any],
    recorded_at: str,
    reminders: Mapping[str, Mapping[str, Any]],
    responses: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    next_reminders = {
        str(reminder_id): dict(reminder_payload)
        for reminder_id, reminder_payload in reminders.items()
        if isinstance(reminder_payload, Mapping)
    }
    next_responses = {
        str(response_id): dict(response_payload)
        for response_id, response_payload in responses.items()
        if isinstance(response_payload, Mapping)
    }

    reminder_id = str(payload.get("reminder_id") or "").strip()
    request_id = str(payload.get("request_id") or "").strip()
    triggered_at = str(payload.get("triggered_at") or "").strip() or recorded_at

    reminder_payload = payload.get("reminder")
    if reminder_id and isinstance(next_reminders.get(reminder_id), Mapping):
        reminder = dict(next_reminders[reminder_id])
    elif isinstance(reminder_payload, Mapping):
        reminder = dict(reminder_payload)
        reminder_id = str(reminder.get("reminder_id") or reminder_id).strip()
    else:
        reminder = {}

    if reminder_id and reminder:
        reminder["reminder_id"] = reminder_id
        reminder["status"] = "triggered"
        reminder["triggered_at"] = triggered_at
        next_reminders = {**next_reminders, reminder_id: dict(reminder)}

    response_payload = payload.get("response")
    if request_id and isinstance(response_payload, Mapping):
        next_responses = {**next_responses, request_id: dict(response_payload)}

    return next_reminders, next_responses


def apply_decision_completed_projection(
    *,
    payload: Mapping[str, Any],
    recorded_at: str,
    decisions: Mapping[str, Mapping[str, Any]],
    tasks: Mapping[str, Mapping[str, Any]],
    schedules: Mapping[str, Mapping[str, Any]],
    reminders: Mapping[str, Mapping[str, Any]],
    actions: Mapping[str, Mapping[str, Any]],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    next_decisions = {
        str(decision_id): dict(decision_payload)
        for decision_id, decision_payload in decisions.items()
        if isinstance(decision_payload, Mapping)
    }
    next_tasks = {
        str(task_id): dict(task_payload)
        for task_id, task_payload in tasks.items()
        if isinstance(task_payload, Mapping)
    }
    next_schedules = {
        str(schedule_id): dict(schedule_payload)
        for schedule_id, schedule_payload in schedules.items()
        if isinstance(schedule_payload, Mapping)
    }
    next_reminders = {
        str(reminder_id): dict(reminder_payload)
        for reminder_id, reminder_payload in reminders.items()
        if isinstance(reminder_payload, Mapping)
    }
    next_actions = {
        str(action_id): dict(action_payload)
        for action_id, action_payload in actions.items()
        if isinstance(action_payload, Mapping)
    }

    decision_id = str(payload.get("decision_id") or "").strip()
    if not decision_id:
        return next_decisions, next_tasks, next_schedules, next_reminders, next_actions

    actor_id = str(payload.get("actor_id") or "").strip()
    decision_timestamp = str(payload.get("timestamp") or "").strip() or recorded_at
    previous_decision = dict(next_decisions.get(decision_id) or {})
    previous_decision.update(
        {
            "decision_id": decision_id,
            "state": "completed",
            "actor_id": actor_id,
            "timestamp": decision_timestamp,
        }
    )
    previous_decision.pop("defer_to_date", None)
    next_decisions = {**next_decisions, decision_id: previous_decision}

    if decision_id in next_tasks:
        updated_task = dict(next_tasks[decision_id])
        updated_task["status"] = "completed"
        updated_task["lifecycle_state"] = "completed"
        updated_task["completed_at"] = decision_timestamp
        next_tasks = {**next_tasks, decision_id: updated_task}

    if decision_id in next_schedules:
        updated_schedule = dict(next_schedules[decision_id])
        updated_schedule["status"] = "cancelled"
        updated_schedule["cancelled_at"] = decision_timestamp
        next_schedules = {**next_schedules, decision_id: updated_schedule}

    if decision_id in next_reminders:
        updated_reminder = dict(next_reminders[decision_id])
        updated_reminder["status"] = "triggered"
        updated_reminder["triggered_at"] = decision_timestamp
        next_reminders = {**next_reminders, decision_id: updated_reminder}

    if decision_id in next_actions:
        updated_action = dict(next_actions[decision_id])
        updated_action["approval_status"] = "committed"
        next_actions = {**next_actions, decision_id: updated_action}

    return next_decisions, next_tasks, next_schedules, next_reminders, next_actions


def apply_decision_deferred_projection(
    *,
    payload: Mapping[str, Any],
    recorded_at: str,
    decisions: Mapping[str, Mapping[str, Any]],
    tasks: Mapping[str, Mapping[str, Any]],
    schedules: Mapping[str, Mapping[str, Any]],
    reminders: Mapping[str, Mapping[str, Any]],
    actions: Mapping[str, Mapping[str, Any]],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    next_decisions = {
        str(decision_id): dict(decision_payload)
        for decision_id, decision_payload in decisions.items()
        if isinstance(decision_payload, Mapping)
    }
    next_tasks = {
        str(task_id): dict(task_payload)
        for task_id, task_payload in tasks.items()
        if isinstance(task_payload, Mapping)
    }
    next_schedules = {
        str(schedule_id): dict(schedule_payload)
        for schedule_id, schedule_payload in schedules.items()
        if isinstance(schedule_payload, Mapping)
    }
    next_reminders = {
        str(reminder_id): dict(reminder_payload)
        for reminder_id, reminder_payload in reminders.items()
        if isinstance(reminder_payload, Mapping)
    }
    next_actions = {
        str(action_id): dict(action_payload)
        for action_id, action_payload in actions.items()
        if isinstance(action_payload, Mapping)
    }

    decision_id = str(payload.get("decision_id") or "").strip()
    if not decision_id:
        return next_decisions, next_tasks, next_schedules, next_reminders, next_actions

    actor_id = str(payload.get("actor_id") or "").strip()
    defer_to_date = str(payload.get("defer_to_date") or "").strip()
    decision_timestamp = str(payload.get("timestamp") or "").strip() or recorded_at

    previous_decision = dict(next_decisions.get(decision_id) or {})
    previous_decision.update(
        {
            "decision_id": decision_id,
            "state": "deferred",
            "actor_id": actor_id,
            "defer_to_date": defer_to_date,
            "timestamp": decision_timestamp,
        }
    )
    next_decisions = {**next_decisions, decision_id: previous_decision}

    if decision_id in next_tasks:
        updated_task = dict(next_tasks[decision_id])
        updated_task["status"] = "pending"
        updated_task["lifecycle_state"] = "deferred"
        updated_task["completed_at"] = None
        updated_task["due_at"] = defer_to_date
        updated_task["due_date"] = defer_to_date
        updated_task["defer_to_date"] = defer_to_date
        next_tasks = {**next_tasks, decision_id: updated_task}

    if decision_id in next_schedules:
        updated_schedule = dict(next_schedules[decision_id])
        updated_schedule["status"] = "scheduled"
        updated_schedule["start_at"] = defer_to_date
        updated_schedule["defer_to_date"] = defer_to_date
        next_schedules = {**next_schedules, decision_id: updated_schedule}

    if decision_id in next_reminders:
        updated_reminder = dict(next_reminders[decision_id])
        updated_reminder["status"] = "active"
        updated_reminder["trigger_at"] = defer_to_date
        updated_reminder["triggered_at"] = None
        updated_reminder["defer_to_date"] = defer_to_date
        next_reminders = {**next_reminders, decision_id: updated_reminder}

    if decision_id in next_actions:
        updated_action = dict(next_actions[decision_id])
        updated_action["approval_status"] = "pending"
        updated_action["defer_to_date"] = defer_to_date
        next_actions = {**next_actions, decision_id: updated_action}

    return next_decisions, next_tasks, next_schedules, next_reminders, next_actions


def apply_decision_ignored_projection(
    *,
    payload: Mapping[str, Any],
    recorded_at: str,
    decisions: Mapping[str, Mapping[str, Any]],
    tasks: Mapping[str, Mapping[str, Any]],
    schedules: Mapping[str, Mapping[str, Any]],
    reminders: Mapping[str, Mapping[str, Any]],
    actions: Mapping[str, Mapping[str, Any]],
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    next_decisions = {
        str(decision_id): dict(decision_payload)
        for decision_id, decision_payload in decisions.items()
        if isinstance(decision_payload, Mapping)
    }
    next_tasks = {
        str(task_id): dict(task_payload)
        for task_id, task_payload in tasks.items()
        if isinstance(task_payload, Mapping)
    }
    next_schedules = {
        str(schedule_id): dict(schedule_payload)
        for schedule_id, schedule_payload in schedules.items()
        if isinstance(schedule_payload, Mapping)
    }
    next_reminders = {
        str(reminder_id): dict(reminder_payload)
        for reminder_id, reminder_payload in reminders.items()
        if isinstance(reminder_payload, Mapping)
    }
    next_actions = {
        str(action_id): dict(action_payload)
        for action_id, action_payload in actions.items()
        if isinstance(action_payload, Mapping)
    }

    decision_id = str(payload.get("decision_id") or "").strip()
    if not decision_id:
        return next_decisions, next_tasks, next_schedules, next_reminders, next_actions

    actor_id = str(payload.get("actor_id") or "").strip()
    decision_timestamp = str(payload.get("timestamp") or "").strip() or recorded_at
    previous_decision = dict(next_decisions.get(decision_id) or {})
    previous_decision.update(
        {
            "decision_id": decision_id,
            "state": "ignored",
            "actor_id": actor_id,
            "timestamp": decision_timestamp,
        }
    )
    previous_decision.pop("defer_to_date", None)
    next_decisions = {**next_decisions, decision_id: previous_decision}

    if decision_id in next_tasks:
        updated_task = dict(next_tasks[decision_id])
        updated_task["status"] = "ignored"
        updated_task["lifecycle_state"] = "ignored"
        updated_task["ignored_at"] = decision_timestamp
        next_tasks = {**next_tasks, decision_id: updated_task}

    if decision_id in next_schedules:
        updated_schedule = dict(next_schedules[decision_id])
        updated_schedule["status"] = "cancelled"
        updated_schedule["cancelled_at"] = decision_timestamp
        next_schedules = {**next_schedules, decision_id: updated_schedule}

    if decision_id in next_reminders:
        updated_reminder = dict(next_reminders[decision_id])
        updated_reminder["status"] = "cancelled"
        next_reminders = {**next_reminders, decision_id: updated_reminder}

    if decision_id in next_actions:
        updated_action = dict(next_actions[decision_id])
        updated_action["approval_status"] = "rejected"
        next_actions = {**next_actions, decision_id: updated_action}

    return next_decisions, next_tasks, next_schedules, next_reminders, next_actions


_NOTIFICATION_ALLOWED_EVENT_SOURCES = frozenset({"ReminderTriggered"})


def allowed_notification_event_sources() -> tuple[str, ...]:
    return tuple(sorted(_NOTIFICATION_ALLOWED_EVENT_SOURCES))


def _notification_message_from_reminder_payload(*, payload: Mapping[str, Any], reminder_id: str) -> str:
    reminder_payload = payload.get("reminder")
    title = ""
    message = ""
    if isinstance(reminder_payload, Mapping):
        title = str(reminder_payload.get("title") or "").strip()
        message = str(reminder_payload.get("message") or "").strip()

    if not message and title:
        message = title
    if not message:
        message = f"Reminder triggered: {reminder_id}"
    return message


def build_notification_from_reminder_triggered(
    *,
    payload: Mapping[str, Any],
    source_event_id: str,
    recorded_at: str,
) -> dict[str, Any] | None:
    event_id = str(source_event_id or "").strip()
    if not event_id:
        return None

    reminder_payload = payload.get("reminder")
    reminder_id = str(payload.get("reminder_id") or "").strip()
    if isinstance(reminder_payload, Mapping):
        reminder_id = str(reminder_payload.get("reminder_id") or reminder_id).strip()

    if not reminder_id:
        return None

    notification_id = f"notification-{event_id}"
    return {
        "notification_id": notification_id,
        "source_event_id": event_id,
        "source_type": "reminder",
        "source_id": reminder_id,
        "message": _notification_message_from_reminder_payload(
            payload=payload,
            reminder_id=reminder_id,
        ),
        "created_at": str(recorded_at or "").strip(),
        "delivery_status": "pending",
    }


def derive_notification_from_reminder_triggered(
    *,
    payload: Mapping[str, Any],
    source_event_id: str,
    recorded_at: str,
    notifications: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any] | None]:
    next_notifications = {
        str(notification_id): dict(notification_payload)
        for notification_id, notification_payload in notifications.items()
        if isinstance(notification_payload, Mapping)
    }

    notification_row = build_notification_from_reminder_triggered(
        payload=payload,
        source_event_id=source_event_id,
        recorded_at=recorded_at,
    )
    if notification_row is None:
        return next_notifications, None

    notification_id = str(notification_row.get("notification_id") or "").strip()
    if not notification_id:
        return next_notifications, None

    next_notifications = {**next_notifications, notification_id: dict(notification_row)}
    return next_notifications, notification_row


def _copy_mapping_rows(raw_value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(raw_value, Mapping):
        return {}
    return {
        str(row_id): dict(row_payload)
        for row_id, row_payload in raw_value.items()
        if isinstance(row_payload, Mapping)
    }


def _sorted_unique_strings(raw_values: Any) -> list[str]:
    if not isinstance(raw_values, list):
        return []
    return sorted({str(item).strip() for item in raw_values if str(item).strip()})


def _extract_projection_date(raw_timestamp: str) -> str:
    normalized = str(raw_timestamp or "").strip()
    if not normalized:
        return ""
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return ""
    return parsed.date().isoformat()


def _record_family_coordination_response(
    *,
    event_type: str,
    payload: Mapping[str, Any],
    responses: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    next_responses = {
        str(response_id): dict(response_payload)
        for response_id, response_payload in responses.items()
        if isinstance(response_payload, Mapping)
    }

    request_id = str(payload.get("request_id") or "").strip()
    if not request_id:
        return next_responses

    response_payload = payload.get("response")
    if isinstance(response_payload, Mapping):
        next_responses = {**next_responses, request_id: dict(response_payload)}
        return next_responses

    next_responses = {
        **next_responses,
        request_id: {
            "request_id": request_id,
            "surface": "family_coordination",
            "event_type": event_type,
        },
    }
    return next_responses


def _normalize_family_coordination_state(surface_state: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(surface_state, Mapping):
        return {
            "members": {},
            "responsibilities": {},
            "events": {},
            "execution_states": {},
            "conflicts": {},
        }

    return {
        "members": _copy_mapping_rows(surface_state.get("members")),
        "responsibilities": _copy_mapping_rows(surface_state.get("responsibilities")),
        "events": _copy_mapping_rows(surface_state.get("events")),
        "execution_states": _copy_mapping_rows(surface_state.get("execution_states")),
        "conflicts": _copy_mapping_rows(surface_state.get("conflicts")),
    }


def apply_family_coordination_event_projection(
    *,
    event_type: str,
    payload: Mapping[str, Any],
    recorded_at: str,
    surface_state: Mapping[str, Any] | None,
    responses: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    normalized_state = _normalize_family_coordination_state(surface_state)

    members = normalized_state["members"]
    responsibilities = normalized_state["responsibilities"]
    coordination_events = normalized_state["events"]
    execution_states = normalized_state["execution_states"]
    conflicts = normalized_state["conflicts"]

    next_responses = _record_family_coordination_response(
        event_type=event_type,
        payload=payload,
        responses=responses,
    )

    if event_type in {"HouseholdMemberAdded", "HouseholdMemberUpdated"}:
        member_payload = payload.get("member")
        raw_member = dict(member_payload) if isinstance(member_payload, Mapping) else dict(payload)
        member_id = str(raw_member.get("member_id") or payload.get("member_id") or "").strip()
        if member_id:
            existing_member = dict(members.get(member_id) or {})
            merged_member = {
                **existing_member,
                **raw_member,
                "member_id": member_id,
                "updated_at": recorded_at,
            }
            merged_member.setdefault("created_at", recorded_at)
            merged_member.setdefault("status", "active")
            members = {**members, member_id: merged_member}

    elif event_type == "ResponsibilityCreated":
        responsibility_payload = payload.get("responsibility")
        raw_responsibility = (
            dict(responsibility_payload) if isinstance(responsibility_payload, Mapping) else dict(payload)
        )
        responsibility_id = str(
            raw_responsibility.get("responsibility_id") or payload.get("responsibility_id") or ""
        ).strip()
        if responsibility_id:
            existing_responsibility = dict(responsibilities.get(responsibility_id) or {})
            assigned_member_ids = _sorted_unique_strings(raw_responsibility.get("assigned_member_ids"))
            if not assigned_member_ids:
                fallback_member_id = str(
                    raw_responsibility.get("assignee_member_id")
                    or raw_responsibility.get("owner_member_id")
                    or ""
                ).strip()
                if fallback_member_id:
                    assigned_member_ids = [fallback_member_id]
            merged_responsibility = {
                **existing_responsibility,
                **raw_responsibility,
                "responsibility_id": responsibility_id,
                "assigned_member_ids": assigned_member_ids,
                "updated_at": recorded_at,
            }
            merged_responsibility.setdefault("created_at", recorded_at)
            merged_responsibility.setdefault("status", "active")
            responsibilities = {**responsibilities, responsibility_id: merged_responsibility}

    elif event_type == "ResponsibilityAssigned":
        responsibility_id = str(payload.get("responsibility_id") or "").strip()
        if responsibility_id:
            existing_responsibility = dict(responsibilities.get(responsibility_id) or {})
            existing_assignees = _sorted_unique_strings(existing_responsibility.get("assigned_member_ids"))
            next_assignees = _sorted_unique_strings(payload.get("assigned_member_ids"))
            fallback_assignee = str(payload.get("assignee_member_id") or payload.get("member_id") or "").strip()
            if fallback_assignee:
                next_assignees = sorted({*next_assignees, fallback_assignee})

            merged_responsibility = {
                **existing_responsibility,
                "responsibility_id": responsibility_id,
                "assigned_member_ids": sorted({*existing_assignees, *next_assignees}),
                "last_assigned_at": recorded_at,
                "updated_at": recorded_at,
            }
            merged_responsibility.setdefault("created_at", recorded_at)
            merged_responsibility.setdefault("status", "active")
            responsibilities = {**responsibilities, responsibility_id: merged_responsibility}

    elif event_type == "ResponsibilityUpdated":
        responsibility_payload = payload.get("responsibility")
        raw_responsibility = (
            dict(responsibility_payload) if isinstance(responsibility_payload, Mapping) else dict(payload)
        )
        responsibility_id = str(
            raw_responsibility.get("responsibility_id") or payload.get("responsibility_id") or ""
        ).strip()
        if responsibility_id:
            existing_responsibility = dict(responsibilities.get(responsibility_id) or {})
            merged_responsibility = {
                **existing_responsibility,
                **raw_responsibility,
                "responsibility_id": responsibility_id,
                "updated_at": recorded_at,
            }
            assigned_member_ids = _sorted_unique_strings(merged_responsibility.get("assigned_member_ids"))
            if assigned_member_ids:
                merged_responsibility["assigned_member_ids"] = assigned_member_ids
            merged_responsibility.setdefault("created_at", recorded_at)
            merged_responsibility.setdefault("status", "active")
            responsibilities = {**responsibilities, responsibility_id: merged_responsibility}

    elif event_type == "EventScheduled":
        event_payload = payload.get("coordination_event")
        raw_event = dict(event_payload) if isinstance(event_payload, Mapping) else dict(payload)
        coordination_event_id = str(
            raw_event.get("coordination_event_id")
            or raw_event.get("event_id")
            or payload.get("coordination_event_id")
            or ""
        ).strip()
        if coordination_event_id:
            existing_event = dict(coordination_events.get(coordination_event_id) or {})
            merged_event = {
                **existing_event,
                **raw_event,
                "coordination_event_id": coordination_event_id,
                "updated_at": recorded_at,
            }
            merged_event.setdefault("created_at", recorded_at)
            merged_event.setdefault("status", "scheduled")
            coordination_events = {**coordination_events, coordination_event_id: merged_event}

    elif event_type == "EventRescheduled":
        event_payload = payload.get("coordination_event")
        raw_event = dict(event_payload) if isinstance(event_payload, Mapping) else dict(payload)
        coordination_event_id = str(
            raw_event.get("coordination_event_id")
            or raw_event.get("event_id")
            or payload.get("coordination_event_id")
            or ""
        ).strip()
        if coordination_event_id:
            existing_event = dict(coordination_events.get(coordination_event_id) or {})
            merged_event = {
                **existing_event,
                **raw_event,
                "coordination_event_id": coordination_event_id,
                "status": "rescheduled",
                "rescheduled_at": recorded_at,
                "updated_at": recorded_at,
            }
            merged_event.setdefault("created_at", recorded_at)
            coordination_events = {**coordination_events, coordination_event_id: merged_event}

    elif event_type == "EventCancelled":
        event_payload = payload.get("coordination_event")
        raw_event = dict(event_payload) if isinstance(event_payload, Mapping) else dict(payload)
        coordination_event_id = str(
            raw_event.get("coordination_event_id")
            or raw_event.get("event_id")
            or payload.get("coordination_event_id")
            or ""
        ).strip()
        if coordination_event_id:
            existing_event = dict(coordination_events.get(coordination_event_id) or {})
            merged_event = {
                **existing_event,
                **raw_event,
                "coordination_event_id": coordination_event_id,
                "status": "cancelled",
                "cancelled_at": str(payload.get("cancelled_at") or recorded_at),
                "updated_at": recorded_at,
            }
            merged_event.setdefault("created_at", recorded_at)
            coordination_events = {**coordination_events, coordination_event_id: merged_event}

    elif event_type == "ExecutionStateChanged":
        target_type = str(payload.get("target_type") or "").strip().lower()
        target_id = str(payload.get("target_id") or "").strip()
        execution_state = str(payload.get("execution_state") or payload.get("state") or "").strip().lower()
        if target_type and target_id and execution_state:
            execution_key = f"{target_type}:{target_id}"
            execution_row = {
                "execution_key": execution_key,
                "target_type": target_type,
                "target_id": target_id,
                "execution_state": execution_state,
                "reason": str(payload.get("reason") or "").strip(),
                "updated_at": recorded_at,
            }
            execution_states = {**execution_states, execution_key: execution_row}

            if target_type == "responsibility":
                existing_responsibility = dict(responsibilities.get(target_id) or {})
                if existing_responsibility:
                    existing_responsibility["execution_state"] = execution_state
                    existing_responsibility["updated_at"] = recorded_at
                    responsibilities = {**responsibilities, target_id: existing_responsibility}
            elif target_type in {"event", "coordination_event"}:
                existing_event = dict(coordination_events.get(target_id) or {})
                if existing_event:
                    existing_event["execution_state"] = execution_state
                    existing_event["updated_at"] = recorded_at
                    coordination_events = {**coordination_events, target_id: existing_event}

    elif event_type == "ConflictDetected":
        conflict_payload = payload.get("conflict")
        raw_conflict = dict(conflict_payload) if isinstance(conflict_payload, Mapping) else dict(payload)
        conflict_id = str(raw_conflict.get("conflict_id") or payload.get("conflict_id") or "").strip()
        if conflict_id:
            existing_conflict = dict(conflicts.get(conflict_id) or {})
            merged_conflict = {
                **existing_conflict,
                **raw_conflict,
                "conflict_id": conflict_id,
                "resolution_state": "open",
                "updated_at": recorded_at,
            }
            merged_conflict.setdefault("created_at", recorded_at)
            merged_conflict.setdefault("severity", "medium")
            merged_conflict.setdefault("conflict_type", "unspecified")
            related_entity_ids = _sorted_unique_strings(merged_conflict.get("related_entity_ids"))
            if related_entity_ids:
                merged_conflict["related_entity_ids"] = related_entity_ids
            conflicts = {**conflicts, conflict_id: merged_conflict}

    elif event_type == "ConflictResolved":
        conflict_payload = payload.get("conflict")
        raw_conflict = dict(conflict_payload) if isinstance(conflict_payload, Mapping) else dict(payload)
        conflict_id = str(raw_conflict.get("conflict_id") or payload.get("conflict_id") or "").strip()
        if conflict_id:
            existing_conflict = dict(conflicts.get(conflict_id) or {"conflict_id": conflict_id})
            merged_conflict = {
                **existing_conflict,
                **raw_conflict,
                "conflict_id": conflict_id,
                "resolution_state": "resolved",
                "resolved_at": str(payload.get("resolved_at") or recorded_at),
                "updated_at": recorded_at,
            }
            merged_conflict.setdefault("created_at", recorded_at)
            merged_conflict.setdefault("severity", "medium")
            merged_conflict.setdefault("conflict_type", "unspecified")
            conflicts = {**conflicts, conflict_id: merged_conflict}

    next_surface_state = {
        "members": members,
        "responsibilities": responsibilities,
        "events": coordination_events,
        "execution_states": execution_states,
        "conflicts": conflicts,
    }
    return next_surface_state, next_responses


def derive_family_coordination_surface_projections(
    *,
    partition_id: str,
    state: Mapping[str, Any],
    reference_timestamp: str,
) -> dict[str, Any]:
    normalized_state = _normalize_family_coordination_state(state)

    members_by_id = normalized_state["members"]
    responsibilities_by_id = normalized_state["responsibilities"]
    events_by_id = normalized_state["events"]
    execution_states_by_key = normalized_state["execution_states"]
    conflicts_by_id = normalized_state["conflicts"]

    projection_date = _extract_projection_date(reference_timestamp)

    member_rows = [
        dict(member_payload)
        for _member_id, member_payload in sorted(members_by_id.items(), key=lambda item: item[0])
    ]
    responsibility_rows = [
        dict(responsibility_payload)
        for _responsibility_id, responsibility_payload in sorted(
            responsibilities_by_id.items(), key=lambda item: item[0]
        )
    ]
    event_rows = [
        dict(event_payload)
        for _event_id, event_payload in sorted(events_by_id.items(), key=lambda item: item[0])
    ]
    execution_state_rows = [
        dict(state_payload)
        for _execution_key, state_payload in sorted(
            execution_states_by_key.items(), key=lambda item: item[0]
        )
    ]

    today_events = [
        dict(event_row)
        for event_row in event_rows
        if str(event_row.get("status") or "scheduled").strip().lower() != "cancelled"
        and (
            not projection_date
            or _extract_projection_date(str(event_row.get("start_at") or "")) == projection_date
        )
    ]
    today_events.sort(
        key=lambda event_row: (
            str(event_row.get("start_at") or ""),
            str(event_row.get("coordination_event_id") or event_row.get("event_id") or ""),
        )
    )

    responsibilities_with_today_events = {
        str(event_row.get("responsibility_id") or "").strip()
        for event_row in today_events
        if str(event_row.get("responsibility_id") or "").strip()
    }

    today_responsibilities = []
    for responsibility_id, responsibility_row in sorted(responsibilities_by_id.items(), key=lambda item: item[0]):
        responsibility_due_date = _extract_projection_date(
            str(responsibility_row.get("due_at") or responsibility_row.get("next_due_at") or "")
        )
        include_row = (
            not projection_date
            or responsibility_due_date == projection_date
            or responsibility_id in responsibilities_with_today_events
        )
        if include_row:
            today_responsibilities.append(dict(responsibility_row))

    member_ids_with_assignments = {
        member_id
        for responsibility_row in today_responsibilities
        for member_id in _sorted_unique_strings(responsibility_row.get("assigned_member_ids"))
    }

    open_conflicts = [
        dict(conflict_row)
        for _conflict_id, conflict_row in sorted(conflicts_by_id.items(), key=lambda item: item[0])
        if str(conflict_row.get("resolution_state") or "open").strip().lower() != "resolved"
    ]
    resolved_conflicts = [
        dict(conflict_row)
        for _conflict_id, conflict_row in sorted(conflicts_by_id.items(), key=lambda item: item[0])
        if str(conflict_row.get("resolution_state") or "open").strip().lower() == "resolved"
    ]

    overlap_conflicts = [
        dict(conflict_row)
        for conflict_row in open_conflicts
        if str(conflict_row.get("conflict_type") or "").strip().lower() in {"overlap", "schedule_overlap"}
    ]
    missing_coverage_conflicts = [
        dict(conflict_row)
        for conflict_row in open_conflicts
        if str(conflict_row.get("conflict_type") or "").strip().lower() in {"missing_coverage", "coverage_gap"}
    ]
    duplication_conflicts = [
        dict(conflict_row)
        for conflict_row in open_conflicts
        if str(conflict_row.get("conflict_type") or "").strip().lower() in {"duplication", "duplicate"}
    ]

    coverage_gaps = [
        str(responsibility_row.get("responsibility_id") or "").strip()
        for responsibility_row in today_responsibilities
        if not _sorted_unique_strings(responsibility_row.get("assigned_member_ids"))
    ]
    coverage_gaps = sorted({item for item in coverage_gaps if item})

    duplicate_title_index: dict[str, list[str]] = {}
    for responsibility_row in today_responsibilities:
        normalized_title = str(responsibility_row.get("title") or "").strip().lower()
        responsibility_id = str(responsibility_row.get("responsibility_id") or "").strip()
        if not normalized_title or not responsibility_id:
            continue
        duplicate_title_index.setdefault(normalized_title, []).append(responsibility_id)

    duplicate_assignments = [
        {
            "title": title,
            "responsibility_ids": sorted(set(responsibility_ids)),
        }
        for title, responsibility_ids in sorted(duplicate_title_index.items(), key=lambda item: item[0])
        if len(set(responsibility_ids)) > 1
    ]

    return {
        "HouseholdStateProjection": {
            "partition_id": str(partition_id or ""),
            "members": member_rows,
            "responsibilities": responsibility_rows,
            "events": event_rows,
            "execution_states": execution_state_rows,
            "updated_at": reference_timestamp,
        },
        "TodayViewProjection": {
            "partition_id": str(partition_id or ""),
            "date": projection_date,
            "members_on_duty": sorted(member_ids_with_assignments),
            "responsibilities": today_responsibilities,
            "events": today_events,
            "open_conflict_ids": sorted(
                str(conflict_row.get("conflict_id") or "").strip()
                for conflict_row in open_conflicts
                if str(conflict_row.get("conflict_id") or "").strip()
            ),
            "coverage_gaps": coverage_gaps,
            "duplicate_assignments": duplicate_assignments,
        },
        "ConflictProjection": {
            "partition_id": str(partition_id or ""),
            "open_conflicts": open_conflicts,
            "resolved_conflicts": resolved_conflicts,
            "categories": {
                "overlap": overlap_conflicts,
                "missing_coverage": missing_coverage_conflicts,
                "duplication": duplication_conflicts,
            },
            "counts": {
                "open": len(open_conflicts),
                "resolved": len(resolved_conflicts),
                "overlap": len(overlap_conflicts),
                "missing_coverage": len(missing_coverage_conflicts),
                "duplication": len(duplication_conflicts),
            },
        },
    }
