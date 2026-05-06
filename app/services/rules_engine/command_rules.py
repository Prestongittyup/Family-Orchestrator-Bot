from __future__ import annotations

print("IMPORT TRACE:", __name__, flush=True)

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping

from decision_card_system.registry import (
    DECISION_CARD_CANONICAL_ORIGIN_API,
    DECISION_CARD_STATE_ACKNOWLEDGED,
    DECISION_CARD_STATE_GENERATED,
    DECISION_CARD_STATE_RESOLVED,
    DECISION_CARD_STATE_SURFACED,
)


TASK_CREATE_COMMAND_TYPES = frozenset({"task.create", "create_task", "task_created"})
TASK_COMPLETE_COMMAND_TYPES = frozenset({"task_completed"})
SCHEDULE_CREATE_COMMAND_TYPES = frozenset({"schedule.create"})
SCHEDULE_CANCEL_COMMAND_TYPES = frozenset({"schedule.cancel"})
REMINDER_CREATE_COMMAND_TYPES = frozenset({"reminder.create"})
REMINDER_CANCEL_COMMAND_TYPES = frozenset({"reminder.cancel"})
REMINDER_TRIGGER_COMMAND_TYPES = frozenset({"reminder.trigger"})
DECISION_COMPLETE_COMMAND_TYPES = frozenset({"decision.complete"})
DECISION_DEFER_COMMAND_TYPES = frozenset({"decision.defer"})
DECISION_IGNORE_COMMAND_TYPES = frozenset({"decision.ignore"})
DECISION_CARD_CREATE_COMMAND_TYPES = frozenset({"decision.card.create"})
DECISION_CARD_SURFACE_COMMAND_TYPES = frozenset({"decision.card.surface"})
DECISION_CARD_ACKNOWLEDGE_COMMAND_TYPES = frozenset({"decision.card.acknowledge"})
DECISION_CARD_RESOLVE_COMMAND_TYPES = frozenset({"decision.card.resolve"})


@dataclass(frozen=True)
class CommandRuleDecision:
    allowed: bool
    reason: str
    code: str


def evaluate_command(
    *,
    command_type: str,
    payload: Mapping[str, Any],
    current_state: Mapping[str, Any] | None,
) -> CommandRuleDecision:
    normalized_command_type = str(command_type or "").strip()
    normalized_payload = dict(payload)

    if normalized_command_type in TASK_CREATE_COMMAND_TYPES:
        title = str(normalized_payload.get("title") or "").strip()
        if not title:
            return CommandRuleDecision(
                allowed=False,
                reason="task title must not be empty",
                code="task_title_required",
            )
        return _allow()

    if normalized_command_type in TASK_COMPLETE_COMMAND_TYPES:
        task_id = str(normalized_payload.get("task_id") or "").strip()
        if not task_id:
            return CommandRuleDecision(
                allowed=False,
                reason="task_id is required for task_completed",
                code="task_id_required",
            )

        tasks = _tasks_from_projection(current_state)
        task = tasks.get(task_id)
        if not isinstance(task, Mapping):
            return CommandRuleDecision(
                allowed=False,
                reason="cannot complete a non-existent task",
                code="task_not_found",
            )

        status = str(task.get("status") or task.get("lifecycle_state") or "pending").strip().lower()
        if status == "completed":
            return CommandRuleDecision(
                allowed=False,
                reason="cannot complete an already completed task",
                code="task_already_completed",
            )

        return _allow()

    if normalized_command_type in SCHEDULE_CREATE_COMMAND_TYPES:
        title = str(normalized_payload.get("title") or "").strip()
        if not title:
            return CommandRuleDecision(
                allowed=False,
                reason="schedule title must not be empty",
                code="schedule_title_required",
            )

        start_at = str(normalized_payload.get("start_at") or "").strip()
        end_at = str(normalized_payload.get("end_at") or "").strip()
        if not start_at or not end_at:
            return CommandRuleDecision(
                allowed=False,
                reason="schedule.create requires start_at and end_at",
                code="schedule_window_required",
            )

        try:
            start_dt = _coerce_iso_timestamp(start_at)
            end_dt = _coerce_iso_timestamp(end_at)
        except ValueError:
            return CommandRuleDecision(
                allowed=False,
                reason="schedule timestamps must be ISO-8601",
                code="schedule_timestamp_invalid",
            )

        if end_dt <= start_dt:
            return CommandRuleDecision(
                allowed=False,
                reason="schedule end_at must be later than start_at",
                code="schedule_window_invalid",
            )

        return _allow()

    if normalized_command_type in SCHEDULE_CANCEL_COMMAND_TYPES:
        schedule_id = str(normalized_payload.get("schedule_id") or "").strip()
        if not schedule_id:
            return CommandRuleDecision(
                allowed=False,
                reason="schedule_id is required for schedule.cancel",
                code="schedule_id_required",
            )

        schedules = _schedules_from_projection(current_state)
        schedule = schedules.get(schedule_id)
        if not isinstance(schedule, Mapping):
            return CommandRuleDecision(
                allowed=False,
                reason="cannot cancel a non-existent schedule",
                code="schedule_not_found",
            )

        status = str(schedule.get("status") or "scheduled").strip().lower()
        if status == "cancelled":
            return CommandRuleDecision(
                allowed=False,
                reason="cannot cancel an already cancelled schedule",
                code="schedule_already_cancelled",
            )

        return _allow()

    if normalized_command_type in REMINDER_CREATE_COMMAND_TYPES:
        title = str(normalized_payload.get("title") or "").strip()
        if not title:
            return CommandRuleDecision(
                allowed=False,
                reason="reminder title must not be empty",
                code="reminder_title_required",
            )

        trigger_at = str(normalized_payload.get("trigger_at") or "").strip()
        if not trigger_at:
            return CommandRuleDecision(
                allowed=False,
                reason="trigger_at is required for reminder.create",
                code="reminder_trigger_at_required",
            )

        try:
            _coerce_iso_timestamp(trigger_at)
        except ValueError:
            return CommandRuleDecision(
                allowed=False,
                reason="trigger_at must be ISO-8601",
                code="reminder_trigger_at_invalid",
            )

        return _allow()

    if normalized_command_type in REMINDER_CANCEL_COMMAND_TYPES:
        reminder_id = str(normalized_payload.get("reminder_id") or "").strip()
        if not reminder_id:
            return CommandRuleDecision(
                allowed=False,
                reason="reminder_id is required for reminder.cancel",
                code="reminder_id_required",
            )

        reminders = _reminders_from_projection(current_state)
        reminder = reminders.get(reminder_id)
        if not isinstance(reminder, Mapping):
            return CommandRuleDecision(
                allowed=False,
                reason="cannot cancel a non-existent reminder",
                code="reminder_not_found",
            )

        status = str(reminder.get("status") or "active").strip().lower()
        if status == "cancelled":
            return CommandRuleDecision(
                allowed=False,
                reason="cannot cancel an already cancelled reminder",
                code="reminder_already_cancelled",
            )
        if status == "triggered":
            return CommandRuleDecision(
                allowed=False,
                reason="cannot cancel a triggered reminder",
                code="reminder_already_triggered",
            )

        return _allow()

    if normalized_command_type in REMINDER_TRIGGER_COMMAND_TYPES:
        reminder_id = str(normalized_payload.get("reminder_id") or "").strip()
        if not reminder_id:
            return CommandRuleDecision(
                allowed=False,
                reason="reminder_id is required for reminder.trigger",
                code="reminder_id_required",
            )

        reminders = _reminders_from_projection(current_state)
        reminder = reminders.get(reminder_id)
        if not isinstance(reminder, Mapping):
            return CommandRuleDecision(
                allowed=False,
                reason="cannot trigger a non-existent reminder",
                code="reminder_not_found",
            )

        status = str(reminder.get("status") or "active").strip().lower()
        if status == "cancelled":
            return CommandRuleDecision(
                allowed=False,
                reason="cannot trigger a cancelled reminder",
                code="reminder_cancelled",
            )
        if status == "triggered":
            return CommandRuleDecision(
                allowed=False,
                reason="cannot trigger an already triggered reminder",
                code="reminder_already_triggered",
            )

        manual_trigger = not bool(normalized_payload.get("system_trigger") is True)
        if manual_trigger:
            reminder_trigger_at = str(reminder.get("trigger_at") or "").strip()
            if reminder_trigger_at:
                try:
                    trigger_dt = _coerce_iso_timestamp(reminder_trigger_at)
                except ValueError:
                    return CommandRuleDecision(
                        allowed=False,
                        reason="reminder trigger_at must be ISO-8601",
                        code="reminder_trigger_at_invalid",
                    )
                if trigger_dt > datetime.now(UTC):
                    return CommandRuleDecision(
                        allowed=False,
                        reason="cannot manually trigger a future reminder",
                        code="reminder_trigger_future_manual",
                    )

        return _allow()

    if normalized_command_type in DECISION_COMPLETE_COMMAND_TYPES:
        decision_id = str(normalized_payload.get("decision_id") or "").strip()
        if not decision_id:
            return CommandRuleDecision(
                allowed=False,
                reason="decision_id is required for decision.complete",
                code="decision_id_required",
            )

        decision_cards = _decision_cards_from_projection(current_state)
        decision_card = decision_cards.get(decision_id)
        if not isinstance(decision_card, Mapping):
            # Legacy compatibility path: runtime may synthesize and acknowledge
            # a canonical decision card before applying decision.complete.
            return _allow()
        origin_api = str(decision_card.get("origin_api") or "").strip()
        if origin_api != DECISION_CARD_CANONICAL_ORIGIN_API:
            return CommandRuleDecision(
                allowed=False,
                reason="decision.complete requires canonical decision card origin",
                code="decision_card_origin_invalid",
            )
        card_state = str(decision_card.get("state") or "").strip().lower()
        if card_state not in {DECISION_CARD_STATE_ACKNOWLEDGED, DECISION_CARD_STATE_RESOLVED}:
            return CommandRuleDecision(
                allowed=False,
                reason=(
                    "decision.complete requires acknowledged or resolved decision card state"
                ),
                code="decision_card_transition_invalid",
            )
        return _allow()

    if normalized_command_type in DECISION_DEFER_COMMAND_TYPES:
        decision_id = str(normalized_payload.get("decision_id") or "").strip()
        if not decision_id:
            return CommandRuleDecision(
                allowed=False,
                reason="decision_id is required for decision.defer",
                code="decision_id_required",
            )

        defer_to_date = str(normalized_payload.get("defer_to_date") or "").strip()
        if not defer_to_date:
            return CommandRuleDecision(
                allowed=False,
                reason="defer_to_date is required for decision.defer",
                code="decision_defer_to_date_required",
            )

        decision_cards = _decision_cards_from_projection(current_state)
        decision_card = decision_cards.get(decision_id)
        if not isinstance(decision_card, Mapping):
            return CommandRuleDecision(
                allowed=False,
                reason="decision.defer requires canonical decision card",
                code="decision_card_required",
            )
        origin_api = str(decision_card.get("origin_api") or "").strip()
        if origin_api != DECISION_CARD_CANONICAL_ORIGIN_API:
            return CommandRuleDecision(
                allowed=False,
                reason="decision.defer requires canonical decision card origin",
                code="decision_card_origin_invalid",
            )
        card_state = str(decision_card.get("state") or "").strip().lower()
        if card_state != DECISION_CARD_STATE_ACKNOWLEDGED:
            return CommandRuleDecision(
                allowed=False,
                reason="decision.defer requires acknowledged decision card state",
                code="decision_card_transition_invalid",
            )

        return _allow()

    if normalized_command_type in DECISION_IGNORE_COMMAND_TYPES:
        decision_id = str(normalized_payload.get("decision_id") or "").strip()
        if not decision_id:
            return CommandRuleDecision(
                allowed=False,
                reason="decision_id is required for decision.ignore",
                code="decision_id_required",
            )

        decision_cards = _decision_cards_from_projection(current_state)
        decision_card = decision_cards.get(decision_id)
        if not isinstance(decision_card, Mapping):
            return CommandRuleDecision(
                allowed=False,
                reason="decision.ignore requires canonical decision card",
                code="decision_card_required",
            )
        origin_api = str(decision_card.get("origin_api") or "").strip()
        if origin_api != DECISION_CARD_CANONICAL_ORIGIN_API:
            return CommandRuleDecision(
                allowed=False,
                reason="decision.ignore requires canonical decision card origin",
                code="decision_card_origin_invalid",
            )
        card_state = str(decision_card.get("state") or "").strip().lower()
        if card_state != DECISION_CARD_STATE_ACKNOWLEDGED:
            return CommandRuleDecision(
                allowed=False,
                reason="decision.ignore requires acknowledged decision card state",
                code="decision_card_transition_invalid",
            )
        return _allow()

    if normalized_command_type in DECISION_CARD_CREATE_COMMAND_TYPES:
        root_cause_key = str(normalized_payload.get("root_cause_key") or "").strip()
        title = str(normalized_payload.get("title") or "").strip()
        if not root_cause_key:
            return CommandRuleDecision(
                allowed=False,
                reason="root_cause_key is required for decision.card.create",
                code="decision_card_root_cause_required",
            )
        if not title:
            return CommandRuleDecision(
                allowed=False,
                reason="title is required for decision.card.create",
                code="decision_card_title_required",
            )
        return _allow()

    if normalized_command_type in DECISION_CARD_SURFACE_COMMAND_TYPES:
        decision_card_id = str(normalized_payload.get("decision_card_id") or "").strip()
        if not decision_card_id:
            return CommandRuleDecision(
                allowed=False,
                reason="decision_card_id is required for decision.card.surface",
                code="decision_card_id_required",
            )
        decision_cards = _decision_cards_from_projection(current_state)
        decision_card = decision_cards.get(decision_card_id)
        if not isinstance(decision_card, Mapping):
            return CommandRuleDecision(
                allowed=False,
                reason="cannot surface non-existent decision card",
                code="decision_card_not_found",
            )
        state = str(decision_card.get("state") or "").strip().lower()
        if state != DECISION_CARD_STATE_GENERATED:
            return CommandRuleDecision(
                allowed=False,
                reason="decision.card.surface requires generated state",
                code="decision_card_transition_invalid",
            )
        return _allow()

    if normalized_command_type in DECISION_CARD_ACKNOWLEDGE_COMMAND_TYPES:
        decision_card_id = str(normalized_payload.get("decision_card_id") or "").strip()
        if not decision_card_id:
            return CommandRuleDecision(
                allowed=False,
                reason="decision_card_id is required for decision.card.acknowledge",
                code="decision_card_id_required",
            )
        decision_cards = _decision_cards_from_projection(current_state)
        decision_card = decision_cards.get(decision_card_id)
        if not isinstance(decision_card, Mapping):
            return CommandRuleDecision(
                allowed=False,
                reason="cannot acknowledge non-existent decision card",
                code="decision_card_not_found",
            )
        state = str(decision_card.get("state") or "").strip().lower()
        if state != DECISION_CARD_STATE_SURFACED:
            return CommandRuleDecision(
                allowed=False,
                reason="decision.card.acknowledge requires surfaced state",
                code="decision_card_transition_invalid",
            )
        return _allow()

    if normalized_command_type in DECISION_CARD_RESOLVE_COMMAND_TYPES:
        decision_card_id = str(normalized_payload.get("decision_card_id") or "").strip()
        if not decision_card_id:
            return CommandRuleDecision(
                allowed=False,
                reason="decision_card_id is required for decision.card.resolve",
                code="decision_card_id_required",
            )
        decision_cards = _decision_cards_from_projection(current_state)
        decision_card = decision_cards.get(decision_card_id)
        if not isinstance(decision_card, Mapping):
            return CommandRuleDecision(
                allowed=False,
                reason="cannot resolve non-existent decision card",
                code="decision_card_not_found",
            )
        state = str(decision_card.get("state") or "").strip().lower()
        if state != DECISION_CARD_STATE_ACKNOWLEDGED:
            return CommandRuleDecision(
                allowed=False,
                reason="decision.card.resolve requires acknowledged state",
                code="decision_card_transition_invalid",
            )
        return _allow()

    return _allow()


def _tasks_from_projection(current_state: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(current_state, Mapping):
        return {}

    tasks = current_state.get("tasks")
    if not isinstance(tasks, Mapping):
        return {}

    return {
        str(task_id): task_payload
        for task_id, task_payload in tasks.items()
    }


def _schedules_from_projection(current_state: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(current_state, Mapping):
        return {}

    schedules = current_state.get("schedules")
    if not isinstance(schedules, Mapping):
        return {}

    return {
        str(schedule_id): schedule_payload
        for schedule_id, schedule_payload in schedules.items()
    }


def _reminders_from_projection(current_state: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(current_state, Mapping):
        return {}

    reminders = current_state.get("reminders")
    if not isinstance(reminders, Mapping):
        return {}

    return {
        str(reminder_id): reminder_payload
        for reminder_id, reminder_payload in reminders.items()
    }


def _decision_cards_from_projection(current_state: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(current_state, Mapping):
        return {}

    decision_cards = current_state.get("decision_cards")
    if not isinstance(decision_cards, Mapping):
        return {}

    return {
        str(decision_card_id): decision_card_payload
        for decision_card_id, decision_card_payload in decision_cards.items()
    }


def _coerce_iso_timestamp(raw_value: str) -> datetime:
    normalized = raw_value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _allow() -> CommandRuleDecision:
    return CommandRuleDecision(
        allowed=True,
        reason="allowed",
        code="allowed",
    )
