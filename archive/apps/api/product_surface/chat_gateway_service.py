from __future__ import annotations

import logging
import re
from threading import BoundedSemaphore
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from archive.apps.assistant_core.planning_engine import _fallback_household_state
from archive.apps.api.endpoints.integrations_router import (
    get_credential_store,
    get_http_client,
    get_oauth_config,
    sync_google_email,
)
from archive.apps.api.endpoints.pantry_router import (
    CookRecipeRequest,
    InventoryDelta,
    PantryAdjustRequest,
    adjust_inventory,
    cook_recipe,
)
from archive.apps.api.hpal.command_gateway import HpalCommandGateway
from archive.apps.api.product_surface.bootstrap_service import UIBootstrapService
from archive.apps.api.product_surface.contracts import (
    ActionCard,
    ChatResponse,
    TaskSummary,
    UIBootstrapState,
)
from archive.apps.api.product_surface.patch_service import UIPatchService
from archive.apps.api.services.calendar_service import create_recurring_event, schedule_event
from archive.apps.api.services.task_service import create_task
from archive.apps.api.schemas.event import SystemEvent
from archive.apps.api.services.canonical_event_adapter import CanonicalEventAdapter
from archive.apps.api.services.canonical_event_router import canonical_event_router
from household_os.runtime.orchestrator import HouseholdOSOrchestrator, OrchestratorRequest, RequestActionType
from archive.apps.api.llm.intent_resolver import LLMIntentResolver

logger = logging.getLogger(__name__)
_MESSAGE_WORKERS = BoundedSemaphore(value=4)

_TASK_CREATE_PATTERN = re.compile(
    r"^(?:create|add|new)\s+(?:a\s+)?task\s+(?P<title>.+)$",
    re.IGNORECASE,
)
_TASK_COMPLETE_PATTERN = re.compile(
    r"^(?:complete|finish|done)\s+(?:task\s+)?(?P<title>.+)$",
    re.IGNORECASE,
)
_TASK_MARK_COMPLETE_PATTERN = re.compile(
    r"^mark\s+(?:task\s+)?(?P<title>.+?)\s+(?:as\s+)?(?:complete|completed|done)$",
    re.IGNORECASE,
)
_CALENDAR_CREATE_PATTERN = re.compile(
    r"^(?:schedule|create|add)\s+(?:a\s+)?(?:calendar\s+)?(?:event|meeting|appointment)\s+(?P<title>.+)$",
    re.IGNORECASE,
)
_PANTRY_COOK_PATTERN = re.compile(
    r"^(?:cook|log)\s+(?:recipe\s+)?(?P<recipe>.+?)(?:\s+for\s+(?P<servings>\d+)\s+servings?)?$",
    re.IGNORECASE,
)
_PANTRY_ADJUST_PATTERN = re.compile(
    r"^(?P<verb>add|restock|stock|increase|remove|use|decrease|reduce)\s+(?P<body>.+)$",
    re.IGNORECASE,
)
_PANTRY_UNIT_TOKENS = {
    "count",
    "can",
    "pack",
    "oz",
    "lb",
    "g",
    "kg",
    "ml",
    "fl_oz",
    "fl oz",
    "l",
    "ea",
    "each",
    "piece",
    "pieces",
    "pcs",
}
_PANTRY_DIRECTIONAL_VERBS = {"add", "restock", "stock", "increase", "remove", "use", "decrease", "reduce"}
_PANTRY_NEGATIVE_VERBS = {"remove", "use", "decrease", "reduce"}
_INBOX_SYNC_PHRASES = {
    "sync inbox",
    "refresh inbox",
    "sync email",
    "sync emails",
    "sync gmail",
    "refresh email",
    "refresh emails",
    "ingest emails",
}
_CAPABILITY_HELP_PHRASES = {
    "what can you do",
    "what can the assistant do",
    "assistant capabilities",
    "help with commands",
    "show commands",
}


class _ChatGatewayRouter:
    @staticmethod
    def emit(event: SystemEvent) -> None:
        try:
            canonical_event_router.route(
                CanonicalEventAdapter.to_envelope(event),
                persist=False,
                dispatch=False,
            )
        except Exception as exc:
            logger.warning("chat_gateway_event_emit_failed: %s", exc)


router = _ChatGatewayRouter()


@dataclass(frozen=True)
class _SessionKey:
    family_id: str
    session_id: str


class ChatGatewayService:
    """UI-safe chat gateway that translates decision output into product-surface contracts."""

    _last_snapshot: dict[_SessionKey, UIBootstrapState] = {}

    def __init__(
        self,
        *,
        orchestrator: HouseholdOSOrchestrator | None = None,
        bootstrap_service: UIBootstrapService | None = None,
        patch_service: UIPatchService | None = None,
        intent_resolver: LLMIntentResolver | None = None,
        hpal_gateway: HpalCommandGateway | None = None,
    ) -> None:
        self._orchestrator = orchestrator or HouseholdOSOrchestrator()
        self._bootstrap_service = bootstrap_service or UIBootstrapService()
        self._patch_service = patch_service or UIPatchService()
        self._intent_resolver = intent_resolver or LLMIntentResolver()
        self._hpal_gateway = hpal_gateway or HpalCommandGateway()

    def process_message(self, *, family_id: str, message: str, session_id: str) -> ChatResponse:
        if not family_id or not family_id.strip():
            raise ValueError("family_id is required")
        if not message or not message.strip():
            raise ValueError("message is required")
        if not session_id or not session_id.strip():
            raise ValueError("session_id is required")

        if not _MESSAGE_WORKERS.acquire(blocking=False):
            router.emit(
                SystemEvent.ChatMessageFailed(
                    household_id=family_id,
                    reason="throttled",
                    error_message="message worker pool saturated",
                    input={
                        "family_id": family_id,
                        "message": message,
                        "session_id": session_id,
                    },
                )
            )
            return self._minimal_fallback_response(
                reason="The assistant is handling high load right now. I kept state consistent; please retry.",
            )

        input_payload = {
            "family_id": family_id,
            "message": message,
            "session_id": session_id,
        }

        try:
            direct_response = self._try_execute_capability_command(
                family_id=family_id,
                session_id=session_id,
                message=message,
            )
            if direct_response is not None:
                router.emit(
                    SystemEvent.ChatMessageSent(
                        household_id=family_id,
                        message_id=f"{session_id}:process",
                        user_id=self._resolve_user_id(session_id),
                        content=direct_response.assistant_message,
                    )
                )
                return direct_response

            contextual_response = self._try_generate_contextual_intelligence_response(
                family_id=family_id,
                session_id=session_id,
                message=message,
            )
            if contextual_response is not None:
                router.emit(
                    SystemEvent.ChatMessageSent(
                        household_id=family_id,
                        message_id=f"{session_id}:process",
                        user_id=self._resolve_user_id(session_id),
                        content=contextual_response.assistant_message,
                    )
                )
                return contextual_response

            state = _fallback_household_state(family_id)
            graph = self._orchestrator.handle_request(
                OrchestratorRequest(
                    action_type=RequestActionType.READ_SENSITIVE_STATE,
                    household_id=family_id,
                    actor={
                        "actor_type": "system_worker",
                        "subject_id": "ui_chat_gateway",
                        "session_id": session_id,
                        "verified": True,
                    },
                    resource_type="chat_context",
                    context={"system_worker_verified": True},
                )
            )

            # --- LLM intent resolution (with rule-based fallback) ---
            resolved = self._intent_resolver.resolve(
                message=message,
                context_snapshot=graph,
                household_id=family_id,
            )
            logger.debug(
                "Intent resolved: type=%s conf=%.2f source=%s clarification=%s",
                resolved.intent_type,
                resolved.confidence,
                resolved.resolution_source,
                resolved.clarification_request,
            )
            # Attach resolved intent to graph for decision engine consumption
            graph["_resolved_intent"] = {
                "intent_type": resolved.intent_type.value if resolved.intent_type else None,
                "confidence": resolved.confidence,
                "extracted": dict(resolved.extracted_fields or {}),
                "resolution_source": resolved.resolution_source,
            }

            decision_result = self._orchestrator.handle_request(
                OrchestratorRequest(
                    action_type=RequestActionType.LEGACY_EXECUTION,
                    household_id=family_id,
                    actor={
                        "actor_type": "system_worker",
                        "subject_id": "ui_chat_gateway",
                        "session_id": session_id,
                        "verified": True,
                    },
                    state=state,
                    user_input=message,
                    fitness_goal=None,
                    context={"system_worker_verified": True, "legacy_execution": True},
                )
            )
            if decision_result.response is None:
                raise ValueError("Orchestrator did not emit a response")
            decision = decision_result.response

            cards = self._action_cards_from_decision(decision.model_dump())
            requires_confirmation = any(card.type in {"confirm", "approve"} for card in cards)
            assistant_message = f"{decision.recommended_action.title}. {decision.recommended_action.description}"

            # Surface clarification request if LLM is uncertain
            if resolved.clarification_request and resolved.confidence < 0.75:
                assistant_message = (
                    f"{resolved.clarification_request}\n\n"
                    f"(Also: {assistant_message})"
                )

            key = _SessionKey(family_id=family_id, session_id=session_id)
            previous = self._last_snapshot.get(key)
            try:
                current = self._bootstrap_service.get_state(family_id=family_id)
                ui_patch = self._patch_service.generate_patches(previous=previous, current=current)
                self._last_snapshot[key] = current
                explanation_summary = current.explanation_digest[:5]
            except Exception as refresh_exc:
                logger.warning("chat_state_refresh_degraded: %s", refresh_exc)
                ui_patch = []
                explanation_summary = []

            response = ChatResponse(
                assistant_message=assistant_message,
                action_cards=cards,
                ui_patch=ui_patch,
                requires_confirmation=requires_confirmation,
                explanation_summary=explanation_summary,
            )
            router.emit(
                SystemEvent.ChatMessageSent(
                    household_id=family_id,
                    message_id=f"{session_id}:process",
                    user_id=family_id,
                    content=assistant_message,
                )
            )
            return response
        except ValueError as exc:
            router.emit(
                SystemEvent.ChatMessageFailed(
                    household_id=family_id,
                    reason="validation_error",
                    error_message=str(exc),
                    input=input_payload,
                )
            )
            logger.warning("chat_process_fallback: %s", exc)
            return self._safe_fallback_response(
                family_id=family_id,
                session_id=session_id,
                reason="I couldn't fully process that request. I kept the household state consistent and you can retry.",
                user_message=message,
            )
        except Exception as exc:
            router.emit(
                SystemEvent.ChatMessageFailed(
                    household_id=family_id,
                    reason="internal_error",
                    error_message=str(exc),
                    input=input_payload,
                )
            )
            logger.warning("chat_process_fallback: %s", exc)
            return self._minimal_fallback_response(
                reason="I couldn't fully process that request. I kept the household state consistent and you can retry.",
            )
        finally:
            _MESSAGE_WORKERS.release()

    def _try_execute_capability_command(
        self,
        *,
        family_id: str,
        session_id: str,
        message: str,
    ) -> ChatResponse | None:
        normalized = self._normalize_message(message)
        if not normalized:
            return None

        lowered = normalized.lower()
        user_id = self._resolve_user_id(session_id)

        if any(phrase in lowered for phrase in _CAPABILITY_HELP_PHRASES):
            return self._stateful_response(
                family_id=family_id,
                session_id=session_id,
                assistant_message=(
                    "I can execute app operations directly. Try commands like: "
                    "'sync inbox', 'add 2 eggs to pantry', 'remove 1 milk from pantry', "
                    "'cook salmon rice plate', 'create task call pediatrician', "
                    "'complete task call pediatrician', or 'schedule event dentist appointment'."
                ),
            )

        if any(phrase in lowered for phrase in _INBOX_SYNC_PHRASES):
            max_results = self._extract_max_results(lowered)
            try:
                result = sync_google_email(
                    user_id=user_id,
                    household_id=family_id,
                    max_results=max_results,
                    config=get_oauth_config(),
                    credential_store=get_credential_store(),
                    http_client=get_http_client(),
                )
            except Exception as exc:
                return self._stateful_response(
                    family_id=family_id,
                    session_id=session_id,
                    assistant_message=(
                        f"Inbox sync failed: {self._describe_command_failure(exc)}. "
                        "Reconnect integrations if needed and try again."
                    ),
                )
            ingested = int(result.get("processed_count") or 0)
            ignored = int(result.get("ignored_count") or 0)
            failed = int(result.get("failed_count") or 0)
            return self._stateful_response(
                family_id=family_id,
                session_id=session_id,
                assistant_message=(
                    f"Inbox sync complete: processed {ingested}, ignored {ignored}, failed {failed}. "
                    "Your inbox dashboard should now reflect the refreshed messages."
                ),
            )

        task_complete_match = _TASK_COMPLETE_PATTERN.match(normalized) or _TASK_MARK_COMPLETE_PATTERN.match(normalized)
        if task_complete_match:
            task_title = str(task_complete_match.group("title") or "").strip()
            if not task_title:
                return None

            target_task, ambiguity = self._resolve_task_for_completion(
                family_id=family_id,
                task_title=task_title,
            )
            if target_task is None:
                if ambiguity > 1:
                    return self._stateful_response(
                        family_id=family_id,
                        session_id=session_id,
                        assistant_message=(
                            "I found multiple matching open tasks. Please include more of the task title so I can complete the right one."
                        ),
                    )
                return self._stateful_response(
                    family_id=family_id,
                    session_id=session_id,
                    assistant_message="I could not find an open task with that title.",
                )

            try:
                self._hpal_gateway.system_override_task_status(
                    family_id=family_id,
                    task_id=target_task.task_id,
                    target_status="completed",
                    reason_code="assistant_chat_completion",
                )
            except Exception as exc:
                return self._stateful_response(
                    family_id=family_id,
                    session_id=session_id,
                    assistant_message=f"Task completion failed: {self._describe_command_failure(exc)}.",
                )
            return self._stateful_response(
                family_id=family_id,
                session_id=session_id,
                assistant_message=f"Completed task: {target_task.title}.",
            )

        task_create_match = _TASK_CREATE_PATTERN.match(normalized)
        if task_create_match:
            title = str(task_create_match.group("title") or "").strip()
            if not title:
                return None
            try:
                task = create_task(household_id=family_id, title=title)
            except Exception as exc:
                return self._stateful_response(
                    family_id=family_id,
                    session_id=session_id,
                    assistant_message=f"Task creation failed: {self._describe_command_failure(exc)}.",
                )
            created_task_id = str(getattr(task, "id", "")).strip()
            task_suffix = f" (id: {created_task_id})" if created_task_id else ""
            return self._stateful_response(
                family_id=family_id,
                session_id=session_id,
                assistant_message=f"Created task: {title}{task_suffix}.",
            )

        calendar_match = _CALENDAR_CREATE_PATTERN.match(normalized)
        if calendar_match:
            raw_title = str(calendar_match.group("title") or "").strip()
            if not raw_title:
                return None
            recurrence, title = self._extract_recurrence(raw_title)
            duration_minutes, title = self._extract_duration_minutes(title)
            try:
                if recurrence in {"daily", "weekly", "monthly"}:
                    event = create_recurring_event(
                        household_id=family_id,
                        user_id=user_id,
                        title=title,
                        frequency=recurrence,
                        duration_minutes=duration_minutes,
                        description="Created by assistant chat command.",
                    )
                else:
                    event = schedule_event(
                        household_id=family_id,
                        user_id=user_id,
                        title=title,
                        duration_minutes=duration_minutes,
                        description="Created by assistant chat command.",
                    )
            except Exception as exc:
                return self._stateful_response(
                    family_id=family_id,
                    session_id=session_id,
                    assistant_message=f"Calendar scheduling failed: {self._describe_command_failure(exc)}.",
                )

            event_id = str(event.get("event_id") or "").strip()
            event_suffix = f" (id: {event_id})" if event_id else ""
            return self._stateful_response(
                family_id=family_id,
                session_id=session_id,
                assistant_message=f"Scheduled calendar event: {title}{event_suffix}.",
            )

        pantry_cook_match = _PANTRY_COOK_PATTERN.match(normalized)
        if pantry_cook_match and "task" not in lowered and "event" not in lowered:
            recipe_name = str(pantry_cook_match.group("recipe") or "").strip()
            servings = int(pantry_cook_match.group("servings") or 1)
            if recipe_name:
                try:
                    result = cook_recipe(
                        family_id=family_id,
                        request=CookRecipeRequest(recipe_name=recipe_name, servings=max(1, min(servings, 12))),
                    )
                except Exception as exc:
                    return self._stateful_response(
                        family_id=family_id,
                        session_id=session_id,
                        assistant_message=f"Recipe logging failed: {self._describe_command_failure(exc)}.",
                    )
                recipe_label = str(result.get("recipe_name") or recipe_name)
                return self._stateful_response(
                    family_id=family_id,
                    session_id=session_id,
                    assistant_message=(
                        f"Logged recipe and updated pantry: {recipe_label} (servings: {max(1, min(servings, 12))})."
                    ),
                )

        pantry_adjust = self._parse_pantry_adjust(normalized)
        if pantry_adjust is not None:
            try:
                result = adjust_inventory(
                    family_id=family_id,
                    request=PantryAdjustRequest(
                        updates=[
                            InventoryDelta(
                                item=str(pantry_adjust["item"]),
                                delta=float(pantry_adjust["delta"]),
                                unit=str(pantry_adjust["unit"]) if pantry_adjust["unit"] is not None else None,
                            )
                        ],
                        note="assistant chat command",
                    ),
                )
            except Exception as exc:
                return self._stateful_response(
                    family_id=family_id,
                    session_id=session_id,
                    assistant_message=f"Pantry update failed: {self._describe_command_failure(exc)}.",
                )
            applied_rows = result.get("applied") if isinstance(result, dict) else None
            if isinstance(applied_rows, list) and applied_rows:
                applied = applied_rows[0]
                item = str(applied.get("item") or pantry_adjust["item"])
                before = applied.get("before")
                after = applied.get("after")
                delta = applied.get("delta")
                return self._stateful_response(
                    family_id=family_id,
                    session_id=session_id,
                    assistant_message=(
                        f"Updated pantry item '{item}': {before} -> {after} (delta {delta})."
                    ),
                )
            return self._stateful_response(
                family_id=family_id,
                session_id=session_id,
                assistant_message="Updated pantry inventory.",
            )

        return None

    def _try_generate_contextual_intelligence_response(
        self,
        *,
        family_id: str,
        session_id: str,
        message: str,
    ) -> ChatResponse | None:
        normalized = self._normalize_message(message).lower()
        if not normalized:
            return None

        inbox_keywords = (
            "inbox",
            "email",
            "follow-up",
            "follow up",
            "prioritize",
            "draft reply",
            "follow-up replies",
        )
        pantry_keywords = (
            "pantry",
            "inventory",
            "restock",
            "meal plan",
            "recipe",
            "grocery",
            "waste",
        )
        schedule_keywords = (
            "rebalance",
            "free time",
            "weekly rollup",
            "weekend",
            "overloaded",
            "calendar windows",
        )

        wants_inbox = any(keyword in normalized for keyword in inbox_keywords)
        wants_pantry = any(keyword in normalized for keyword in pantry_keywords)
        wants_schedule = any(keyword in normalized for keyword in schedule_keywords)

        if not (wants_inbox or wants_pantry or wants_schedule):
            return None

        current = self._bootstrap_service.get_state(family_id=family_id)
        sections: list[str] = []
        if wants_inbox:
            sections.append(self._build_inbox_intelligence(current=current))
        if wants_pantry:
            sections.append(self._build_pantry_intelligence(current=current))
        if wants_schedule:
            sections.append(self._build_schedule_intelligence(current=current))

        content = "\n\n".join(section for section in sections if section.strip())
        if not content:
            return None

        return self._stateful_response(
            family_id=family_id,
            session_id=session_id,
            assistant_message=content,
        )

    @staticmethod
    def _build_inbox_intelligence(*, current: UIBootstrapState) -> str:
        notifications = getattr(current, "notifications", [])
        if not isinstance(notifications, list):
            notifications = []

        email_rows = [
            row
            for row in notifications
            if str(getattr(row, "notification_id", "")).startswith("notif:email_summary:")
        ]
        if not email_rows:
            return "Inbox intelligence: No email debrief items are available yet. Run Sync Inbox and ask again."

        def _row_rank(row: Any) -> tuple[int, int]:
            level = str(getattr(row, "level", "info")).lower()
            priority = 3 if level == "critical" else 2 if level == "warning" else 1
            message = str(getattr(row, "message", ""))
            action_count = ChatGatewayService._extract_signal_count(message, "action item")
            calendar_count = ChatGatewayService._extract_signal_count(message, "calendar candidate")
            return (priority, action_count + calendar_count)

        ranked = sorted(email_rows, key=_row_rank, reverse=True)
        lines: list[str] = []
        for index, row in enumerate(ranked[:5], start=1):
            title = str(getattr(row, "title", "Email update")).replace("Email:", "").strip() or "Email update"
            message = str(getattr(row, "message", "")).strip()
            lines.append(f"{index}. {title} - {ChatGatewayService._truncate_text(message, limit=120)}")

        actionable = sum(
            1
            for row in email_rows
            if ChatGatewayService._extract_signal_count(str(getattr(row, "message", "")), "action item") > 0
        )
        return (
            f"Inbox intelligence: {len(email_rows)} debrief item(s), {actionable} with explicit action signals.\n"
            f"Top priorities:\n" + "\n".join(lines)
            + "\nNext step: Confirm which of these should be converted into tasks versus calendar events."
        )

    @staticmethod
    def _build_pantry_intelligence(*, current: UIBootstrapState) -> str:
        pantry = getattr(current, "pantry", None)
        if pantry is None:
            return "Pantry intelligence: pantry state is unavailable. Refresh household state and retry."

        inventory_rows = getattr(pantry, "inventory_items", [])
        if not isinstance(inventory_rows, list):
            inventory_rows = []

        low_stock_items = [
            str(getattr(row, "name", "")).strip()
            for row in inventory_rows
            if str(getattr(row, "status", "")).strip() in {"low", "out_of_stock"}
        ]
        low_stock_preview = ", ".join(item for item in low_stock_items[:6] if item) or "none"

        recipe_rows = getattr(pantry, "weekly_recipe_suggestions", [])
        if not isinstance(recipe_rows, list):
            recipe_rows = []

        recipe_lines: list[str] = []
        for index, row in enumerate(recipe_rows[:3], start=1):
            recipe_name = str(getattr(row, "recipe_name", "Meal suggestion")).strip() or "Meal suggestion"
            missing = getattr(row, "missing_ingredients", [])
            missing_count = len(missing) if isinstance(missing, list) else 0
            recipe_lines.append(f"{index}. {recipe_name} (missing ingredients: {missing_count})")

        recipe_section = "\n".join(recipe_lines) if recipe_lines else "No weekly recipe suggestions available yet."
        low_stock_count = int(getattr(pantry, "low_stock_count", 0) or 0)
        return (
            f"Pantry intelligence: {low_stock_count} low-stock item(s).\n"
            f"Priority restock list: {low_stock_preview}.\n"
            f"Best meal candidates:\n{recipe_section}\n"
            "Next step: Confirm your preferred recipes and I can translate them into restock and prep tasks."
        )

    @staticmethod
    def _build_schedule_intelligence(*, current: UIBootstrapState) -> str:
        today = getattr(current, "today_overview", None)
        calendar = getattr(current, "calendar", None)
        task_board = getattr(current, "task_board", None)

        open_tasks = int(getattr(today, "open_task_count", 0) or 0)
        scheduled_events = int(getattr(today, "scheduled_event_count", 0) or 0)
        notification_count = int(getattr(today, "notification_count", 0) or 0)
        calendar_events = getattr(calendar, "events", []) if calendar is not None else []
        if not isinstance(calendar_events, list):
            calendar_events = []

        pending_tasks = getattr(task_board, "pending", []) if task_board is not None else []
        in_progress_tasks = getattr(task_board, "in_progress", []) if task_board is not None else []
        pending_count = len(pending_tasks) if isinstance(pending_tasks, list) else 0
        in_progress_count = len(in_progress_tasks) if isinstance(in_progress_tasks, list) else 0

        pressure = open_tasks + scheduled_events + notification_count
        workload = "high" if pressure >= 12 else "moderate" if pressure >= 6 else "light"

        return (
            "Schedule intelligence: "
            f"workload is {workload} ({open_tasks} open task(s), {scheduled_events} event(s), {notification_count} notification(s)).\n"
            f"Execution queue: {pending_count} pending task(s), {in_progress_count} in progress, {len(calendar_events)} event(s) in calendar window.\n"
            "Next step: Identify one movable event and two movable tasks to reduce weekly congestion."
        )

    @staticmethod
    def _extract_signal_count(message: str, label: str) -> int:
        match = re.search(rf"(\d+)\s+{re.escape(label)}s?", message, flags=re.IGNORECASE)
        if match is None:
            return 0
        return int(match.group(1))

    @staticmethod
    def _truncate_text(value: str, *, limit: int) -> str:
        compact = " ".join(value.split())
        if len(compact) <= limit:
            return compact
        return f"{compact[: max(limit - 3, 0)].rstrip()}..."

    def _stateful_response(
        self,
        *,
        family_id: str,
        session_id: str,
        assistant_message: str,
        action_cards: list[ActionCard] | None = None,
        requires_confirmation: bool = False,
    ) -> ChatResponse:
        key = _SessionKey(family_id=family_id, session_id=session_id)
        previous = self._last_snapshot.get(key)
        try:
            current = self._bootstrap_service.get_state(family_id=family_id)
            ui_patch = self._patch_service.generate_patches(previous=previous, current=current)
            self._last_snapshot[key] = current
            explanation_summary = current.explanation_digest[:5]
        except Exception as exc:
            logger.warning("stateful_response_degraded: %s", exc)
            ui_patch = []
            explanation_summary = []

        return ChatResponse(
            assistant_message=assistant_message,
            action_cards=list(action_cards or []),
            ui_patch=ui_patch,
            requires_confirmation=requires_confirmation,
            explanation_summary=explanation_summary,
        )

    @staticmethod
    def _extract_max_results(message: str) -> int:
        match = re.search(r"\b(\d{1,3})\b", message)
        if not match:
            return 100
        return max(1, min(int(match.group(1)), 100))

    @staticmethod
    def _normalize_message(message: str) -> str:
        return " ".join(message.strip().split())

    @staticmethod
    def _resolve_user_id(session_id: str) -> str:
        if ":" in session_id:
            candidate = session_id.split(":", 1)[0].strip()
            if candidate:
                return candidate
        candidate = session_id.strip()
        return candidate or "user-admin"

    @staticmethod
    def _extract_recurrence(text: str) -> tuple[str, str]:
        match = re.search(r"\b(daily|weekly|monthly)\b", text, flags=re.IGNORECASE)
        if not match:
            return "none", text.strip()

        recurrence = str(match.group(1)).lower()
        cleaned = f"{text[:match.start()]} {text[match.end():]}".strip(" ,")
        return recurrence, cleaned or text.strip()

    @staticmethod
    def _extract_duration_minutes(text: str) -> tuple[int, str]:
        minute_match = re.search(
            r"\bfor\s+(?P<value>\d{1,3})\s*(?P<unit>m|min|mins|minute|minutes|h|hr|hrs|hour|hours)\b",
            text,
            flags=re.IGNORECASE,
        )
        if minute_match is None:
            return 30, text.strip()

        value = int(minute_match.group("value"))
        unit = str(minute_match.group("unit")).lower()
        minutes = value
        if unit in {"h", "hr", "hrs", "hour", "hours"}:
            minutes = value * 60

        cleaned = f"{text[:minute_match.start()]} {text[minute_match.end():]}".strip(" ,")
        bounded = max(5, min(minutes, 480))
        return bounded, cleaned or text.strip()

    def _resolve_task_for_completion(self, *, family_id: str, task_title: str) -> tuple[TaskSummary | None, int]:
        current = self._bootstrap_service.get_state(family_id=family_id)
        candidates = list(current.task_board.pending) + list(current.task_board.in_progress)
        if not candidates:
            return None, 0

        normalized_target = self._normalize_lookup_text(task_title)
        if not normalized_target:
            return None, 0

        exact_matches = [task for task in candidates if self._normalize_lookup_text(task.title) == normalized_target]
        if len(exact_matches) == 1:
            return exact_matches[0], 1
        if len(exact_matches) > 1:
            return None, len(exact_matches)

        partial_matches = [
            task
            for task in candidates
            if normalized_target in self._normalize_lookup_text(task.title)
            or self._normalize_lookup_text(task.title) in normalized_target
        ]
        if len(partial_matches) == 1:
            return partial_matches[0], 1
        if len(partial_matches) > 1:
            return None, len(partial_matches)

        return None, 0

    @staticmethod
    def _normalize_lookup_text(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", " ", value.strip().lower()).strip()

    @staticmethod
    def _describe_command_failure(exc: Exception) -> str:
        detail = getattr(exc, "detail", None)
        if isinstance(detail, str) and detail.strip():
            return detail.strip()
        if isinstance(detail, dict):
            message = str(detail.get("message") or "").strip()
            if message:
                return message

        text = str(exc).strip()
        return text or "unexpected_error"

    @staticmethod
    def _parse_pantry_adjust(message: str) -> dict[str, Any] | None:
        match = _PANTRY_ADJUST_PATTERN.match(message)
        if match is None:
            return None

        verb = str(match.group("verb") or "").strip().lower()
        if verb not in _PANTRY_DIRECTIONAL_VERBS:
            return None

        body = str(match.group("body") or "").strip()
        lowered = body.lower()
        if lowered.startswith("task ") or lowered.startswith("event "):
            return None

        scoped = "pantry" in lowered or "inventory" in lowered
        body = re.sub(r"\b(?:to|in|from)\s+(?:the\s+)?(?:pantry|inventory)\b", "", body, flags=re.IGNORECASE).strip()

        quantity = 1.0
        quantity_match = re.match(r"^(?P<qty>\d+(?:\.\d+)?)\s+(?P<rest>.+)$", body)
        if quantity_match is not None:
            quantity = float(quantity_match.group("qty"))
            body = str(quantity_match.group("rest") or "").strip()
        elif not scoped:
            return None

        body = re.sub(r"^of\s+", "", body, flags=re.IGNORECASE).strip()
        if not body:
            return None

        item = body
        unit: str | None = None
        tokens = body.split()
        if len(tokens) >= 2:
            tail = " ".join(tokens[-2:]).lower()
            if tail in _PANTRY_UNIT_TOKENS:
                unit = tail
                item = " ".join(tokens[:-2]).strip()

        if unit is None and tokens:
            tail = tokens[-1].lower()
            if tail in _PANTRY_UNIT_TOKENS:
                unit = tail
                item = " ".join(tokens[:-1]).strip()

        item = re.sub(r"\s+", " ", item).strip(" ,")
        if not item:
            return None

        delta = -quantity if verb in _PANTRY_NEGATIVE_VERBS else quantity
        normalized_unit = unit.replace(" ", "_") if unit else None
        return {
            "item": item,
            "delta": delta,
            "unit": normalized_unit,
        }

    def execute_action(
        self,
        *,
        family_id: str,
        session_id: str,
        action_card_id: str,
        payload: dict,
    ) -> ChatResponse:
        """
        Execute an action card in a deterministic way.

        Action cards from the decision engine carry lifecycle action IDs.
        Resolve and execute them through the orchestrator approve/reject path.
        Fallback calendar scheduling is preserved for explicit title payloads.
        """
        user_id = str(payload.get("user_id") or "user-admin")
        title = payload.get("title")
        recurrence = str(payload.get("recurrence") or "none")
        action_ids = self._resolve_action_ids(payload)
        action_mode = self._resolve_action_mode(action_card_id=action_card_id, payload=payload)

        input_payload = {
            "family_id": family_id,
            "session_id": session_id,
            "action_card_id": action_card_id,
            "payload": payload,
        }

        try:
            assistant_message = "Action executed."

            if action_ids:
                action_graph = self._orchestrator.handle_request(
                    OrchestratorRequest(
                        action_type=RequestActionType.READ_SENSITIVE_STATE,
                        household_id=family_id,
                        actor={
                            "actor_type": "system_worker",
                            "subject_id": "ui_chat_gateway",
                            "session_id": session_id,
                            "verified": True,
                        },
                        resource_type="action_lifecycle",
                        context={"system_worker_verified": True},
                    )
                )
                request_id = self._resolve_request_id(
                    action_graph=action_graph,
                    payload=payload,
                    action_ids=action_ids,
                )

                if action_mode == "reject":
                    self._orchestrator.handle_request(
                        OrchestratorRequest(
                            action_type=RequestActionType.REJECT,
                            household_id=family_id,
                            actor={
                                "actor_type": "system_worker",
                                "subject_id": "ui_chat_gateway",
                                "session_id": session_id,
                                "verified": True,
                            },
                            request_id=request_id,
                            action_ids=action_ids,
                            context={"system_worker_verified": True},
                        )
                    )
                    assistant_message = "Action rejected."
                else:
                    self._orchestrator.handle_request(
                        OrchestratorRequest(
                            action_type=RequestActionType.APPROVE,
                            household_id=family_id,
                            actor={
                                "actor_type": "system_worker",
                                "subject_id": "ui_chat_gateway",
                                "session_id": session_id,
                                "verified": True,
                            },
                            request_id=request_id,
                            action_ids=action_ids,
                            context={"system_worker_verified": True},
                        )
                    )
                    assistant_message = "Action approved and executed."
            elif title:
                if recurrence in {"daily", "weekly", "monthly"}:
                    create_recurring_event(
                        household_id=family_id,
                        user_id=user_id,
                        title=str(title),
                        frequency=recurrence,
                        duration_minutes=int(payload.get("duration_minutes") or 30),
                        description=payload.get("description"),
                    )
                else:
                    schedule_event(
                        household_id=family_id,
                        user_id=user_id,
                        title=str(title),
                        description=payload.get("description"),
                        duration_minutes=int(payload.get("duration_minutes") or 30),
                        start_time=payload.get("start_time"),
                    )
            else:
                assistant_message = "No executable payload found for this action card."

            current = self._bootstrap_service.get_state(family_id=family_id)
            key = _SessionKey(family_id=family_id, session_id=session_id)
            previous = self._last_snapshot.get(key)
            ui_patch = self._patch_service.generate_patches(previous=previous, current=current)
            self._last_snapshot[key] = current

            response = ChatResponse(
                assistant_message=assistant_message,
                action_cards=[],
                ui_patch=ui_patch,
                requires_confirmation=False,
                explanation_summary=current.explanation_digest[:5],
            )
            router.emit(
                SystemEvent.ChatMessageSent(
                    household_id=family_id,
                    message_id=action_card_id,
                    user_id=user_id,
                    content=assistant_message,
                )
            )
            return response
        except ValueError as exc:
            router.emit(
                SystemEvent.ChatMessageFailed(
                    household_id=family_id,
                    reason="validation_error",
                    error_message=str(exc),
                    input=input_payload,
                )
            )
            raise
        except Exception as exc:
            router.emit(
                SystemEvent.ChatMessageFailed(
                    household_id=family_id,
                    reason="internal_error",
                    error_message=str(exc),
                    input=input_payload,
                )
            )
            raise

    @staticmethod
    def _resolve_action_mode(*, action_card_id: str, payload: dict) -> str:
        if isinstance(payload.get("action_mode"), str):
            return str(payload.get("action_mode")).strip().lower()
        if isinstance(payload.get("decision"), str):
            return str(payload.get("decision")).strip().lower()
        if ":" in action_card_id:
            return action_card_id.rsplit(":", 1)[-1].strip().lower()
        return "confirm"

    @staticmethod
    def _resolve_action_ids(payload: dict) -> list[str]:
        raw_action_ids = payload.get("action_ids")
        if isinstance(raw_action_ids, list):
            ids = [str(item).strip() for item in raw_action_ids if str(item).strip()]
            if ids:
                return sorted(set(ids))

        single_action_id = payload.get("action_id")
        if isinstance(single_action_id, str) and single_action_id.strip():
            return [single_action_id.strip()]

        return []

    @staticmethod
    def _resolve_request_id(*, action_graph: dict, payload: dict, action_ids: list[str]) -> str:
        explicit = payload.get("request_id")
        if isinstance(explicit, str) and explicit.strip():
            return explicit.strip()

        action_map = action_graph.get("action_lifecycle", {}).get("actions", {}) if isinstance(action_graph, dict) else {}
        for action_id in action_ids:
            action_payload = action_map.get(action_id)
            if isinstance(action_payload, dict):
                request_id = action_payload.get("request_id")
                if isinstance(request_id, str) and request_id.strip():
                    return request_id.strip()

        raise ValueError("request_id could not be resolved for action execution")

    def _safe_fallback_response(
        self,
        *,
        family_id: str,
        session_id: str,
        reason: str,
        user_message: str | None = None,
    ) -> ChatResponse:
        key = _SessionKey(family_id=family_id, session_id=session_id)
        previous = self._last_snapshot.get(key)
        current: UIBootstrapState | None = None
        ui_patch: list[dict[str, Any]] = []
        explanation_summary: list[Any] = []
        try:
            current = self._bootstrap_service.get_state(family_id=family_id)
            ui_patch = self._patch_service.generate_patches(previous=previous, current=current)
            self._last_snapshot[key] = current
            explanation_summary = current.explanation_digest[:5]
        except Exception as exc:
            logger.warning("safe_fallback_response_degraded: %s", exc)

        assistant_message = reason
        if current is not None:
            contextual_guidance = self._build_contextual_guidance(current=current, user_message=user_message)
            if contextual_guidance:
                assistant_message = f"{reason}\n\n{contextual_guidance}"

        return ChatResponse(
            assistant_message=assistant_message,
            action_cards=[],
            ui_patch=ui_patch,
            requires_confirmation=False,
            explanation_summary=explanation_summary,
        )

    @staticmethod
    def _minimal_fallback_response(*, reason: str) -> ChatResponse:
        return ChatResponse(
            assistant_message=reason,
            action_cards=[],
            ui_patch=[],
            requires_confirmation=False,
            explanation_summary=[],
        )

    @staticmethod
    def _build_contextual_guidance(*, current: UIBootstrapState, user_message: str | None) -> str:
        if not user_message:
            return ""

        normalized = user_message.lower()

        if any(keyword in normalized for keyword in ("pantry", "inventory", "restock", "meal", "recipe", "grocery")):
            pantry = current.pantry
            if pantry is None:
                return "Pantry context is still loading. Try syncing and then ask for a restock or meal plan again."

            recommendations = ", ".join(pantry.grocery_recommendations[:5]) or "none"
            return (
                f"Pantry snapshot: {pantry.low_stock_count} low-stock item(s), "
                f"{len(pantry.inventory_items)} total tracked item(s), "
                f"{len(pantry.weekly_recipe_suggestions)} recipe suggestion(s). "
                f"Top grocery recommendations: {recommendations}."
            )

        if any(keyword in normalized for keyword in ("email", "inbox", "follow-up", "follow up", "debrief")):
            email_rows = [
                row
                for row in current.notifications
                if row.notification_id.startswith("notif:email_summary:")
            ]
            actionable = sum(
                1
                for row in email_rows
                if "action item" in row.message.lower() or "action required" in row.message.lower()
            )
            return (
                f"Inbox snapshot: {len(email_rows)} debrief item(s) currently surfaced, "
                f"{actionable} with action signals. Open Inbox for detailed triage and run Sync Inbox for a fresh pull."
            )

        if any(keyword in normalized for keyword in ("calendar", "schedule", "event", "meeting", "replan", "reschedule")):
            return (
                f"Calendar snapshot: {len(current.calendar.events)} event(s) in the active window "
                f"({current.calendar.window_start} to {current.calendar.window_end})."
            )

        if any(keyword in normalized for keyword in ("task", "todo", "to-do", "plan")):
            task_board = current.task_board
            open_count = len(task_board.pending) + len(task_board.in_progress)
            return (
                f"Task snapshot: {open_count} open task(s), "
                f"{len(task_board.completed)} completed, {len(task_board.failed)} failed."
            )

        return (
            f"Current overview: {current.today_overview.open_task_count} open task(s), "
            f"{current.today_overview.scheduled_event_count} scheduled event(s), "
            f"{current.today_overview.notification_count} notification(s)."
        )

    @staticmethod
    def _action_cards_from_decision(payload: dict) -> list[ActionCard]:
        recommended = payload.get("recommended_action", {})
        grouped = payload.get("grouped_approval_payload", {})
        action_id = str(recommended.get("action_id", ""))
        recommendation_title = str(recommended.get("title") or "recommended action").strip()
        recommendation_description = str(recommended.get("description", "")).strip()
        urgency = str(recommended.get("urgency", "medium"))
        risk_level = "high" if urgency == "high" else "medium"

        cards: list[ActionCard] = [
            ActionCard(
                id=f"card:{action_id}:confirm",
                type="confirm",
                title=f"Execute: {recommendation_title}",
                description=recommendation_description,
                related_entity=action_id,
                required_action_payload={
                    "group_id": grouped.get("group_id"),
                    "action_ids": grouped.get("action_ids", []),
                    "action_mode": "confirm",
                },
                risk_level=risk_level,
            ),
            ActionCard(
                id=f"card:{action_id}:reject",
                type="reject",
                title=f"Decline: {recommendation_title}",
                description="Dismiss this recommendation without applying changes.",
                related_entity=action_id,
                required_action_payload={
                    "action_ids": [action_id],
                    "action_mode": "reject",
                },
                risk_level="low",
            ),
        ]

        if recommended.get("scheduled_for"):
            cards.append(
                ActionCard(
                    id=f"card:{action_id}:reschedule",
                    type="reschedule",
                    title=f"Reschedule: {recommendation_title}",
                    description="Pick a different time for this recommendation.",
                    related_entity=action_id,
                    required_action_payload={
                        "action_id": action_id,
                        "current_schedule": recommended.get("scheduled_for"),
                        "action_mode": "reschedule",
                    },
                    risk_level="medium",
                )
            )

        cards.sort(key=lambda card: card.id)
        return cards
