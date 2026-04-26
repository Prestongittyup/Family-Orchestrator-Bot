from __future__ import annotations

import logging
from threading import BoundedSemaphore
from dataclasses import dataclass
from datetime import UTC, datetime

from apps.assistant_core.planning_engine import _fallback_household_state
from apps.api.product_surface.bootstrap_service import UIBootstrapService
from apps.api.product_surface.contracts import (
    ActionCard,
    ChatResponse,
    UIBootstrapState,
)
from apps.api.product_surface.patch_service import UIPatchService
from apps.api.services.calendar_service import create_recurring_event, schedule_event
from apps.api.schemas.event import SystemEvent
from apps.api.services.canonical_event_adapter import CanonicalEventAdapter
from apps.api.services.canonical_event_router import canonical_event_router
from household_os.runtime.orchestrator import HouseholdOSOrchestrator, OrchestratorRequest, RequestActionType
from apps.api.llm.intent_resolver import LLMIntentResolver

logger = logging.getLogger(__name__)
_MESSAGE_WORKERS = BoundedSemaphore(value=4)


class _ChatGatewayRouter:
    @staticmethod
    def emit(event: SystemEvent) -> None:
        canonical_event_router.route(
            CanonicalEventAdapter.to_envelope(event),
            persist=False,
            dispatch=False,
        )


router = _ChatGatewayRouter()


@dataclass(frozen=True)
class _SessionKey:
    family_id: str
    session_id: str


class ChatGatewayService:
    """UI-safe chat gateway that translates decision output into product-surface contracts."""

    def __init__(
        self,
        *,
        orchestrator: HouseholdOSOrchestrator | None = None,
        bootstrap_service: UIBootstrapService | None = None,
        patch_service: UIPatchService | None = None,
        intent_resolver: LLMIntentResolver | None = None,
    ) -> None:
        self._orchestrator = orchestrator or HouseholdOSOrchestrator()
        self._bootstrap_service = bootstrap_service or UIBootstrapService()
        self._patch_service = patch_service or UIPatchService()
        self._intent_resolver = intent_resolver or LLMIntentResolver()
        self._last_snapshot: dict[_SessionKey, UIBootstrapState] = {}

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
            return self._safe_fallback_response(
                family_id=family_id,
                session_id=session_id,
                reason="The assistant is handling high load right now. I kept state consistent; please retry.",
            )

        input_payload = {
            "family_id": family_id,
            "message": message,
            "session_id": session_id,
        }

        try:
            state = _fallback_household_state(family_id)
            graph = self._orchestrator.handle_request(
                OrchestratorRequest(
                    action_type=RequestActionType.READ_SENSITIVE_STATE,
                    household_id=family_id,
                    actor={
                        "actor_type": "api_user",
                        "subject_id": family_id,
                        "session_id": session_id,
                        "verified": True,
                    },
                    resource_type="chat_context",
                    context={"system_worker_verified": False},
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
                    action_type=RequestActionType.RUN,
                    household_id=family_id,
                    actor={
                        "actor_type": "api_user",
                        "subject_id": family_id,
                        "session_id": session_id,
                        "verified": True,
                    },
                    state=state,
                    user_input=message,
                    fitness_goal=None,
                    context={"system_worker_verified": False},
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

            current = self._bootstrap_service.get_state(family_id=family_id)
            key = _SessionKey(family_id=family_id, session_id=session_id)
            previous = self._last_snapshot.get(key)
            ui_patch = self._patch_service.generate_patches(previous=previous, current=current)
            self._last_snapshot[key] = current

            response = ChatResponse(
                assistant_message=assistant_message,
                action_cards=cards,
                ui_patch=ui_patch,
                requires_confirmation=requires_confirmation,
                explanation_summary=current.explanation_digest[:5],
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
            return self._safe_fallback_response(
                family_id=family_id,
                session_id=session_id,
                reason="I couldn't fully process that request. I kept the household state consistent and you can retry.",
            )
        finally:
            _MESSAGE_WORKERS.release()

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

        For P0 this supports calendar creation payloads. If no actionable
        payload is present, a no-op response is returned with refreshed patches.
        """
        user_id = str(payload.get("user_id") or "user-admin")
        title = payload.get("title")
        recurrence = str(payload.get("recurrence") or "none")

        input_payload = {
            "family_id": family_id,
            "session_id": session_id,
            "action_card_id": action_card_id,
            "payload": payload,
        }

        try:
            if title:
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

            current = self._bootstrap_service.get_state(family_id=family_id)
            key = _SessionKey(family_id=family_id, session_id=session_id)
            previous = self._last_snapshot.get(key)
            ui_patch = self._patch_service.generate_patches(previous=previous, current=current)
            self._last_snapshot[key] = current

            response = ChatResponse(
                assistant_message="Action executed.",
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
                    content=str(title or "Action executed."),
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

    def _safe_fallback_response(self, *, family_id: str, session_id: str, reason: str) -> ChatResponse:
        current = self._bootstrap_service.get_state(family_id=family_id)
        key = _SessionKey(family_id=family_id, session_id=session_id)
        previous = self._last_snapshot.get(key)
        ui_patch = self._patch_service.generate_patches(previous=previous, current=current)
        self._last_snapshot[key] = current
        return ChatResponse(
            assistant_message=reason,
            action_cards=[],
            ui_patch=ui_patch,
            requires_confirmation=False,
            explanation_summary=current.explanation_digest[:5],
        )

    @staticmethod
    def _action_cards_from_decision(payload: dict) -> list[ActionCard]:
        recommended = payload.get("recommended_action", {})
        grouped = payload.get("grouped_approval_payload", {})
        action_id = str(recommended.get("action_id", ""))
        urgency = str(recommended.get("urgency", "medium"))
        risk_level = "high" if urgency == "high" else "medium"

        cards: list[ActionCard] = [
            ActionCard(
                id=f"card:{action_id}:confirm",
                type="confirm",
                title="Confirm recommendation",
                description=str(recommended.get("description", "")),
                related_entity=action_id,
                required_action_payload={
                    "group_id": grouped.get("group_id"),
                    "action_ids": grouped.get("action_ids", []),
                },
                risk_level=risk_level,
            ),
            ActionCard(
                id=f"card:{action_id}:reject",
                type="reject",
                title="Reject recommendation",
                description="Dismiss this recommendation without applying changes.",
                related_entity=action_id,
                required_action_payload={"action_ids": [action_id]},
                risk_level="low",
            ),
        ]

        if recommended.get("scheduled_for"):
            cards.append(
                ActionCard(
                    id=f"card:{action_id}:reschedule",
                    type="reschedule",
                    title="Adjust schedule",
                    description="Pick a different time for this recommendation.",
                    related_entity=action_id,
                    required_action_payload={
                        "action_id": action_id,
                        "current_schedule": recommended.get("scheduled_for"),
                    },
                    risk_level="medium",
                )
            )

        cards.sort(key=lambda card: card.id)
        return cards
