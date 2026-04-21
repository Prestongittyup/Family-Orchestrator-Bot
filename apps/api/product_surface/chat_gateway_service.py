from __future__ import annotations

import logging
from threading import BoundedSemaphore
from dataclasses import dataclass

from apps.assistant_core.planning_engine import _fallback_household_state, _request_id
from apps.api.product_surface.bootstrap_service import UIBootstrapService
from apps.api.product_surface.contracts import (
    ActionCard,
    ChatResponse,
    UIBootstrapState,
)
from apps.api.product_surface.patch_service import UIPatchService
from apps.api.services.calendar_service import create_recurring_event, schedule_event
from apps.api.realtime.broadcaster import broadcaster
from household_os.core import HouseholdOSDecisionEngine, HouseholdStateGraphStore
from apps.api.llm.intent_resolver import LLMIntentResolver

logger = logging.getLogger(__name__)
_MESSAGE_WORKERS = BoundedSemaphore(value=4)


@dataclass(frozen=True)
class _SessionKey:
    family_id: str
    session_id: str


class ChatGatewayService:
    """UI-safe chat gateway that translates decision output into product-surface contracts."""

    def __init__(
        self,
        *,
        decision_engine: HouseholdOSDecisionEngine | None = None,
        graph_store: HouseholdStateGraphStore | None = None,
        bootstrap_service: UIBootstrapService | None = None,
        patch_service: UIPatchService | None = None,
        intent_resolver: LLMIntentResolver | None = None,
    ) -> None:
        self._decision_engine = decision_engine or HouseholdOSDecisionEngine()
        self._graph_store = graph_store or HouseholdStateGraphStore()
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
            return self._safe_fallback_response(
                family_id=family_id,
                session_id=session_id,
                reason="The assistant is handling high load right now. I kept state consistent; please retry.",
            )

        try:
            state = _fallback_household_state(family_id)
            graph = self._graph_store.refresh_graph(
                household_id=family_id,
                state=state,
                query=message,
                fitness_goal=None,
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

            request_id = _request_id(message, family_id, 10, None)
            decision = self._decision_engine.run(
                household_id=family_id,
                query=message,
                graph=graph,
                request_id=request_id,
            )

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
            broadcaster.publish_sync(
                household_id=family_id,
                event_type="chat_message_processed",
                payload={
                    "session_id": session_id,
                    "assistant_message": assistant_message,
                    "action_cards": [card.model_dump() for card in cards],
                    "requires_confirmation": requires_confirmation,
                },
            )
            return response
        except Exception as exc:
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
        broadcaster.publish_sync(
            household_id=family_id,
            event_type="action_card_executed",
            payload={
                "session_id": session_id,
                "action_card_id": action_card_id,
            },
        )
        return response

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
