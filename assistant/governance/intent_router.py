"""
Intent Router - Confidence-gated routing and multi-route planning layer.

Sits BETWEEN IntentLock (classification) and the decision engine:

    IntentLock.classify()
        ↓
    IntentRouter.route()
        ↓  returns RoutingDecision
    decision engine (domains constrained per routing decision)

Routing cases:
  Case A — High confidence (> 0.7) → single-domain, proceed normally.
  Case B — Medium confidence (0.4–0.7) → top-2 domains only.
  Case C — Low confidence (< 0.4) → DO NOT run decision engine; return clarification.

LifeState integration:
  LifeState is a weighting signal only. It does not change intent classification,
  does not bypass IntentLock constraints, and never triggers execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from assistant.governance.intent_lock import IntentClassification, IntentLock, IntentType
from assistant.state.life_state_model import LifeState


class RoutingCase(str, Enum):
    """Which routing path was chosen."""

    HIGH_CONFIDENCE = "high_confidence"
    MEDIUM_CONFIDENCE = "medium_confidence"
    LOW_CONFIDENCE = "low_confidence"


@dataclass(frozen=True)
class SecondarySuggestion:
    """A single suggestion produced by a secondary intent pass."""

    intent: IntentType
    domain: str
    title: str
    description: str
    why: str


@dataclass
class RoutingDecision:
    """Complete routing outcome passed directly to the orchestrator tick()."""

    routing_case: RoutingCase
    allowed_domains: list[str]
    classification: IntentClassification
    clarification_text: str | None = None
    secondary_suggestions: list[SecondarySuggestion] = field(default_factory=list)
    multi_intent_domains: list[list[str]] = field(default_factory=list)

    @property
    def should_execute(self) -> bool:
        return self.routing_case != RoutingCase.LOW_CONFIDENCE

    @property
    def is_multi_intent(self) -> bool:
        return bool(self.classification.secondary_intents)


_CLARIFICATION_TEMPLATE = (
    "Do you want help with {options}? "
    "A bit more detail helps me give you a more useful suggestion."
)

_INTENT_LABELS: dict[IntentType, str] = {
    IntentType.DAILY_FOCUS: "planning your day",
    IntentType.FITNESS: "fitness or exercise",
    IntentType.MEAL: "meal planning",
    IntentType.MEDICAL: "a medical appointment",
    IntentType.SCHEDULING: "scheduling or calendar",
}


class IntentRouter:
    """Confidence-gated router with optional LifeState weighting."""

    @classmethod
    def route(
        cls,
        classification: IntentClassification,
        life_state: LifeState | None = None,
    ) -> RoutingDecision:
        """
        Map a classification to a routing decision.

        LifeState only affects weighting/order within already allowed constraints.
        It never modifies primary intent classification.
        """
        if classification.is_low_confidence:
            return cls._low_confidence_route(classification, life_state)

        if classification.is_medium_confidence:
            return cls._medium_confidence_route(classification, life_state)

        return cls._high_confidence_route(classification, life_state)

    @classmethod
    def route_message(
        cls,
        user_message: str,
        life_state: LifeState | None = None,
    ) -> RoutingDecision:
        classification = IntentLock.classify(user_message)
        return cls.route(classification, life_state)

    @classmethod
    def _high_confidence_route(
        cls,
        c: IntentClassification,
        life_state: LifeState | None,
    ) -> RoutingDecision:
        secondary_intents = cls._rank_secondary_intents(
            secondary_intents=list(c.secondary_intents),
            life_state=life_state,
        )
        multi_domains = [IntentLock.INTENT_DOMAIN_MAP[sec] for sec in secondary_intents]

        return RoutingDecision(
            routing_case=RoutingCase.HIGH_CONFIDENCE,
            allowed_domains=c.allowed_domains,
            classification=c,
            multi_intent_domains=multi_domains,
        )

    @classmethod
    def _medium_confidence_route(
        cls,
        c: IntentClassification,
        life_state: LifeState | None,
    ) -> RoutingDecision:
        ranked_secondary = cls._rank_secondary_intents(
            secondary_intents=list(c.secondary_intents),
            life_state=life_state,
        )

        allowed_domains: list[str] = list(c.allowed_domains)
        if ranked_secondary:
            second_domain_set = IntentLock.INTENT_DOMAIN_MAP[ranked_secondary[0]]
            for domain in second_domain_set:
                if domain not in allowed_domains:
                    allowed_domains.append(domain)

        allowed_domains = allowed_domains[:2]

        multi_domains = [
            IntentLock.INTENT_DOMAIN_MAP[sec]
            for sec in ranked_secondary[1:]
        ]

        return RoutingDecision(
            routing_case=RoutingCase.MEDIUM_CONFIDENCE,
            allowed_domains=allowed_domains,
            classification=c,
            multi_intent_domains=multi_domains,
        )

    @classmethod
    def _low_confidence_route(
        cls,
        c: IntentClassification,
        life_state: LifeState | None,
    ) -> RoutingDecision:
        options_intents: list[IntentType] = [c.primary_intent] + list(c.secondary_intents)
        if not options_intents:
            options_intents = list(IntentType)[:3]

        ranked_options = cls._rank_secondary_intents(options_intents, life_state)
        if life_state and life_state.workload_score >= 0.75 and IntentType.DAILY_FOCUS not in ranked_options:
            ranked_options.insert(0, IntentType.DAILY_FOCUS)

        option_labels = [_INTENT_LABELS[i] for i in ranked_options[:3]]
        if len(option_labels) == 1:
            options_str = option_labels[0]
        elif len(option_labels) == 2:
            options_str = f"{option_labels[0]} or {option_labels[1]}"
        else:
            options_str = f"{option_labels[0]}, {option_labels[1]}, or {option_labels[2]}"

        return RoutingDecision(
            routing_case=RoutingCase.LOW_CONFIDENCE,
            allowed_domains=[],
            classification=c,
            clarification_text=_CLARIFICATION_TEMPLATE.format(options=options_str),
        )

    @classmethod
    def _rank_secondary_intents(
        cls,
        secondary_intents: list[IntentType],
        life_state: LifeState | None,
    ) -> list[IntentType]:
        """
        Weight ordering only; never adds intents outside the provided set.

        Rules:
        - high workload: bias DAILY_FOCUS earlier
        - high stress: reduce FITNESS/MEDICAL aggressiveness by demoting them
        - unstable routine: prioritize SCHEDULING stability actions
        """
        ranked = list(secondary_intents)
        if not ranked or life_state is None:
            return ranked

        def pull_to_front(intent_type: IntentType) -> None:
            if intent_type in ranked:
                ranked.remove(intent_type)
                ranked.insert(0, intent_type)

        def push_to_back(intent_type: IntentType) -> None:
            if intent_type in ranked:
                ranked.remove(intent_type)
                ranked.append(intent_type)

        if life_state.workload_score >= 0.75:
            pull_to_front(IntentType.DAILY_FOCUS)

        if life_state.routine_stability <= 0.4:
            pull_to_front(IntentType.SCHEDULING)

        if life_state.stress_index >= 0.7:
            push_to_back(IntentType.FITNESS)
            push_to_back(IntentType.MEDICAL)

        return ranked

    @classmethod
    def build_secondary_suggestions(
        cls,
        secondary_intents: list[IntentType],
        graph: dict[str, Any],
        life_state: LifeState | None = None,
    ) -> list[SecondarySuggestion]:
        suggestions: list[SecondarySuggestion] = []
        ranked = cls._rank_secondary_intents(list(secondary_intents), life_state)
        for intent in ranked:
            suggestion = cls._suggestion_for_intent(intent=intent, graph=graph, life_state=life_state)
            if suggestion:
                suggestions.append(suggestion)
        return suggestions

    @classmethod
    def _suggestion_for_intent(
        cls,
        *,
        intent: IntentType,
        graph: dict[str, Any],
        life_state: LifeState | None,
    ) -> SecondarySuggestion | None:
        reference_time = str(graph.get("reference_time", "today"))[:10]
        high_stress = bool(life_state and life_state.stress_index >= 0.7)
        unstable_routine = bool(life_state and life_state.routine_stability <= 0.4)

        if intent == IntentType.FITNESS:
            fitness_routines = list(graph.get("fitness_routines", []))
            goal = fitness_routines[-1] if fitness_routines else "a consistent exercise habit"
            description = (
                f"Schedule a 30-minute workout this week to work toward {goal}. "
                "A short session is better than none."
            )
            why = "Consistent movement builds long-term habits."
            if high_stress:
                description = (
                    "Start with a gentle 15-minute movement session this week. "
                    "Keep intensity low and focus on consistency."
                )
                why = "A lighter routine is easier to sustain under stress."
            return SecondarySuggestion(
                intent=intent,
                domain="fitness",
                title="Start a fitness session",
                description=description,
                why=why,
            )

        if intent == IntentType.MEAL:
            return SecondarySuggestion(
                intent=intent,
                domain="meal",
                title="Plan your meals",
                description=(
                    "Spend 15 minutes planning meals for the next 3 days. "
                    "Check your pantry first to reduce grocery trips."
                ),
                why="Planned meals reduce stress and cut food waste.",
            )

        if intent == IntentType.MEDICAL:
            description = (
                "Book a routine health appointment. Most checkups take under an hour "
                "and prevent larger issues later."
            )
            why = "Preventive care is easier to fit in than reactive care."
            if high_stress:
                description = "Check whether any health follow-up is due this month and pick a low-friction slot."
                why = "A lighter planning step keeps healthcare on track without overload."
            return SecondarySuggestion(
                intent=intent,
                domain="calendar",
                title="Schedule a health check",
                description=description,
                why=why,
            )

        if intent == IntentType.DAILY_FOCUS:
            return SecondarySuggestion(
                intent=intent,
                domain="general",
                title="Review your priorities",
                description=(
                    f"Take 20 minutes on {reference_time} to list your top 3 tasks "
                    "and decide what can wait."
                ),
                why="A clear focus list lowers decision fatigue throughout the day.",
            )

        if intent == IntentType.SCHEDULING:
            description = (
                "Block time for your most important commitment this week. "
                "Even 1 hour of protected time changes outcomes."
            )
            why = "Proactive scheduling prevents last-minute conflicts."
            if unstable_routine:
                description = (
                    "Create two fixed planning blocks this week (one morning, one evening) "
                    "to stabilize your routine."
                )
                why = "Stable time anchors improve follow-through when routines are inconsistent."
            return SecondarySuggestion(
                intent=intent,
                domain="calendar",
                title="Organise your calendar",
                description=description,
                why=why,
            )

        return None
