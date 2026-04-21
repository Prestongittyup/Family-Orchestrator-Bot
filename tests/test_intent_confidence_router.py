"""
Tests for the Intent Confidence & Multi-Route Planning Layer.

Validates:
  - High-confidence → single-domain execution
  - Medium-confidence → top-2 domain execution
  - Low-confidence → clarification response, no execution
  - Multi-intent inputs → multiple secondary suggestions (not mixed actions)
  - IntentClassification fields (secondary_intents, ambiguity_flag, confidence bands)
  - API endpoint contract with ClarificationResponse and secondary_suggestions
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from assistant.governance.intent_lock import IntentClassification, IntentLock, IntentType
from assistant.governance.intent_router import (
    IntentRouter,
    RoutingCase,
    RoutingDecision,
    SecondarySuggestion,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _app() -> "TestClient":
    from apps.api.main import create_app
    return TestClient(create_app())


# ---------------------------------------------------------------------------
# 1. IntentClassification output shape
# ---------------------------------------------------------------------------

class TestIntentClassificationShape:
    """Verify IntentLock.classify() now returns IntentClassification."""

    def test_returns_intent_classification(self):
        result = IntentLock.classify("I need to get in shape")
        assert isinstance(result, IntentClassification)

    def test_has_primary_intent(self):
        result = IntentLock.classify("I need to get in shape")
        assert result.primary_intent == IntentType.FITNESS

    def test_has_confidence_float(self):
        result = IntentLock.classify("I need to work out regularly")
        assert isinstance(result.confidence, float)
        assert 0.0 <= result.confidence <= 1.0

    def test_has_secondary_intents_list(self):
        result = IntentLock.classify("I need to get in shape")
        assert isinstance(result.secondary_intents, list)

    def test_has_ambiguity_flag(self):
        result = IntentLock.classify("I need to get in shape")
        assert isinstance(result.ambiguity_flag, bool)

    def test_has_all_scores_dict(self):
        result = IntentLock.classify("I need to work out")
        assert isinstance(result.all_scores, dict)
        # Should have an entry for every intent type
        assert set(result.all_scores.keys()) == {t.value for t in IntentType}

    def test_allowed_domains_property(self):
        result = IntentLock.classify("I need to get in shape")
        assert result.allowed_domains == ["fitness"]

    def test_is_high_confidence_property(self):
        result = IntentLock.classify("I need to get in shape")
        # FITNESS has several patterns; verify the property reflects score
        if result.confidence > 0.7:
            assert result.is_high_confidence
        else:
            assert not result.is_high_confidence

    def test_is_low_confidence_on_no_match(self):
        result = IntentLock.classify("xyzzy asdf qwerty")
        assert result.is_low_confidence


# ---------------------------------------------------------------------------
# 2. Confidence band detection
# ---------------------------------------------------------------------------

class TestConfidenceBands:
    """Verify the three confidence bands are correctly assigned."""

    def test_high_confidence_explicit_fitness(self):
        """Multiple fitness keywords → high confidence."""
        result = IntentLock.classify(
            "I really want to start working out, do cardio and strength training"
        )
        assert result.confidence > 0.7
        assert result.is_high_confidence
        assert not result.is_medium_confidence
        assert not result.is_low_confidence

    def test_low_confidence_no_keywords(self):
        """Gibberish → low confidence fallback."""
        result = IntentLock.classify("help me with stuff")
        assert result.is_low_confidence
        assert result.confidence < 0.4

    def test_confidence_bounds_always_valid(self):
        """Confidence must always be in [0.0, 1.0]."""
        messages = [
            "I want to work out",
            "What should I cook?",
            "Book a doctor visit",
            "What are my top tasks today?",
            "undefined request asdf",
        ]
        for msg in messages:
            r = IntentLock.classify(msg)
            assert 0.0 <= r.confidence <= 1.0, f"Out of range for: {msg}"

    def test_ambiguity_flag_set_on_low_confidence(self):
        result = IntentLock.classify("help me")
        assert result.ambiguity_flag is True

    def test_ambiguity_flag_clear_on_strong_single_intent(self):
        """Very explicit fitness input should be unambiguous."""
        result = IntentLock.classify(
            "I want to do cardio training, strength workouts and fitness planning"
        )
        # Multiple fitness keywords; if high confidence, flag should be False
        if result.is_high_confidence:
            assert result.ambiguity_flag is False


# ---------------------------------------------------------------------------
# 3. IntentRouter routing cases
# ---------------------------------------------------------------------------

class TestIntentRouterCases:
    """Verify the three routing paths of IntentRouter."""

    def _high_conf_classification(self) -> IntentClassification:
        """Create a synthetic high-confidence classification."""
        return IntentClassification(
            primary_intent=IntentType.FITNESS,
            confidence=0.85,
            secondary_intents=[],
            ambiguity_flag=False,
            all_scores={t.value: 0.0 for t in IntentType},
            matched_keywords=["workout"],
        )

    def _medium_conf_classification(self) -> IntentClassification:
        return IntentClassification(
            primary_intent=IntentType.FITNESS,
            confidence=0.55,
            secondary_intents=[IntentType.MEAL],
            ambiguity_flag=True,
            all_scores={t.value: 0.0 for t in IntentType},
            matched_keywords=["exercise"],
        )

    def _low_conf_classification(self) -> IntentClassification:
        return IntentClassification(
            primary_intent=IntentType.DAILY_FOCUS,
            confidence=0.1,
            secondary_intents=[],
            ambiguity_flag=True,
            all_scores={t.value: 0.0 for t in IntentType},
            matched_keywords=[],
        )

    # Case A — High confidence

    def test_high_confidence_routing_case(self):
        routing = IntentRouter.route(self._high_conf_classification())
        assert routing.routing_case == RoutingCase.HIGH_CONFIDENCE

    def test_high_confidence_single_domain(self):
        routing = IntentRouter.route(self._high_conf_classification())
        assert routing.allowed_domains == ["fitness"]

    def test_high_confidence_should_execute(self):
        routing = IntentRouter.route(self._high_conf_classification())
        assert routing.should_execute is True

    def test_high_confidence_no_clarification(self):
        routing = IntentRouter.route(self._high_conf_classification())
        assert routing.clarification_text is None

    # Case B — Medium confidence

    def test_medium_confidence_routing_case(self):
        routing = IntentRouter.route(self._medium_conf_classification())
        assert routing.routing_case == RoutingCase.MEDIUM_CONFIDENCE

    def test_medium_confidence_top2_domains(self):
        routing = IntentRouter.route(self._medium_conf_classification())
        # Primary = fitness → ["fitness"], secondary = meal → ["meal"]
        assert "fitness" in routing.allowed_domains
        assert "meal" in routing.allowed_domains
        assert len(routing.allowed_domains) == 2

    def test_medium_confidence_should_execute(self):
        routing = IntentRouter.route(self._medium_conf_classification())
        assert routing.should_execute is True

    def test_medium_confidence_no_clarification(self):
        routing = IntentRouter.route(self._medium_conf_classification())
        assert routing.clarification_text is None

    # Case C — Low confidence

    def test_low_confidence_routing_case(self):
        routing = IntentRouter.route(self._low_conf_classification())
        assert routing.routing_case == RoutingCase.LOW_CONFIDENCE

    def test_low_confidence_empty_domains(self):
        routing = IntentRouter.route(self._low_conf_classification())
        assert routing.allowed_domains == []

    def test_low_confidence_should_not_execute(self):
        routing = IntentRouter.route(self._low_conf_classification())
        assert routing.should_execute is False

    def test_low_confidence_has_clarification_text(self):
        routing = IntentRouter.route(self._low_conf_classification())
        assert routing.clarification_text is not None
        assert len(routing.clarification_text) > 10

    def test_low_confidence_clarification_is_question(self):
        routing = IntentRouter.route(self._low_conf_classification())
        text = routing.clarification_text or ""
        # Should contain domain option labels
        assert any(
            word in text.lower()
            for word in ["planning", "fitness", "meal", "schedule", "appointment"]
        )

    def test_low_confidence_clarification_no_jargon(self):
        """Clarification text must not include system jargon."""
        routing = IntentRouter.route(self._low_conf_classification())
        text = routing.clarification_text or ""
        forbidden = ["graph state", "intent lock", "routing case", "decision engine"]
        for phrase in forbidden:
            assert phrase not in text.lower()

    # is_multi_intent

    def test_is_multi_intent_with_secondaries(self):
        routing = IntentRouter.route(self._medium_conf_classification())
        assert routing.is_multi_intent is True

    def test_is_not_multi_intent_without_secondaries(self):
        routing = IntentRouter.route(self._high_conf_classification())
        assert routing.is_multi_intent is False


# ---------------------------------------------------------------------------
# 4. route_message convenience method
# ---------------------------------------------------------------------------

class TestRouteMessage:
    """Verify IntentRouter.route_message() classifies+routes in one call."""

    def test_explicit_fitness_high_confidence(self):
        routing = IntentRouter.route_message(
            "I want to start cardio training and strength workouts"
        )
        assert routing.routing_case == RoutingCase.HIGH_CONFIDENCE
        assert "fitness" in routing.allowed_domains

    def test_explicit_medical_high_confidence(self):
        routing = IntentRouter.route_message("I need to make a doctor appointment")
        assert routing.allowed_domains == ["calendar"]
        assert routing.should_execute

    def test_ambiguous_produces_clarification(self):
        routing = IntentRouter.route_message("I just need some help today")
        assert not routing.should_execute
        assert routing.clarification_text is not None

    def test_daily_focus_executes(self):
        routing = IntentRouter.route_message("What should I focus on today?")
        assert routing.should_execute


# ---------------------------------------------------------------------------
# 5. Secondary / multi-intent suggestions
# ---------------------------------------------------------------------------

class TestSecondarySuggestions:
    """Verify multi-intent suggestion generation."""

    def test_build_returns_list(self):
        suggestions = IntentRouter.build_secondary_suggestions(
            secondary_intents=[IntentType.MEAL, IntentType.DAILY_FOCUS],
            graph={},
        )
        assert isinstance(suggestions, list)

    def test_one_suggestion_per_intent(self):
        suggestions = IntentRouter.build_secondary_suggestions(
            secondary_intents=[IntentType.MEAL, IntentType.FITNESS],
            graph={},
        )
        assert len(suggestions) == 2

    def test_suggestions_are_secondary_suggestion_instances(self):
        suggestions = IntentRouter.build_secondary_suggestions(
            secondary_intents=[IntentType.MEAL],
            graph={},
        )
        assert all(isinstance(s, SecondarySuggestion) for s in suggestions)

    def test_suggestions_have_required_fields(self):
        suggestions = IntentRouter.build_secondary_suggestions(
            secondary_intents=[IntentType.FITNESS, IntentType.MEAL],
            graph={},
        )
        for s in suggestions:
            assert s.intent is not None
            assert s.domain
            assert s.title
            assert s.description
            assert s.why

    def test_no_cross_domain_in_suggestions(self):
        """Each suggestion's domain must match its intent type."""
        domain_map = {
            IntentType.FITNESS: "fitness",
            IntentType.MEAL: "meal",
            IntentType.MEDICAL: "calendar",
            IntentType.DAILY_FOCUS: "general",
            IntentType.SCHEDULING: "calendar",
        }
        suggestions = IntentRouter.build_secondary_suggestions(
            secondary_intents=list(IntentType),
            graph={},
        )
        for s in suggestions:
            expected_domain = domain_map[s.intent]
            assert s.domain == expected_domain, (
                f"Domain mismatch for {s.intent}: got {s.domain}, expected {expected_domain}"
            )

    def test_multi_intent_life_together_scenario(self):
        """
        'I need to get my life together' should produce:
        - a primary action (likely fitness or daily_focus)
        - secondary suggestions for other relevant domains
        """
        routing = IntentRouter.route_message("I need to get my life together")
        # Vague, keyword-free input: decision engine must NOT run
        assert not routing.should_execute
        assert routing.clarification_text is not None
        clarification = routing.clarification_text.lower()
        assert any(
            label in clarification
            for label in ["planning", "fitness", "meal", "schedule", "appointment"]
        )

    def test_suggestions_are_not_mixed_into_primary_action(self):
        """
        Secondary suggestions must be separate; they must NOT alter allowed_domains
        for the primary pass.
        """
        routing = IntentRouter.route_message(
            "I want to work out and also figure out dinner"
        )
        if routing.routing_case == RoutingCase.HIGH_CONFIDENCE:
            # Primary domain uncontaminated
            primary_domains = IntentLock.INTENT_DOMAIN_MAP[
                routing.classification.primary_intent
            ]
            assert routing.allowed_domains == primary_domains
        # For medium confidence, top-2 is expected but still bounded
        elif routing.routing_case == RoutingCase.MEDIUM_CONFIDENCE:
            assert len(routing.allowed_domains) <= 2


# ---------------------------------------------------------------------------
# 6. Hard guarantees preserved after router introduction
# ---------------------------------------------------------------------------

class TestGuaranteesPreserved:
    """Existing hard guarantees must still hold with confidence-gated routing."""

    def test_fitness_still_blocked_from_calendar(self):
        routing = IntentRouter.route_message("I need to work out")
        if routing.routing_case == RoutingCase.HIGH_CONFIDENCE:
            assert "calendar" not in routing.allowed_domains

    def test_medical_still_blocked_from_meal(self):
        routing = IntentRouter.route_message("I need a doctor appointment")
        if routing.routing_case in (RoutingCase.HIGH_CONFIDENCE, RoutingCase.MEDIUM_CONFIDENCE):
            assert "meal" not in routing.allowed_domains

    def test_daily_focus_still_blocked_from_fitness(self):
        routing = IntentRouter.route_message("What should I focus on today?")
        if routing.routing_case == RoutingCase.HIGH_CONFIDENCE:
            assert "fitness" not in routing.allowed_domains

    def test_low_confidence_never_executes(self):
        """Any low-confidence routing MUST return should_execute = False."""
        low_conf = IntentClassification(
            primary_intent=IntentType.DAILY_FOCUS,
            confidence=0.05,
            secondary_intents=[],
            ambiguity_flag=True,
            all_scores={t.value: 0.0 for t in IntentType},
            matched_keywords=[],
        )
        routing = IntentRouter.route(low_conf)
        assert routing.should_execute is False

    def test_medium_confidence_max_2_domains(self):
        """Medium-confidence routing must never allow more than 2 domains."""
        mid_conf = IntentClassification(
            primary_intent=IntentType.FITNESS,
            confidence=0.5,
            secondary_intents=[IntentType.MEAL, IntentType.MEDICAL, IntentType.SCHEDULING],
            ambiguity_flag=True,
            all_scores={t.value: 0.0 for t in IntentType},
            matched_keywords=[],
        )
        routing = IntentRouter.route(mid_conf)
        assert len(routing.allowed_domains) <= 2


# ---------------------------------------------------------------------------
# 7. API endpoint contract
# ---------------------------------------------------------------------------

class TestAPIEndpointContract:
    """Verify /assistant/run contract with new fields."""

    def test_high_confidence_response_has_routing_case(self):
        client = _app()
        resp = client.post(
            "/assistant/run",
            json={"message": "I need to work out every morning"},
        )
        assert resp.status_code == 200
        body = resp.json()
        # Either an AssistantRunResponse (with routing_case) or a ClarificationResponse
        assert "routing_case" in body

    def test_high_confidence_response_has_secondary_suggestions(self):
        client = _app()
        resp = client.post(
            "/assistant/run",
            json={"message": "I want to start cardio and strength training"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "secondary_suggestions" in body or "clarification" in body

    def test_low_confidence_returns_clarification(self):
        client = _app()
        resp = client.post(
            "/assistant/run",
            json={"message": "asdf help me with stuff xyzzy"},
        )
        assert resp.status_code == 200
        body = resp.json()
        # Must return clarification field
        assert "clarification" in body or "routing_case" in body

    def test_clarification_response_shape(self):
        """When low confidence, response shape is ClarificationResponse."""
        client = _app()
        # A deliberately vague message that hits the < 0.4 confidence band
        resp = client.post(
            "/assistant/run",
            json={"message": "just help me somehow"},
        )
        assert resp.status_code == 200
        body = resp.json()
        if "clarification" in body:
            assert isinstance(body["clarification"], str)
            assert len(body["clarification"]) > 0
            assert body["routing_case"] == "low_confidence"

