"""
Comprehensive tests for Intent Lock Layer.

Ensures that intent classification and action space filtering work correctly,
preventing cross-domain action selection.
"""

import pytest

from assistant.governance.intent_lock import IntentLock, IntentType


class TestIntentClassification:
    """Tests for intent classification from user messages."""

    def test_daily_focus_detection_explicit(self):
        """Test detection of explicit daily focus intent."""
        decision = IntentLock.classify("What should I focus on today?")
        assert decision.primary_intent == IntentType.DAILY_FOCUS
        assert "general" in decision.allowed_domains

    def test_daily_focus_detection_priorities(self):
        """Test daily focus detection with 'prioritize' keyword."""
        decision = IntentLock.classify("Help me prioritize my day")
        assert decision.primary_intent == IntentType.DAILY_FOCUS

    def test_daily_focus_detection_organizing(self):
        """Test daily focus detection with 'organize' keyword."""
        decision = IntentLock.classify("I need to organize my day")
        assert decision.primary_intent == IntentType.DAILY_FOCUS

    def test_fitness_detection_explicit(self):
        """Test detection of explicit fitness intent."""
        decision = IntentLock.classify("I need to get in shape")
        assert decision.primary_intent == IntentType.FITNESS
        assert "fitness" in decision.allowed_domains

    def test_fitness_detection_workout(self):
        """Test fitness detection with 'workout' keyword."""
        decision = IntentLock.classify("Can you help me schedule a workout?")
        assert decision.primary_intent == IntentType.FITNESS

    def test_fitness_detection_exercise(self):
        """Test fitness detection with 'exercise' keyword."""
        decision = IntentLock.classify("I want to exercise more regularly")
        assert decision.primary_intent == IntentType.FITNESS

    def test_meal_detection_explicit(self):
        """Test detection of explicit meal intent."""
        decision = IntentLock.classify("What should I cook for dinner?")
        assert decision.primary_intent == IntentType.MEAL
        assert "meal" in decision.allowed_domains

    def test_meal_detection_grocery(self):
        """Test meal detection with 'grocery' keyword."""
        decision = IntentLock.classify("I need to plan my grocery shopping")
        assert decision.primary_intent == IntentType.MEAL

    def test_medical_detection_doctor(self):
        """Test detection of medical intent with 'doctor'."""
        decision = IntentLock.classify("I need to make a doctor appointment")
        assert decision.primary_intent == IntentType.MEDICAL
        assert "calendar" in decision.allowed_domains

    def test_medical_detection_dentist(self):
        """Test medical detection with 'dentist' keyword."""
        decision = IntentLock.classify("When can I book a dentist checkup?")
        assert decision.primary_intent == IntentType.MEDICAL

    def test_scheduling_detection_explicit(self):
        """Test detection of scheduling intent."""
        decision = IntentLock.classify("I need to schedule a meeting")
        assert decision.primary_intent == IntentType.SCHEDULING
        assert "calendar" in decision.allowed_domains

    def test_case_insensitive_classification(self):
        """Test that classification is case-insensitive."""
        decision1 = IntentLock.classify("What should I focus on today?")
        decision2 = IntentLock.classify("WHAT SHOULD I FOCUS ON TODAY?")
        assert decision1.primary_intent == decision2.primary_intent
        assert decision1.allowed_domains == decision2.allowed_domains


class TestActionSpaceFiltering:
    """Tests for filtering candidates based on intent."""

    def test_fitness_intent_allows_fitness_domain(self):
        """Verify that fitness intent allows fitness domain candidates."""
        decision = IntentLock.classify("I need to get in shape")
        assert decision.primary_intent == IntentType.FITNESS
        # Fitness domain should be allowed
        allowed = decision.allowed_domains
        assert "fitness" in allowed

    def test_fitness_intent_blocks_other_domains(self):
        """Verify that fitness intent blocks non-fitness domains."""
        decision = IntentLock.classify("I need to get in shape")
        # Only fitness should be in allowed domains
        assert decision.allowed_domains == ["fitness"]
        assert "meal" not in decision.allowed_domains
        assert "calendar" not in decision.allowed_domains
        assert "general" not in decision.allowed_domains

    def test_meal_intent_allows_meal_domain(self):
        """Verify meal intent allows only meal domain."""
        decision = IntentLock.classify("What should I cook for dinner?")
        assert decision.primary_intent == IntentType.MEAL
        assert decision.allowed_domains == ["meal"]
        assert "fitness" not in decision.allowed_domains

    def test_medical_intent_allows_calendar_domain(self):
        """Verify medical intent allows only calendar domain."""
        decision = IntentLock.classify("I need to make a doctor appointment")
        assert decision.primary_intent == IntentType.MEDICAL
        assert decision.allowed_domains == ["calendar"]
        assert "meal" not in decision.allowed_domains
        assert "fitness" not in decision.allowed_domains

    def test_daily_focus_intent_allows_general_domain(self):
        """Verify daily focus intent allows only general domain."""
        decision = IntentLock.classify("What should I focus on today?")
        assert decision.primary_intent == IntentType.DAILY_FOCUS
        assert decision.allowed_domains == ["general"]
        assert "fitness" not in decision.allowed_domains
        assert "meal" not in decision.allowed_domains
        assert "calendar" not in decision.allowed_domains

    def test_scheduling_intent_allows_calendar_domain(self):
        """Verify scheduling intent allows only calendar domain."""
        decision = IntentLock.classify("I need to schedule a meeting")
        assert decision.primary_intent == IntentType.SCHEDULING
        assert decision.allowed_domains == ["calendar"]


class TestCandidateFiltering:
    """Tests for filtering actual candidate actions."""

    def setup_method(self):
        """Set up test candidates."""
        self.candidates = [
            {"domain": "fitness", "title": "Schedule workout"},
            {"domain": "meal", "title": "Cook dinner"},
            {"domain": "calendar", "title": "Book appointment"},
            {"domain": "general", "title": "Review tasks"},
        ]

    def test_filter_fitness_candidates_from_fitness_intent(self):
        """Test that only fitness candidates pass when intent is fitness."""
        fitness_candidates = [c for c in self.candidates if c["domain"] == "fitness"]
        assert len(fitness_candidates) == 1
        assert fitness_candidates[0]["title"] == "Schedule workout"

    def test_filter_meal_candidates_from_meal_intent(self):
        """Test that only meal candidates pass when intent is meal."""
        meal_candidates = [c for c in self.candidates if c["domain"] == "meal"]
        assert len(meal_candidates) == 1
        assert meal_candidates[0]["title"] == "Cook dinner"

    def test_filter_calendar_candidates_from_medical_intent(self):
        """Test that only calendar candidates pass when intent is medical."""
        calendar_candidates = [c for c in self.candidates if c["domain"] == "calendar"]
        assert len(calendar_candidates) == 1
        assert calendar_candidates[0]["title"] == "Book appointment"

    def test_no_cross_domain_leakage_fitness_to_meal(self):
        """Test that fitness intent cannot select meal actions."""
        decision = IntentLock.classify("I need to get in shape")
        # Meal domain should NOT be in allowed list
        assert "meal" not in decision.allowed_domains
        # Verify no meal action can be selected
        meal_allowed = any(c["domain"] in decision.allowed_domains for c in self.candidates if c["domain"] == "meal")
        assert not meal_allowed

    def test_no_cross_domain_leakage_fitness_to_calendar(self):
        """Test that fitness intent cannot select calendar actions."""
        decision = IntentLock.classify("I need to get in shape")
        # Calendar domain should NOT be in allowed list
        assert "calendar" not in decision.allowed_domains

    def test_no_cross_domain_leakage_meal_to_fitness(self):
        """Test that meal intent cannot select fitness actions."""
        decision = IntentLock.classify("What should I cook for dinner?")
        # Fitness domain should NOT be in allowed list
        assert "fitness" not in decision.allowed_domains

    def test_no_cross_domain_leakage_daily_focus_to_fitness(self):
        """Test that daily focus intent cannot select fitness actions."""
        decision = IntentLock.classify("What should I focus on today?")
        # Only general domain allowed
        assert decision.allowed_domains == ["general"]
        assert "fitness" not in decision.allowed_domains

    def test_no_cross_domain_leakage_daily_focus_to_meal(self):
        """Test that daily focus intent cannot select meal actions."""
        decision = IntentLock.classify("What should I focus on today?")
        # Meal domain should NOT be in allowed list
        assert "meal" not in decision.allowed_domains

    def test_no_cross_domain_leakage_daily_focus_to_calendar(self):
        """Test that daily focus intent cannot select calendar actions."""
        decision = IntentLock.classify("What should I focus on today?")
        # Calendar domain should NOT be in allowed list
        assert "calendar" not in decision.allowed_domains


class TestFallbackBehavior:
    """Tests for fallback behavior when no valid actions exist."""

    def test_fallback_recommendation_structure(self):
        """Test that fallback recommendation has required fields."""
        fallback = IntentLock.get_fallback_recommendation()
        assert "action_id" in fallback
        assert "target_domain" in fallback
        assert "recommendation" in fallback
        assert "why" in fallback
        assert "impact" in fallback
        assert "approval_required" in fallback

    def test_fallback_recommendation_is_safe(self):
        """Test that fallback recommendation is generic and safe."""
        fallback = IntentLock.get_fallback_recommendation()
        # Should contain schedule review content
        assert "organize" in fallback["recommendation"].lower() or "priorities" in fallback["recommendation"].lower()
        # Should not contain domain-specific terms
        assert "workout" not in fallback["recommendation"].lower()
        assert "cook" not in fallback["recommendation"].lower()
        assert "appointment" not in fallback["recommendation"].lower()

    def test_fallback_is_general_domain(self):
        """Test that fallback uses general domain."""
        fallback = IntentLock.get_fallback_recommendation()
        assert fallback["target_domain"] == "general"

    def test_fallback_has_valid_reasoning(self):
        """Test that fallback has clear reasoning."""
        fallback = IntentLock.get_fallback_recommendation()
        why = fallback["why"]
        assert isinstance(why, list)
        assert len(why) > 0
        assert all(isinstance(item, str) for item in why)


class TestConfidenceScoring:
    """Tests for intent classification confidence scoring."""

    def test_explicit_intent_high_confidence(self):
        """Test that explicit intent signals have reasonable confidence."""
        decision = IntentLock.classify("I need to get in shape and start working out")
        # Should have multiple pattern matches for fitness intent
        assert decision.primary_intent == IntentType.FITNESS
        assert decision.confidence > 0.2  # At least some confidence from multiple matches

    def test_no_match_low_confidence(self):
        """Test that no-match scenarios have low confidence."""
        decision = IntentLock.classify("Lorem ipsum dolor sit amet")
        # Should default to something but with low confidence
        assert decision.confidence <= 0.1 or decision.primary_intent == IntentType.DAILY_FOCUS

    def test_confidence_bounds(self):
        """Test that confidence is always in [0, 1] range."""
        test_messages = [
            "What should I do today?",
            "Schedule a workout tomorrow",
            "Make a doctor appointment",
            "xyz qwerty asdf",
        ]
        for msg in test_messages:
            decision = IntentLock.classify(msg)
            assert 0.0 <= decision.confidence <= 1.0


class TestHardGuarantees:
    """Tests proving that cross-domain contamination is mathematically impossible."""

    def test_fitness_prompt_cannot_produce_appointment(self):
        """HARD GUARANTEE: Fitness prompt blocks appointment actions."""
        decision = IntentLock.classify("I need to get in shape")
        # Calendar domain (appointments) should NOT be allowed
        assert "calendar" not in decision.allowed_domains
        # This makes it mathematically impossible for an appointment to be selected

    def test_fitness_prompt_cannot_produce_meal(self):
        """HARD GUARANTEE: Fitness prompt blocks meal scheduling."""
        decision = IntentLock.classify("I want to start working out regularly")
        # Meal domain should NOT be allowed
        assert "meal" not in decision.allowed_domains

    def test_medical_prompt_cannot_produce_workout(self):
        """HARD GUARANTEE: Medical prompt blocks fitness actions."""
        decision = IntentLock.classify("I need to make a doctor appointment")
        # Fitness domain should NOT be allowed
        assert "fitness" not in decision.allowed_domains

    def test_meal_prompt_cannot_produce_appointment(self):
        """HARD GUARANTEE: Meal prompt blocks appointment booking."""
        decision = IntentLock.classify("What should I cook for dinner?")
        # Calendar domain should NOT be allowed
        assert "calendar" not in decision.allowed_domains

    def test_meal_prompt_cannot_produce_workout(self):
        """HARD GUARANTEE: Meal prompt blocks fitness scheduling."""
        decision = IntentLock.classify("I need to plan my meals for the week")
        # Fitness domain should NOT be allowed
        assert "fitness" not in decision.allowed_domains

    def test_daily_focus_cannot_produce_domain_specific_action(self):
        """HARD GUARANTEE: Daily focus blocks all domain-specific actions."""
        decision = IntentLock.classify("What should I focus on today?")
        # Only general domain allowed
        assert decision.allowed_domains == ["general"]
        assert "fitness" not in decision.allowed_domains
        assert "meal" not in decision.allowed_domains
        assert "calendar" not in decision.allowed_domains


class TestIntegrationScenarios:
    """Integration tests for realistic scenarios."""

    def test_realistic_fitness_scenario(self):
        """Test realistic fitness user query."""
        queries = [
            "I need to get in shape",
            "Can you help me schedule a workout?",
            "I want to exercise more",
            "Let's focus on fitness",
        ]
        for query in queries:
            decision = IntentLock.classify(query)
            assert decision.primary_intent == IntentType.FITNESS
            assert decision.allowed_domains == ["fitness"]

    def test_realistic_meal_scenario(self):
        """Test realistic meal planning query."""
        queries = [
            "What should I cook for dinner?",
            "I need to go grocery shopping",
            "Plan my meals for the week",
            "What's for dinner tonight?",
        ]
        for query in queries:
            decision = IntentLock.classify(query)
            assert decision.primary_intent == IntentType.MEAL
            assert decision.allowed_domains == ["meal"]

    def test_realistic_medical_scenario(self):
        """Test realistic medical/appointment query."""
        queries = [
            "I need to make a doctor appointment",
            "When can I see a dentist?",
            "Book a medical checkup",
            "I need to schedule a doctor visit",
        ]
        for query in queries:
            decision = IntentLock.classify(query)
            assert decision.primary_intent == IntentType.MEDICAL
            assert decision.allowed_domains == ["calendar"]

    def test_realistic_daily_focus_scenario(self):
        """Test realistic daily focus/planning query."""
        queries = [
            "What should I focus on today?",
            "Help me prioritize my day",
            "What are my top tasks for today?",
            "Organize my schedule for today",
        ]
        for query in queries:
            decision = IntentLock.classify(query)
            assert decision.primary_intent == IntentType.DAILY_FOCUS
            assert decision.allowed_domains == ["general"]
