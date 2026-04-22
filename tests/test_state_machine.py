"""
Comprehensive tests for ActionState FSM and StateMachine.

Tests validate:
- All allowed transitions
- All invalid transitions raise errors
- Retry policy enforcement
- Timeout calculation
- Error classification
- Event emission
- Idempotency
"""

import pytest
from datetime import UTC, datetime, timedelta

from apps.api.core.state_machine import (
    ActionState,
    StateMachine,
    StateTransitionEvent,
    TransitionError,
    RetryableError,
    NonRetryableError,
    can_transition,
    validate_transition,
    classify_error,
    RETRY_POLICY,
    ALLOWED_TRANSITIONS,
    STATE_TIMEOUTS,
    RETRYABLE_ERRORS,
    NON_RETRYABLE_ERRORS,
)


class TestTransitionRules:
    """Test basic transition validation."""

    def test_proposed_to_pending_approval_allowed(self):
        """proposed → pending_approval is allowed."""
        assert can_transition(ActionState.PROPOSED, ActionState.PENDING_APPROVAL)

    def test_proposed_to_approved_allowed(self):
        """proposed → approved is allowed."""
        assert can_transition(ActionState.PROPOSED, ActionState.APPROVED)

    def test_proposed_to_rejected_allowed(self):
        """proposed → rejected is allowed."""
        assert can_transition(ActionState.PROPOSED, ActionState.REJECTED)

    def test_proposed_to_failed_allowed(self):
        """proposed → failed is allowed."""
        assert can_transition(ActionState.PROPOSED, ActionState.FAILED)

    def test_pending_approval_to_approved_allowed(self):
        """pending_approval → approved is allowed."""
        assert can_transition(ActionState.PENDING_APPROVAL, ActionState.APPROVED)

    def test_pending_approval_to_rejected_allowed(self):
        """pending_approval → rejected is allowed."""
        assert can_transition(ActionState.PENDING_APPROVAL, ActionState.REJECTED)

    def test_approved_to_committed_allowed(self):
        """approved → committed is allowed."""
        assert can_transition(ActionState.APPROVED, ActionState.COMMITTED)

    def test_approved_to_failed_allowed(self):
        """approved → failed is allowed."""
        assert can_transition(ActionState.APPROVED, ActionState.FAILED)

    def test_failed_to_proposed_allowed(self):
        """failed → proposed (retry) is allowed."""
        assert can_transition(ActionState.FAILED, ActionState.PROPOSED)

    def test_committed_is_terminal(self):
        """committed state has no outgoing transitions."""
        assert len(ALLOWED_TRANSITIONS[ActionState.COMMITTED]) == 0

    def test_rejected_is_terminal(self):
        """rejected state has no outgoing transitions."""
        assert len(ALLOWED_TRANSITIONS[ActionState.REJECTED]) == 0

    def test_approved_to_pending_approval_denied(self):
        """approved → pending_approval is NOT allowed."""
        assert not can_transition(ActionState.APPROVED, ActionState.PENDING_APPROVAL)

    def test_pending_approval_to_proposed_denied(self):
        """pending_approval → proposed is NOT allowed."""
        assert not can_transition(ActionState.PENDING_APPROVAL, ActionState.PROPOSED)


class TestTransitionValidation:
    """Test validate_transition() with guards."""

    def test_noop_transition_raises(self):
        """No-op transition (A → A) raises TransitionError."""
        with pytest.raises(TransitionError, match="No-op transition"):
            validate_transition(ActionState.PROPOSED, ActionState.PROPOSED)

    def test_invalid_transition_raises(self):
        """Invalid transition raises TransitionError."""
        with pytest.raises(TransitionError, match="Invalid transition"):
            validate_transition(ActionState.APPROVED, ActionState.PROPOSED)

    def test_assistant_cannot_approve(self):
        """Assistant actor cannot approve (suggest-only)."""
        context = {"actor_type": "assistant"}
        with pytest.raises(TransitionError, match="suggest-only"):
            validate_transition(
                ActionState.PENDING_APPROVAL,
                ActionState.APPROVED,
                context=context,
            )

    def test_skipping_approval_not_allowed(self):
        """Cannot skip approval gate if requires_approval=true."""
        context = {"requires_approval": True}
        with pytest.raises(TransitionError, match="must transition through"):
            validate_transition(
                ActionState.PROPOSED,
                ActionState.APPROVED,
                context=context,
            )

    def test_valid_transition_passes(self):
        """Valid transition should not raise."""
        validate_transition(ActionState.PROPOSED, ActionState.PENDING_APPROVAL)
        # No exception raised


class TestStateMachine:
    """Test StateMachine executor."""

    def test_initial_state_is_proposed(self):
        """New StateMachine starts in proposed state."""
        fsm = StateMachine(action_id="test-123")
        assert fsm.state == ActionState.PROPOSED
        assert fsm.retry_count == 0

    def test_transition_changes_state(self):
        """transition_to() changes state."""
        fsm = StateMachine(action_id="test-123")
        event = fsm.transition_to(
            ActionState.PENDING_APPROVAL,
            reason="User triggered approval",
        )
        assert fsm.state == ActionState.PENDING_APPROVAL
        assert event.to_state == ActionState.PENDING_APPROVAL
        assert event.from_state == ActionState.PROPOSED

    def test_multiple_transitions(self):
        """Multiple transitions are recorded."""
        fsm = StateMachine(action_id="test-123")
        fsm.transition_to(ActionState.PENDING_APPROVAL, reason="First")
        fsm.transition_to(ActionState.APPROVED, reason="Second")
        assert len(fsm.transitions) == 2
        assert fsm.state == ActionState.APPROVED

    def test_invalid_transition_raises(self):
        """Invalid transition raises TransitionError."""
        fsm = StateMachine(action_id="test-123")
        fsm.transition_to(ActionState.APPROVED)
        with pytest.raises(TransitionError):
            fsm.transition_to(ActionState.PROPOSED)  # Not allowed backward

    def test_transition_event_has_metadata(self):
        """Transition events capture metadata."""
        fsm = StateMachine(action_id="test-123")
        event = fsm.transition_to(
            ActionState.PENDING_APPROVAL,
            reason="Test reason",
            correlation_id="corr-123",
            metadata={"custom": "data"},
        )
        assert event.reason == "Test reason"
        assert event.correlation_id == "corr-123"
        assert event.metadata == {"custom": "data"}


class TestRetryPolicy:
    """Test retry policy implementation."""

    def test_can_retry_on_failed_state(self):
        """Failed state allows retry if count < max_retries."""
        fsm = StateMachine(action_id="test-123")
        fsm.transition_to(ActionState.APPROVED)
        fsm.transition_to(ActionState.FAILED, error_code="network_timeout")
        assert fsm.can_retry() is True

    def test_cannot_retry_after_max_retries(self):
        """Cannot retry after max_retries exceeded."""
        fsm = StateMachine(action_id="test-123", retry_count=RETRY_POLICY["max_retries"])
        fsm.state = ActionState.FAILED  # Simulate failed state
        assert fsm.can_retry() is False

    def test_retry_delay_exponential_backoff(self):
        """Retry delay follows exponential backoff schedule."""
        fsm = StateMachine(action_id="test-123")
        fsm.state = ActionState.FAILED
        fsm.retry_count = 1

        # First retry: 1 second + 0.5 second jitter = ~1.5s
        delay1 = fsm.get_retry_delay()
        assert 0.5 <= delay1.total_seconds() <= 2.0

        # Second retry: 4 seconds + 1 second jitter = ~5s
        fsm.retry_count = 2
        delay2 = fsm.get_retry_delay()
        assert 3 <= delay2.total_seconds() <= 6

        # Third retry reaches max retries and returns no delay.
        fsm.retry_count = 3
        delay3 = fsm.get_retry_delay()
        assert delay3.total_seconds() == 0

    def test_max_retries_constant(self):
        """RETRY_POLICY has max_retries = 3."""
        assert RETRY_POLICY["max_retries"] == 3

    def test_retry_transition_increments_count(self):
        """Retry transition (failed → proposed) increments retry_count."""
        fsm = StateMachine(action_id="test-123")
        initial_count = fsm.retry_count
        fsm.transition_to(ActionState.APPROVED)
        fsm.transition_to(ActionState.FAILED)
        fsm.transition_to(ActionState.PROPOSED)  # Retry
        assert fsm.retry_count == initial_count + 1


class TestTimeouts:
    """Test timeout enforcement."""

    def test_proposed_has_600s_timeout(self):
        """proposed state times out after 600 seconds."""
        assert STATE_TIMEOUTS[ActionState.PROPOSED] == 600

    def test_pending_approval_has_1800s_timeout(self):
        """pending_approval state times out after 1800 seconds."""
        assert STATE_TIMEOUTS[ActionState.PENDING_APPROVAL] == 1800

    def test_approved_has_3600s_timeout(self):
        """approved state times out after 3600 seconds."""
        assert STATE_TIMEOUTS[ActionState.APPROVED] == 3600

    def test_committed_has_no_timeout(self):
        """committed (terminal) state has no timeout."""
        assert STATE_TIMEOUTS[ActionState.COMMITTED] is None

    def test_has_timed_out_true_after_timeout(self):
        """has_timed_out() returns True after timeout period."""
        fsm = StateMachine(action_id="test-123", state=ActionState.PROPOSED)
        fsm.updated_at = datetime.now(UTC) - timedelta(seconds=700)
        assert fsm.has_timed_out() is True

    def test_has_timed_out_false_before_timeout(self):
        """has_timed_out() returns False before timeout."""
        fsm = StateMachine(action_id="test-123", state=ActionState.PROPOSED)
        fsm.updated_at = datetime.now(UTC) - timedelta(seconds=500)
        assert fsm.has_timed_out() is False

    def test_get_timeout_seconds(self):
        """get_timeout_seconds() returns correct timeout."""
        fsm = StateMachine(action_id="test-123")
        fsm.state = ActionState.PROPOSED
        assert fsm.get_timeout_seconds() == 600


class TestErrorClassification:
    """Test error classification."""

    def test_network_timeout_is_retryable(self):
        """network_timeout is retryable."""
        assert classify_error("network_timeout") == "retryable"

    def test_database_connection_error_is_retryable(self):
        """database_connection_error is retryable."""
        assert classify_error("database_connection_error") == "retryable"

    def test_validation_error_is_not_retryable(self):
        """validation_error is not retryable."""
        assert classify_error("validation_error") == "non_retryable"

    def test_authorization_denied_is_not_retryable(self):
        """authorization_denied is not retryable."""
        assert classify_error("authorization_denied") == "non_retryable"

    def test_unknown_error_defaults_to_non_retryable(self):
        """Unknown errors default to non_retryable (safe default)."""
        assert classify_error("unknown_error_xyz") == "non_retryable"

    def test_all_retryable_errors_defined(self):
        """All RETRYABLE_ERRORS are in the set."""
        retryable_list = [
            "database_connection_error",
            "temporary_service_unavailable",
            "network_timeout",
            "deadlock_detected",
            "partial_write_failure",
        ]
        for error in retryable_list:
            assert error in RETRYABLE_ERRORS


class TestTerminalStates:
    """Test terminal state enforcement."""

    def test_committed_is_terminal(self):
        """committed state is terminal."""
        fsm = StateMachine(action_id="test-123")
        fsm.state = ActionState.COMMITTED
        assert fsm.is_terminal() is True

    def test_rejected_is_terminal(self):
        """rejected state is terminal."""
        fsm = StateMachine(action_id="test-123")
        fsm.state = ActionState.REJECTED
        assert fsm.is_terminal() is True

    def test_failed_not_terminal_if_can_retry(self):
        """failed state is not terminal if can_retry()."""
        fsm = StateMachine(action_id="test-123")
        fsm.state = ActionState.FAILED
        fsm.retry_count = 0
        assert fsm.is_terminal() is False

    def test_failed_is_terminal_after_max_retries(self):
        """failed state is terminal after max retries."""
        fsm = StateMachine(action_id="test-123")
        fsm.state = ActionState.FAILED
        fsm.retry_count = RETRY_POLICY["max_retries"]
        assert fsm.is_terminal() is True


class TestStateTransitionEvent:
    """Test StateTransitionEvent serialization."""

    def test_event_to_dict(self):
        """StateTransitionEvent serializes to dict."""
        event = StateTransitionEvent(
            action_id="test-123",
            from_state=ActionState.PROPOSED,
            to_state=ActionState.APPROVED,
            reason="Test transition",
            correlation_id="corr-123",
        )
        d = event.to_dict()
        assert d["action_id"] == "test-123"
        assert d["from_state"] == "proposed"
        assert d["to_state"] == "approved"
        assert d["reason"] == "Test transition"

    def test_event_has_timestamp(self):
        """StateTransitionEvent includes timestamp."""
        event = StateTransitionEvent(
            action_id="test-123",
            from_state=ActionState.PROPOSED,
            to_state=ActionState.APPROVED,
        )
        assert event.timestamp is not None
        assert isinstance(event.timestamp, datetime)


class TestEdgeCases:
    """Test edge cases and unusual scenarios."""

    def test_multiple_retries(self):
        """Can transition multiple times with retries."""
        fsm = StateMachine(action_id="test-123")
        for i in range(RETRY_POLICY["max_retries"]):
            fsm.transition_to(ActionState.APPROVED)
            fsm.transition_to(ActionState.FAILED, error_code="network_timeout")
            if fsm.can_retry():
                fsm.transition_to(ActionState.PROPOSED)

        assert fsm.retry_count > 0
        assert fsm.state == ActionState.FAILED

    def test_transition_reason_stored(self):
        """Transition reason is captured."""
        fsm = StateMachine(action_id="test-123")
        event = fsm.transition_to(
            ActionState.PENDING_APPROVAL,
            reason="User initiated approval",
        )
        assert event.reason == "User initiated approval"
        assert fsm.transitions[0].reason == event.reason


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
