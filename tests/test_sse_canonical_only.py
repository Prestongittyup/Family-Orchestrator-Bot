"""Unit tests for SSE canonical-only enforcement.

GOAL: Ensure ONLY SystemEvent (CanonicalEventEnvelope) instances can be emitted
through SSE. DomainEvent and raw dicts must be rejected with RuntimeError.
"""

from unittest.mock import MagicMock, patch, ANY
from datetime import UTC, datetime
import pytest

from apps.api.realtime.broadcaster import HouseholdBroadcaster
from apps.api.schemas.canonical_event import CanonicalEventEnvelope
from apps.api.schemas.event import SystemEvent


class TestSSECanonicalOnly:
    """Enforce that ONLY CanonicalEventEnvelope instances reach SSE."""

    @pytest.fixture
    def broadcaster(self):
        """Create a broadcaster instance for testing."""
        return HouseholdBroadcaster()

    @pytest.fixture
    def valid_envelope(self):
        """Create a valid CanonicalEventEnvelope."""
        return CanonicalEventEnvelope(
            event_id="test-event-123",
            event_type="task_created",
            actor_type="user",
            household_id="household-456",
            timestamp=datetime.now(UTC),
            source="task_service",
            severity="info",
            payload={
                "task_id": "task-789",
                "title": "Test Task",
                "status": "pending",
            },
        )

    def test_canonical_event_envelope_passes(self, broadcaster, valid_envelope):
        """TEST: CanonicalEventEnvelope passes validation and publishes."""
        # Should NOT raise
        try:
            broadcaster._validate_canonical_event(valid_envelope)
        except RuntimeError:
            pytest.fail("CanonicalEventEnvelope should pass validation")

    def test_domain_event_rejected(self, broadcaster):
        """TEST: DomainEvent-like object is rejected with RuntimeError."""
        
        # Create a DomainEvent-like object (has event_type but is not CanonicalEventEnvelope)
        class DomainEvent:
            def __init__(self):
                self.event_type = "task_proposed"
                self.data = {"task_id": "123"}
        
        domain_event = DomainEvent()
        
        with pytest.raises(RuntimeError) as exc_info:
            broadcaster._validate_canonical_event(domain_event)
        
        error_msg = str(exc_info.value)
        assert "Non-canonical event attempted on SSE stream" in error_msg
        assert "DomainEvent" in error_msg
        assert "CanonicalEventEnvelope" in error_msg

    def test_raw_dict_rejected(self, broadcaster):
        """TEST: Raw dict is rejected with RuntimeError."""
        
        raw_dict = {
            "event_type": "task_created",
            "task_id": "123",
            "status": "pending",
        }
        
        with pytest.raises(RuntimeError) as exc_info:
            broadcaster._validate_canonical_event(raw_dict)
        
        error_msg = str(exc_info.value)
        assert "Non-canonical event attempted on SSE stream" in error_msg
        assert "dict" in error_msg
        assert "CanonicalEventEnvelope" in error_msg

    def test_system_event_rejected_if_not_canonical_envelope(self, broadcaster):
        """TEST: SystemEvent (Pydantic model) is rejected if it's not wrapped in CanonicalEventEnvelope."""
        
        # SystemEvent is different from CanonicalEventEnvelope
        system_event = SystemEvent(
            event_id="test-event",
            household_id="household-456",
            type="task_created",
            source="task_service",
            payload={"task_id": "123"},
        )
        
        # Should be rejected because it's not a CanonicalEventEnvelope
        with pytest.raises(RuntimeError) as exc_info:
            broadcaster._validate_canonical_event(system_event)
        
        error_msg = str(exc_info.value)
        assert "Non-canonical event attempted on SSE stream" in error_msg
        assert "SystemEvent" in error_msg
        assert "CanonicalEventEnvelope" in error_msg

    def test_null_event_rejected(self, broadcaster):
        """TEST: None/null event is rejected."""
        
        with pytest.raises(RuntimeError) as exc_info:
            broadcaster._validate_canonical_event(None)
        
        error_msg = str(exc_info.value)
        assert "Non-canonical event attempted on SSE stream" in error_msg

    def test_validation_called_in_publish_sync(self, broadcaster, valid_envelope):
        """TEST: publish_sync() calls _validate_canonical_event()."""
        
        # Mock the validation method and other dependencies
        with patch.object(broadcaster, '_validate_canonical_event') as mock_validate, \
             patch.object(broadcaster, '_resolve_watermark', return_value=1), \
             patch.object(broadcaster, '_ring_buffers', {}), \
             patch.object(broadcaster, '_transport'):
            
            broadcaster.publish_sync(valid_envelope)
            
            # Assert validation was called with the envelope
            mock_validate.assert_called_once_with(valid_envelope)

    def test_validation_called_in_publish_async(self, broadcaster, valid_envelope):
        """TEST: publish() async method calls _validate_canonical_event()."""
        import asyncio
        
        # Mock the validation and other dependencies
        with patch.object(broadcaster, '_validate_canonical_event') as mock_validate, \
             patch.object(broadcaster, '_resolve_watermark', return_value=1), \
             patch.object(broadcaster, '_ring_buffers', {}), \
             patch.object(broadcaster, '_transport'), \
             patch('apps.api.observability.metrics.metrics'), \
             patch('apps.api.observability.logging.log_event'):
            
            async def run_test():
                await broadcaster.publish(valid_envelope)
            
            asyncio.run(run_test())
            
            # Assert validation was called
            mock_validate.assert_called_once_with(valid_envelope)

    @patch('apps.api.observability.logging.log_error')
    def test_logging_includes_event_type_and_origin(self, mock_log_error, broadcaster):
        """TEST: Rejection logging includes event_type, origin_module, and rejection_reason."""
        
        # Create a DomainEvent-like object
        class DomainEvent:
            event_type = "task_proposed"
        
        domain_event = DomainEvent()
        
        with pytest.raises(RuntimeError):
            broadcaster._validate_canonical_event(domain_event)
        
        # Assert log_error was called
        assert mock_log_error.called
        call_args = mock_log_error.call_args
        
        # Check that required fields are present in the call
        # log_error is called with: (event_name, message, **kwargs)
        assert "non_canonical_event_rejected_on_sse_stream" in call_args[0]
        
        # Check kwargs contain required fields
        kwargs = call_args[1]
        assert "event_type" in kwargs or "event_type" in str(call_args)
        assert "origin_module" in kwargs or "origin_module" in str(call_args)
        assert "rejection_reason" in kwargs or "rejection_reason" in str(call_args)

    def test_origin_module_detection(self, broadcaster):
        """TEST: _detect_origin_module() returns a reasonable module path."""
        
        origin = broadcaster._detect_origin_module()
        
        # Should return a string (either 'unknown' or a path)
        assert isinstance(origin, str)
        # Should not contain the broadcaster itself
        assert "broadcaster" not in origin.lower() or "unknown" in origin

    def test_reject_integer_event(self, broadcaster):
        """TEST: Integer (nonsensical) event is rejected."""
        
        with pytest.raises(RuntimeError) as exc_info:
            broadcaster._validate_canonical_event(42)
        
        error_msg = str(exc_info.value)
        assert "Non-canonical event attempted on SSE stream" in error_msg

    def test_reject_string_event(self, broadcaster):
        """TEST: String event is rejected."""
        
        with pytest.raises(RuntimeError) as exc_info:
            broadcaster._validate_canonical_event("not_an_event")
        
        error_msg = str(exc_info.value)
        assert "Non-canonical event attempted on SSE stream" in error_msg

    def test_only_canonical_envelope_passes(self, broadcaster):
        """TEST: Only CanonicalEventEnvelope instances pass without error."""
        
        # This should work
        envelope = CanonicalEventEnvelope(
            event_type="task_created",
            household_id="hh-1",
            timestamp=datetime.now(UTC),
            source="service",
            payload={},
        )
        
        # Should NOT raise
        broadcaster._validate_canonical_event(envelope)
        
        # Verify by calling it again without pytest.raises
        try:
            broadcaster._validate_canonical_event(envelope)
        except RuntimeError:
            pytest.fail("CanonicalEventEnvelope should always pass validation")


class TestSSECanonicalRejectionMessage:
    """Verify rejection messages are clear and actionable."""

    @pytest.fixture
    def broadcaster(self):
        return HouseholdBroadcaster()

    def test_rejection_message_mentions_allowed_type(self, broadcaster):
        """TEST: Error message explicitly states CanonicalEventEnvelope is required."""
        
        bad_event = {"event_type": "task_created"}
        
        with pytest.raises(RuntimeError) as exc_info:
            broadcaster._validate_canonical_event(bad_event)
        
        msg = str(exc_info.value)
        assert "CanonicalEventEnvelope" in msg
        assert "allowed" in msg.lower() or "only" in msg.lower()

    def test_rejection_message_shows_received_type(self, broadcaster):
        """TEST: Error message shows what type was actually received."""
        
        bad_event = {"key": "value"}
        
        with pytest.raises(RuntimeError) as exc_info:
            broadcaster._validate_canonical_event(bad_event)
        
        msg = str(exc_info.value)
        assert "dict" in msg  # Should mention the actual type received

    def test_rejection_message_includes_origin(self, broadcaster):
        """TEST: Error message includes origin module for debugging."""
        
        bad_event = None
        
        with pytest.raises(RuntimeError) as exc_info:
            broadcaster._validate_canonical_event(bad_event)
        
        msg = str(exc_info.value)
        # Should mention origin for debugging
        assert "Origin:" in msg
