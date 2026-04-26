"""Unit tests for task_service event emission."""

from unittest.mock import MagicMock, patch, call
from datetime import UTC, datetime

from apps.api.services.task_service import update_task_metadata
from apps.api.models.task import Task


def test_update_task_metadata_emits_event():
    """Test that update_task_metadata emits task_metadata_updated event."""
    
    # Mock task and session
    mock_task = MagicMock(spec=Task)
    mock_task.id = "task-123"
    mock_task.household_id = "household-456"
    mock_task.priority = "high"
    mock_task.description = "original description"
    
    mock_session = MagicMock()
    mock_session.get.return_value = mock_task
    
    # Patch router, adapter, and session
    with patch("apps.api.services.task_service.canonical_event_router") as mock_router, \
         patch("apps.api.services.task_service.CanonicalEventAdapter") as mock_adapter, \
         patch("apps.api.services.task_service.SessionLocal", return_value=mock_session):
        
        # Execute: update metadata with different priority and category
        update_task_metadata("task-123", priority="low", category="new category")
        
        # Assert: session.commit was called once
        mock_session.commit.assert_called_once()
        
        # Assert: router.route was called exactly once
        assert mock_router.route.call_count == 1, \
            f"Expected router.route to be called once, got {mock_router.route.call_count}"
        
        # Extract the event envelope passed to router.route
        route_call_args = mock_router.route.call_args
        event_envelope = route_call_args[0][0]
        
        # Verify adapter was called to create envelope
        mock_adapter.to_envelope.assert_called_once()
        
        # Extract the SystemEvent from adapter call
        adapter_call_args = mock_adapter.to_envelope.call_args
        event = adapter_call_args[0][0]
        
        # Assert: event type is correct
        assert event.type == "task_metadata_updated", \
            f"Expected event type 'task_metadata_updated', got '{event.type}'"
        
        # Assert: payload contains task_id, old_metadata, new_metadata
        payload = event.payload
        assert payload["task_id"] == "task-123"
        assert "old_metadata" in payload
        assert "new_metadata" in payload
        
        # Assert: old_metadata captures original values
        assert payload["old_metadata"]["priority"] == "high"
        assert payload["old_metadata"]["description"] == "original description"
        
        # Assert: new_metadata reflects Updated values after assignment
        assert payload["new_metadata"]["priority"] == "low"
        assert payload["new_metadata"]["description"] == "new category"
        
        # Assert: old_metadata != new_metadata when changes occur
        assert payload["old_metadata"] != payload["new_metadata"], \
            "Expected old_metadata to differ from new_metadata"


def test_update_task_metadata_no_change_still_emits():
    """Test that event is emitted even when metadata doesn't actually change."""
    
    # Mock task and session
    mock_task = MagicMock(spec=Task)
    mock_task.id = "task-789"
    mock_task.household_id = "household-999"
    mock_task.priority = "medium"
    mock_task.description = None
    
    mock_session = MagicMock()
    mock_session.get.return_value = mock_task
    
    with patch("apps.api.services.task_service.canonical_event_router") as mock_router, \
         patch("apps.api.services.task_service.CanonicalEventAdapter") as mock_adapter, \
         patch("apps.api.services.task_service.SessionLocal", return_value=mock_session):
        
        # Execute: update with same priority (no actual change)
        update_task_metadata("task-789", priority="medium", category=None)
        
        # Assert: router.route was still called once
        assert mock_router.route.call_count == 1
        
        # Extract event
        adapter_call_args = mock_adapter.to_envelope.call_args
        event = adapter_call_args[0][0]
        
        # Assert: event type is still emitted
        assert event.type == "task_metadata_updated"
        
        # Assert: old and new are identical when no change
        payload = event.payload
        assert payload["old_metadata"]["priority"] == "medium"
        assert payload["new_metadata"]["priority"] == "medium"
        assert payload["old_metadata"]["description"] is None
        assert payload["new_metadata"]["description"] is None


def test_update_task_metadata_nonexistent_task_no_event():
    """Test that no event is emitted if task does not exist."""
    
    mock_session = MagicMock()
    mock_session.get.return_value = None  # Task not found
    
    with patch("apps.api.services.task_service.canonical_event_router") as mock_router, \
         patch("apps.api.services.task_service.SessionLocal", return_value=mock_session):
        
        # Execute: try to update nonexistent task
        update_task_metadata("nonexistent-id", priority="low")
        
        # Assert: router.route was NOT called
        assert mock_router.route.call_count == 0, \
            "Expected no event emission when task does not exist"
        
        # Assert: session.commit was NOT called
        mock_session.commit.assert_not_called()
