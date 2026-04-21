"""
Conversation Orchestration Layer (COL v1) - Public API
=======================================================

Exports all public types and the primary orchestrator.
"""

# Data models
from apps.api.conversation_orchestration.schema import (
    SessionState,
    NextAction,
    ConversationMessage,
    PartialIntent,
    ConversationSession,
    COLResponse,
    ClarificationRequest,
    ExecutionHandoff,
)

# Intent refinement
from apps.api.conversation_orchestration.intent_refinement import (
    IntentRefinementEngine,
    REQUIRED_FIELDS_BY_INTENT,
)

# State machine
from apps.api.conversation_orchestration.state_machine import (
    ConversationStateMachine,
    StateTransitionError,
)

# Session store
from apps.api.conversation_orchestration.store import (
    ConversationSessionStore,
    DEFAULT_SESSION_STORE,
)

# Pipeline orchestrator
from apps.api.conversation_orchestration.pipeline import (
    ConversationOrchestrator,
)

__all__ = [
    # Schema
    "SessionState",
    "NextAction",
    "ConversationMessage",
    "PartialIntent",
    "ConversationSession",
    "COLResponse",
    "ClarificationRequest",
    "ExecutionHandoff",
    # Refinement
    "IntentRefinementEngine",
    "REQUIRED_FIELDS_BY_INTENT",
    # State machine
    "ConversationStateMachine",
    "StateTransitionError",
    # Session store
    "ConversationSessionStore",
    "DEFAULT_SESSION_STORE",
    # Orchestrator
    "ConversationOrchestrator",
]
