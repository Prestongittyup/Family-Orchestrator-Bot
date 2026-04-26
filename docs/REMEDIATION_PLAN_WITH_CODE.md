# Permission Enforcement Remediation Plan
## Family Orchestration Bot - Implementation Guidelines

**Version:** 1.0  
**Target Completion:** 2-3 weeks (phased approach)  
**Priority**: Critical path items first

---

## Phase 1: Actor Type Propagation (2-3 days)

### Step 1.1: Extract Actor Type from Auth Claims

**File**: `apps/api/core/auth_middleware.py`

**Current Code** (lines 70-84):
```python
request.state.auth_claims = claims
request.state.user = claims
response = await call_next(request)
```

**Proposed Changes**:
```python
# Add this function after imports
def _extract_actor_type_from_claims(claims: dict[str, Any]) -> str:
    """
    Extract actor type from JWT claims.
    
    Precedence:
    1. Explicit 'actor_type' claim (recommended)
    2. 'role' claim mapped to type
    3. Default to 'api_user' for standard auth
    
    Valid values: 'api_user', 'assistant', 'system_worker'
    """
    actor_type = claims.get("actor_type")
    if actor_type in {"api_user", "assistant", "system_worker"}:
        return actor_type
    
    # Fallback: map role to actor_type if present
    role = claims.get("role")
    if role == "assistant":
        return "assistant"
    
    # Default API call from authenticated user
    return "api_user"

# In auth_guard function (around line 73):
request.state.auth_claims = claims
request.state.user = claims
request.state.actor_type = _extract_actor_type_from_claims(claims)  # ← NEW
response = await call_next(request)
```

**Testing**:
```python
# tests/test_auth_middleware_actor_type.py
import pytest
from apps.api.core.auth_middleware import _extract_actor_type_from_claims

class TestActorTypeExtraction:
    def test_explicit_actor_type_in_claims(self):
        claims = {"actor_type": "assistant"}
        assert _extract_actor_type_from_claims(claims) == "assistant"
    
    def test_role_based_mapping(self):
        claims = {"role": "assistant"}
        assert _extract_actor_type_from_claims(claims) == "assistant"
    
    def test_default_to_api_user(self):
        claims = {"household_id": "123"}
        assert _extract_actor_type_from_claims(claims) == "api_user"
    
    def test_invalid_actor_type_defaults_to_api_user(self):
        claims = {"actor_type": "invalid"}
        assert _extract_actor_type_from_claims(claims) == "api_user"
```

**Effort**: 0.5 hours

---

### Step 1.2: Update Route Handlers to Use Extracted Actor Type

**File**: `apps/api/assistant_runtime_router.py`

**Current Code** (lines 158-179):
```python
@router.post("/approve", response_model=AssistantApproveResponse)
@trace_function(entrypoint="assistant_runtime.approve", 
                actor_type="api_user", source="api")  # ← Hardcoded
def approve_assistant_action(request: AssistantApproveRequest) -> AssistantApproveResponse:
    graph = runtime_orchestrator.state_store.load_graph(request.household_id)
    action_payload = graph.get("action_lifecycle", {}).get("actions", {}).get(request.action_id)
    if action_payload is None:
        raise HTTPException(status_code=404, detail="Action not found")

    request_id = str(action_payload.get("request_id", ""))
    if not request_id:
        raise HTTPException(status_code=400, detail="Action is missing request association")

    approval_result = runtime_orchestrator.approve_and_execute(  # ← Missing actor_type
        household_id=request.household_id,
        request_id=request_id,
        action_ids=[request.action_id],
    )
```

**Proposed Changes**:
```python
# Import FastAPI Request to access state
from fastapi import APIRouter, HTTPException, Request

@router.post("/approve", response_model=AssistantApproveResponse)
@trace_function(entrypoint="assistant_runtime.approve", 
                actor_type="api_user", source="api")
def approve_assistant_action(
    request: AssistantApproveRequest,
    http_request: Request,  # ← Add FastAPI request object
) -> AssistantApproveResponse:
    # Extract actor type from auth middleware state
    actor_type = getattr(http_request.state, "actor_type", "api_user")
    
    graph = runtime_orchestrator.state_store.load_graph(request.household_id)
    action_payload = graph.get("action_lifecycle", {}).get("actions", {}).get(request.action_id)
    if action_payload is None:
        raise HTTPException(status_code=404, detail="Action not found")

    request_id = str(action_payload.get("request_id", ""))
    if not request_id:
        raise HTTPException(status_code=400, detail="Action is missing request association")

    # PASS actor_type to orchestrator
    approval_result = runtime_orchestrator.approve_and_execute(
        household_id=request.household_id,
        request_id=request_id,
        action_ids=[request.action_id],
        actor_type=actor_type,  # ← NEW PARAMETER
        user_id=http_request.state.user.get("sub") if http_request.state.user else None,  # ← NEW
    )
    
    # ... rest of implementation
```

**Similar Changes Required in**:
- `/run` endpoint (line 102)
- `/reject` endpoint (line 234)
- `/today` endpoint (line 190)
- All HPAL endpoints in `apps/api/hpal/router.py`

**Effort**: 1.5-2 hours (6-8 route updates)

---

### Step 1.3: Update Orchestrator Signature

**File**: `household_os/runtime/orchestrator.py`

**Current Code** (lines 200-225):
```python
def approve_and_execute(
    self,
    *,
    household_id: str,
    request_id: str,
    action_ids: list[str],
    now: str | datetime | None = None,
) -> RuntimeApprovalResult:
    graph = self.state_store.load_graph(household_id)
    timestamp = self._coerce_datetime(now or graph.get("reference_time"))
    approved_actions = self.action_pipeline.approve_actions(
        graph=graph,
        request_id=request_id,
        action_ids=action_ids,
        now=timestamp,
    )
```

**Proposed Changes**:
```python
def approve_and_execute(
    self,
    *,
    household_id: str,
    request_id: str,
    action_ids: list[str],
    now: str | datetime | None = None,
    actor_type: str | None = None,  # ← NEW (optional for backward compat)
    user_id: str | None = None,     # ← NEW (for household validation)
) -> RuntimeApprovalResult:
    """
    Approve and execute actions.
    
    Args:
        actor_type: 'api_user', 'assistant', 'system_worker' 
                    (required for proper authorization)
        user_id: User ID making the request (for ownership validation)
    """
    # NEW: Validate household ownership
    if user_id:
        if not self.state_store.verify_household_owner(household_id, user_id):
            raise HTTPException(
                status_code=403,
                detail="User does not own this household"
            )
    
    # NEW: Validate against assistant self-approval
    if actor_type == "assistant":
        raise HTTPException(
            status_code=403,
            detail="Assistant cannot approve actions"
        )
    
    graph = self.state_store.load_graph(household_id)
    timestamp = self._coerce_datetime(now or graph.get("reference_time"))
    
    # PASS actor_type down
    approved_actions = self.action_pipeline.approve_actions(
        graph=graph,
        request_id=request_id,
        action_ids=action_ids,
        now=timestamp,
        actor_type=actor_type,  # ← NEW
    )
    
    # ... rest of method
```

**Also Update**:
- `tick()` method (line 62) - add optional actor_type, user_id
- `reject_and_log()` if exists

**Effort**: 1 hour

---

### Step 1.4: Update ActionPipeline.approve_actions()

**File**: `household_os/runtime/action_pipeline.py`

**Locate approve_actions() method** (~line 550-600) and update:

```python
def approve_actions(
    self,
    *,
    graph: dict[str, Any],
    request_id: str,
    action_ids: list[str],
    now: datetime,
    actor_type: str | None = None,  # ← NEW PARAMETER
) -> list[LifecycleAction]:
    """
    Approve pending actions.
    
    Args:
        actor_type: Actor requesting approval (for FSM guard)
    """
    approved_actions: list[LifecycleAction] = []
    
    actions = graph.get("action_lifecycle", {}).get("actions", {})
    for action_id in action_ids:
        action = actions.get(action_id)
        if action is None:
            continue
        
        fsm = StateMachine(action_id=action_id)
        fsm.state = parse_lifecycle_state(action.get("current_state"))
        
        # PASS context to FSM transition
        context = {
            "actor_type": actor_type,
            "requires_approval": action.get("approval_required", False),
        }
        
        try:
            # This will now invoke the FSM guard!
            event = fsm.transition_to(
                ActionState.APPROVED,
                reason=f"Approved by {actor_type or 'unknown'}",
                context=context,  # ← CRITICAL: NOW BEING PASSED
            )
            
            # Update graph with new state
            action["current_state"] = ActionState.APPROVED.value
            action["updated_at"] = now.isoformat()
            action["transitions"].append({
                "from_state": event.from_state.value,
                "to_state": event.to_state.value,
                "changed_at": event.timestamp.isoformat(),
                "reason": event.reason,
                "metadata": {"actor_type": actor_type},
            })
            
            approved_actions.append(LifecycleAction.model_validate(action))
        
        except TransitionError as e:
            # FSM guard blocked the transition—log and skip
            log_event(
                "action_approval_rejected",
                action_id=action_id,
                reason=str(e),
                actor_type=actor_type,
            )
            # Don't add to approved_actions; action stays PENDING_APPROVAL
            continue
    
    return approved_actions
```

**Key Change**: `fsm.transition_to()` now receives `context` dict!

**Effort**: 1.5 hours

---

### Step 1.5: Update StateMachine.transition_to() to Accept Context

**File**: `apps/api/core/state_machine.py`

**Locate transition_to() method** (line ~353) and update:

```python
def transition_to(
    self,
    target_state: ActionState,
    *,
    reason: str = "",
    correlation_id: str = "",
    error_code: str | None = None,
    metadata: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,  # ← NEW PARAMETER
) -> StateTransitionEvent:
    """
    Perform validated state transition with optional context guards.
    
    Args:
        context: Authorization context dict with keys:
                - 'actor_type': 'api_user' | 'assistant' | 'system_worker'
                - 'requires_approval': bool
                - Any other relevant metadata
    """
    metadata = metadata or {}
    
    # VALIDATE with context
    validate_transition(
        from_state=self.state,
        to_state=target_state,
        context=context,  # ← NOW BEING PASSED
    )
    
    # ... rest of method unchanged
```

**Effort**: 0.5 hours

---

### Step 1.6: Add Integration Tests

**File**: `tests/test_actor_type_enforcement.py`

```python
"""Test that actor_type flows through and is enforced at FSM."""

import pytest
from unittest.mock import patch

def test_api_user_can_approve_action(app, client):
    """Verify API user can approve pending action."""
    # Setup: Create action in PENDING_APPROVAL state
    household_id = "test-household"
    action_id = "test-action-123"
    request_id = "req-456"
    
    # Mock request context with actor_type
    with patch("request.state") as mock_state:
        mock_state.actor_type = "api_user"
        mock_state.user = {"sub": "user-123", "household_id": household_id}
        
        response = client.post(
            "/assistant/approve",
            json={
                "household_id": household_id,
                "action_id": action_id,
            },
            headers={"Authorization": "Bearer valid_token"},
        )
    
    assert response.status_code == 200
    assert response.json()["status"] == "committed"

def test_assistant_cannot_approve_action_via_api(app, client):
    """Verify assistant actor is rejected from approving."""
    household_id = "test-household"
    action_id = "test-action-123"
    
    with patch("request.state") as mock_state:
        mock_state.actor_type = "assistant"  # ← Assistant actor
        mock_state.user = {"sub": "assistant-ai", "household_id": household_id}
        
        response = client.post(
            "/assistant/approve",
            json={
                "household_id": household_id,
                "action_id": action_id,
            },
            headers={"Authorization": "Bearer assistant_token"},
        )
    
    # Should be rejected BEFORE reaching FSM (at orchestrator level)
    assert response.status_code == 403
    assert "cannot approve" in response.json()["detail"].lower()

def test_actor_type_flows_to_fsm_guard():
    """Unit test: Verify FSM receives actor_type in context."""
    from apps.api.core.state_machine import (
        StateMachine, ActionState, TransitionError, validate_transition
    )
    
    fsm = StateMachine(action_id="test-123")
    fsm.state = ActionState.PENDING_APPROVAL
    
    # Attempt transition with assistant actor
    context = {
        "actor_type": "assistant",
        "requires_approval": True,
    }
    
    with pytest.raises(TransitionError, match="suggest-only"):
        fsm.transition_to(
            ActionState.APPROVED,
            reason="Test",
            context=context,  # ← Context now passed
        )

def test_system_worker_can_approve_if_allowed():
    """System worker can approve when appropriate."""
    fsm = StateMachine(action_id="test-123")
    fsm.state = ActionState.PENDING_APPROVAL
    
    # System worker should be able to approve (they're not "assistant")
    context = {"actor_type": "system_worker"}
    
    event = fsm.transition_to(
        ActionState.APPROVED,
        reason="System approval",
        context=context,
    )
    
    assert event.to_state == ActionState.APPROVED
```

**Effort**: 1.5 hours

---

**Phase 1 Total Effort: 6-7 hours**

---

## Phase 2: Household Scope Validation (1-2 days)

### Step 2.1: Add Repository Method for Ownership Verification

**File**: `household_os/core/household_state_graph.py` (or appropriate repo)

```python
class HouseholdStateGraphStore:
    """Store for household state graphs."""
    
    def verify_household_owner(
        self, 
        household_id: str, 
        user_id: str,
    ) -> bool:
        """
        Verify that user_id owns/belongs to household_id.
        
        Args:
            household_id: Household UUID
            user_id: User ID from auth token
            
        Returns:
            True if user owns/belongs to household, False otherwise
        """
        # Query user identity repository
        from apps.api.identity.repositories import UserRepository
        user_repo = UserRepository()
        
        # Check if user is member of household
        return user_repo.is_member_of_household(user_id, household_id)

    def load_graph(self, household_id: str) -> dict[str, Any]:
        """
        Load household state graph.
        
        Note: This method does NOT validate ownership.
        Use verify_household_owner() first when handling user requests.
        """
        # existing implementation
        ...
```

**Effort**: 1 hour

---

### Step 2.2: Update Orchestrator.tick() with Validation

**File**: `household_os/runtime/orchestrator.py`

```python
@trace_function(
    entrypoint="orchestrator.tick", 
    actor_type="system_worker", 
    source="orchestrator"
)
def tick(
    self,
    *,
    household_id: str,
    state: HouseholdState | None = None,
    user_input: str | None = None,
    fitness_goal: str | None = None,
    actor_type: str | None = None,     # ← NEW
    user_id: str | None = None,        # ← NEW
    now: str | datetime | None = None,
) -> RuntimeTickResult:
    """
    Process one household orchestration cycle.
    
    Args:
        actor_type: 'api_user' | 'assistant' | 'system_worker'
                    'system_worker' for scheduled/workflow runs
        user_id: User making request (required if actor_type='api_user')
    """
    # Validate household ownership for user-initiated calls
    if actor_type == "api_user" and user_id:
        if not self.state_store.verify_household_owner(household_id, user_id):
            raise PermissionError(
                f"User {user_id} does not own household {household_id}"
            )
    
    # Rest of existing implementation
    graph = self._prepare_graph(...)
    # ... (unchanged)
```

**Effort**: 1 hour

---

### Step 2.3: Update Workflow Entry Points

**File**: `household_os/runtime/daily_cycle.py`

```python
class DailyCycle:
    def tick(self, household_id: str) -> RuntimeTickResult:
        """Execute daily cycle for household (scheduled/internal)."""
        tick = self.orchestrator.tick(
            household_id=household_id,
            actor_type="system_worker",  # ← Mark as system-initiated
            # No user_id—this is scheduled execution
        )
        return tick
```

**Similar updates in**:
- Workflow runners
- Event replay handlers
- Any scheduled/internal orchestrator calls

**Effort**: 1.5 hours

---

**Phase 2 Total Effort: 3.5-4 hours**

---

## Phase 3: Event Replay Authorization (1 day)

### Step 3.1: Include Actor Type in DomainEvent

**File**: `household_os/runtime/domain_event.py`

```python
@dataclass
class DomainEvent:
    """Event in event store."""
    
    event_id: str
    aggregate_id: str  # action_id
    event_type: str
    event_version: int
    recorded_at: datetime
    data: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)  # ← EXTEND THIS
    
    def __post_init__(self):
        # Ensure metadata includes actor type
        if "actor_type" not in self.metadata:
            self.metadata["actor_type"] = "unknown"
```

**Update code that creates events** to include actor_type:

```python
# In action_pipeline.py, when creating transition event
event = DomainEvent(
    event_id=str(uuid.uuid4()),
    aggregate_id=action_id,
    event_type="action_state_changed",
    event_version=1,
    recorded_at=now,
    data={
        "from_state": old_state.value,
        "to_state": new_state.value,
    },
    metadata={
        "actor_type": actor_type or "unknown",
        "reason": reason,
    }  # ← Include actor_type
)
```

**Effort**: 1 hour

---

### Step 3.2: Update State Reducer to Use Event Metadata

**File**: `household_os/runtime/state_reducer.py`

```python
def reduce_state(
    events: list[DomainEvent],
    initial_state: dict[str, Any],
    household_id: str,
) -> dict[str, Any]:
    """Reconstruct state by replaying events with validation."""
    
    state = deepcopy(initial_state)
    
    for event in events:
        # Extract actor_type from event metadata
        event_actor_type = event.metadata.get("actor_type", "unknown")
        
        if event.event_type in LIFECYCLE_EVENT_TYPES:
            from_state = event.data.get("from_state")
            to_state = event.data.get("to_state")
            
            # VALIDATE with actor context from event
            try:
                validate_transition(
                    from_state=ActionState(from_state),
                    to_state=ActionState(to_state),
                    context={
                        "actor_type": event_actor_type,
                        "requires_approval": event.data.get("requires_approval", False),
                    },  # ← NOW PASSING CONTEXT
                )
            except TransitionError as e:
                log_error(
                    "event_replay_validation_failed",
                    event_id=event.event_id,
                    reason=str(e),
                    actor_type=event_actor_type,
                )
                # Option 1: Skip invalid event (strict)
                # Option 2: Continue with warning (lenient for backcompat)
                # Choose based on your requirements
                raise  # For now, fail loudly
        
        # Apply event to state (unchanged)
        _apply_event(state, event)
    
    return state
```

**Effort**: 1.5 hours

---

**Phase 3 Total Effort: 2.5 hours**

---

## Phase 4: Execution Context Object (1-2 days)

### Step 4.1: Create ExecutionContext Class

**File**: `household_os/core/execution_context.py` (NEW FILE)

```python
"""
Unified execution context for authorization and audit throughout the system.

This object flows through orchestrator → action_pipeline → FSM,
providing consistent actor, household, and user information.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Optional


@dataclass
class ExecutionContext:
    """
    Authorization and audit context for an operation.
    
    Thread through all orchestrator/pipeline calls to ensure
    consistent actor validation throughout the execution flow.
    """
    
    # Identity
    actor_type: str  # 'api_user' | 'assistant' | 'system_worker'
    user_id: Optional[str] = None  # Who made the request
    
    # Scope
    household_id: str = ""  # Which household
    
    # Operation metadata
    request_id: str = ""  # Trace ID for this operation
    trace_id: str = ""  # Correlation ID across system
    
    # Timing
    initiated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    
    # Audit
    metadata: dict[str, Any] = field(default_factory=dict)
    
    def to_fsm_context(self) -> dict[str, Any]:
        """Convert to dict for FSM guard validation."""
        return {
            "actor_type": self.actor_type,
            "user_id": self.user_id,
            "household_id": self.household_id,
        }
    
    def to_event_metadata(self) -> dict[str, Any]:
        """Convert to dict for event metadata."""
        return {
            "actor_type": self.actor_type,
            "user_id": self.user_id,
            "initiated_at": self.initiated_at.isoformat(),
            "request_id": self.request_id,
            **self.metadata,
        }
    
    @classmethod
    def from_api_request(
        cls,
        household_id: str,
        actor_type: str,
        user_id: Optional[str] = None,
        request_id: str = "",
        **kwargs,
    ) -> ExecutionContext:
        """Create context from API request state."""
        return cls(
            actor_type=actor_type,
            household_id=household_id,
            user_id=user_id,
            request_id=request_id,
            **kwargs,
        )
    
    @classmethod
    def system_context(
        cls,
        household_id: str,
        trigger_type: str = "scheduled",
    ) -> ExecutionContext:
        """Create context for system-initiated operations."""
        return cls(
            actor_type="system_worker",
            household_id=household_id,
            user_id=None,
            metadata={"trigger_type": trigger_type},
        )
```

**Effort**: 1 hour

---

### Step 4.2: Update Orchestrator to Use ExecutionContext

**File**: `household_os/runtime/orchestrator.py`

```python
from household_os.core.execution_context import ExecutionContext


class HouseholdOSOrchestrator:
    
    def approve_and_execute(
        self,
        *,
        household_id: str,
        request_id: str,
        action_ids: list[str],
        now: str | datetime | None = None,
        context: ExecutionContext | None = None,  # ← NEW: accept context
    ) -> RuntimeApprovalResult:
        """
        Approve and execute actions.
        
        Args:
            context: ExecutionContext with actor_type and user_id
        """
        # Create default context if not provided (for backward compat)
        if context is None:
            context = ExecutionContext(
                actor_type="unknown",
                household_id=household_id,
                request_id=request_id,
            )
        
        # Validate permissions
        if context.actor_type == "assistant":
            raise PermissionError("Assistant cannot approve actions")
        
        if context.user_id and context.actor_type == "api_user":
            if not self.state_store.verify_household_owner(
                context.household_id, 
                context.user_id
            ):
                raise PermissionError(
                    f"User {context.user_id} does not own {context.household_id}"
                )
        
        graph = self.state_store.load_graph(household_id)
        timestamp = self._coerce_datetime(now or graph.get("reference_time"))
        
        # PASS context to action pipeline
        approved_actions = self.action_pipeline.approve_actions(
            graph=graph,
            request_id=request_id,
            action_ids=action_ids,
            now=timestamp,
            context=context,  # ← NEW
        )
        
        # ... rest unchanged
```

**Effort**: 2 hours

---

### Step 4.3: Update ActionPipeline to Use ExecutionContext

**File**: `household_os/runtime/action_pipeline.py`

```python
def approve_actions(
    self,
    *,
    graph: dict[str, Any],
    request_id: str,
    action_ids: list[str],
    now: datetime,
    context: ExecutionContext | None = None,  # ← NEW
) -> list[LifecycleAction]:
    """Approve actions with execution context."""
    
    context = context or ExecutionContext(actor_type="unknown")
    
    approved_actions: list[LifecycleAction] = []
    
    for action_id in action_ids:
        action = graph.get("action_lifecycle", {}).get("actions", {}).get(action_id)
        if not action:
            continue
        
        fsm = StateMachine(action_id=action_id)
        fsm.state = parse_lifecycle_state(action.get("current_state"))
        
        try:
            # Use context for FSM validation
            event = fsm.transition_to(
                ActionState.APPROVED,
                reason=f"Approved by {context.actor_type}",
                context=context.to_fsm_context(),  # ← Use context object
                metadata={"context": context.to_event_metadata()},
            )
            
            # Update action and record event
            action["current_state"] = ActionState.APPROVED.value
            action["updated_at"] = now.isoformat()
            
            # Record event with full context
            self._record_lifecycle_event(
                action_id=action_id,
                event_type="action_approved",
                data={
                    "from_state": event.from_state.value,
                    "to_state": event.to_state.value,
                },
                metadata=context.to_event_metadata(),
            )
            
            approved_actions.append(LifecycleAction.model_validate(action))
        
        except TransitionError as e:
            log_error(
                "action_approval_blocked",
                action_id=action_id,
                reason=str(e),
                actor_type=context.actor_type,
                user_id=context.user_id,
            )
            continue
    
    return approved_actions
```

**Effort**: 1.5 hours

---

**Phase 4 Total Effort: 4.5 hours**

---

## Summary: Implementation Roadmap

| Phase | Tasks | Effort | Priority |
|-------|-------|--------|----------|
| **1** | Actor type propagation end-to-end | 6-7h | 🔴 Critical |
| **2** | Household scope validation | 3-4h | 🔴 Critical |
| **3** | Event replay authorization | 2.5h | 🟡 High |
| **4** | ExecutionContext object | 4.5h | 🟡 High |
| **Total** | | **16-17 hours** | |

### Recommended Sequence

1. **Week 1**: Complete Phase 1 + Phase 2 (9-11 hours)
   - Actor type flows end-to-end
   - FSM guard becomes active
   - Household validation in place

2. **Week 2**: Complete Phase 3 + Phase 4 (7-8 hours)
   - Event replay properly authorized
   - ExecutionContext object unified
   - Comprehensive testing

### Testing at Each Phase

After Phase 1:
```
✓ test_actor_type_flows_to_fsm_guard
✓ test_assistant_cannot_approve_via_api
✓ test_fsm_guard_blocks_invalid_actor
```

After Phase 2:
```
✓ test_orchestrator_validates_household_ownership
✓ test_system_worker_marks_correctly
✓ test_cross_household_request_rejected
```

After Phase 3:
```
✓ test_event_replay_applies_current_validation
✓ test_old_event_fails_new_guard
```

After Phase 4:
```
✓ test_execution_context_flows_through_pipeline
✓ test_context_in_event_metadata
✓ test_audit_trail_complete
```

---

## Deployment Checklist

- [ ] Phase 1 code review & approval
- [ ] Phase 1 merged to main
- [ ] Phase 1 smoke tests in production (7 days)
- [ ] Phase 2 code review & approval
- [ ] Phase 2 merged to main
- [ ] Phase 2 smoke tests in production (7 days)
- [ ] Phase 3 & 4 code review & approval
- [ ] Phase 3 & 4 merged to main
- [ ] Full integration test suite passes
- [ ] Security review by external team (recommended)
- [ ] Documentation updated
- [ ] Alerts configured for permission-related failures

---

## Rollback Plan

Each phase is independently deployable but depends on previous phases.

**If Phase 1 needs rollback:**
- API routes still work (actor_type parameter was optional)
- FSM guard still won't execute (context never passed)
- No upstream breakage

**If Phase 2 needs rollback:**
- Remove household validation check
- existing code continues working without verification
- No data loss risk

**Always maintain backward compatibility** by making context/actor_type optional with sensible defaults.

