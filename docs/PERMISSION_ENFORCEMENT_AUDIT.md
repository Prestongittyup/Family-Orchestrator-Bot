# Permission Enforcement & Access Control Audit
## Family Orchestration Bot - Comprehensive Security Analysis

**Document Version:** 1.0  
**Date:** 2024  
**Scope:** Complete permission boundary analysis across API, Orchestrator, FSM, and State Management layers  
**Status:** DRAFT - Critical Gaps Identified

---

## Executive Summary

This audit examines how permissions are enforced across the entire system, from API ingress through state machine transitions. The analysis reveals:

### Key Findings

**✅ Strengths:**
- Bearer token authentication at API middleware level
- Household-scoped isolation in auth middleware
- FSM guard preventing assistant self-approval
- State mutation firewall with ContextVar enforcement
- Strong event-sourced audit trail

**⚠️ Critical Gaps Identified:**
1. **Actor Type Not Propagated** - `actor_type` stops at API layer; not passed to decision engine or FSM
2. **Multiple Ingress Points Unguarded** - Orchestrator, workflows, and scheduled tasks lack actor context
3. **Incomplete Context Flow** - `requires_approval` flag handled, but actor authorization not fully threaded
4. **Household Scope Enforcement Incomplete** - Auth middleware checks scope, but internal calls don't propagate
5. **No Role-Based Authorization** - Only basic "assistant" vs "api_user" distinction; no RBAC model
6. **Event Replay Lacks Authorization** - State reducer replays events without re-validating permissions
7. **Time-Tick Triggers Bypass User Context** - System-initiated triggers run without actor context

### Risk Assessment

| Category | Risk Level | Impact |
|----------|-----------|--------|
| Unauthorized State Mutations | 🔴 HIGH | Assistant could approve via internal paths |
| Cross-Household Data Leak | 🔴 HIGH | Workflow triggers might process wrong household |
| Permission Escalation | 🟡 MEDIUM | Role-based decisions not enforced consistently |
| Audit Trail Gaps | 🟡 MEDIUM | Some operations lack clear actor attribution |
| Event Replay Bypass | 🟡 MEDIUM | Historical event application not re-gated |

---

## System Architecture Overview

### API Entry Points

```
┌─────────────────────────────────────────────────────────┐
│ API Routes (FastAPI)                                    │
├─────────────────────────────────────────────────────────┤
│ • /assistant/run          → @trace_function(..., actor_type="api_user")
│ • /assistant/approve      → @trace_function(..., actor_type="api_user")
│ • /assistant/reject       → @trace_function(..., actor_type="api_user")
│ • /hpal/families/:id/*    → @trace_function(..., actor_type="api_user")
└─────────────────────────────────────────────────────────┘
         ↓ auth_guard middleware
┌─────────────────────────────────────────────────────────┐
│ Auth Middleware (auth_middleware.py)                   │
├─────────────────────────────────────────────────────────┤
│ ✓ Validates bearer token via TokenService             │
│ ✓ Sets request.state.user = claims                    │
│ ✓ Enforces household_id scope match                   │
│ ✗ Does NOT propagate actor_type into request context  │
│ ✗ No role extraction from claims                       │
└─────────────────────────────────────────────────────────┘
         ↓ request → handler
┌─────────────────────────────────────────────────────────┐
│ Route Handler (e.g., approve_assistant_action)         │
├─────────────────────────────────────────────────────────┤
│ ✓ Traces entrypoint with actor_type="api_user"        │
│ ✗ Does NOT extract actor_type from request            │
│ ✗ Does NOT pass actor_type to orchestrator            │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ HouseholdOSOrchestrator                                 │
├─────────────────────────────────────────────────────────┤
│ • approve_and_execute()                                │
│ • tick()                                               │
│ ✗ No actor_type parameter received                    │
│ ✗ No authorization checks performed                   │
│ ✗ Directly calls action_pipeline without context      │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ ActionPipeline                                          │
├─────────────────────────────────────────────────────────┤
│ • approve_actions()                                    │
│ • execute_approved_actions()                          │
│ ✗ No actor context; cannot apply FSM guards          │
│ ✓ Calls StateMachine.transition_to() (good!)         │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ StateMachine (FSM) - apps/api/core/state_machine.py   │
├─────────────────────────────────────────────────────────┤
│ ✓ validate_transition() takes optional context dict   │
│ ✓ FSM guard: if actor_type=="assistant"              │
│ ✓       → REJECT transition to APPROVED state        │
│ ✗ No caller currently passes context parameter       │
│ ✗ Guard is defined but never invoked                 │
└─────────────────────────────────────────────────────────┘
```

### Orchestrator Entry Points (Not Protected)

```
┌─────────────────────────────────────────────────────────┐
│ System Ingress (DailyyCycle, Workflows, Scheduled)      │
├─────────────────────────────────────────────────────────┤
│ • daily_cycle.tick()                                   │
│ • orchestrator.tick() with @trace_function(            │
│     actor_type="system_worker", source="orchestrator") │
│ • Workflow runners                                     │
│ • Event replay (reduce_state)                          │
├─────────────────────────────────────────────────────────┤
│ ✗ No household_id scope validation                    │
│ ✗ No actor context propagated                        │
│ ✗ No FSM guard invocation                            │
│ ✗ Event replay lacks re-authorization                │
└─────────────────────────────────────────────────────────┘
```

---

## Detailed Permission Analysis

### 1. Authentication & Token Validation ✓

**Location:** `apps/api/core/auth_middleware.py`

**What Works:**
```python
# Line 54-62: Bearer token validation
claims = _TOKEN_SERVICE.validate_access_token(token)
if claims is None:
    return JSONResponse({"detail": "invalid_or_expired_token"}, status_code=401)

# Line 75-81: Household scope verification
request_household = (
    request.headers.get("x-hpal-household-id") or
    request.query_params.get("family_id")
)
token_household = str(claims.get("household_id", ""))
if request_household and request_household != token_household:
    return JSONResponse({"detail": "household_scope_mismatch"}, status_code=403)

# Sets context
request.state.user = claims
request.state.auth_claims = claims
```

**Gaps:**
- ❌ Does not extract `role` or `actor_type` from claims
- ❌ No propagation to downstream services
- ❌ `request.state.user` not accessible to orchestrator/FSM

**Evidence:**
```python
# In trace_function decorator, actor_type is hardcoded per route
@router.post("/approve")
@trace_function(entrypoint="assistant_runtime.approve", 
                actor_type="api_user", source="api")  # ← Hardcoded
def approve_assistant_action(request: AssistantApproveRequest):
    # request.state.user exists but is not used
    # no actor_type is extracted or passed further
```

### 2. FSM Guard for Assistant Self-Approval ✓ (Defined but Not Used)

**Location:** `apps/api/core/state_machine.py`, lines 273-280

**Implementation:**
```python
def validate_transition(
    from_state: ActionState,
    to_state: ActionState,
    context: dict[str, Any] | None = None,
) -> None:
    context = context or {}
    
    # ...validation checks...
    
    # Advanced guard: assistant cannot approve own actions
    if (
        to_state == ActionState.APPROVED
        and context.get("actor_type") == "assistant"
    ):
        raise TransitionError(
            "Assistant cannot approve actions (suggest-only capability)"
        )
    
    # Advanced guard: cannot skip approval if requires_approval=true
    if (
        from_state == ActionState.PROPOSED
        and to_state == ActionState.APPROVED
        and context.get("requires_approval") is True
    ):
        raise TransitionError(
            "Action requires approval; must transition through pending_approval state"
        )
```

**Test Coverage:**
```python
# tests/test_state_machine.py, lines 106-113
def test_assistant_cannot_approve(self):
    """Assistant actor cannot approve (suggest-only)."""
    context = {"actor_type": "assistant"}
    with pytest.raises(TransitionError, match="suggest-only"):
        validate_transition(
            ActionState.PENDING_APPROVAL,
            ActionState.APPROVED,
            context=context,
        )
```

**Critical Gap: Guard is Never Invoked**

The `context` parameter is optional and never passed by ActionPipeline or StateMachine callers:

```python
# In household_os/runtime/action_pipeline.py:597
transition_event = fsm.transition_to(
    ActionState.PENDING_APPROVAL,
    reason="User triggered approval",
    # ❌ No context parameter passed
)

# In household_os/runtime/state_firewall.py:49
return state_machine.transition_to(
    ActionState.PENDING_APPROVAL,
    reason="Firewall enforcement",
    # ❌ No context parameter passed
)
```

**Proof of Exploitation:**

If an attacker can call the orchestrator directly (via workflow or event replay), no guard prevents unauthorized approval:

```python
# Attacker path (hypothetical):
# 1. Call orchestrator.approve_and_execute() without auth context
# 2. orchestrator calls action_pipeline.approve_actions()
# 3. action_pipeline calls fsm.transition_to() WITHOUT context
# 4. FSM guard never executes—transition succeeds
```

### 3. Actor Type Propagation 🔴 (Missing End-to-End)

**Propagation Chain:**

| Layer | Status | Details |
|-------|--------|---------|
| API Route Decorator | ✓ Defined | `@trace_function(..., actor_type="api_user")` |
| Auth Middleware | ✓ Extracts | `claims.get("actor_type")` available in claims |
| Route Handler | ❌ Not Extracted | `request.state.user` exists but not used |
| Orchestrator.tick() | ✓ Declared | `@trace_function(..., actor_type="system_worker")` hardcoded |
| Orchestrator.approve_and_execute() | ❌ No Context | No parameter to accept actor_type |
| ActionPipeline | ❌ No Context | No parameter to accept actor_type |
| StateMachine.transition_to() | ❌ Not Passed | No context dict provided |
| FSM Guard | ❌ Unreachable | Guard logic exists but context always None/empty |

**Why It Matters:**

The system has **three distinct actor types**:
1. **`api_user`** - Human household member via API
2. **`assistant`** - AI assistant generating suggestions
3. **`system_worker`** - Scheduled tasks, event replay, workflows

Only `api_user` should be able to approve actions, but the FSM guard cannot enforce this because actor_type never reaches it.

### 4. Household Scope Enforcement 🟡 (Partial)

**Protected by Auth Middleware:**
```python
# apps/api/core/auth_middleware.py:75-81
request_household = request.headers.get("x-hpal-household-id") or ...
token_household = str(claims.get("household_id", ""))
if request_household and request_household != token_household:
    return JSONResponse({"detail": "household_scope_mismatch"}, status_code=403)
```

✓ Prevents cross-household API requests

**NOT Protected:**
- ❌ Orchestrator.tick() - accepts `household_id` parameter but doesn't validate owner
- ❌ Workflows - can be triggered for any household without scope check
- ❌ Event replay (reduce_state) - processes events without household validation
- ❌ Scheduled daily_cycle.tick() - no household context check

**Evidence:**
```python
# In household_os/runtime/orchestrator.py:62
@trace_function(entrypoint="orchestrator.tick", actor_type="system_worker", source="orchestrator")
def tick(
    self,
    *,
    household_id: str,  # ← Accepted but never validated against request context
    state: HouseholdState | None = None,
    user_input: str | None = None,
    now: str | datetime | None = None,
) -> RuntimeTickResult:
    # No validation that household_id belongs to requesting user
```

### 5. FSM Transition Guard Mechanism ✓✓ (Well-Designed But Disabled)

**Location:** `apps/api/core/state_machine.py:233-308`

**Guardian Checks Implemented:**

| Guard | Status | Condition |
|-------|--------|-----------|
| No-op transition | ✓ Active | `from_state == to_state` → REJECT |
| Invalid backward transition | ✓ Active | `APPROVED → PROPOSED` → REJECT |
| Forward transition rules | ✓ Active | `ALLOWED_TRANSITIONS` map enforced |
| Assistant self-approval | ✓ Defined | `actor_type=="assistant" & to_state==APPROVED → REJECT` |
| Approval bypass check | ✓ Defined | `requires_approval=true & skip approval → REJECT` |

**Problem**: Last two guards depend on `context` parameter that's never passed.

### 6. Lifecycle & State Firewall Mechanism ✓✓ (Well-Designed)

**Location:** `household_os/runtime/state_firewall.py`

**How It Works:**
```python
class StateMutationFirewall:
    """Prevents unauthorized direct state mutation via ContextVar"""
    
    def can_mutate(self, object_id: str) -> bool:
        """Check if mutation is authorized for this object_id"""
        authorized = _authorized_object_ids.get(None)  # ContextVar
        return object_id in (authorized or set())
    
    def block_direct_mutation(self, obj, name, value, source):
        """Raise if state.__setattr__ called outside authorized scope"""
        raise StateViolationError(...)
```

**How It's Used:**
```python
# In LifecycleAction.__setattr__
def __setattr__(self, name: str, value: Any) -> None:
    if name == "current_state" and getattr(self, "_state_guard_ready", False):
        action_id = self.__dict__.get("action_id")
        if action_id and not FIREWALL.can_mutate(action_id):
            FIREWALL.block_direct_mutation(self, name, value, source="LifecycleAction.__setattr__")
    super().__setattr__(name, value)
```

**Strength**: Prevents bypassing StateMachine via direct attribute assignment.

**Gap**: ContextVar is set at what points? Need to verify all `transition_to()` calls set the context.

### 7. Action Approval Flow 🔴 (No Actor Validation)

Current path:
```
API /approve 
  → approve_assistant_action(request)        ← Has request.state.user
  → runtime_orchestrator.approve_and_execute()  ← Never receives actor context
  → action_pipeline.approve_actions()           ← No actor parameter
  → fsm.transition_to()                         ← No context dict
  → validate_transition()                       ← context={} (empty!)
  → FSM guard logic never executes             ← BYPASS!
```

**Should Be:**
```
API /approve 
  → approve_assistant_action(request)
  → extract actor_type from request.state.user
  → orchestrator.approve_and_execute(
        ..., 
        actor_type=extract_from_request(request)  ← NEW
    )
  → action_pipeline.approve_actions(
        ..., 
        actor_type=actor_type  ← NEW
    )
  → fsm.transition_to(
        ..., 
        context={"actor_type": actor_type, "requires_approval": ...}  ← NEW
    )
  → validate_transition() runs WITH context  ← GUARDED!
```

### 8. Event Replay & State Reduction 🟡 (Historical Bypass)

**Location:** `household_os/runtime/state_reducer.py:61-165`

The reducer replays historical events to reconstruct state:

```python
def reduce_state(
    events: list[DomainEvent],
    initial_state: dict[str, Any],
    household_id: str,
) -> dict[str, Any]:
    """Reconstruct state by applying all historical events."""
    for event in events:  # ← Loop through all past events
        # Manually apply state changes
        # Call validate_transition() to audit the event
        validate_transition(
            from_state=current_state,
            to_state=parsed_to_state,
            # ❌ No context parameter—guard never runs
        )
```

**Problem**: If an old event contains an invalid transition (e.g., captured before validation was strict), replaying it will NOT re-apply authorization.

**Example Attack Vector**:
1. Replay historical event: `{action_state: "proposed" → "approved"}`
2. `validate_transition()` called with empty context
3. `context.get("actor_type") == "assistant"` evaluates to False
4. Guard doesn't block—state updated
5. Authorization bypass via historical record

---

## Critical Issues Ranked by Severity

### 🔴 Critical (Immediate Fix Required)

#### Issue #1: Actor Type Not Passed to FSM Guard
**Status**: Needs Implementation  
**Impact**: Assistant could approve actions by triggering orchestrator via internal paths  
**Evidence**: 
- FSM guard defined in `state_machine.py:273-280`
- Guard never receives context in `action_pipeline.py:597`
- Test only validates hypothetical scenario `test_state_machine.py:106-113`

**Fix Required**:
1. Add `actor_type: str | None = None` parameter to `orchestrator.approve_and_execute()`
2. Thread actor_type through `action_pipeline.approve_actions()`
3. Pass `context={"actor_type": actor_type, ...}` in `fsm.transition_to()`
4. Add integration test requiring `actor_type="api_user"` to approve

**Estimated Effort**: 2-3 hours (parameter threading across 3-4 functions)

---

#### Issue #2: Orchestrator Methods Lack Household Validation
**Status**: No Input Validation  
**Impact**: Workflows or scheduled tasks could mutate wrong household state  
**Evidence**: 
- `orchestrator.tick()` accepts `household_id` but never validates
- No check that caller owns the household
- AuthContext not available at orchestrator level

**Fix Required**:
1. Add `user_id: str | None = None` to orchestrator methods
2. Call `state_store.verify_household_owner(household_id, user_id)` at start
3. For system-triggered calls (workflows, schedules), create explicit "system" actor with appropriate claims

**Estimated Effort**: 3-4 hours (add validation layer + test cases)

---

#### Issue #3: Actor Type Extraction From Auth Claims
**Status**: Token has data; not extracted  
**Impact**: Actor type available but discarded at auth middleware  
**Evidence**: 
- `auth_middleware.py` sets `request.state.user = claims`
- Claims contain household_id but no explicit actor_type
- Route handlers use hardcoded `actor_type="api_user"`

**Fix Required**:
1. Extract `actor_type` or `role` from JWT claims
2. Set `request.state.actor_type` in auth middleware
3. Update route handlers to use `request.state.actor_type` instead of hardcoded value
4. Establish claim structure (e.g., `"role"`, `"actor_type"`)

**Estimated Effort**: 1-2 hours (middleware + claim schema)

---

### 🟡 High (Should Fix)

#### Issue #4: Event Replay Without Re-Authorization
**Status**: Documented but not addressed  
**Impact**: Historical events could apply unauthorized state changes  
**Evidence**: 
- `state_reducer.py` calls `validate_transition()` without context
- If old event predates strict validation, re-application won't be guarded

**Fix Required**:
1. Track actor_type in DomainEvent metadata
2. Pass event metadata as context during replay
3. Add test: old event with invalid transition should fail on current rules

**Estimated Effort**: 2-3 hours (event schema + reducer updates)

---

#### Issue #5: Context Propagation Through Entire Pipeline
**Status**: Not Established  
**Impact**: Permission context is fragmented; hard to audit who did what  
**Evidence**: 
- `@trace_function` captures actor_type but only for logging
- Tracing not used by authorization logic
- No unified "request context" object threaded through calls

**Fix Required**:
1. Create `ExecutionContext` dataclass with actor_type, household_id, user_id, timestamp
2. Thread through orchestrator → action_pipeline → FSM
3. Make part of ContextVar with fallback to method parameters
4. Use in all authorization/audit decisions

**Estimated Effort**: 4-5 hours (context class + threading + testing)

---

### 🟠 Medium (Recommended)

#### Issue #6: No Role-Based Authorization Model
**Status**: Not Implemented  
**Impact**: All API users treated the same; no permission differentiation  
**Evidence**: 
- Only actor_type="assistant" is special-cased
- No household member roles (ADMIN, MANAGER, VIEWER)
- No action-level permission checks

**Fix Required**:
1. Define role enum: ADMIN, MANAGER, VIEWER, READ_ONLY
2. Check claims for role; map to permissions
3. Gate specific actions (e.g., budget changes → ADMIN only)
4. Audit all action types for role requirements

**Estimated Effort**: 6-8 hours (design + implementation + comprehensive testing)

---

#### Issue #7: Workflow Trigger Audit Gap
**Status**: No Actor Attribution  
**Impact**: Workflows run without clear permission/actor context  
**Evidence**: 
- `daily_cycle.py` calls `orchestrator.tick()` with no auth context
- Workflow runners don't establish actor identity
- Audit logs may not show who initiated

**Fix Required**:
1. Mark workflow-triggered actions with `actor_type="system_worker"` or workflow name
2. Establish that "system" actions have appropriate scoping
3. Add observability: trace every action to its root cause (API call vs. scheduled)

**Estimated Effort**: 2-3 hours (tracing + audit logging)

---

#### Issue #8: Household Owner Verification Missing
**Status**: No Method  
**Impact**: If household_id validation bypassed, no recourse  
**Evidence**: 
- Auth middleware checks `token_household == request_household`
- But orchestrator has no equivalent check for direct calls
- No `HouseholdRepository.verify_ownership()` method

**Fix Required**:
1. Add method to repository to verify household owner
2. Call on all orchestrator entry points (after extracting user_id)
3. Return 403 if ownership mismatch

**Estimated Effort**: 1-2 hours (repository method + validation)

---

## Recommendations

### Immediate (Week 1)

- [ ] **Issue #1**: Thread `actor_type` to FSM guard
- [ ] **Issue #3**: Extract actor_type from auth claims
- [ ] **Issue #5**: Create ExecutionContext class
- [ ] Add integration test: assistant actor attempting approval (must fail)

### Short-term (Week 2-3)

- [ ] **Issue #2**: Add household owner validation in orchestrator
- [ ] **Issue #4**: Include metadata in DomainEvent for replay
- [ ] **Issue #6**: Define role-based permission model
- [ ] Audit all LifecycleAction mutation points

### Medium-term (Month 1)

- [ ] **Issue #7**: Establish workflow audit trail
- [ ] Complete RBAC implementation with action gates
- [ ] Comprehensive permission tests across all ingress points
- [ ] Security review with focus on bypasses

---

## Testing Strategy

### Unit Tests Needed

```python
# tests/test_permission_enforcement.py

class TestActorTypeEnforcement:
    def test_assistant_cannot_approve_via_api(self):
        """POST /approve with assistant token → 403"""
        # Token with actor_type="assistant"
        # Should reject before reaching FSM
    
    def test_assistant_cannot_approve_via_orchestrator(self):
        """Direct orchestrator call with actor_type="assistant" → TransitionError"""
        # actor_type must be threaded to FSM
    
    def test_requires_approval_enforced_for_all_actors(self):
        """If requires_approval=true, PROPOSED→APPROVED must fail"""

class TestHouseholdScope:
    def test_orchestrator_validates_household_ownership(self):
        """tick(household_id=X, user_id=Y) verifies Y owns X"""
    
    def test_cross_household_workflow_fails(self):
        """Workflow cannot mutate state for different household"""

class TestEventReplay:
    def test_replay_applies_current_validation_rules(self):
        """Historical event must pass today's FSM guards"""
        # Include actor_type in replayed event metadata
```

### Integration Tests Needed

```python
# tests/test_permission_end_to_end.py

class TestApprovalPermissions:
    def test_api_user_can_approve(self):
        """Happy path: API user approves pending action"""
    
    def test_assistant_cannot_approve_via_any_path(self):
        """Assistant blocked → API, orchestrator, workflow paths"""
    
    def test_household_owner_verified_for_all_operations(self):
        """User prevented from accessing other household state"""
```

---

## Audit Findings Summary

| Component | Current State | Gap | Risk |
|-----------|---------------|-----|------|
| API Authentication | ✓ Strong | Actor type not extracted | 🟢 Low |
| Auth Middleware | ✓ Validates token & scope | Doesn't propagate context | 🟠 Medium |
| FSM Guard Logic | ✓ Well-designed | Never receives context | 🔴 **Critical** |
| Orchestrator Entry | ✓ Decorated | No household validation | 🔴 **Critical** |
| Action Pipeline | ✓ Calls FSM | No actor context | 🔴 **Critical** |
| Event Replay | ✓ Validates events | No actor in context | 🟡 High |
| Trace Function | ✓ Captures context | Only for logging, not auth | 🟠 Medium |
| State Firewall | ✓ Prevents direct mutation | Only works if FSM calls it | ✓ Good |
| Household Scope | ✓ At API layer | Not at orchestrator layer | 🟠 Medium |
| Role-Based Access | ✗ Not implemented | No RBAC model | 🟡 High |

---

## Conclusion

The system has **well-designed permission mechanisms** (FSM guard, state firewall, auth middleware) but they are **not fully connected end-to-end**. The architecture supports role-based authorization via the context parameter, but:

1. **Context is not extracted** from auth claims
2. **Context is not threaded** through the orchestrator pipeline
3. **Most entry points lack household/user validation**
4. **Guards are defined but not invoked** due to missing parameters

**Priority Actions**:
1. Thread `actor_type` from auth claims → FSM guard (Issue #1, #3)
2. Add orchestrator household validation (Issue #2)
3. Establish execution context object for consistent threading (Issue #5)

Once these are fixed, the existing guard logic will become effective and the system will have comprehensive permission enforcement.

