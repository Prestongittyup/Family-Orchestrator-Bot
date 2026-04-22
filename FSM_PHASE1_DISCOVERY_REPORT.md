# PHASE 1: FINITE STATE MACHINE DISCOVERY & GAP ANALYSIS

**Date:** April 21, 2026  
**Scope:** Household Orchestration Bot - Task/Action Lifecycle Management  
**Status:** COMPLETE WITH CRITICAL GAPS IDENTIFIED

---

## SECTION 1: WHAT EXISTS TODAY

### 1.1 Current State Definitions

**Location:** `household_os/runtime/action_pipeline.py:15-23`

```python
LifecycleState = Literal[
    "proposed",
    "pending_approval",
    "approved",
    "executed",
    "rejected",
    "ignored",
]
```

**Comparison to Required Spec:**
| Required State | Current Implementation | Status |
|---|---|---|
| proposed | ✅ proposed | MATCH |
| pending_approval | ✅ pending_approval | MATCH |
| approved | ✅ approved | MATCH |
| committed | ❌ MISSING (uses "executed") | GAP |
| failed | ❌ MISSING (uses "ignored" for timeout only) | CRITICAL GAP |
| rejected | ✅ rejected | MATCH |

---

### 1.2 Existing State Transitions

**Implemented In:** `household_os/runtime/action_pipeline.py` methods

#### Valid Transition Paths (Inferred from Code):

```
proposed 
  ├─→ pending_approval (if approval_required=true)
  └─→ approved (if approval_required=false)
  
pending_approval 
  ├─→ approved (user approval)
  ├─→ rejected (user rejection)
  └─→ ignored (timeout fired)
  
approved
  └─→ executed (system worker execution)

executed (TERMINAL)
rejected (TERMINAL)
ignored (TERMINAL)
```

**Issues with Current Transitions:**
- No explicit terminal state enforcement
- "executed" ≠ "committed" (semantic gap)
- "ignored" used only for timeout, not general failure
- Missing: failed ← executed (for execution errors)
- Missing: failed → proposed (for retries)
- Missing: approved → failed (for pre-execution errors)

---

### 1.3 Existing Transition Guards & Validation

**Location:** `household_os/runtime/action_pipeline.py:111-150` (approve_actions method)

```python
if action.request_id != request_id or action.current_state not in {"proposed", "pending_approval"}:
    continue  # Skip invalid transitions silently
```

**Validation Coverage:**
- ✅ Request ID matching (prevents cross-request approval)
- ✅ Current state check (prevents invalid source states)
- ❌ **No explicit transition table** (uses inline if/checks)
- ❌ **Silently skips invalid transitions** (no error raising)
- ❌ No guard on mutation of action.current_state field
- ❌ No idempotency key validation

---

### 1.4 Existing Retry/Error Handling

**Current Retry Capability:** NONE IMPLEMENTED

- No retry mechanism for failed actions
- No exponential backoff policy
- No max retry counter
- No error classification (retryable vs non-retryable)
- No failed state with retry transition rule

**Timeout Handling ONLY:**
```python
def reject_action_timeout(self, *, graph, trigger, now) -> LifecycleAction | None:
    # pending_approval → ignored (on timeout)
    # Hardcoded to ignore, not generic failure
```

---

### 1.5 Existing Timeout Logic

**Implemented:** `household_os/runtime/action_pipeline.py:203-241`

**Coverage:**
- ✅ pending_approval state timeout → ignored
- ❌ proposed state timeout (NO TIMEOUT ENFORCED)
- ❌ approved state timeout (NO TIMEOUT ENFORCED)
- ❌ committed state timeout (N/A, but should be terminal)

**Issues:**
- Timeout transition hardcoded to "ignored"
- No timeout_seconds enforcement at state level
- No background timeout monitor
- Timeout only triggered by explicit APPROVAL_PENDING_TIMEOUT trigger

---

### 1.6 Observability & Event Emission

**Current Event Logging:** `graph["event_history"]` + `graph["action_lifecycle"]["transition_log"]`

**Captured Events:**
```python
# action_pipeline.py emits:
{
    "event_type": "action_proposed",
    "action_id": action.action_id,
    "request_id": request.request_id,
    "trigger_type": trigger.trigger_type,
    "recorded_at": timestamp,
}

# Also:
action.transitions.append(LifecycleTransition(...))
  # Contains: from_state, to_state, changed_at, reason, metadata
```

**What's Missing:**
- ❌ No SSE (Server-Sent Events) emission
- ❌ No event_id (UUID for deduplication)
- ❌ No idempotency_key in events
- ❌ No visibility flags (silent | ui_update | user_alert)
- ❌ No channel multiplexing (household | user | system)
- ❌ No delivery guarantees (at-least-once, dedupe window)
- ❌ No strict event ordering enforcement
- ❌ No payload schema validation

---

## SECTION 2: CRITICAL GAPS ANALYSIS

### 2.1 Gap Priority Matrix

| Gap | Severity | Impact | Status |
|---|---|---|---|
| Missing "committed" state | **CRITICAL** | Blocks idempotency tracking, violates spec literally | P0 |
| Missing "failed" state + retry policy | **CRITICAL** | No error recovery, no resilience | P0 |
| No centralized state machine validator | **HIGH** | Transitions scattered, hard to audit, duplicate logic | P1 |
| No SSE event emission | **HIGH** | No real-time UI updates, no audit trail | P1 |
| No idempotency enforcement | **HIGH** | Duplicate action execution risk | P1 |
| No exponential backoff | **HIGH** | Retry storms on transient failures | P1 |
| No permission enforcement | **MEDIUM** | No role-based transition validation | P2 |
| Silent transition failures | **MEDIUM** | Hard to debug, silent data loss | P2 |
| No error classification | **MEDIUM** | No way to distinguish retryable vs fatal | P2 |
| Timeout only on pending_approval | **MEDIUM** | No timeout enforcement on other states | P2 |

### 2.2 Blockers to Safe Execution

#### BLOCKER #1: No Failed State Transition Loop
**Risk Level:** CRITICAL
**Scenario:** Action execution fails (e.g., calendar creation error)
- Current behavior: No handling
- Required behavior: approved → failed, then failed → proposed (retry) with exponential backoff
- Impact: Lost actions, silent data corruption

#### BLOCKER #2: No Idempotency Tracking
**Risk Level:** CRITICAL
**Scenario:** Approval request retried due to network partition
- Current behavior: Approval accepted twice → Action executed twice
- Required behavior: Idempotency key check → return cached result
- Impact: Duplicate events, corrupted state

#### BLOCKER #3: No Centralized Transition Validator
**Risk Level:** HIGH
**Scenario:** New code path added that violates transition rules
- Current behavior: Code review catches it, but no runtime validation
- Required behavior: StateMachine.validate_transition(from, to) enforces all rules
- Impact: Silent state machine corruption

#### BLOCKER #4: Missing SSE Event Emissions
**Risk Level:** HIGH
**Scenario:** UI needs to reflect action state changes in real-time
- Current behavior: No SSE support, UI must poll or use batch endpoints
- Required behavior: state_changed event emitted → SSE stream → UI updates
- Impact: Stale UI, poor UX

---

## SECTION 3: IMPLEMENTATION READINESS

### 3.1 Code Quality Assessment

**Existing Action Pipeline: 6/10**
- Strengths: Clear method names, good transition documentation in code, event logging
- Weaknesses: Scattered validation, no central rules, no retry logic, silent failures

**Risk if Not Refactored:** 3/10
- Refactoring will unify logic, reduce cognitive load, improve testability

### 3.2 Recommended Implementation Path

**Phase 2 (Building):**
1. Create `apps/api/core/state_machine.py` - Centralized FSM validator
2. Create `apps/api/core/event_bus.py` - SSE event emission
3. Refactor `household_os/runtime/action_pipeline.py` - Use StateMachine
4. Extend `household_state/contracts.py` - Add proper state enums
5. Add retry policy enforcement in orchestrator

**Phase 3 (Validation & Hardening):**
- Comprehensive transition coverage tests
- Retry policy correctness tests
- SSE ordering and dedup tests
- Permission enforcement tests
- End-to-end approval workflow tests

---

## SECTION 4: TRANSITION VALIDATION MATRIX

**Required Comprehensive Rules:**

```
proposed → pending_approval (requires_approval=true) ✅ IMPLEMENTED
proposed → approved (requires_approval=false) ✅ IMPLEMENTED
proposed → rejected (cancel before approval) ❌ EXISTS AS BEHAVIOR, NOT EXPLICIT
pending_approval → approved (user approval) ✅ IMPLEMENTED
pending_approval → rejected (user rejection) ✅ IMPLEMENTED
pending_approval → ignored (timeout) ⚠️ IMPLEMENTED BUT WRONG STATE (should be failed)
approved → committed (execution success) ❌ MISSING (uses executed)
approved → failed (execution error) ❌ MISSING
committed → (TERMINAL) ✅ IMPLIED
rejected → (TERMINAL) ✅ IMPLIED
failed → proposed (retry after transient error) ❌ MISSING
failed → (TERMINAL after max retries) ❌ MISSING
```

---

## SECTION 5: SUMMARY TABLE

### What Exists Today

| Capability | Status | Details |
|---|---|---|
| State enum | ✅ | 6 states (missing committed, failing at false failed) |
| Basic transitions | ✅ | proposed → approved → executed |
| Transition logging | ✅ | Stored in transition_log array |
| Approval workflow | ✅ | register → approve/reject → execute |
| Rejection handling | ✅ | explicit reject_actions method |
| Timeout handling | ⚠️ | Only pending_approval → ignored |
| Event history | ✅ | Basic event tracking in graph |
| Guards | ⚠️ | Request ID, state checks, but scattered |

### What Was NOT Implemented

| Capability | Impact | Severity |
|---|---|---|
| Centralized StateMachine | Scattered validation logic | HIGH |
| Committed state | Blocks idempotency tracking | CRITICAL |
| Failed state | No error recovery | CRITICAL |
| Retry policy | No resilience | CRITICAL |
| Exponential backoff | Retry storms possible | HIGH |
| Idempotency keys | Duplicate execution risk | CRITICAL |
| SSE events | No real-time UI updates | HIGH |
| Permission matrix | No RBAC enforcement | MEDIUM |
| Error classification | Can't retry intelligently | MEDIUM |
| Timeout on all states | Incomplete timeout coverage | MEDIUM |
| Silent failure handling | Hard to debug | MEDIUM |

---

## PHASE 1 CONCLUSION

**Status:** ✅ DISCOVERY COMPLETE

**Findings:**
1. Existing action_pipeline.py has ~60% of required functionality
2. Missing critical states (committed, failed) prevent full compliance
3. No centralized validator = high risk of corruption
4. No SSE integration = poor real-time UX
5. No retry mechanism = no resilience to transient failures

**Readiness for Phase 2:** ✅ Ready to implement

**Recommended First Step:** Create centralized StateMachine module with full transition rules, then refactor action_pipeline to use it.
