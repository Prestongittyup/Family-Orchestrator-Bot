# Lifecycle CQRS Model

Date: 2026-04-22
Architecture: Formal CQRS (Command Query Responsibility Segregation)
Scope: Lifecycle state management for Family Orchestration Bot

## 1. Overview

The lifecycle system is formally organized into three layers following CQRS principles:

1. **Write Model** - Single lifecycle mutation authority
2. **Read Model** - Read-only projections and derivations  
3. **Presentation Model** - External API contract layer

## 2. Write Model (Command Side)

### Single Mutation Authority

**Module:** `apps/api/core/state_machine.py`

**Class:** `StateMachine`

**Method:** `StateMachine.transition_to(target_state, **kwargs)`

This is the **ONLY** location in the codebase where lifecycle state is mutated:

```python
self.state = target_state  # Line: mutation authority
self.updated_at = event.timestamp
self.transitions.append(event)
```

**Guarantees:**
- All state transitions pass through `validate_transition()`
- All transitions produce `StateTransitionEvent` records
- No state mutation without explicit `transition_to()` invocation
- Retry and timeout policies enforced at this layer only

**Transition Rules:**
- Defined in `ALLOWED_TRANSITIONS` (frozenset mapping)
- Validated by `validate_transition()` function
- Advanced guards (e.g., assistant cannot approve) enforced
- Single source of truth for legal state paths

### FSM Policy Classes

**FSMRetryPolicy:**
- Governs retry logic for failed states
- Backoff schedule with jitter
- Max retry count enforcement

**FSMTimeoutPolicy:**
- Defines timeout thresholds per state
- Timeout check logic

### Related Enums

**ActionState:**
- PROPOSED, PENDING_APPROVAL, APPROVED, COMMITTED, REJECTED, FAILED
- Single enum definition (no duplication)
- JSON-serializable (str-compatible)

### Error Classification

**classify_error(error_code):**
- Classifies errors as retryable or non-retryable
- Referenced by `transition_to()` when transitioning to FAILED
- Allows retry logic to make informed backoff decisions

---

## 3. Read Model (Query Side)

### 3.1 Event Sourcing / State Reduction

**Module:** `household_os/runtime/state_reducer.py`

**Function:** `reduce_state(events: list[DomainEvent]) -> LifecycleState`

**Purpose:**
- Derives current lifecycle state by replaying events in order
- Enables temporal queries (state at any point in history)
- Audit trail and event sourcing correctness

**Key Principle:**
- Pure function: same input events always produce same state
- No side effects
- Idempotent

**Transition Logic:**
- Maps event types to target states
- Validates event sequence legality
- **DOES validate transition legality** but only for **replay validation**, not for determining next state

**Important Constraint:**
- Reducer MUST NOT define transition rules (those belong to FSM only)
- Reducer MUST NOT decide what transitions are legal (FSM decides)
- Reducer validates that historical events follow FSM rules for audit purposes
- Reducer validates transitions using `validate_transition()` from FSM for consistency

**Event-to-State Mapping:**
- ACTION_PROPOSED → PROPOSED
- ACTION_APPROVED → APPROVED  
- ACTION_REJECTED → REJECTED
- ACTION_FAILED → FAILED
- ACTION_COMMITTED → COMMITTED

### 3.2 Persistence Layer Loaders

**Modules:**
- `household_os/core/household_state_graph.py`
- `household_state/household_state_manager.py`

**Pattern:** Hydration-only pattern (no mutation)

**Current Implementation:**
- `_parse_lifecycle_sections()` methods load lifecycle data
- Create non-persisted `LifecycleHydrationView` or `_lifecycle_hydration` snapshot objects
- `_assert_lifecycle_sections()` validates payloads read-only
- `_strip_lifecycle_hydration()` removes snapshots before persistence
- No lifecycle field mutations during load

**Read-Only Validation Rules:**
- Parse raw string values into canonical enums for type safety
- Validate enum values exist and are legal
- Never rewrite/mutate fields during load
- All validation is read-only predicates

**Hydration Snapshots:**
- Internal implementation detail (non-persisted)
- Contain raw state values for testability and debugging
- Stripped before any persistence write
- Enable non-mutating projections

### 3.3 Presentation Mapper

**Module:** `household_os/presentation/lifecycle_presentation_mapper.py`

**Pattern:** One-way mapping (internal → external)

**Mapping:**
- COMMITTED → "executed" (external API label)
- Direct enum → string conversion
- **Single place** where internal state name diverges from external API contract

**Constraints:**
- Read-only transformation only
- No logic inference or decision-making
- Pure projection function
- No field mutations

---

## 4. Presentation Model (API Contract)

**API Layer:** `apps/api/assistant_runtime_router.py`

**Contract:**
- All lifecycle state exposed via `LifecyclePresentationMapper.to_api_state(...)`
- No raw internal enum values exposed
- No direct lifecycle state access
- Read-only API contract

**Hardened Boundaries:**
- Ingress parsing via `enforce_boundary_state()` for defensive parsing
- Egress only via mapper for consistent external naming
- No lifecycle transitions triggered by API layer (API is read-only)

---

## 5. Data Flow

```
Write Flow:
===========
Command/Decision System
    ↓
StateMachine.transition_to()  [SINGLE MUTATION AUTHORITY]
    ↓
validate_transition()
    ↓
self.state = target_state
    ↓
StateTransitionEvent emitted
    ↓
Event stored to event stream


Read Flow (Current State):
==========================
Persisted State Graph
    ↓
Loader._parse_lifecycle_sections()  [READ-ONLY, HYDRATION-ONLY]
    ↓
LifecycleHydrationView (non-persisted)
    ↓
Application uses state


Replay Flow (Temporal Queries):
================================
Event Stream (historical)
    ↓
reduce_state(events)  [PURE FUNCTION, EVENT SOURCING]
    ↓
validate_transition() called for audit  [FSM RULES AUTHORITY]
    ↓
Derived state at point-in-time


API Flow:
=========
Application State (internal enum)
    ↓
LifecyclePresentationMapper.to_api_state()  [READ-ONLY PROJECTION]
    ↓
External string label
    ↓
HTTP Response
```

---

## 6. Architecture Constraints and Rules

### Rule 1: Single Mutation Authority

- **Only** `StateMachine.transition_to()` mutates `state` field
- No other function may assign/modify lifecycle state in persistence objects
- Enforced via code review and automated testing

### Rule 2: Read-Only Loaders

- Loaders are projection systems only
- Hydration may interpret and validate, never rewrite
- All snapshots/views MUST be non-persisted (stripped before write)
- Fields like `current_state`, `from_state`, `to_state` MUST NOT be written during load

### Rule 3: Reducer is Validation-Only

- Reducer replays events for state reconstruction
- Reducer validates transitions (calls `validate_transition()` from FSM)
- **Reducer does NOT define transition rules** (FSM owns these)
- Reducer does NOT choose next state (event type determines it)
- Reducer does NOT infer state meaning

### Rule 4: API is Presentation-Only

- All state exposed via `LifecyclePresentationMapper`
- Mapper never infers state meaning or logic
- Mapper performs pure one-way transformation
- No business logic in mapper

### Rule 5: Transition Rules are Centralized

- All transition legality rules in `ALLOWED_TRANSITIONS` (FSM module)
- No transition matrices duplicated elsewhere
- Other modules invoke `validate_transition()` from FSM if they need to validate
- Transition logic is never replicated outside FSM

---

## 7. Hard Invariants (Enforced by Tests)

1. **No lifecycle field mutations outside FSM**
   - Test: `test_fsm_is_sole_mutation_authority`
   - Scanning for assignments to: `current_state`, `from_state`, `to_state` outside FSM

2. **Loaders do not mutate**
   - Test: `test_graph_loader_is_read_only`
   - Inputs and outputs compared for lifecycle field identity

3. **Reducer does not define legality**
   - Test: `test_reducer_validates_not_defines`
   - Verify reducer calls `validate_transition()`, doesn't reimplement rules

4. **API never emits raw enum**
   - Test: `test_api_only_uses_mapper`
   - Verify all state outputs in API routes use mapper

5. **No normalize_state() performs mutation**
   - Test: `test_normalize_is_read_only`
   - Verify normalize functions don't modify inputs

6. **Singular transition record**
   - Test: `test_transition_rules_are_singular`
   - Verify `ALLOWED_TRANSITIONS` is the only transition rule source

---

## 8. Benefits of CQRS Lifecycle Model

1. **Clarity:** Single authority for mutations is obvious
2. **Testability:** Each layer can be tested independently
3. **Auditability:** All state changes tied to FSM transition events
4. **Correctness:** Validator and executor separated from business logic
5. **Extensibility:** New read models can be added without affecting write model
6. **Maintenance:** Changes to transition rules only need FSM updates
7. **Temporal Correctness:** Event replay can reconstruct state at any point in history

---

## 9. Migration Status

**Completed:**
- ✓ FSM identified as sole write authority
- ✓ Persistence loaders refactored to hydration-only pattern
- ✓ API layer enforces presentation mapper
- ✓ Lifecycle closure achieved with zero shadow normalization

**In Progress:**
- Formalizing and documenting CQRS model (this document)
- Reinforcing reducer constraints
- Adding CQRS invariant tests

**Planned:**
- Comprehensive CQRS consolidation report
- CI-level enforcement of invariants

---

## 10. References

- **Write Model:** `apps/api/core/state_machine.py`
- **Command Authority:** `StateMachine.transition_to()`
- **Transition Rules:** `ALLOWED_TRANSITIONS` constant
- **Validation Authority:** `validate_transition()` function
- **Read Model (Reducer):** `household_os/runtime/state_reducer.py`
- **Read Model (Loaders):** 
  - `household_os/core/household_state_graph.py`
  - `household_state/household_state_manager.py`
- **Presentation Model:** `household_os/presentation/lifecycle_presentation_mapper.py`
- **API Contract:** `apps/api/assistant_runtime_router.py`

