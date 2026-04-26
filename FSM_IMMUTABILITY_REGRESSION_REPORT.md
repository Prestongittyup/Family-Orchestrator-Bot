# FSM Immutability Regression Report

Date: 2026-04-22
Audit mode: Verification-only (no code changes)

## 1. Executive Result

- IMMUTABLE STATE GUARANTEE: NO
- Confidence score: 95

Conclusion basis:
- Primary runtime lifecycle transition path is FSM-authoritative.
- However, shadow normalization paths still rewrite lifecycle state fields outside StateMachine.transition_to().
- Therefore the strict claim, "transition_to() is the ONLY possible mutation authority in the entire system," is not true at repository scope.

## 2. Full Mutation Surface Map

### A. Runtime execution layer

1. Authoritative transition path (enforced)
- File: household_os/runtime/action_pipeline.py
- Evidence: transition creation via fsm.transition_to(...) and persisted payload update
- Mutation form: updated_action = action.model_copy(update={"current_state": resolved_to_state, ...})
- Classification: AUTHORIZED

2. Runtime mutation guard
- File: household_os/runtime/action_pipeline.py
- Evidence: LifecycleAction.__setattr__ blocks direct current_state writes unless FIREWALL.can_mutate(...)
- Classification: PROTECTIVE CONTROL

3. Firewall and proxy guard
- File: household_os/runtime/state_firewall.py
- Evidence: block_direct_mutation raises StateMutationViolation
- File: household_os/runtime/state_proxy.py
- Evidence: current_state setter always calls FIREWALL.block_direct_mutation(...)
- Classification: PROTECTIVE CONTROL

### B. FSM core layer

1. FSM authority implementation
- File: apps/api/core/state_machine.py
- Evidence: StateMachine.transition_to() validates via validate_transition(), then mutates self.state
- Classification: AUTHORIZED

2. Retry centralization
- File: apps/api/core/state_machine.py
- Evidence: FSMRetryPolicy.should_retry(), get_retry_delay_seconds(); StateMachine delegates can_retry/get_retry_delay
- Classification: CENTRALIZED

3. Timeout centralization
- File: apps/api/core/state_machine.py
- Evidence: FSMTimeoutPolicy.has_timed_out(); StateMachine.has_timed_out delegates
- Classification: CENTRALIZED

### C. Persistence layer

1. Graph store boundary checks (new consolidated path)
- File: household_os/core/household_state_graph.py
- Evidence: validate_state_before_persist(...) in _assert_lifecycle_sections()
- Classification: GUARDED

2. Shadow lifecycle normalization rewrite (active)
- File: household_os/core/household_state_graph.py
- Evidence: _parse_lifecycle_sections rewrites current_state/from_state/to_state/status via enforce_boundary_state(...)
- Classification: NON-FSM MUTATION PATH (normalization)

3. Legacy/parallel state manager normalization rewrite
- File: household_state/household_state_manager.py
- Evidence: _parse_lifecycle_sections does payload["current_state"] = enforce_boundary_state(state)
- Classification: NON-FSM MUTATION PATH (normalization)

### D. Event system

1. Event replay/reducer
- File: household_os/runtime/state_reducer.py
- Evidence: computes current_state via pure local variable transitions; no storage writes
- Classification: DERIVATION ONLY (no direct persistence mutation)

2. Command/event generation
- File: household_os/runtime/command_handler.py
- Evidence: validates and emits DomainEvent; no direct lifecycle field assignment
- Classification: EVENT-DRIVEN (no direct field mutation)

### E. Ingress layer

1. API runtime ingress
- File: apps/api/assistant_runtime_router.py
- Evidence: /approve and /reject call orchestrator/pipeline; no direct lifecycle state assignment in routes
- Classification: ROUTED TO RUNTIME PIPELINE

2. Orchestrator ingress
- File: household_os/runtime/orchestrator.py
- Evidence: tick/approve_and_execute invoke ActionPipeline methods; no direct state assignment
- Classification: ROUTED TO RUNTIME PIPELINE

### F. Permission / actor layer

1. Actor tags present in tracing
- Files: household_os/runtime/action_pipeline.py, household_os/runtime/orchestrator.py
- Evidence: actor_type="system_worker" in trace annotations

2. FSM actor guard exists but is not wired in runtime calls
- File: apps/api/core/state_machine.py
- Evidence: validate_transition blocks assistant approval only when context includes actor_type="assistant"
- Runtime call evidence: ActionPipeline passes context={"requires_approval": ...} without actor_type
- Classification: PARTIAL ACTOR CONTROL (guard exists, not broadly exercised)

### G. Test/tooling layer

1. Intentional direct mutation tests
- File: tests/test_household_os_runtime.py (setattr(action, "current_state", ...), action.state = ...)
- File: tests/test_state_machine.py (direct fsm.state assignments in simulation-style tests)
- Classification: TEST-ONLY NON-PRODUCTION MUTATION

2. CI mutation guard
- File: ci/state_mutation_guard.py
- Evidence: detects attr assignment/setattr/__dict__ mutation patterns for lifecycle objects
- Classification: PROTECTIVE CONTROL

## 3. Bypass Analysis

Paths not using StateMachine.transition_to():

1. household_os/core/household_state_graph.py::_parse_lifecycle_sections
- Rewrites lifecycle fields through normalization (enforce_boundary_state)
- Not transition-validated by FSM transition matrix
- Impact: medium (normalization path, not a transition intent path)

2. household_state/household_state_manager.py::_parse_lifecycle_sections
- Rewrites payload["current_state"] directly
- Used by assistant/runtime/assistant_runtime.py and apps/assistant_core/assistant_router.py
- Impact: high for strict immutability claim because it is an active module path

3. Event reducer state application (state_reducer._apply_event)
- Applies transition logic without invoking StateMachine.transition_to()
- Persistence impact: low (derivation only), but violates literal "only authority" wording for state transition logic execution

Assessment:
- If the requirement is literal repository-wide exclusivity, bypasses exist.
- If the requirement is only "no direct runtime action.current_state assignment in execution path," current main runtime path is compliant.

## 4. Actor Safety Check

Result: PARTIAL

- No evidence of API user/assistant/system_worker route directly assigning lifecycle state in runtime ingress handlers.
- State mutation goes through ActionPipeline transition path in active runtime flow.
- However, actor-type enforcement in FSM validate_transition() is conditional on context["actor_type"] and that context is not passed in active ActionPipeline transition calls.
- No explicit actor bypass write found, but actor guard coverage is incomplete.

## 5. Persistence Safety Confirmation

Result: NO (strict criterion)

- Guarded writes: household_os/core/household_state_graph.py validates persisted lifecycle fields.
- Remaining direct lifecycle rewrites outside transition_to():
  - household_os/core/household_state_graph.py::_parse_lifecycle_sections
  - household_state/household_state_manager.py::_parse_lifecycle_sections

Therefore: direct lifecycle writes still exist in persistence-adjacent normalization paths.

## 6. Event Replay Safety

Result: YES for persistence mutation safety, NO for literal transition authority exclusivity

- replay_events/reduce_state are pure reconstruction flows; they do not write lifecycle state to storage directly.
- reducer applies transition semantics internally (without transition_to), so strict "single transition authority method" is not literal across all transition logic implementations.

## 7. Final Verdict

- SAFE TO PROCEED TO PERMISSION MODEL: NO (under strict immutability definition)

Reason:
- The strict success criterion asks whether StateMachine.transition_to() is the ONLY possible mutation authority across the entire system (actors, persistence, replay, tooling).
- Repository evidence shows active non-transition_to lifecycle rewrite paths in normalization layers.

## Trace Matrix (SOURCE -> VALIDATION -> TRANSITION -> PERSISTENCE -> READBACK)

1. Runtime approve/reject/execute path (primary)
- SOURCE: API/orchestrator call into ActionPipeline
- VALIDATION: validate_transition() through StateMachine.transition_to()
- TRANSITION: StateMachine.transition_to()
- PERSISTENCE: graph action_lifecycle.actions[...]=payload + event_store.append(...)
- READBACK: _get_derived_state() via event replay + graph load
- FSM.transition_to present: YES

2. Timeout-triggered rejection path
- SOURCE: TriggerDetector emits APPROVAL_PENDING_TIMEOUT
- VALIDATION: FSMTimeoutPolicy.has_timed_out() + transition validation in ActionPipeline
- TRANSITION: StateMachine.transition_to()
- PERSISTENCE: graph write + event append
- READBACK: orchestrator/state store reload
- FSM.transition_to present: YES

3. Graph normalization path (core store)
- SOURCE: load_graph()/save_graph() parse lifecycle sections
- VALIDATION: enforce_boundary_state() (type coercion)
- TRANSITION: none (no transition matrix check)
- PERSISTENCE: may be persisted on subsequent _write_graph()
- READBACK: normalized graph payload
- FSM.transition_to present: NO

4. Legacy/parallel state manager normalization path
- SOURCE: HouseholdStateManager.load_graph() in assistant runtime/router
- VALIDATION: enforce_boundary_state()
- TRANSITION: none
- PERSISTENCE: persisted through HouseholdStateManager._write_graph()
- READBACK: manager cache/load
- FSM.transition_to present: NO

5. Event replay derivation path
- SOURCE: EventStore history
- VALIDATION: event payload/state checks in reducer
- TRANSITION: reducer _apply_event() logic
- PERSISTENCE: none (derivation only)
- READBACK: derived LifecycleState returned to callers
- FSM.transition_to present: NO
