# STATE_MACHINE_COMPLETION_REPORT

Generated: 2026-04-22 UTC

## 1. Executive Summary

State machine COMPLETE: **PARTIAL**

Confidence score: **89 / 100**

Answer to core question:
Is lifecycle state machine fully implemented, fully enforced, and free of bypass paths across the entire system?
**No.**

What is true:
- Canonical lifecycle states are defined and actively used in runtime.
- Transition validation exists and is enforced in the primary runtime transition path.
- Invalid transitions are blocked in multiple layers.

What is not fully complete:
- Retry policy is defined but not enforced in runtime lifecycle execution path.
- Timeout policy is only partially enforced (pending_approval timeout only).
- Transition authority is fragmented across multiple transition tables with semantic drift.
- Persistence-layer APIs can accept direct current_state writes without transition validation.

## 2. State Coverage Table

| State | Definition Location(s) | Usage Location(s) | Notes |
| --- | --- | --- | --- |
| proposed | household_os/core/lifecycle_state.py | household_os/runtime/action_pipeline.py, household_os/runtime/state_reducer.py, apps/api/core/state_machine.py, household_os/runtime/trigger_detector.py, apps/api/assistant_runtime_router.py, tests/* | Canonical + duplicated in ActionState |
| pending_approval | household_os/core/lifecycle_state.py | household_os/runtime/action_pipeline.py, household_os/runtime/trigger_detector.py, apps/api/core/state_machine.py, apps/api/assistant_runtime_router.py, tests/* | Internal state; no dedicated event type |
| approved | household_os/core/lifecycle_state.py | household_os/runtime/action_pipeline.py, household_os/runtime/state_reducer.py, apps/api/core/state_machine.py, tests/* | Canonical + duplicated in ActionState |
| committed | household_os/core/lifecycle_state.py | household_os/runtime/action_pipeline.py, household_os/runtime/state_reducer.py, apps/api/core/state_machine.py, tests/* | Canonical terminal state |
| failed | household_os/core/lifecycle_state.py | household_os/runtime/action_pipeline.py, household_os/runtime/state_reducer.py, apps/api/core/state_machine.py, household_os/runtime/command_handler.py, tests/* | Retry semantics conflict across layers |
| rejected | household_os/core/lifecycle_state.py | household_os/runtime/action_pipeline.py, household_os/runtime/state_reducer.py, apps/api/core/state_machine.py, apps/api/assistant_runtime_router.py, tests/* | Canonical terminal state |

Canonical definitions found:
- household_os/core/lifecycle_state.py

Duplicate active definition found:
- apps/api/core/state_machine.py (ActionState enum duplicates canonical values)

Legacy conflicting definitions present (not imported by active runtime paths scanned):
- legacy/lifecycle/execution_state_machine.py
- legacy/lifecycle/state_transitions.py

## 3. Transition Matrix

### 3.1 Explicit Allowed Transitions (FSM Validator)
Source: apps/api/core/state_machine.py

- proposed -> pending_approval, approved, rejected, failed
- pending_approval -> approved, rejected, failed
- approved -> committed, failed
- committed -> (none)
- rejected -> (none)
- failed -> proposed  (retry)

### 3.2 Explicit Allowed Transitions (Event Reducer)
Source: household_os/runtime/state_reducer.py

- proposed -> approved, failed, rejected
- pending_approval -> approved, failed, rejected
- approved -> committed, failed
- committed -> (none; terminal)
- rejected -> (none; terminal)
- failed -> (none; terminal)

### 3.3 Transition Drift / Missing / Implicit

1. **Drift: failed -> proposed**
- Allowed in FSM validator.
- Not allowed in reducer (failed is terminal).
- Impact: inconsistent source-of-truth behavior across validation vs replay.

2. **Implicit pending_approval handling**
- No ACTION_PENDING_APPROVAL event in household_os/runtime/domain_event.py.
- action_pipeline encodes pending_approval as in-memory/object fallback state when reducer returns proposed.
- Impact: pending_approval is not fully event-sourced; replay-only reconstruction cannot independently derive it.

3. **Fragmented transition authority**
- Transition rules exist in at least three places:
  - apps/api/core/state_machine.py
  - household_os/runtime/state_reducer.py
  - household_os/runtime/command_handler.py (state/event checks)
- Impact: higher drift risk and inconsistent enforcement behavior.

### 3.4 Invalid Transitions Detected in Code/Test Evidence

Invalid transition blocking is explicitly asserted and tested:
- approved -> proposed blocked (tests/test_state_machine.py)
- first event != action_proposed blocked (tests/test_event_sourcing.py)
- proposed -> committed (event sequence skip) blocked (tests/test_event_sourcing.py)
- terminal -> non-terminal blocked in reducer and tests

No production code path found intentionally performing invalid lifecycle transitions during normal flow.

## 4. Enforcement Analysis

### 4.1 Transition Rules Defined and Enforced
Status: **PARTIAL PASS**

Evidence:
- action_pipeline _append_transition() validates non-bootstrap transitions via validate_transition().
- reducer blocks invalid event sequences.
- command_handler blocks invalid command/state combinations.

Gap:
- Multiple transition authorities with inconsistent semantics (failed retry drift).

### 4.2 Retry Rules Defined and Enforced
Status: **PARTIAL / INCOMPLETE**

Defined:
- RETRY_POLICY exists in apps/api/core/state_machine.py.
- StateMachine has can_retry(), get_retry_delay(), retry_count.

Enforced in active runtime flow:
- No runtime path in household_os/runtime/action_pipeline.py uses StateMachine.can_retry() or get_retry_delay().
- No observed runtime transition path from failed -> proposed in ActionPipeline lifecycle methods.
- execute_approved_actions() does not convert execution exceptions into failed transitions.

Conclusion:
- Retry policy is implemented in FSM utility and tests, but not fully wired into runtime lifecycle execution.

### 4.3 Timeout Rules Defined and Enforced
Status: **PARTIAL / INCOMPLETE**

Defined:
- STATE_TIMEOUTS exists for proposed, pending_approval, approved in apps/api/core/state_machine.py.

Enforced:
- TriggerDetector enforces pending_approval timeout via APPROVAL_PENDING_TIMEOUT trigger.
- ActionPipeline.reject_action_timeout() transitions pending_approval -> rejected.

Not enforced:
- No runtime enforcement found for proposed timeout.
- No runtime enforcement found for approved timeout.
- StateMachine.has_timed_out() appears unintegrated with runtime orchestration paths.

### 4.4 Invalid Transition Blocking
Status: **PASS (with fragmentation)**

Evidence:
- validate_transition() has explicit ALLOWED_TRANSITIONS + INVALID_TRANSITIONS + guard rules.
- reducer throws StateReductionError on invalid sequences.
- command handler rejects invalid command transitions.

Concern:
- Validation logic is duplicated across layers rather than centralized.

## 5. Bypass Analysis

### 5.1 Direct Mutation Paths

Blocked path (positive control):
- LifecycleAction.__setattr__ + FIREWALL blocks unauthorized current_state mutation.
- tests/test_household_os_runtime.py verifies direct setattr(current_state, ...) is blocked.

Potential bypass capability (not transition-validated):
- household_os/core/household_state_graph.py save_graph()/internal lifecycle assertions only require enum type validity, not transition legality.
- household_state/household_state_manager.py has similar behavior.
- Any caller with graph dict access can set action_lifecycle.actions[action_id].current_state to a valid enum and persist without validate_transition() or event append.

Assessment:
- **Bypass surface exists at persistence API boundary.**

### 5.2 Indirect Mutation Paths

Primary safe path:
- action_pipeline _append_transition() -> validate_transition() -> authorized mutation -> transition log + domain event append.

Secondary paths:
- HouseholdStateGraphStore.apply_approval() mutates approval payload status, not lifecycle current_state.
- Recommendation adjustment path mutates scheduling fields, not lifecycle state.

Risk:
- Public graph persistence APIs permit lifecycle field writes if caller constructs payloads directly.

### 5.3 Unsafe Legacy Paths

Legacy lifecycle FSM modules remain in repository:
- legacy/lifecycle/* contains alternate state model (created/queued/running/etc).

Runtime import scan (apps/**, household_os/**, assistant/**, tests/**):
- No direct imports to legacy.lifecycle detected in active runtime paths scanned.

Residual risk:
- Legacy modules still executable if manually invoked.
- Documentation and migration scripts still reference old terms during migration context.

## 6. Legacy Drift Findings

### 6.1 Old terminology still present

Found:
- scripts/migrate_lifecycle_states.py maps executed -> committed and ignored -> failed
- scripts/migrate_event_stream.py maps executed -> committed and ignored -> failed
- docs/LIFECYCLE_STATE_UNIFICATION.md references executed/ignored mapping context

Boundary behavior:
- parse_lifecycle_state() rejects non-canonical states.
- boundary enforcement tests verify executed/ignored are rejected as lifecycle state inputs.

Additional note:
- Some non-lifecycle response strings still use "executed" as a user-facing status label (not stored as lifecycle current_state).

## 7. Critical Gaps

Blocking issues preventing full completion:

1. **Transition authority drift**
- failed -> proposed retry exists in FSM but not reducer replay model.

2. **Retry not fully operational in runtime lifecycle path**
- retry/backoff methods defined but not integrated into orchestrator/action pipeline execution flow.

3. **Timeout enforcement incomplete**
- proposed and approved timeout policies exist but no active runtime enforcement path found.

4. **Persistence-level bypass surface**
- graph save APIs validate type, not transition legality or event-sourced consistency.

5. **Pending approval not fully event-sourced**
- pending_approval has no dedicated domain event; represented as fallback/in-memory state.

## 8. Final Verdict

READY FOR NEXT PHASE: **NO**

Required fixes to reach full completion:

1. Unify transition authority:
- Ensure reducer + validator + command handler share one transition matrix semantics.
- Resolve failed retry drift (either support failed -> proposed in reducer/event model or remove from FSM).

2. Integrate retry enforcement into runtime execution path:
- On execution failure, transition approved -> failed with error classification.
- Apply max retries + backoff before any retry transition.

3. Complete timeout enforcement:
- Enforce timeout handling for proposed and approved states in runtime scheduler/trigger layer.

4. Close persistence bypass:
- Prevent direct persisted current_state writes unless they are transition-validated and event-backed.
- Add transition validation guard at save_graph boundaries for lifecycle changes.

5. Decide and formalize pending_approval event semantics:
- Either introduce an explicit event or codify and enforce fallback semantics consistently across replay/validation.

---

Overall conclusion:
The lifecycle FSM is substantially implemented and test-covered, but **not yet fully complete** against strict completion criteria due to enforcement gaps, semantic drift, and persistence bypass capability.
