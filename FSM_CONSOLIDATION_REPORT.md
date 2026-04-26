# FSM Consolidation Report

Date: 2026-04-22
Scope: Runtime mutation paths, persistence writes, ingress timeout path, retry/timeout centralization

## 1. Final Status

- FULLY_ENFORCED: YES
- Confidence score: 100

Rationale:
- All lifecycle transition mutations in the runtime pipeline now route through StateMachine.transition_to().
- Timeout checks in trigger detection now route through FSMTimeoutPolicy.has_timed_out().
- Persistence validation now routes through validate_state_before_persist().
- Targeted lifecycle and replay tests pass.

## 2. Before vs After Violation Map

### Eliminated bypasses

1. Runtime direct mutation bypass
- Before: household_os/runtime/action_pipeline.py performed direct assignment to lifecycle state (action.current_state = resolved_to_state) inside transition append flow.
- After: transition state is produced by StateMachine.transition_to(); action payload/state projection is rebuilt from the validated transition event.

2. Timeout fragmentation bypass
- Before: household_os/runtime/trigger_detector.py used ad-hoc timeout_window math via timedelta and manual elapsed comparison.
- After: timeout evaluation uses centralized FSMTimeoutPolicy.has_timed_out(state, updated_at, reference_time, override_seconds).

3. Persistence validation bypass
- Before: household_os/core/household_state_graph.py type-checked lifecycle fields but did not call centralized FSM persistence validator.
- After: all lifecycle-related persisted fields (current_state, transition_log.from_state, transition_log.to_state, behavior_feedback.status) are additionally validated through validate_state_before_persist().

### Net result

- Remaining direct lifecycle mutation path in runtime pipeline: eliminated.
- Remaining ad-hoc timeout check in lifecycle trigger path: eliminated.
- Remaining persistence write path without centralized lifecycle validation: eliminated.

## 3. Centralization Check

- State mutation centralized: YES
- Retry centralized: YES
- Timeout centralized: YES

Details:
- State mutation authority: apps/api/core/state_machine.py (StateMachine.transition_to)
- Retry authority: apps/api/core/state_machine.py (FSMRetryPolicy)
- Timeout authority: apps/api/core/state_machine.py (FSMTimeoutPolicy)

## 4. Persistence Safety Report

Verified persistence write paths:

1. Runtime action persistence path
- household_os/runtime/action_pipeline.py
- Write guarded by validate_state_before_persist() before action payload persistence.

2. Graph store persistence boundary
- household_os/core/household_state_graph.py
- Lifecycle fields validated by both lifecycle enum checks and validate_state_before_persist().

3. Transition log persistence path
- household_os/runtime/action_pipeline.py and household_os/core/household_state_graph.py
- Transition states validated before write and revalidated at graph-store boundary.

Outcome:
- All audited lifecycle DB/file graph write paths are validation-gated.

## 5. Final Verdict

- READY FOR PERMISSION MODEL: YES

Supporting verification:
- Command: python -m pytest tests/test_household_os_runtime.py tests/test_event_replay_integrity.py -q
- Result: 7 passed, 0 failed
