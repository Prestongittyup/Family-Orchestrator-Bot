# Lifecycle Surface Consistency Report

Date: 2026-04-22
Scope: Repository-wide lifecycle vocabulary audit and non-runtime normalization pass
Mode: Normalization and consistency only (no runtime FSM behavior refactor)

## 1. Summary

- Total lifecycle-term occurrences scanned: 1593
- Legacy non-canonical occurrences (executed/ignored): 156
- Final consistency verdict: INCONSISTENT

Reason: legacy lifecycle vocabulary remains in historical documentation/report artifacts and a small set of runtime user-facing strings that are intentionally excluded from this pass.

## 2. Category Breakdown

### All lifecycle terms

- runtime: 410
- tests: 391
- scripts: 78
- migrations: 0
- docs: 630
- fixtures: 9
- other: 75

### Legacy non-canonical terms (executed/ignored)

- runtime: 24
- tests: 35
- scripts: 2
- migrations: 0
- docs: 86
- fixtures: 1
- other: 8

## 3. Classification Model Applied

- runtime-critical (not changed): active runtime semantics and externally exposed response-contract behavior
- test-only (safe to update): test assertions/fixtures not validating legacy-compatibility behavior
- documentation/fixture (safe to normalize): examples and docs that are not historical forensic artifacts
- migration/backfill (compatibility mapping): scripts that intentionally translate legacy values to canonical values

## 4. Cleanup Changes Applied

### Scripts and migration/backfill layer

- Added shared compatibility conversion utility:

  - scripts/lifecycle_conversion.py

- Centralized legacy-to-canonical mapping usage in migration scripts:

  - scripts/migrate_lifecycle_states.py
  - scripts/migrate_event_stream.py

### Tests and fixtures normalization

- Replaced raw canonical literals in non-compatibility checks with enum-value based usage:

  - tests/test_boundary_enforcement.py
  - tests/test_fsm_immutability_enforcement.py

- Removed isolated raw status assertion and normalized to canonical enum via helper:

  - tests/test_assistant_runtime_router.py

### New guard test (runtime code)

- Added AST-based CI guard test to block non-canonical lifecycle literals in runtime state contexts:

  - tests/test_lifecycle_surface_consistency_guard.py

Guard scope:

- scans runtime roots: apps/, household_os/, assistant/, services/
- excludes: tests/, docs/, migrations/, archive/, .venv/
- fails on legacy literals in lifecycle/state context (dict payload state fields, state comparisons, state parser calls)

## 5. Files Cleaned or Normalized

- scripts/lifecycle_conversion.py
- scripts/migrate_lifecycle_states.py
- scripts/migrate_event_stream.py
- tests/test_assistant_runtime_router.py
- tests/test_boundary_enforcement.py
- tests/test_fsm_immutability_enforcement.py
- tests/test_lifecycle_surface_consistency_guard.py

## 6. Intentionally Excluded

### Runtime-critical

- apps/api/assistant_runtime_router.py
  - status="executed" is currently part of existing response behavior and was not changed in this pass.

### Historical reports and forensic docs

- FSM_PHASE1_DISCOVERY_REPORT.md
- docs/LIFECYCLE_STATE_UNIFICATION.md
- docs/EVENT_SOURCING_STATE_NAMES_FIX.md
- STATE_MACHINE_COMPLETION_REPORT.md
- EVENT_SOURCING_ISSUE_ANALYSIS.md
- FSM_EXECUTIVE_SUMMARY.md
- FSM_FINAL_SUMMARY.md
- FSM_PHASE2_PHASE3_IMPLEMENTATION_REPORT.md

### Non-runtime fixture artifact

- data/execution_traces/event_replay_traces.json
  - contains intentional legacy invalid-state trace data for replay-validation context.

## 7. Validation Results

Executed:

- python -m pytest tests/test_boundary_enforcement.py tests/test_fsm_immutability_enforcement.py tests/test_lifecycle_surface_consistency_guard.py -q

Result:

- 16 passed, 0 failed

Note:

- A broader run including tests/test_assistant_runtime_router.py currently fails due existing /assistant/run route resolution (404) in this environment; this is unrelated to the lifecycle normalization edits.

## 8. Remaining Work to Reach CONSISTENT

1. Normalize or archive historical docs that still present legacy lifecycle terms as active vocabulary.
2. Decide and apply API contract migration strategy for runtime status label executed -> committed (if contract change is approved).
3. Re-run full repository audit and require legacy runtime/doc counts to reach zero (outside explicit compatibility/migration fixtures).
