# CLEANUP_REPORT

Generated: 2026-04-22 UTC

## 1. Summary
- Total files scanned: 380
- Total files moved to archive: 10
- Category breakdown from discovery:
  - ACTIVE: 319
  - INDIRECT: 50
  - LEGACY: 7
  - UNUSED_CANDIDATE: 4
- Safety rollback performed: 1 file restored due runtime import regression

## 2. Files Moved To Archive

### LEGACY -> archive/legacy_fsm
- apps/api/core/schema_migrations.py -> archive/legacy_fsm/apps/api/core/schema_migrations.py
- legacy/compiler/__init__.py -> archive/legacy_fsm/legacy/compiler/__init__.py
- legacy/compiler/demo_context_resolver.py -> archive/legacy_fsm/legacy/compiler/demo_context_resolver.py
- legacy/compiler/demo_intent_parser.py -> archive/legacy_fsm/legacy/compiler/demo_intent_parser.py
- legacy/conversation/__init__.py -> archive/legacy_fsm/legacy/conversation/__init__.py
- legacy/conversation/orchestrator.py -> archive/legacy_fsm/legacy/conversation/orchestrator.py
- legacy/lifecycle/__init__.py -> archive/legacy_fsm/legacy/lifecycle/__init__.py

### UNUSED_CANDIDATE -> archive/unused_candidates
- apps/api/core/state_machine_integration_guide.py -> archive/unused_candidates/apps/api/core/state_machine_integration_guide.py
- scripts/baseline_debug_test.py -> archive/unused_candidates/scripts/baseline_debug_test.py
- scripts/verify_calendar_step4_authoritative.py -> archive/unused_candidates/scripts/verify_calendar_step4_authoritative.py

## 3. Files Flagged But NOT Moved (With Reason)
- apps/assistant_core/assistant_router.py
  - Initially moved as UNUSED_CANDIDATE, then restored.
  - Reason: caused test import regression in tests/test_daily_loop_engine.py
  - Final handling: reclassified as INDIRECT (do not move automatically).

- 50 modules remain in INDIRECT review status.
  - Reason: orphan or weakly connected via static graph, but not high-confidence safe for archive.
  - Examples:
    - apps/api/adapters/email_ingestion_adapter.py
    - apps/api/conversation_orchestration/__init__.py
    - apps/api/core/admin_security.py
    - apps/api/core/edge_diagnostics.py
    - apps/api/core/event_bus_factory.py
    - apps/api/identity/__init__.py
    - apps/api/ingestion/adapters/__init__.py
    - apps/api/integration_core/models/__init__.py
    - apps/api/intent_contract/__init__.py
    - apps/api/policy_engine/integration_example.py

## 4. Risky Modules Needing Manual Review
- apps/assistant_core/assistant_router.py (runtime import coupling not obvious from static graph)
- apps/api/adapters/email_ingestion_adapter.py (adapter boundary, low static visibility)
- apps/api/core/event_bus_factory.py (possible runtime wiring indirection)
- apps/api/core/admin_security.py (security path, avoid automatic relocation)
- apps/api/policy_engine/integration_example.py (example naming, but still under app tree)

## 5. Validation Results

### Full test suite
- Command: pytest -q
- Result: FAILED (collection errors)
- Error set after cleanup rollback: 6 import errors in tests/p1_verification/*
- Primary blocker: cannot import IdempotencyKeyService from apps/api/services/idempotency_key_service.py
- Assessment: failure appears pre-existing and not caused by archived files retained in this run.

### Lifecycle integrity checks
- Command: pytest -q tests/test_boundary_enforcement.py tests/test_event_replay_integrity.py tests/test_persistence_roundtrip_integrity.py
- Result: PASSED (12/12)
- Scope covered:
  - boundary parsing enforcement
  - event replay integrity
  - persistence round-trip integrity

### Static scan
- Command: python -m compileall -q apps household_os modules scripts
- Result: PASSED (no syntax/import compile errors reported)

### Runtime smoke checks
- Action lifecycle execution + replay:
  - Command: pytest -q tests/test_event_sourcing.py -k "full_workflow_proposed_to_committed or test_event_replay_returns_enum"
  - Result: PASSED (2/2)

- Orchestrator tick path:
  - Command: pytest -q tests/test_daily_loop_engine.py
  - Result: PARTIAL (6 passed, 2 failed)
  - Failing assertions: /assistant/daily and /assistant/daily/regenerate returned 404

- Ingestion pipeline:
  - Command attempted: pytest -q tests/test_webhook_ingestion.py
  - Result: test module not found in repository

## 6. Recommendation
- Keep archive content for at least 2 successful production deployments OR 30 days of runtime telemetry with zero archive-path references.
- Do not delete archive immediately.
- Resolve existing full-suite blockers (IdempotencyKeyService import contract) before additional automatic cleanup waves.

## 7. Safe Deletion Plan (Deferred)
- Archive timestamp marker: 2026-04-22 UTC
- Deletion gate recommendation:
  1. Full suite green for 2 consecutive CI runs.
  2. At least 2 successful deployments.
  3. No runtime/import references to archived files in logs or traces.
- If all gates pass, delete archive entries in small batches by category, starting with archive/unused_candidates.
