# Goal Progress Map (Audit Regenerated 2026-05-01)

Source of truth: docs/architecture/RFC-001.md

This map is implementation-grounded and audit-only. It does not override RFC-001. If there is conflict, RFC-001 prevails.

Governance freeze notice: this file is non-authoritative and observational only, and must not be used as enforcement input for pass/fail decisions.

Reporting mode: derived state only.

- Entries in this file must map to executable evidence (tests, guards, workflows, or direct code locations).
- Governance semantics are referenced from `ARCHITECTURAL_CONTRACT_SPEC.md` and `tests/governance/contract_kernel.py`.
- This file records verification state and drift posture only; it does not define governance policy text.

## North-Star Goal

Operate a deterministic Household OS where all write-capable behavior is enforced through the RFC-001 canonical flow:

User Request -> Command Validation -> Execution Gateway -> Rules Engine -> LLM Advisory (optional) -> Risk Engine -> Decision -> Saga Execution -> Event Commit -> Projection Update -> Response

The intended end state is a system where this flow is not just documented, but continuously enforced by runtime boundaries, static architecture validation, and CI blocking checks.

## Current System Reality

- The canonical command runtime exists and is event-first. Command handling, policy binding, risk classification, saga execution, and projection replay are implemented in app/services/commands/runtime.py.
- Event authority is implemented as append-only, signed events with constrained mutation callers in household_os/runtime/event_store.py, with replay-based state derivation in household_os/runtime/state_reducer.py.
- Deterministic control and policy layers are live: core/control/control_plane.py and core/policy/policy_version_registry.py are integrated into saga execution in app/services/commands/runtime.py.
- Runtime startup and import boundary architecture checks are wired through core/architecture/architecture_guard.py from app/main.py and package init modules.
- CI workflow wiring exists for architecture scripts and pytest marker tiers across governance and RFC audit workflows.
- ACS is active as compressed governance specification and test coverage now enforces semantic invariants without ACS rule-ID assertions.
- Feature intake governance is test-layer gated through mandatory per-suite intake declarations with frozen projection/read-model/kernel interaction tags.
- Current governance status for read-model boundary/determinism/dependency tracks is green in local verification.

## Verified Completed Systems

### V1. Contract And Layer Boundaries Are Machine-Readable

Evidence: core/architecture/contract_loader.py parses docs/architecture/RFC-001.md and docs/architecture/LAYER_MAP.md into allowed boundaries and forbidden patterns.

### V2. Blocking Static Architecture Validator Is Active And Green

Evidence: scripts/ci/architecture_validator.py returns architecture_valid=true with no violations.

Local result: 2026-04-30 run returned {"architecture_valid": true, "violations": []}.

### V3. Layer Boundary Static Guard Is Active And Green

Evidence: scripts/architecture_layer_guard.py.

Local result: 2026-04-30 run returned LAYER_GUARD_PASS.

### V4. Policy Versioning And Drift Detection Are Implemented And Tested

Evidence: core/policy/policy_version_registry.py.

Test evidence: tests/test_policy_versioning.py passed in the targeted regression run.

### V5. Deterministic Control-Plane Admission Is Implemented And Tested

Evidence: core/control/control_plane.py includes conflict detection, risk throttling, circuit behavior, and replay consistency checks.

Test evidence: tests/test_control_plane.py passed in the targeted regression run.

### V6. Saga Compensation And Replay Validation Are Implemented And Tested

Evidence: core/sagas/saga_orchestrator.py.

Test evidence: tests/test_saga_orchestrator.py passed in the targeted regression run.

### V7. Multi-Flow Deterministic Interaction Coverage Is Green

Evidence: tests/test_multi_flow_interactions.py.

Local result: all tests in this file passed in the targeted regression run.

### V8. Trust-Boundary Closure Coverage Is Green For Scoped Surfaces

Evidence: household_os/security/trust_boundary_enforcer.py and household_os/runtime/event_store.py.

Test evidence: tests/test_trust_surface_final_closure.py passed and reported final verdict CLOSED.

### V9. Marker-Tier CI Model Is Implemented

Evidence: pytest.ini defines ci_gate, migration, and reliability markers.

Evidence: .github/workflows/governance-gate.yml and .github/workflows/rfc001-audit.yml enforce blocking ci_gate behavior with non-blocking migration and reliability execution where configured.

### V10. ACS Semantic Contract Model Is Active

Evidence: `tests/governance/contract_kernel.py` is the single semantic governance kernel for test-layer references, and governance/read-model suites consume kernel constants instead of redefining policy text.

Local result: targeted ACS governance matrix and required regressions are green on 2026-05-01.

### V11. Feature Intake Declaration Governance Is Active (Test Layer Only)

Evidence: `tests/governance/test_feature_intake_contract.py` enforces declaration schema and impact-tag discipline for governed feature suites defined in `tests/governance/contract_kernel.py`.

Evidence: governed feature suites include `FEATURE_INTAKE_DECLARATION` headers and kernel interaction classification where applicable.

Local result: feature intake contract suite and required determinism/boundary/replay checks are green on 2026-05-01.

### V12. Feature Intake Contract Freeze Is Consolidated (Sprint 8.2)

Evidence: `tests/governance/contract_kernel.py` defines a locked three-field declaration schema (`projection_impact`, `read_model_impact`, `kernel_interaction`) and controlled enums.

Evidence: `tests/governance/test_feature_intake_contract.py` is the authoritative FIR enforcement suite and now validates governed suites plus canonical template alignment.

Evidence: `tests/governance/feature_intake_declaration_template.py` is aligned to the same frozen schema and test-layer-only scope.

Local result: Sprint 8.2 FIR freeze matrix is green on 2026-05-02.

## Partially Complete Systems

### P1. Blocking Contract Guard Is Wired But Not Converged

Evidence: scripts/contract_guard.py currently exits non-zero on /health references.

Local result (2026-04-30): exit code 1 with findings in app/api/assistant_runtime.py, app/api/command.py, tests/test_health_normalization.py, and tools/rfc_audit.py.

### P2. Docs-Pruning Governance Expectation Is Converged For Canonical Set

Evidence: tests/test_governance_gates.py::test_docs_markdown_is_pruned_to_canonical_set.

Local result (2026-05-01 ci_gate run): pass.

### P3. Runtime Architecture Guard Test Surface Is Still Legacy-Weighted

Evidence: the active guard implementation is core/architecture/architecture_guard.py, while integration architecture guard tests are concentrated under tests/legacy.

### P4. Compatibility Surface Still Depends On Archive Routing Paths

Evidence: app/main.py dynamically loads archive.* routers and middleware fallback paths.

Evidence: apps/api/integration_core/orchestrator.py still imports archive.apps.api.integration_core.* modules.

### P5. Local Task Automation Is Partially Stale

Evidence: .vscode/tasks.json includes many test paths that no longer exist, such as tests/test_integration_architecture_guard.py, causing false-negative local task runs.

## Drift Risks (ACTIVE)

### R1. CI Blocking Consistency Risk (Reduced)

Architecture validator and docs-pruning governance checks are passing in current local runs, but broader legacy surfaces still warrant continuous monitoring.

Risk: reduced compared to prior audit state.

### R2. Legacy Compatibility Path Risk

Archive fallbacks in app/main.py and archive imports in apps/api/integration_core/orchestrator.py increase the chance of boundary ambiguity and bypass during refactors.

### R3. Enforcement Coverage Asymmetry Risk

Static scans and trust-surface tests are strong in scoped areas, but end-to-end non-legacy coverage for all architecture-guard paths is not yet uniform.

### R4. Developer Workflow Drift Risk

Outdated local task definitions can lead to incorrect pass/fail assumptions and missed execution of the real guard set.

## System Maturity Rating

Phase 4 - Deterministic Execution and Active Enforcement (with governance-model alignment)

Why Phase 4:

- Deterministic command, policy, control-plane, saga, and replay systems are implemented and test-verified.
- Architecture validation and layer guard enforcement are wired and currently green.

Why not Phase 5:

- Governance model for ACS enforcement has transitioned to semantic invariants and current blocking governance checks in scope are green.
- Legacy compatibility surfaces still participate in runtime/API loading paths.
- Local developer guard automation is not fully converged with current repository topology.

## Forward Work (ONLY IF REQUIRED)

Required now only for remaining non-governance modernization surfaces.

- Resolve contract_guard false positives or policy mismatch around /health references in scripts/contract_guard.py and affected files.

- Maintain semantic-contract governance model by preventing reintroduction of ACS rule-ID assertions in test suites.

- Replace stale .vscode/tasks.json test paths with existing suites aligned to current ci_gate and architecture guard reality.

- Reduce archive fallback scope in app/main.py and retire archive imports from apps/api/integration_core/orchestrator.py as canonical replacements are confirmed.

- Add or migrate non-legacy tests that directly exercise core/architecture/architecture_guard.py behavior under current runtime package paths.
