# Enforcement Checklist

This document is a derivative enforcement matrix for the immutable architecture contract in `docs/architecture/RFC-001.md`. Every rule is binary and must be evaluated as pass/fail.

Source of truth: `docs/architecture/RFC-001.md`

If this checklist conflicts with RFC-001, RFC-001 prevails.

## RFC-001 IMMUTABILITY RULE

- RFC-001 is the root architectural contract.
- Any deviation requires an explicit versioned RFC update.
- Implementation must defer to RFC-001 when ambiguity exists.
- Tests must assume RFC-001 compliance as an invariant.

| Rule | Description | Detection Method | Enforcement Location | [PASS] | [FAIL CONDITION] |
| --- | --- | --- | --- | --- | --- |
| CI-0 RFC-001 Immutability | RFC-001 must exist and remain the architecture root of truth referenced by core architecture docs. | CI scan for RFC violations, missing RFC artifact, or missing references from implementation plan/runtime flow/checklist. | `scripts/architecture_layer_guard.py`; `tests/system/test_architecture_suite.py`; `.githooks/pre-commit`; `.github/workflows/governance-gate.yml` | RFC artifact exists, docs reference RFC-001, and guards pass. | RFC missing, reference missing, or docs/rules drift from RFC semantics. |

## Core Invariants

| Rule | Description | Detection Method | Enforcement Location | [PASS] | [FAIL CONDITION] |
| --- | --- | --- | --- | --- | --- |
| CI-1 Event Log Is Source Of Truth | Mutation state must be represented as committed canonical events, not file-backed runtime state. | Run architecture and lifecycle invariant suites. Inspect mutation paths for canonical event append usage. | `scripts/architecture_layer_guard.py`; `tests/system/test_architecture_suite.py`; `app/services/events/canonical_router_service.py`; `app/adapters/db/event_log_repository.py` | All state mutations append canonical events and replay derives state. | Any runtime path writes authoritative state outside canonical event append. |
| CI-2 All Mutations Use Command Runtime Gateway | Command mutation calls (`handle_command`) must be constrained to the command runtime allowlist. Read-model endpoints may use `get_projection` for projection-derived queries. | Verify allowlist-only usage of mutation entry points and verify projection reads remain in canonical read-model endpoints. Verify single command POST contract remains in command API. | `scripts/architecture_layer_guard.py`; `tests/system/test_architecture_suite.py`; `app/api/command.py` | Mutation entry points are called only from allowlisted app API modules, and projection reads stay in canonical read-model endpoints. | Any non-allowlisted API module invokes `handle_command`, projection reads appear in non-canonical API surfaces, or additional command POST surfaces are introduced. |
| CI-3 Gateway Is Single Choke Point | LLM provider access must occur only through gateway/providers allowlist. | Scan runtime code for direct provider SDK imports or gateway bypass call signatures. | `scripts/architecture_layer_guard.py`; `tests/system/test_architecture_suite.py`; `app/adapters/llm/gateway.py` | Provider network execution remains in gateway/providers boundary. | Any service/router/runtime module imports provider SDKs or endpoints directly. |
| CI-4 Feature Intake Declaration Contract (Test Layer Only) | Feature-facing test suites must declare projection_impact/read_model_impact/kernel_interaction (no extra declaration fields) and remain aligned with deterministic projection/read-model contracts. | Run governance intake contract suite; validate declaration completeness, impact tagging, deterministic read-model evidence, and runtime-path non-leakage. | `tests/governance/test_feature_intake_contract.py`; `tests/governance/contract_kernel.py`; `tests/governance/feature_intake_declaration_template.py` | Every governed feature suite has a valid declaration and intake checks remain test-layer-only. | Feature-facing suite missing declaration, invalid impact tags, deterministic read-model evidence absent, or intake logic appears in runtime paths. |

## LLM Constraints

| Rule | Description | Detection Method | Enforcement Location | [PASS] | [FAIL CONDITION] |
| --- | --- | --- | --- | --- | --- |
| LC-1 No Execution Authority For LLM | LLM output is advisory only and cannot directly commit side effects. | Verify response flows keep approval gating before execution. Verify fallback behavior on LLM failures. | `app/services/llm_gateway/service.py`; `app/services/routing/intelligence_routing.py`; `tests/chaos/test_llm_gateway_runtime_hardening.py` | LLM output is schema-validated and converted to advisory payload only. | Any LLM output path directly triggers mutation/execution without gateway, risk, and approval gates. |
| LC-2 Gateway-Only LLM Access | App services must not bypass `LLMGateway`. | Static scan for provider imports and direct provider call patterns in service/runtime scopes. | `scripts/architecture_layer_guard.py`; `tests/system/test_architecture_suite.py` | Service layer uses gateway facades only for LLM access. | Any service imports provider modules or external LLM SDKs directly. |
| LC-3 Output Contract Enforcement | LLM responses must satisfy strict schema before use. | Validate parser/schema checks and fallback reason paths for invalid JSON/schema outputs. | `app/services/llm_gateway/service.py`; `tests/chaos/test_llm_gateway_runtime_hardening.py` | Invalid outputs are rejected and replaced with deterministic fallback payload. | Unvalidated LLM payload reaches execution or decision surfaces. |

## Runtime Boundaries

| Rule | Description | Detection Method | Enforcement Location | [PASS] | [FAIL CONDITION] |
| --- | --- | --- | --- | --- | --- |
| RB-1 No Archive Imports In Runtime Paths | Runtime scopes must not import archive namespace directly. | Run archive import guard scan and architecture suite checks. | `scripts/architecture_layer_guard.py`; `tests/system/test_architecture_suite.py` | No `import archive` or `from archive` patterns exist in runtime scopes. | Any direct archive import appears in runtime scope files. |
| RB-2 No Direct Provider Calls | Direct provider network call patterns are forbidden outside allowlisted gateway/providers. | Static scan for provider SDK strings and gateway bypass call signatures. | `scripts/architecture_layer_guard.py`; `tests/system/test_architecture_suite.py` | Provider calls are isolated to gateway/providers only. | Any runtime file outside allowlist performs direct provider network/SDK calls. |
| RB-3 No Parallel State Systems | Runtime must not maintain alternate authoritative state stores or duplicate mutation systems. | Scan for file-based runtime state authority and duplicate orchestration paths. | `tests/system/test_architecture_suite.py`; `tests/legacy/test_cqrs_lifecycle_invariants.py` | Read models are derived and mutation authority is singular. | File-backed runtime state authority or duplicate mutation layer appears. |

## Data Integrity

| Rule | Description | Detection Method | Enforcement Location | [PASS] | [FAIL CONDITION] |
| --- | --- | --- | --- | --- | --- |
| DI-1 Append-Only Event Log | Event history cannot be modified in place; only append is allowed. | Validate append-only and duplicate rejection tests in event-store/event-sourcing suites. | `household_os/runtime/event_store.py`; `tests/system/test_event_store_invariants.py`; `tests/system/test_event_sourcing.py` | Append succeeds only for valid events and ordering semantics hold. | Update/delete style event mutation or duplicate overwrite is possible. |
| DI-2 Idempotency Enforced | Duplicate command/event operations must not produce duplicate mutations. | Verify event-router idempotency key checks and command runtime duplicate detection behavior. | `household_os/runtime/event_router.py`; `app/services/commands/runtime.py`; `app/adapters/db/event_log_repository.py` | Duplicate idempotency keys return duplicate/no-op behavior. | Replayed duplicate commands/events can commit additional mutations. |
| DI-3 Projection Rebuildability | Query projections must be reproducible by replaying committed events. | Force replay projection path and compare deterministic checksum/state results. | `app/services/commands/runtime.py`; `household_os/runtime/state_reducer.py`; `tests/system/test_event_sourcing.py`; `tests/legacy/test_cqrs_lifecycle_invariants.py` | Replay from event log reproduces the same projection for identical event history. | Projection depends on hidden mutable state or diverges under replay. |

## Security / Permissions

| Rule | Description | Detection Method | Enforcement Location | [PASS] | [FAIL CONDITION] |
| --- | --- | --- | --- | --- | --- |
| SP-1 Household Scoping Enforced | Non-system actors must be household-scoped before read/write authorization. | Verify authorization gate checks and auth lifecycle tests for household claim binding. | `household_os/security/authorization_gate.py`; `tests/legacy/p1_verification/test_auth_lifecycle.py` | Access is denied when household ownership verification fails. | Cross-household access is possible with mismatched identity context. |
| SP-2 Role-Based Access Control | Actor types drive explicit allow/deny decisions for action classes. | Inspect `authorize_action` semantics and trust boundary tests. | `household_os/security/authorization_gate.py`; `tests/legacy/test_trust_boundary_enforcement.py`; `tests/legacy/test_unified_trust_boundary.py` | Restricted roles cannot execute protected action types. | Restricted role can approve/reject/execute protected action classes. |
| SP-3 No Implicit Inference | Unknown actor types or unverifiable auth context must fail closed. | Verify actor normalization and explicit verification requirements in command and runtime APIs. | `household_os/security/authorization_gate.py`; `app/api/command.py`; `app/api/assistant_runtime.py` | Unverified/unknown actor inputs are rejected. | Runtime auto-promotes malformed actor context to privileged access. |

## Failure Handling

| Rule | Description | Detection Method | Enforcement Location | [PASS] | [FAIL CONDITION] |
| --- | --- | --- | --- | --- | --- |
| FH-1 LLM Fallback To Rules | LLM outage/invalid output must degrade to deterministic rule responses. | Execute routing resilience and gateway hardening tests. | `app/services/routing/intelligence_routing.py`; `app/services/llm_gateway/service.py`; `tests/integration/test_intelligence_routing_resilience.py`; `tests/chaos/test_llm_gateway_runtime_hardening.py` | Failure paths return `RULE_ONLY` or validated fallback payload with explicit reason. | LLM failure propagates unsafe partial payload into execution path. |
| FH-2 Provider Failure Isolation | Provider failures must not break non-provider deterministic runtime behavior. | Validate fallback/degradation metadata and bounded retry/fallback behavior. | `app/adapters/llm/gateway.py`; `app/services/routing/intelligence_routing.py`; `tests/chaos/test_llm_gateway_runtime_hardening.py` | Provider failure is isolated to advisory path and deterministic runtime remains operational. | Provider dependency failure crashes command/read execution surfaces globally. |
| FH-3 Saga Failure Signaling | Partial execution failures must emit explicit saga failure signal and avoid silent partial commits. | Inspect command runtime exception path for `saga.failed` emission and re-raise behavior. | `app/services/commands/runtime.py`; `household_os/runtime/action_pipeline.py`; `tests/legacy/test_trust_boundary_enforcement.py` | On pipeline exception, failure event is emitted and command does not report committed success. | Partial step failure returns success without failure event or compensation signal. |
