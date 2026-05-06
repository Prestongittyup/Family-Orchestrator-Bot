# RFC Prompt Standard

This is the canonical contract template for all files under prompts/core, prompts/execution, prompts/audit, and prompts/ci.

No prompt is valid unless every section in this template is present and completed.

## NAME

- prompt_id: <snake_case_unique_id>
- prompt_title: <human_readable_title>
- category: <core|execution|audit|ci|reliability>
- version: <semver>

## RFC-001 BINDING

The prompt MUST bind explicitly to RFC-001 with section references.

Required minimum references:

- RFC-001 Section 1: Canonical System Architecture
- RFC-001 Section 2: Canonical Runtime Flow
- RFC-001 Section 3.1: Event Log Is Authoritative
- RFC-001 Section 3.2: Execution Gateway Is Mandatory
- RFC-001 Section 3.3: Rules and Risk Control Execution
- RFC-001 Section 3.4: LLM Is Advisory Only
- RFC-001 Section 3.5: Saga, Idempotency, and Concurrency
- RFC-001 Section 3.6: SSE and Watermark Correctness
- RFC-001 Section 5: Forbidden Anti-Patterns

## EXECUTION PHASES

Every prompt MUST define and implement this ordered lifecycle:

1. Verify
2. Assess
3. Implement
4. Test
5. Verify

For read-only prompts, Implement MUST be explicitly declared as no-op.

## POST-REFACTOR CANONICAL VERIFICATION GATE (MANDATORY)

Before any Step 6 / completion verification artifact is executed, prompts MUST enforce a fresh static repository scan and hard-block completion on violations.

Mandatory static scan targets:

- `derive_*health` (function definitions; only canonical `core/health/system_health.py::derive_system_health` is allowed)
- `_policy_determinism_from_projection`
- `_saga_completion_validity`
- `_derive_projection_health`
- inline health aggregation logic (pattern-based detection)

Hard block semantics:

- If any forbidden occurrence is detected, Step 6 MUST NOT execute.
- System state MUST remain `MIGRATION`.
- Workflow MUST re-enter `Implement -> Test` cycle.

Mandatory re-verification before completion:

- Re-run Step 1 (static scan only).
- Re-run Step 2 (RFC alignment check).
- Compare re-run outputs against prior Step 1/2 artifacts.

Ephemeral verification state requirement:

- Step 1 / Step 2 / Step 6 outputs MUST include `execution_id`, `repo_state_hash`, and `scan_timestamp`.
- `execution_id` MUST be unique per execution cycle.
- `repo_state_hash` MUST bind verification outputs to the current repository state snapshot.
- Cached or reused validation artifacts from prior runs are forbidden as Step 6 inputs.

Execution isolation requirement:

- Step 1, Step 2, and Step 6 MUST execute in isolated clean runtime contexts.
- Shared in-memory objects, module-level caches, memoization artifacts, reused AST/module graphs, and singleton reuse across phases are forbidden.
- Separate process execution per phase is the required isolation mode unless full teardown + deterministic reinitialization is explicitly proven.

Consistency rule:

- If Step 1 or Step 2 differs from prior execution, prior Step 5/6 artifacts are invalid.
- Completion remains blocked until a clean revalidation cycle converges.
- If repository state changes during the verification cycle, Step 6 is hard-blocked and the flow re-enters `Implement -> Test -> Verify`.

Completion semantics:

- Step 6 is a verification artifact, not a confirmation artifact.
- Step 6 may execute only when static scan has zero violations and RFC alignment is true.
- All compliant outputs MUST emit `execution_isolation_enforced`, `no_shared_runtime_state`, `no_cross_phase_memory`, and `clean_execution_required` as `true`.

## INPUTS

Define all inputs as typed fields with required flags.

Example contract:

- request_id: string, required
- scope: string, required
- bounded_slice: array<string>, required
- constraints: array<string>, required
- ci_context: object, optional

All undeclared inputs are invalid.

## OUTPUT JSON SCHEMA (STRICT)

Each prompt MUST publish a strict JSON schema and emit output conforming to it.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "RFC Prompt Output Contract",
  "type": "object",
  "required": [
    "prompt_id",
    "category",
    "rfc_binding",
    "execution_phases",
    "ci_mapping",
    "status",
    "next_step"
  ],
  "properties": {
    "prompt_id": {"type": "string", "minLength": 1},
    "category": {"type": "string", "enum": ["core", "execution", "audit", "ci", "reliability"]},
    "rfc_binding": {
      "type": "array",
      "items": {"type": "string", "minLength": 1},
      "minItems": 1
    },
    "execution_phases": {
      "type": "object",
      "required": ["verify", "assess", "implement", "test", "verify_post"],
      "properties": {
        "verify": {"type": "object"},
        "assess": {"type": "object"},
        "implement": {"type": "object"},
        "test": {"type": "object"},
        "verify_post": {"type": "object"}
      },
      "additionalProperties": false
    },
    "ci_mapping": {
      "type": "object",
      "required": ["tier", "blocking"],
      "properties": {
        "tier": {"type": "string", "enum": ["ci_gate", "migration", "reliability"]},
        "blocking": {"type": "boolean"}
      },
      "additionalProperties": false
    },
    "status": {"type": "string", "enum": ["pass", "fail", "blocked"]},
    "next_step": {"type": "string", "minLength": 1}
  },
  "additionalProperties": false
}
```

## CONSTRAINTS

Each prompt MUST declare what it cannot modify.

Mandatory baseline constraints:

- No bypass of Execution Gateway
- No direct state mutation outside canonical event append path
- No alternate write authority outside event log
- No mutation authority granted to LLM output
- No undocumented side effects

## FAILURE MODES

Each prompt MUST define deterministic failure codes.

Minimum set:

- RFC_BINDING_MISSING
- PHASE_ORDER_VIOLATION
- SCHEMA_VALIDATION_FAILURE
- GATEWAY_BYPASS_DETECTED
- EVENT_AUTHORITY_VIOLATION
- FSM_AUTHORITY_VIOLATION
- LLM_AUTHORITY_VIOLATION
- CI_CLASSIFICATION_MISMATCH
- POST_REFACTOR_CANONICAL_GATE_FAILED
- POST_REFACTOR_REVERIFICATION_FAILED
- EPHEMERAL_VERIFICATION_STATE_FAILED
- STALE_VERIFICATION_ARTIFACT_DETECTED
- EXECUTION_ISOLATION_FAILED
- SHARED_RUNTIME_STATE_DETECTED
- NON_EXPLICIT_STEP6_INPUT_DETECTED

## CI MAPPING

Each prompt MUST declare one CI classification:

- ci_gate: blocking invariants only
- migration: implementation and audit flows
- reliability: stress and chaos validation

## ESCALATION RULES

Each prompt MUST define escalation behavior:

- If RFC invariants are violated, emit fail status and block completion.
- If scope cannot be completed safely, emit blocked status with precise blocker.
- If prompt category and behavior mismatch, reject execution.

## PROMPT REJECTION RULES

Reject prompt execution immediately when any condition is true:

- Missing RFC-001 section references
- Missing strict input or output schema
- Missing execution phase definition
- Missing CI classification
- Bypass of gateway-first flow
- Direct state mutation assumptions
- LLM treated as authoritative executor
