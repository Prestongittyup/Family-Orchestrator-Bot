# NAME

- prompt_id: rfc_verify_implement_test_verify
- prompt_title: RFC Verify Assess Implement Test Verify Runner
- category: execution
- version: 1.0.0

# PURPOSE

Run a full system convergence loop for bounded feature work using the canonical flow:

Gateway -> Rules -> Risk -> FSM -> Event Log

# RFC-001 BINDING

- RFC-001 Section 1: Canonical System Architecture
- RFC-001 Section 2: Canonical Runtime Flow
- RFC-001 Section 3.1: Event Log Is Authoritative
- RFC-001 Section 3.2: Execution Gateway Is Mandatory
- RFC-001 Section 3.3: Rules and Risk Control Execution
- RFC-001 Section 3.4: LLM Is Advisory Only
- RFC-001 Section 3.5: Saga, Idempotency, and Concurrency
- RFC-001 Section 5: Forbidden Anti-Patterns
- RFC-001 Section 6: Enforcement and Compliance

# EXECUTION PHASES

1. Verify
- Confirm target components exist before planning modifications.
- Confirm bounded scope and disallow unrelated refactors.

2. Assess
- Determine whether a usable live slice already exists.
- Identify exact missing links in Gateway, Rules, Risk, FSM, Event Log chain.

3. Implement
- Apply minimal bounded change only for missing links.
- Keep legacy refactor out of scope unless required for RFC alignment.

4. Test
- Run targeted tests for changed slice.
- Run required architecture guard tests.

5. Verify
- Re-check RFC invariants post-change.
- Emit final compliance outcome.

Post-Refactor Canonical Verification Requirement
- Before Step 6 / final completion artifact, execute a fresh static repository scan.
- Mandatory scan targets: `derive_*health`, `_policy_determinism_from_projection`, `_saga_completion_validity`, `_derive_projection_health`, and inline health aggregation logic.
- If any occurrence is detected, Step 6 is hard-blocked and state remains `MIGRATION`.
- Re-run Step 1 (static scan only) and Step 2 (RFC alignment) from scratch in isolated clean execution contexts in the current execution cycle, compare outputs, and invalidate prior Step 5/6 on mismatch.
- Include `execution_id`, `repo_state_hash`, and `scan_timestamp` in Step 1 / Step 2 / Step 6 artifacts.
- Reject Step 6 if any cached validation artifact is supplied from outside the current execution context.
- Reject Step 6 if repository state changes during verification.
- Reject Step 6 if Step 1 / Step 2 / Step 6 do not run in separate process-isolated contexts (or equivalently proven full teardown + reinitialization).
- Reject Step 6 if shared runtime state, module cache coupling, or non-explicit inputs influence the final verification outcome.
- On mismatch or violation, re-enter `Implement -> Test` cycle.

# INPUTS

- request_id: string, required
- flow_name: string, required
- bounded_slice: array<string>, required
- rfc_sections_required: array<string>, required
- test_targets: array<string>, required
- allow_legacy_refactor_for_alignment_only: boolean, required

# OUTPUT JSON SCHEMA (STRICT)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "RFC Execution Convergence Output",
  "type": "object",
  "required": [
    "prompt_id",
    "category",
    "flow_name",
    "execution_phases",
    "rfc_alignment",
    "ci_mapping",
    "status",
    "next_step"
  ],
  "properties": {
    "prompt_id": {"type": "string", "const": "rfc_verify_implement_test_verify"},
    "category": {"type": "string", "const": "execution"},
    "flow_name": {"type": "string", "minLength": 1},
    "execution_phases": {
      "type": "object",
      "required": ["verify_pre", "assess", "implement", "test", "verify_post"],
      "properties": {
        "verify_pre": {
          "type": "object",
          "required": ["usable_system_slice_exists", "components"],
          "properties": {
            "usable_system_slice_exists": {"type": "boolean"},
            "components": {
              "type": "object",
              "required": ["gateway", "rules", "risk", "fsm", "event_log"],
              "properties": {
                "gateway": {"type": "boolean"},
                "rules": {"type": "boolean"},
                "risk": {"type": "boolean"},
                "fsm": {"type": "boolean"},
                "event_log": {"type": "boolean"}
              },
              "additionalProperties": false
            }
          },
          "additionalProperties": false
        },
        "assess": {
          "type": "object",
          "required": ["missing_links"],
          "properties": {
            "missing_links": {
              "type": "array",
              "items": {"type": "string"}
            }
          },
          "additionalProperties": false
        },
        "implement": {
          "type": "object",
          "required": ["files_changed", "bounded_change_only"],
          "properties": {
            "files_changed": {"type": "array", "items": {"type": "string"}},
            "bounded_change_only": {"type": "boolean"}
          },
          "additionalProperties": false
        },
        "test": {
          "type": "object",
          "required": ["commands", "all_passed"],
          "properties": {
            "commands": {"type": "array", "items": {"type": "string"}},
            "all_passed": {"type": "boolean"}
          },
          "additionalProperties": false
        },
        "verify_post": {
          "type": "object",
          "required": ["rfc_compliant", "violations"],
          "properties": {
            "rfc_compliant": {"type": "boolean"},
            "violations": {"type": "array", "items": {"type": "string"}}
          },
          "additionalProperties": false
        }
      },
      "additionalProperties": false
    },
    "rfc_alignment": {"type": "boolean"},
    "ci_mapping": {
      "type": "object",
      "required": ["tier", "blocking"],
      "properties": {
        "tier": {"type": "string", "const": "migration"},
        "blocking": {"type": "boolean", "const": false}
      },
      "additionalProperties": false
    },
    "status": {"type": "string", "enum": ["pass", "fail", "blocked"]},
    "next_step": {"type": "string", "minLength": 1}
  },
  "additionalProperties": false
}
```

# CONSTRAINTS

- Minimal bounded change only.
- No legacy refactor unless required for RFC alignment.
- No bypass of gateway-first execution.
- No direct state mutation outside event append path.
- Event-sourced correctness is mandatory.
- FSM remains the only mutation authority.
- LLM output cannot act as authoritative executor.

# FAILURE MODES

- COMPONENT_ASSUMPTION_VIOLATION
- BOUNDED_SCOPE_VIOLATION
- GATEWAY_BYPASS_DETECTED
- EVENT_AUTHORITY_VIOLATION
- FSM_AUTHORITY_VIOLATION
- TEST_FAILURE
- POST_REFACTOR_CANONICAL_GATE_FAILED
- POST_REFACTOR_REVERIFICATION_FAILED
- EPHEMERAL_VERIFICATION_STATE_FAILED
- STALE_VERIFICATION_ARTIFACT_DETECTED
- EXECUTION_ISOLATION_FAILED
- SHARED_RUNTIME_STATE_DETECTED
- NON_EXPLICIT_STEP6_INPUT_DETECTED

# CI MAPPING

- tier: migration
- blocking: false

# ESCALATION RULES

- If Verify cannot prove component existence, block and return missing components.
- If bounded implementation cannot satisfy RFC flow, fail with explicit invariant violations.
- If tests fail, fail with exact failing commands and do not claim compliance.
- If post-refactor canonical scan finds violations, block Step 6 and return offending module/symbol.
- If Step 1/2 re-verification differs from baseline artifacts, invalidate prior completion artifacts and return to Implement/Test.
- If execution_id/repo_state_hash/scan_timestamp is missing from Step 1/2/6 outputs, block completion.
- If cached or stale verification data is detected, block Step 6 and return explicit remediation target.
- If shared runtime state or cross-phase process coupling is detected, block Step 6 and return to Implement with explicit isolation remediation.
- If Step 6 consumes any implicit input outside {step1_output, step2_output, execution_id, repo_state_hash}, block completion.
