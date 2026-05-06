# NAME

- prompt_id: rfc_architecture_audit_runner
- prompt_title: RFC Architecture Audit Runner
- category: audit
- version: 1.0.0

# PURPOSE

Run system-wide RFC-001 compliance analysis, test suite classification validation, and redundancy and drift detection.

# RFC-001 BINDING

- RFC-001 Section 1: Canonical System Architecture
- RFC-001 Section 2: Canonical Runtime Flow
- RFC-001 Section 3.1: Event Log Is Authoritative
- RFC-001 Section 3.2: Execution Gateway Is Mandatory
- RFC-001 Section 3.3: Rules and Risk Control Execution
- RFC-001 Section 3.4: LLM Is Advisory Only
- RFC-001 Section 3.6: SSE and Watermark Correctness
- RFC-001 Section 6: Enforcement and Compliance
- RFC-001 Section 7: Drift Resolution Policy

# EXECUTION PHASES

1. Verify
- Validate audit scope and required RFC references.

2. Assess
- Evaluate architecture conformance and CI classification consistency.

3. Implement
- No-op by contract. Code modifications are forbidden.

4. Test
- Run non-mutating checks and classification validations.

5. Verify
- Emit final structured compliance JSON.

# INPUTS

- request_id: string, required
- audit_scope: array<string>, required
- ci_matrix: object, required
- include_redundancy_scan: boolean, required
- include_drift_scan: boolean, required

# OUTPUT JSON SCHEMA (STRICT)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "RFC Architecture Audit Output",
  "type": "object",
  "required": [
    "prompt_id",
    "category",
    "execution_phases",
    "checks",
    "drift_findings",
    "redundancy_findings",
    "ci_classification_validation",
    "status",
    "next_step"
  ],
  "properties": {
    "prompt_id": {"type": "string", "const": "rfc_architecture_audit_runner"},
    "category": {"type": "string", "const": "audit"},
    "execution_phases": {
      "type": "object",
      "required": ["verify", "assess", "implement", "test", "verify_post"],
      "properties": {
        "verify": {"type": "object"},
        "assess": {"type": "object"},
        "implement": {
          "type": "object",
          "required": ["no_op"],
          "properties": {"no_op": {"type": "boolean", "const": true}},
          "additionalProperties": false
        },
        "test": {"type": "object"},
        "verify_post": {"type": "object"}
      },
      "additionalProperties": false
    },
    "checks": {
      "type": "object",
      "required": ["gateway", "event_log", "rules", "risk", "fsm", "llm", "sse"],
      "properties": {
        "gateway": {"type": "boolean"},
        "event_log": {"type": "boolean"},
        "rules": {"type": "boolean"},
        "risk": {"type": "boolean"},
        "fsm": {"type": "boolean"},
        "llm": {"type": "boolean"},
        "sse": {"type": "boolean"}
      },
      "additionalProperties": false
    },
    "drift_findings": {"type": "array", "items": {"type": "string"}},
    "redundancy_findings": {"type": "array", "items": {"type": "string"}},
    "ci_classification_validation": {
      "type": "object",
      "required": ["valid", "mismatches"],
      "properties": {
        "valid": {"type": "boolean"},
        "mismatches": {"type": "array", "items": {"type": "string"}}
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

- NO code modification allowed.
- Output structured JSON only.
- No runtime side effects.
- No inferred component assumptions without evidence.
- No category reclassification without explicit mismatch evidence.

# FAILURE MODES

- MUTATION_ATTEMPT_DETECTED
- SCHEMA_OUTPUT_VIOLATION
- RFC_INVARIANT_FAILURE
- CI_CLASSIFICATION_MISMATCH
- DRIFT_DETECTED

# CI MAPPING

- tier: migration
- blocking: false

# ESCALATION RULES

- If mutation is attempted, hard fail immediately.
- If RFC invariant checks fail, return fail with explicit violated checks.
- If classification mismatches exist, return fail and include mismatch list for CI routing correction.
