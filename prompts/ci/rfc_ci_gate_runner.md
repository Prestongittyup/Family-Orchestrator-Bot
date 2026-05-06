# NAME

- prompt_id: rfc_ci_gate_runner
- prompt_title: RFC CI Gate Runner
- category: ci
- version: 1.0.0

# PURPOSE

Execute deterministic, blocking validation of RFC-001 invariants for merge gating.

# RFC-001 BINDING

- RFC-001 Section 1: Canonical System Architecture
- RFC-001 Section 2: Canonical Runtime Flow
- RFC-001 Section 3.1: Event Log Is Authoritative
- RFC-001 Section 3.2: Execution Gateway Is Mandatory
- RFC-001 Section 3.3: Rules and Risk Control Execution
- RFC-001 Section 3.5: Saga, Idempotency, and Concurrency
- RFC-001 Section 3.6: SSE and Watermark Correctness
- RFC-001 Section 6: Enforcement and Compliance

# EXECUTION PHASES

1. Verify
- Confirm required invariant checks are configured.

2. Assess
- Resolve deterministic check set and expected outcomes.

3. Implement
- No-op by contract. Validation only.

4. Test
- Execute deterministic checks for:
  - gateway exclusivity
  - event log authority
  - FSM mutation authority
  - idempotency correctness
  - SSE replay correctness

5. Verify
- Emit blocking pass or fail output.

# INPUTS

- request_id: string, required
- target_commit: string, required
- invariant_checks: array<string>, required
- test_artifacts: object, optional

# OUTPUT JSON SCHEMA (STRICT)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "RFC CI Gate Output",
  "type": "object",
  "required": [
    "prompt_id",
    "category",
    "ci_mapping",
    "blocking",
    "checks",
    "status",
    "exit_code",
    "next_step"
  ],
  "properties": {
    "prompt_id": {"type": "string", "const": "rfc_ci_gate_runner"},
    "category": {"type": "string", "const": "ci"},
    "ci_mapping": {"type": "string", "const": "ci_gate"},
    "blocking": {"type": "boolean", "const": true},
    "checks": {
      "type": "object",
      "required": [
        "gateway_exclusivity",
        "event_log_authority",
        "fsm_mutation_authority",
        "idempotency_correctness",
        "sse_replay_correctness"
      ],
      "properties": {
        "gateway_exclusivity": {"type": "boolean"},
        "event_log_authority": {"type": "boolean"},
        "fsm_mutation_authority": {"type": "boolean"},
        "idempotency_correctness": {"type": "boolean"},
        "sse_replay_correctness": {"type": "boolean"}
      },
      "additionalProperties": false
    },
    "violations": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["check", "details"],
        "properties": {
          "check": {"type": "string"},
          "details": {"type": "string"}
        },
        "additionalProperties": false
      }
    },
    "status": {"type": "string", "enum": ["pass", "fail"]},
    "exit_code": {"type": "integer", "enum": [0, 1]},
    "next_step": {"type": "string", "minLength": 1}
  },
  "additionalProperties": false
}
```

# CONSTRAINTS

- Runs in ci_gate only.
- Blocking behavior only.
- Deterministic validation only.
- No code modification allowed.
- No suppression of invariant failures.

# FAILURE MODES

- GATEWAY_EXCLUSIVITY_FAILURE
- EVENT_LOG_AUTHORITY_FAILURE
- FSM_AUTHORITY_FAILURE
- IDEMPOTENCY_FAILURE
- SSE_REPLAY_FAILURE
- NON_DETERMINISTIC_OUTPUT

# CI MAPPING

- tier: ci_gate
- blocking: true

# ESCALATION RULES

- Any failed invariant sets status to fail and exit_code to 1.
- If required check output is missing, treat as fail.
- If deterministic reproducibility cannot be proven, treat as fail.
