# Prompt Classification System

This file defines canonical behavior and CI routing for prompt categories.

## Classification Matrix

| Type | Behavior | CI Mapping |
| --- | --- | --- |
| execution | modifies system | migration |
| audit | read-only analysis | migration (non-blocking) |
| ci_gate | deterministic validation | ci_gate (blocking) |
| reliability | stress / chaos validation | scheduled only |

## Registry Rules

- Every prompt belongs to exactly one category.
- No ad-hoc prompt files are allowed outside prompts/core, prompts/execution, prompts/audit, prompts/ci, prompts/templates.
- Every prompt must define RFC-001 binding, execution phases, typed inputs, strict output JSON schema, constraints, failure modes, CI mapping, and escalation rules.

## RFC-001 Binding Rule

Every prompt contract MUST include and enforce:

- explicit RFC-001 section references
- gateway-first execution constraint
- event log as source of truth
- FSM as only mutation authority

## Prompt Rejection Conditions

Reject prompt execution when any condition is true:

- prompt bypasses gateway flow
- prompt assumes direct state mutation
- prompt treats LLM as authoritative executor
- prompt omits RFC references
- prompt omits strict output schema
- prompt omits CI classification

## CI Integration Contract

- execution prompts route to migration tier and are non-blocking by default
- audit prompts route to migration tier and are non-blocking
- ci_gate prompts route to ci_gate tier and are blocking
- reliability prompts run on scheduled reliability pipelines only
