# Execution Runtime Directive (RFC-001 Aligned)

## Authority

RFC-001 is the sole source of truth for system behavior and execution boundaries.

## System Purpose

- This system is an event-sourced household orchestration backend.
- All operations are command-driven and gateway-controlled.
- State is derived from the canonical event log.

## Canonical Execution Flow

User Request
→ Command Validation
→ Execution Gateway
→ Rules Engine
→ LLM Advisory (optional)
→ Risk Engine
→ Decision (auto/approve/block)
→ Saga Execution
→ Event Commit
→ Projection Update
→ Response

## Allowed System Behavior

- Event-driven architecture is mandatory.
- Execution Gateway is the single mutation surface.
- Saga orchestration is required for multi-step operations.
- Rules engine is deterministic and side-effect free.
- Risk engine controls execution gating.
- LLM is advisory only.

## Storage Model

- Event log is the source of truth.
- SQLite is an implementation detail of the event persistence layer only.
- No system design constraints are tied to database choice.

## Module Constraints (RFC-001 Aligned)

- execution_gateway: ONLY mutation surface.
- rules_engine: deterministic logic only.
- risk_engine: decision gating only.
- saga: multi-step orchestration.
- projections: read models only.
- llm_gateway: advisory only.

## Forbidden Behaviors

- bypassing execution gateway.
- direct state mutation outside event log.
- treating projections as source of truth.
- LLM-driven execution.
- bypassing saga for multi-step workflows.
- non-event-sourced state writes.

## First Operational Requirement (Updated)

System must support:

- POST /command
- Valid command routed through Execution Gateway
- Event emitted to canonical event log
- Projection updated deterministically
- Response returned from projection