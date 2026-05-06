# Family Orchestration Bot

## Governance Freeze Notice

This README is non-authoritative and observational only.
It must not be used as enforcement input by tests, guard scripts, workflows, or runtime logic.
Behavioral enforcement remains in executable governance artifacts (tests and guard scripts) aligned to RFC-001.
Feature Intake Rule (FIR) enforcement is test-only via `tests/governance/test_feature_intake_contract.py`.

## 1. What This System Is

Event-sourced household orchestration system with bounded LLM advisory.

The backend accepts household commands, processes them through a single runtime gateway, commits canonical events, and serves derived projections. LLM output is optional and advisory only.

## 2. Core Architecture

Canonical write/read path:

Command -> Execution Gateway -> Canonical Event Commit -> Projection Replay -> Response

Current implementation anchors:

- API entrypoint assembly: app/main.py
- Canonical command endpoint: app/api/command.py
- Command runtime gateway: app/services/commands/runtime.py
- Canonical event routing: app/services/events/canonical_router_service.py
- Event router envelope/idempotency: household_os/runtime/event_router.py
- Event persistence adapter: app/adapters/db/event_log_repository.py
- Projection replay in runtime: app/services/commands/runtime.py
- Lifecycle replay reducer: household_os/runtime/state_reducer.py

LLM path:

- Intelligence endpoints: app/api/email.py
- Deterministic routing decisions: app/services/routing/intelligence_routing.py
- LLM advisory service facade: app/services/llm_gateway/service.py
- Provider boundary (single choke point): app/adapters/llm/gateway.py

## 3. Key Concepts

### Event Sourcing

Committed events are the authority. Runtime state must be derivable from event history.

### Command vs Event

- Command: an intent to change state (for example, assistant.run, email.ingest).
- Event: immutable fact emitted by runtime processing (for example, command.received, assistant.response_proposed, saga.failed, projection.snapshot).

### Execution Gateway

All write-side command mutation flows through CommandRuntimeService in app/services/commands/runtime.py.

### Risk Engine

Risk/routing behavior currently exists as a distributed concern:

- app/services/routing/intelligence_routing.py
- household_os/core/decision_engine.py

Phase 3 work is consolidating this into a more explicit boundary.

### Saga Execution

Command runtime executes action-pipeline steps and emits saga.failed on partial pipeline failures before surfacing errors.

## 4. Project Structure

Primary active structure:

- app/api: command/intelligence HTTP surfaces
- app/services: command runtime, events, routing, rules, usage, llm gateway
- app/adapters: DB and LLM/provider adapters
- app/core: shared app-layer runtime support
- household_os: lifecycle runtime, security/trust boundaries, reducer/event contracts
- docs/architecture: canonical architecture plan/spec/checklist
- tests: runtime, architecture, and integration regression suites

Compatibility/legacy surface:

- archive/apps/api modules are still loaded dynamically by app/main.py for compatibility routes.

## 5. How to Run

### Install

1. Create virtual environment:

   python -m venv .venv

2. Activate (PowerShell):

   .\.venv\Scripts\Activate.ps1

3. Install dependencies:

   pip install -r requirements.txt

### Run Server

Use canonical ASGI app:

uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

### Run Tests

Full suite:

python -m pytest

Architecture guard-focused suites:

python -m pytest tests/test_architecture_suite.py -q
python -m pytest tests/test_integration_architecture_guard.py -q

## 6. Invariants (Non-Negotiable)

- No direct state mutation outside canonical command/runtime/event boundaries.
- No provider calls outside the gateway/providers boundary.
- No LLM execution authority (LLM is advisory only).
- Event log is authoritative; projections are derived read models.

Enforcement sources:

- scripts/architecture_layer_guard.py
- tests/test_architecture_suite.py
- tests/test_integration_architecture_guard.py

## 7. Current Status

- Runtime architecture is stabilized around command-gateway and canonical event logging.
- Architecture constraints are codified in static guard checks and regression tests.
- Phase 3 consolidation is in progress (risk/saga/projection/auth boundary normalization).
- Cleanup and alignment are active to reduce dead structure and keep documentation matched to executable behavior.
