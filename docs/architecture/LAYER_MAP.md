# Family Orchestration Bot Layer Map

This repository currently uses 7 operational layers, plus one compatibility facade.

## Purpose

The system turns family and household intent into deterministic, policy-checked actions.
It keeps user-facing conversation flow separate from intent validation, policy decisions,
integration state assembly, and runtime execution.

## Layer Count

- Operational layers: 7
- Compatibility facade: 1 (not counted as an operational layer)

## Canonical Layers

1. API and Product Surface
- Folders:
  - apps/api/endpoints
  - apps/assistant_core
- Role:
  - HTTP entrypoints and API contracts
  - request routing and response shaping

2. Conversation Orchestration Layer (COL)
- Folder:
  - apps/api/conversation_orchestration
- Role:
  - multi-turn conversation state
  - clarification loops and pipeline progression
  - structured handoff generation

3. Intent Contract Layer (ICL)
- Folder:
  - apps/api/intent_contract
- Role:
  - intent classification
  - schema validation
  - deterministic action planning

4. Policy and Safety Layer
- Folder:
  - apps/api/policy_engine
- Role:
  - allow/confirm/block policy evaluation
  - safety, compliance, and action gating

5. Integration Core Layer
- Folder:
  - apps/api/integration_core
- Role:
  - provider state assembly and event windowing
  - orchestration of integration data into household state
  - canonical integration decision engine for this boundary

6. Execution Runtime Layer
- Folders:
  - household_os/core
  - household_os/runtime
  - apps/api/hpal
  - apps/api/services
- Role:
  - domain action execution
  - lifecycle transitions
  - runtime orchestration and service coordination

7. Cross-Cutting Governance and Observability
- Folders:
  - apps/api/core
  - apps/api/observability
  - ci
  - tests
  - scripts
- Role:
  - architectural boundary enforcement
  - lifecycle mutation safety
  - telemetry, diagnostics, and regression guards

## Compatibility Facade (Intentional, Not Business Logic)

- Folder:
  - integration_core
- Role:
  - thin re-export shim for import compatibility
- Rule:
  - keep facade files as one-line re-exports only
  - no business logic in facade package

## Redundancy Policy

To avoid accidental duplication:

- Canonical integration pipeline logic lives under apps/api/integration_core.
- Compatibility facade under integration_core must stay logic-free.
- New orchestrator or decision-engine implementations must be introduced only within the
  bounded context that owns them, with explicit naming and tests.
- CI gates should fail on accidental duplicate pipeline implementations.
