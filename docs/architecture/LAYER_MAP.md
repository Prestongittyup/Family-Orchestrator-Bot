# Layer Map v2

## Authority

RFC-001 is the immutable architecture source of truth for this repository.

- Root contract: docs/architecture/RFC-001.md
- If this document conflicts with RFC-001, RFC-001 prevails.
- This document is architecture documentation only and does not introduce runtime behavior beyond RFC-001.

## Canonical Layer Map v2

### 1. API & Product Surface Layer

Path:

- app
- app/main.py
- app/schemas/*
- app/api/*
- apps/api/endpoints
- apps/assistant_core

Responsibilities:

- HTTP entrypoints
- request/response shaping
- input validation (shape only)
- streaming interfaces

Forbidden:

- execution decisions
- policy evaluation
- event mutation
- orchestration logic

### 2. Conversation Orchestration Layer (COL)

Path:

- apps/api/conversation_orchestration

Responsibilities:

- multi-turn conversation state
- clarification loops
- intent refinement across turns
- pipeline progression signals

Forbidden:

- execution logic
- policy evaluation
- state mutation
- event emission

### 3. Intent Contract Layer (ICL)

Path:

- apps/api/intent_contract

Responsibilities:

- intent classification
- schema validation
- deterministic intent decomposition
- structured action planning

Forbidden:

- execution
- policy decisions
- runtime state changes

### 4. Policy & Safety Layer

Path:

- app/services/policy_engine
- app/services/risk_engine
- app/services/rules_engine
- household_os/security/*

Responsibilities:

- allow / block / confirm decisions
- safety and compliance evaluation
- execution gating decisions

Forbidden:

- execution
- workflow orchestration
- event emission

### 5. Integration Core Layer

Path:

- app/adapters/*
- app/services/llm_gateway
- app/services/provider_sync
- app/services/integration_core

Responsibilities:

- external system normalization
- provider event ingestion
- event windowing and formatting

Forbidden:

- triggering execution
- modifying event log directly
- overriding policy decisions

### 6. Execution Runtime Layer (Single Mutation Surface)

Path:

- household_os/runtime
- app/services/execution_gateway
- app/services/saga
- app/services/commands
- app/services/events
- app/services/runtime
- app/services/agents

Responsibilities:

- command execution
- saga orchestration
- event emission ordering
- idempotency handling
- compensation logic
- concurrency control

Hard rule:

- ONLY layer allowed to mutate state or emit canonical events.
- Execution Gateway is the single canonical mutation surface and belongs to Layer 6 only.

Forbidden:

- policy decisions
- intent classification
- API response shaping logic

### 7. Cross-Cutting Governance Layer

Path:

- app/services
- app/services/cache
- app/services/evaluation
- app/services/intent_resolution
- app/services/routing
- app/services/usage
- app/services/observability
- tests/*
- scripts/*
- ci/*

Responsibilities:

- architecture validation
- drift detection
- replay validation
- contract enforcement
- observability

Forbidden:

- runtime execution influence
- state mutation
- business logic

IMPORTANT:

- Tests are enforcement tools, not an architectural layer.

## Execution Authority Rule

Only the Execution Runtime Layer (Layer 6) may:

- mutate system state
- emit canonical events
- execute sagas
- perform side effects

All other layers are strictly non-mutating.

## RFC-001 Runtime Flow Alignment

All write-capable runtime behavior must follow the canonical RFC-001 flow:

User Request -> Command Validation -> Execution Gateway -> Rules Engine -> LLM Advisory (optional) -> Risk Engine -> Decision (auto/approve/block) -> Saga Execution -> Event Commit -> Projection Update -> Response
