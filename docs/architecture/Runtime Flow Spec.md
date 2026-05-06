# Runtime Flow Spec

This specification is a derivative runtime mapping of the immutable RFC contract in `docs/architecture/RFC-001.md`.

- Source of truth: `docs/architecture/RFC-001.md`
- This document must not override RFC-001.
- If ambiguity exists, implementation and tests must defer to RFC-001.

This specification defines canonical runtime behavior for command execution and advisory intelligence in the current repository. It is strict and enforceable.

## 1. Single Command Flow (Strict Order)

Required order:

User Request -> Command Validation -> Execution Gateway -> Rules Engine -> LLM Advisory (optional) -> Risk Engine -> Decision (auto / approve / block) -> Saga Execution -> Event Commit -> Projection Update -> Response

Current code mapping for strict order:

| Step | Stage | Current Implementation |
| --- | --- | --- |
| 1 | User Request | `app/api/command.py`, `app/api/assistant_runtime.py`, `app/api/email.py` |
| 2 | Command Validation | Pydantic request validation and actor normalization before runtime invocation |
| 3 | Execution Gateway | `app/services/commands/runtime.py::CommandRuntimeService.handle_command` |
| 4 | Rules Engine | `app/services/rules_engine/engine.py` for deterministic routing payloads; `household_os/core/decision_engine.py` for deterministic planning heuristics |
| 5 | LLM Advisory (optional) | `app/services/llm_gateway/service.py` through `app/adapters/llm/gateway.py`; if unavailable, deterministic path continues |
| 6 | Risk Engine | Distributed risk signals in `app/services/routing/intelligence_routing.py` and `household_os/core/decision_engine.py` |
| 7 | Decision | Routing decision states `RULE_ONLY`, `NEEDS_LLM`, `BLOCKED`; execution remains approval-gated where required |
| 8 | Saga Execution | Current partial saga behavior in `CommandRuntimeService._apply_action_pipeline`; emits `saga.failed` on exception |
| 9 | Event Commit | `CanonicalRouterService.route` -> `CanonicalEventRouter.route` -> `EventLogRepository.append_event` |
| 10 | Projection Update | `CommandRuntimeService.get_projection(..., force_replay=True)` and `_replay_projection`; emits `projection.snapshot` |
| 11 | Response | Runtime returns canonical result/effects/projection payload |

No deviations allowed: no mutation before execution gateway, no provider call outside gateway, no projection-as-truth writes. These constraints are mandatory because RFC-001 compliance is an invariant.

Read-model query endpoints (`/tasks`, `/schedule`, `/reminders`, `/notifications`) may call `get_command_runtime_service().get_projection(...)` for projection-derived reads. This access is read-only, does not invoke `handle_command`, and is compliant with the single mutation surface defined by RFC-001 and LAYER_MAP.

## 2. Sequence Diagram (Text)

```text
Client
  -> app/api/{command|assistant_runtime|email} endpoint
  -> request schema validation + actor extraction
  -> CommandRuntimeService.handle_command
  -> emit command.received (canonical event)
  -> rules evaluation (deterministic)
  -> optional LLM advisory via LLMGatewayService -> LLMGateway
  -> routing/risk decision (RULE_ONLY | NEEDS_LLM | BLOCKED)
  -> action pipeline step (approve/reject/execute/ingest path)
  -> on step failure: emit saga.failed and raise
  -> CanonicalRouterService.route (persist through EventLogRepository)
  -> projection replay from event log rows
  -> emit projection.snapshot
  -> return response payload to client
```

## 3. Data Contracts

### Command Contract

Canonical app command request (`app/api/command.py`):

```json
{
  "command_type": "string (required, non-empty)",
  "household_id": "string (required, non-empty)",
  "payload": "object (required, default {})",
  "idempotency_key": "string|null"
}
```

Validation requirements: `command_type` and `household_id` are non-empty; actor context resolves to canonical actor fields before mutation; unsupported command types fail through error path.

### Event Contract

Canonical envelope (`household_os/runtime/event_router.py`):

```json
{
  "event_id": "string (required)",
  "event_type": "string (required)",
  "user_id": "string|null",
  "household_id": "string (required)",
  "timestamp": "datetime (required)",
  "source": "string (required)",
  "payload": "object (required)",
  "version": "integer >= 1",
  "severity": "string|null",
  "idempotency_key": "string|null",
  "actor_type": "string|null",
  "watermark": "integer|null",
  "signature": "string|null"
}
```

Validation requirements: envelope fields validate before persist; duplicate idempotency keys return duplicate status and skip append.

### LLM Contracts

| Contract | Requirement |
| --- | --- |
| Input context allowlist | Prompt context is service-constructed and bounded to domain fields (email metadata, pantry normalized items, schedule title/details). |
| Input context denylist | Raw provider credentials, direct mutation instructions, and unbounded internal runtime objects are not permitted. |
| Output schema (email) | `priority`, `needs_attention`, `actions`, `state_summary`, `reason` |
| Output schema (pantry) | `state_summary`, `suggested_meals`, `reason` |
| Output schema (schedule) | `state_summary`, `suggestions`, `reason` |
| Output enforcement | Invalid JSON/schema is rejected and deterministic fallback payload is used. |

## 4. Failure Modes

| Failure Mode | Trigger | Required Behavior |
| --- | --- | --- |
| LLM failure | Provider unavailable, timeout, invalid JSON, schema mismatch | Advisory path degrades to deterministic output and routing reason is explicit. |
| DB failure | Event append/repository failure during canonical commit | Command fails fast via exception; no committed success response is emitted. |
| Provider failure | LLM/provider sync throws timeout/network/service error | Advisory path degrades; deterministic route remains operational. |
| Partial saga failure | Exception during action pipeline handling | Emit `saga.failed`, re-raise exception, and do not report committed success. |

## 5. Concurrency + Idempotency

| Area | Current Behavior |
| --- | --- |
| Command dedupe key | Semantic fingerprint from `{command_type, household_id, actor, payload}`; synthesized key format is `command:{household_id}:{command_type}:{semantic_fingerprint}` when caller key absent. |
| Duplicate shortcut carveout | Duplicate short-circuit is bypassed for `assistant.run`, `assistant.query`, `assistant.approve`, `assistant.reject`. |
| Event idempotency | Canonical router returns duplicate status and skips append when `idempotency_key` already exists. |
| Replay determinism | Projection replay orders rows by `(timestamp, event_id)` and computes checksum from replayed state. |
| Replay safety | Lifecycle replay validates event signatures and actor types; unauthorized replay callers are blocked by trust boundaries. |
| LLM inflight concurrency | Gateway applies inflight deduplication and bounded cache/retry behavior for identical inflight keys. |
