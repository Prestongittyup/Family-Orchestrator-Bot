# Lifecycle Contract Boundary Report

Date: 2026-04-22
Scope: Internal FSM lifecycle state vs external API representation
Mode: Boundary audit and serialization cleanup only

## 1. Executive Result

- Strict separation guarantee status: YES
- Confidence score: 92

Conclusion basis:
- Internal lifecycle state remains `LifecycleState` enum only.
- API serialization now routes lifecycle output through a single boundary mapper.
- Presentation label `executed` remains API-only and is rejected by internal state parsing.

## 2. Final Contract Definition

### Internal state contract
- Internal lifecycle state type: `LifecycleState`
- Allowed internal values:
  - `proposed`
  - `pending_approval`
  - `approved`
  - `committed`
  - `rejected`
  - `failed`
- Internal consumers:
  - FSM validation
  - event replay / reducer
  - runtime orchestration
  - persistence and migration normalization

### External API state contract
- Boundary mapper: `LifecyclePresentationMapper`
- API state mapping:
  - `LifecycleState.PROPOSED -> "proposed"`
  - `LifecycleState.PENDING_APPROVAL -> "pending_approval"`
  - `LifecycleState.APPROVED -> "approved"`
  - `LifecycleState.COMMITTED -> "executed"`
  - `LifecycleState.REJECTED -> "rejected"`
  - `LifecycleState.FAILED -> "failed"`

### Hard rules
- FSM must never consume API presentation labels.
- Persistence must never store API presentation labels as lifecycle state.
- API routes may serialize presentation labels only through `LifecyclePresentationMapper`.
- Approval workflow fields such as `approval_status` are not lifecycle state and are outside FSM state vocabulary.

## 3. API Surface Audit

### Surfaces exposing lifecycle state
1. `POST /assistant/approve`
- File: `apps/api/assistant_runtime_router.py`
- Response field: `AssistantApproveResponse.status`
- Current boundary behavior: `COMMITTED` is serialized as `"executed"` through mapper.

2. `POST /assistant/reject`
- File: `apps/api/assistant_runtime_router.py`
- Response field: `AssistantRejectResponse.status`
- Current boundary behavior: `REJECTED` is serialized as `"rejected"` through mapper.

3. `GET /assistant/today`
- File: `apps/api/assistant_runtime_router.py`
- Response field: `pending_actions[].state`
- Current boundary behavior: pending lifecycle states are serialized through mapper.

### Related API payloads reviewed but not lifecycle state
1. `HouseholdOSRunResponse.recommended_action.approval_status`
- File: `household_os/core/contracts.py`
- Meaning: approval workflow label, not FSM lifecycle state.

2. `HouseholdOSRunResponse.grouped_approval_payload.approval_status`
- File: `household_os/core/contracts.py`
- Meaning: approval workflow label, not FSM lifecycle state.

## 4. Violations Found

### Fixed violations
1. Direct presentation literal in API response constructor
- File: `apps/api/assistant_runtime_router.py`
- Before: `AssistantApproveResponse(status="executed", ...)`
- After: `LifecyclePresentationMapper.to_api_state(LifecycleState.COMMITTED)`

2. Direct raw internal enum string exposure in API payload
- File: `apps/api/assistant_runtime_router.py`
- Before: `pending_actions[].state = state.value`
- After: `pending_actions[].state = LifecyclePresentationMapper.to_api_state(state)`

3. Direct literal lifecycle serialization without mapper
- File: `apps/api/assistant_runtime_router.py`
- Before: `AssistantRejectResponse(status="rejected")`
- After: `LifecyclePresentationMapper.to_api_state(LifecycleState.REJECTED)`

### Remaining non-violations explicitly classified
- `approval_status="approved"` in run-response contracts is approval metadata, not FSM lifecycle state.
- log messages containing words like `executed` or `failed` are not lifecycle-state violations unless they are used as state payloads.

## 5. Enforcement Layer

### Mapper introduced
- File: `household_os/presentation/lifecycle_presentation_mapper.py`
- Responsibility: one-way serialization from internal lifecycle enum to API label.

### Guard tests added
- File: `tests/test_lifecycle_contract_boundary.py`
- Enforces:
  - mapper contract values
  - internal parser rejects `"executed"`
  - API response lifecycle output does not bypass mapper in audited API layer

### Existing supporting guard
- File: `tests/test_boundary_enforcement.py`
- Confirms boundary parsing rejects legacy presentation label input for internal lifecycle state.

## 6. Persistence and FSM Boundary Check

- FSM parse path: `parse_lifecycle_state("executed")` raises `ValueError`
- Persistence migration path converts presentation/legacy labels to canonical internal state before storage.
- No persistence writer was updated to store `executed` as lifecycle state.

## 7. Documentation Normalization

Updated:
- `docs/LIFECYCLE_STATE_UNIFICATION.md`

Documentation now explicitly distinguishes:
- internal state
- API state label
- presentation-only mapping at the boundary

## 8. Validation

Executed:
- `python -m pytest tests/test_lifecycle_contract_boundary.py tests/test_boundary_enforcement.py -q`

Result:
- 12 passed, 0 failed

## 9. Files Changed

- `household_os/presentation/lifecycle_presentation_mapper.py`
- `apps/api/assistant_runtime_router.py`
- `tests/test_assistant_runtime_router.py`
- `tests/test_lifecycle_contract_boundary.py`
- `docs/LIFECYCLE_STATE_UNIFICATION.md`

## 10. Remaining Risks

- The API-boundary guard currently audits the owning lifecycle route file rather than every `apps/api/**` module, to avoid false positives on unrelated status fields.
- If additional lifecycle-bearing API routes are introduced, they should be added to the same mapper boundary and guard coverage.
