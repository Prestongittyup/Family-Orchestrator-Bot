# ARCHITECTURAL CONTRACT SPEC (ACS)

Status: Active
Version: 1.0.0
Effective Date: 2026-05-01

## 1. Execution Model

ACS-EXE-001:
Write-capable behavior is constrained to Command -> Event -> Projection -> Read Model.

ACS-EXE-002:
BYPASS of command admission or event commit boundaries is prohibited.

ACS-EXE-003:
Projection state MUST be derivable from committed events.

## 2. Authority Model

ACS-AUTH-001:
Event log is the sole write-side authority.

ACS-AUTH-002:
Derived state (including projection and cache state) is non-authoritative.

ACS-AUTH-003:
Tests are enforcement artifacts and cannot define runtime authority.

## 3. Layer Model

ACS-LAYER-001 (Command Layer):
Runtime command boundary exclusively owns state mutation admission and canonical event emission.

ACS-LAYER-002 (Domain Layer):
Domain modules own read-model pipeline assembly and response construction.

ACS-LAYER-003 (Shared Utility Layer):
Shared helpers are utility-only stateless deterministic transformations.

ACS-LAYER-004 (Test Validation Layer):
Test layer validates invariants only and cannot execute runtime behavior.

ACS-LAYER-005:
Cross-layer execution leakage is prohibited.

## 4. Read Model Contract

ACS-RM-001:
Read-model pipeline order is fixed: projection fetch -> materialization -> filter/search -> sort(tie-break) -> pagination -> response.

ACS-RM-002:
Sorting is deterministic and stable for identical primary keys via deterministic tie-break rules.

ACS-RM-003:
Pagination is deterministic for identical normalized query input and identical projection fingerprint.

ACS-RM-004:
Cache keys/fingerprints MUST be projection- and query-derived, and cache is non-authoritative.

ACS-RM-005:
Read-model outputs are projection-derived only.

## 5. Dependency Rules

ACS-DEP-001:
Shared utility modules may not import domain read-model modules.

ACS-DEP-002:
Domain read-model modules may import shared utility modules.

ACS-DEP-003:
Shared/domain dependency direction MUST remain one-directional and acyclic.

ACS-DEP-004:
Tests may import runtime and domain modules only for read-only validation/enforcement.

## 6. Determinism Rules

ACS-DET-001:
Identical effective input MUST yield identical output.

ACS-DET-002:
Replay equivalence is required: replay-derived and runtime-derived state must converge.

## 7. Test Governance Rules

ACS-TST-001:
Governed architecture/read-model tests are categorized as: contract, boundary, determinism, integration.

ACS-TST-002:
A single governed test file may not mix more than two categories.

ACS-TST-003:
Boundary tests enforce structure/dependency isolation and must not assert business logic semantics.

ACS-TST-004:
Contract tests enforce schema/shape invariants and must not assert cache lifecycle behavior.

ACS-TST-005:
Guard suites reference ACS as policy authority; tests enforce policy and do not redefine policy text.
