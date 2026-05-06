# Architecture Drift Log

Source of truth: `docs/architecture/RFC-001.md`

Drift log policy: derived-state records only.

Governance freeze notice: this log is non-authoritative and observational only, and must not be used as enforcement input for pass/fail decisions.

- Each entry must reference concrete enforcement artifacts (tests/guards/workflows).
- This log does not define governance rules; it records observed drift and resolved deltas.

## 2026-05-02 - FIR Contract Freeze Consolidated (Sprint 8.2)

Drift vectors detected and resolved:

- FIR declarations had an extra non-semantic metadata field (`feature_name`) that was not required for intake gating.
- Template/docs/schema references could drift independently without a direct canonical-template assertion.
- FIR freeze posture needed explicit documentation that no new intake semantics/categories are allowed.

Resolution applied:

- Locked FIR declaration shape to exactly `projection_impact`, `read_model_impact`, and `kernel_interaction` in `tests/governance/contract_kernel.py`.
- Updated all governed feature suites and declaration template to remove the extra field and keep only frozen semantics.
- Extended `tests/governance/test_feature_intake_contract.py` to validate canonical template alignment in addition to governed suite compliance.
- Updated governance/architecture documentation to point to the single authoritative FIR contract suite and test-layer-only scope.

Status: RESOLVED

## 2026-05-01 - Feature Intake Declaration Rule Introduced (Test Layer Only)

Drift vectors detected and resolved:

- Feature-facing test suites could evolve behavior scope without explicit projection/read-model/kernel interaction declarations.
- Intake discipline was implicit and therefore vulnerable to silent feature creep.

Resolution applied:

- Introduced `tests/governance/test_feature_intake_contract.py` to enforce mandatory feature intake declarations in governed feature suites.
- Added `tests/governance/feature_intake_declaration_template.py` and centralized intake schema in `tests/governance/contract_kernel.py`.
- Enforced test-layer-only boundary: no intake enforcement logic in runtime/command paths.

Status: RESOLVED

## 2026-05-01 - Governance Contract Kernel Consolidation

Drift vectors detected and resolved:

- Semantic governance expectations were repeated across multiple test files as local constants/marker lists.
- Shared-layer freeze semantics (utility-only and no policy/enforcement/domain interpretation) were distributed instead of centrally referenced.

Resolution applied:

- Introduced `tests/governance/contract_kernel.py` as the single governance contract kernel for test-layer semantic constants.
- Refactored governance and read-model guard suites to reference kernel definitions instead of redefining semantic rule text.
- Kept runtime/event/schema behavior unchanged; consolidation is test/docs only.

Status: RESOLVED

## 2026-05-01 - ACS Rule-ID Test Coupling Removed

Drift violations detected and resolved:

- Governance/read-model tests were coupled to ACS rule identifiers via direct marker assertions.
- Test enforcement mixed semantic checks with identifier anchoring, reducing ACS structure freedom.
- Progress/state metadata lagged the governance model transition.

Resolution applied:

- Removed ACS rule-ID assertions and ACS section-anchor assertions from governance/read-model test suites.
- Preserved semantic invariant enforcement: determinism, replay equivalence, boundary isolation, dependency direction, cache non-authority, and pipeline ordering.
- Updated progress/state artifacts to mark semantic contract model adoption.

Status: RESOLVED

## 2026-04-29 - RFC-001 Contract Adoption

Drift violations detected and resolved:

- Missing canonical RFC artifact (`docs/architecture/RFC-001.md` did not exist).
- Architecture docs had no authoritative RFC reference chain.
- `docs/architecture/Architecture Implementation Plan.md` contained stale statement claiming RFC artifact was absent.
- Enforcement references used outdated architecture test paths.

Resolution applied:

- Created immutable RFC artifact at `docs/architecture/RFC-001.md`.
- Updated architecture docs to explicitly defer to RFC-001 as source of truth.
- Added RFC immutability rule in `docs/architecture/Enforcement Checklist.md`.
- Extended architecture guards and architecture tests to enforce RFC presence and required references.

Status: RESOLVED
