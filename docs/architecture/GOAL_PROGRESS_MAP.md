# Goal Progress Map (as of 2026-04-25)

## North-Star Goal

Build a deterministic Household OS architecture where:
- One canonical integration pipeline exists for integration-core behavior.
- Conversation, intent, policy, integration, and runtime concerns are cleanly separated.
- Redundancy and architecture drift are blocked by automated tests and CI gates.

## Where We Are Now

1. Layer model is defined and documented.
- Source: docs/architecture/LAYER_MAP.md
- Current model: 7 operational layers + 1 compatibility facade.

2. Purpose is explicit.
- The system translates household intent into policy-checked, deterministic actions.
- Layer boundaries define who owns conversation flow, intent validation, policy decisions, integration state build, and runtime execution.

3. Redundancy controls are active.
- New guard test: tests/test_layer_redundancy_guard.py
- Existing single-pipeline freeze tests remain active: tests/test_single_pipeline_enforcement.py
- Governance CI runs these checks: .github/workflows/governance-gate.yml

4. Root artifact sprawl is controlled.
- Deterministic organizer: scripts/organize_root_artifacts.py
- Root artifact blocking tests: tests/test_hard_freeze_regression.py

5. Legacy documentation noise is pruned.
- Repo root markdown reduced to canonical set only.
- docs markdown reduced to Home OS-focused canonical set only.
- Governance tests enforce these allowlists.

## Where We Are Headed

1. Single-source-of-truth architecture.
- Canonical logic continues to live under apps/api/integration_core for integration-core pipeline concerns.
- Compatibility facade under integration_core remains import-only.

2. Stable, test-enforced boundaries.
- All new architectural changes should be accepted only when boundary and redundancy tests pass.

3. Cleaner repo topology over time.
- Operationally generated artifacts remain outside repo root.
- Architecture and status documentation stays under docs/architecture.

## What Is Left To Do

1. Finalize and commit current structural changes.
- Current workspace includes modified and newly added governance/architecture files.
- Root report deletions should be reviewed and committed intentionally.

2. Keep doc hygiene strict as new docs are added.
- Add new docs only if they directly support Home OS delivery.
- Place them under docs (preferably docs/architecture) and update docs/README.md.

3. Expand drift protections incrementally.
- Add boundary tests for any newly introduced critical folders before new features land.
- Keep CI gate test list synchronized with required architecture guards.

## Practical Completion Snapshot

- Foundation architecture mapping: complete
- Redundancy guardrails for integration-core layering: complete
- Governance CI wiring for those guardrails: complete
- Full documentation consolidation across entire repo: complete (canonical set retained)
- Ongoing architecture hardening as features evolve: ongoing
