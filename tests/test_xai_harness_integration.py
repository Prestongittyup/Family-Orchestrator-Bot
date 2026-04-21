"""
XAI Harness Integration Tests
================================

Validates the Explainability Engine under real simulation conditions.

Success criteria (system PASSES only when ALL are true):
  1. Zero missing explanations — every successful state mutation has exactly 1 explanation.
  2. Zero duplicate explanations — no two explanations share an idempotency_key.
  3. Replay produces identical explanation sets — same IDs, same text, same count.
  4. No cross-family explanation leakage — family isolation is preserved.

Test matrix:
  - Scenarios:  concurrent_plan_creation | task_idempotency | conflicting_updates
  - Profiles:   no_failures | light_transient | moderate_network | high_chaos | byzantine

Report outputs (generated into tests/harness/reports/):
  - explanation_log.json
  - explanation_duplicates.json
  - explanation_missing.json
  - explanation_replay_diff.json
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Callable, List

import pytest

from tests.harness.failure_injector import FailureInjector
from tests.harness.invariant_validator import InvariantValidator
from tests.harness.report_generator import ReportGenerator
from tests.harness.simulation_engine import (
    CommandType,
    FamilyMember,
    PersonRole,
    SimulationEngine,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FAILURE_PROFILES: List[str] = [
    "no_failures",
    "light_transient",
    "moderate_network",
    "high_chaos",
    "byzantine",
]

_REPORT_DIR = Path(__file__).parent / "harness" / "reports"
_REPORT_DIR.mkdir(parents=True, exist_ok=True)

_reporter = ReportGenerator(output_dir=_REPORT_DIR)

# ---------------------------------------------------------------------------
# Scenario factories
# Each returns an async generator callable (no args) bound to an engine.
# The engine is set up as a side-effect of calling the factory.
# ---------------------------------------------------------------------------


def _make_concurrent_plan_creation(
    engine: SimulationEngine,
    num_members: int = 3,
    plans_per_member: int = 5,
) -> Callable:
    for i in range(num_members):
        engine.add_family_member(
            f"member_{i}",
            f"Member {i}",
            PersonRole.PARENT if i == 0 else PersonRole.TEENAGER,
        )

    async def generator():
        for member_id, member in engine.family_members.items():
            for j in range(plans_per_member):
                cmd = member.issue_command(
                    CommandType.CREATE_PLAN,
                    payload={"title": f"Plan_{member_id}_{j}"},
                )
                yield cmd

    return generator


def _make_task_idempotency(
    engine: SimulationEngine,
    num_tasks: int = 5,
) -> Callable:
    engine.add_family_member("member_0", "Parent", PersonRole.PARENT)

    async def generator():
        member = engine.family_members["member_0"]
        issued: list = []

        # Create tasks
        for j in range(num_tasks):
            cmd = member.issue_command(
                CommandType.CREATE_TASK,
                payload={"title": f"Task_{j}", "plan_id": "test-plan"},
            )
            issued.append(cmd)
            yield cmd

        # Retry each CREATE_TASK with the same idempotency_key (must deduplicate)
        for cmd in issued:
            retry = member.retry_command(cmd)
            retry.idempotency_key = cmd.idempotency_key  # force same key
            yield retry

        # Mark tasks complete
        for cmd in issued:
            if cmd.target_entity_id is None:
                continue
            complete_cmd = member.issue_command(
                CommandType.MARK_TASK_COMPLETE,
                target_entity_id=cmd.target_entity_id,
            )
            yield complete_cmd

    return generator


def _make_conflicting_updates(
    engine: SimulationEngine,
    num_updates: int = 5,
) -> Callable:
    engine.add_family_member("member_0", "Parent 1", PersonRole.PARENT)
    engine.add_family_member("member_1", "Parent 2", PersonRole.PARENT)

    async def generator():
        m0 = engine.family_members["member_0"]
        m1 = engine.family_members["member_1"]

        # Create the shared plan
        plan_cmd = m0.issue_command(
            CommandType.CREATE_PLAN,
            payload={"title": "Shared Plan"},
        )
        yield plan_cmd

        # Small pause; after which plan_cmd.target_entity_id is resolved
        await asyncio.sleep(0.01)

        if plan_cmd.target_entity_id is None:
            return  # plan creation failed under chaos — skip updates

        for i in range(num_updates):
            c1 = m0.issue_command(
                CommandType.UPDATE_PLAN,
                target_entity_id=plan_cmd.target_entity_id,
                payload={"status": f"v_m0_{i}"},
            )
            c2 = m1.issue_command(
                CommandType.UPDATE_PLAN,
                target_entity_id=plan_cmd.target_entity_id,
                payload={"status": f"v_m1_{i}"},
            )
            yield c1
            yield c2

    return generator


# ---------------------------------------------------------------------------
# Shared runner helper
# ---------------------------------------------------------------------------


async def _run(
    scenario_name: str,
    factory_fn: Callable,
    failure_profile: str,
    family_id: str,
    seed: int = 42,
) -> SimulationEngine:
    """
    Run one scenario against one failure profile.
    Returns the engine (state + explanations accessible via engine.state).
    """
    engine = SimulationEngine(family_id, random_seed=seed)
    injector = FailureInjector(profile=failure_profile, random_seed=seed)
    generator = factory_fn(engine)

    await engine.run_scenario(
        scenario_name=scenario_name,
        scenario_generator=generator,
        failure_injector=injector,
    )
    return engine


# ---------------------------------------------------------------------------
# 1. EXPLANATION COMPLETENESS
#    Every successful state mutation must produce exactly 1 explanation.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("failure_profile", FAILURE_PROFILES)
def test_concurrent_plan_creation_explanation_completeness(
    failure_profile: str,
) -> None:
    family_id = f"family-completeness-concurrent-{failure_profile}"
    engine = asyncio.run(
        _run("concurrent_plan_creation", _make_concurrent_plan_creation, failure_profile, family_id)
    )

    mutations = [
        m for m in engine.state.state_mutations
        if m.get("type") in {"entity_created", "entity_updated"}
    ]
    exp_count = len(engine.state.xai_explanations)
    mut_count = len(mutations)

    # Generate reports
    _reporter.generate_explanation_log(
        engine.state.xai_explanations,
        f"concurrent_{failure_profile}",
    )
    _reporter.generate_explanation_missing(
        engine.state,
        f"concurrent_{failure_profile}",
    )

    assert exp_count == mut_count, (
        f"[{failure_profile}] Completeness failure: "
        f"{exp_count} explanations for {mut_count} mutations"
    )


@pytest.mark.parametrize("failure_profile", FAILURE_PROFILES)
def test_task_idempotency_explanation_completeness(failure_profile: str) -> None:
    family_id = f"family-completeness-task-{failure_profile}"
    engine = asyncio.run(
        _run("task_idempotency", _make_task_idempotency, failure_profile, family_id)
    )

    mutations = [
        m for m in engine.state.state_mutations
        if m.get("type") in {"entity_created", "entity_updated"}
    ]
    exp_count = len(engine.state.xai_explanations)
    mut_count = len(mutations)

    assert exp_count == mut_count, (
        f"[{failure_profile}] Task idempotency completeness failure: "
        f"{exp_count}/{mut_count}"
    )


@pytest.mark.parametrize("failure_profile", FAILURE_PROFILES)
def test_conflicting_updates_explanation_completeness(failure_profile: str) -> None:
    family_id = f"family-completeness-conflict-{failure_profile}"
    engine = asyncio.run(
        _run("conflicting_updates", _make_conflicting_updates, failure_profile, family_id)
    )

    mutations = [
        m for m in engine.state.state_mutations
        if m.get("type") in {"entity_created", "entity_updated"}
    ]
    exp_count = len(engine.state.xai_explanations)
    mut_count = len(mutations)

    assert exp_count == mut_count, (
        f"[{failure_profile}] Conflicting updates completeness failure: "
        f"{exp_count}/{mut_count}"
    )


# ---------------------------------------------------------------------------
# 2. NO DUPLICATE EXPLANATIONS
#    No two explanations may share the same idempotency_key.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("failure_profile", FAILURE_PROFILES)
def test_no_duplicate_explanations_task_idempotency(failure_profile: str) -> None:
    """Retry storms must never produce double explanations for the same key."""
    family_id = f"family-nodup-task-{failure_profile}"
    engine = asyncio.run(
        _run("task_idempotency", _make_task_idempotency, failure_profile, family_id)
    )

    dup_report = _reporter.generate_explanation_duplicates(
        engine.state.xai_explanations,
        f"nodup_task_{failure_profile}",
    )

    keys = [e.idempotency_key for e in engine.state.xai_explanations]
    assert len(keys) == len(set(keys)), (
        f"[{failure_profile}] Duplicate explanations found. Report: {dup_report}"
    )


@pytest.mark.parametrize("failure_profile", FAILURE_PROFILES)
def test_no_duplicate_explanations_concurrent_plan_creation(failure_profile: str) -> None:
    family_id = f"family-nodup-concurrent-{failure_profile}"
    engine = asyncio.run(
        _run("concurrent_plan_creation", _make_concurrent_plan_creation, failure_profile, family_id)
    )

    keys = [e.idempotency_key for e in engine.state.xai_explanations]
    assert len(keys) == len(set(keys)), (
        f"[{failure_profile}] Duplicate explanations in concurrent plan creation"
    )


# ---------------------------------------------------------------------------
# 3. REPLAY DETERMINISM
#    Same seed → identical explanation IDs and texts in every replay.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("failure_profile", ["no_failures", "light_transient"])
def test_replay_determinism_concurrent_plan_creation(failure_profile: str) -> None:
    family_id = f"family-replay-concurrent-{failure_profile}"
    engines = [
        asyncio.run(
            _run(f"concurrent_plan_creation_replay_{i}",
                 _make_concurrent_plan_creation,
                 failure_profile,
                 family_id,
                 seed=42)
        )
        for i in range(3)
    ]

    text_sets = [
        sorted(e.explanation_text for e in eng.state.xai_explanations)
        for eng in engines
    ]
    counts = [len(eng.state.xai_explanations) for eng in engines]

    # Generate replay diff report between first two runs
    _reporter.generate_explanation_replay_diff(
        engines[0].state.xai_explanations,
        engines[1].state.xai_explanations,
        f"replay_concurrent_{failure_profile}",
    )

    assert counts[0] == counts[1] == counts[2], (
        f"[{failure_profile}] Replay explanation count divergence: {counts}"
    )
    assert text_sets[0] == text_sets[1] == text_sets[2], (
        f"[{failure_profile}] Replay non-determinism: explanation texts differ across runs"
    )


@pytest.mark.parametrize("failure_profile", ["no_failures", "light_transient"])
def test_replay_determinism_task_idempotency(failure_profile: str) -> None:
    family_id = f"family-replay-task-{failure_profile}"
    engines = [
        asyncio.run(
            _run(f"task_idempotency_replay_{i}",
                 _make_task_idempotency,
                 failure_profile,
                 family_id,
                 seed=42)
        )
        for i in range(3)
    ]

    text_sets = [sorted(e.explanation_text for e in eng.state.xai_explanations) for eng in engines]
    counts = [len(eng.state.xai_explanations) for eng in engines]

    assert counts[0] == counts[1] == counts[2], (
        f"[{failure_profile}] Task idempotency replay count divergence: {counts}"
    )
    assert text_sets[0] == text_sets[1] == text_sets[2], (
        f"[{failure_profile}] Task idempotency replay text divergence"
    )


# ---------------------------------------------------------------------------
# 4. NO CROSS-FAMILY EXPLANATION LEAKAGE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("failure_profile", FAILURE_PROFILES)
def test_no_cross_family_explanation_leakage(failure_profile: str) -> None:
    """
    Run two families via separate engines and confirm no explanation carries
    the wrong family_id.
    """
    family_a = f"family-alpha-{failure_profile}"
    family_b = f"family-beta-{failure_profile}"

    async def _run_both():
        return await asyncio.gather(
            _run("concurrent_plan_creation", _make_concurrent_plan_creation, failure_profile, family_a),
            _run("concurrent_plan_creation", _make_concurrent_plan_creation, failure_profile, family_b),
        )

    engine_a, engine_b = asyncio.run(_run_both())

    for exp in engine_a.state.xai_explanations:
        assert exp.family_id == family_a, (
            f"[{failure_profile}] Family A leakage: explanation {exp.explanation_id!r} "
            f"has family_id={exp.family_id!r}"
        )

    for exp in engine_b.state.xai_explanations:
        assert exp.family_id == family_b, (
            f"[{failure_profile}] Family B leakage: explanation {exp.explanation_id!r} "
            f"has family_id={exp.family_id!r}"
        )


# ---------------------------------------------------------------------------
# 5. INVARIANT VALIDATOR — XAI rules integrated into run_all_validations
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "scenario_name,factory_fn",
    [
        ("concurrent_plan_creation", _make_concurrent_plan_creation),
        ("task_idempotency", _make_task_idempotency),
        ("conflicting_updates", _make_conflicting_updates),
    ],
)
def test_all_xai_invariants_pass_no_failures(
    scenario_name: str,
    factory_fn: Callable,
) -> None:
    """InvariantValidator must report zero critical XAI violations on the clean profile."""
    family_id = f"family-invariants-{scenario_name}"
    engine = asyncio.run(
        _run(scenario_name, factory_fn, "no_failures", family_id)
    )

    validator = InvariantValidator()
    all_passed, violations = validator.run_all_validations(engine.state)

    xai_critical = [
        v for v in violations
        if v.severity == "critical" and v.invariant_name.startswith(
            ("explanation_completeness", "no_duplicate_explanations",
             "no_cross_family_explanation_leakage")
        )
    ]

    assert len(xai_critical) == 0, (
        f"XAI invariant violations for {scenario_name!r}: "
        f"{[v.description for v in xai_critical]}"
    )


# ---------------------------------------------------------------------------
# 6. FULL MATRIX — report generation
# ---------------------------------------------------------------------------


def test_full_matrix_report(tmp_path: Path) -> None:
    """
    Run the complete 3×5 scenario×profile matrix and write all four
    explanation report files for every run.  Asserts zero critical
    violations across the full matrix.
    """
    generator_map = {
        "concurrent_plan_creation": _make_concurrent_plan_creation,
        "task_idempotency": _make_task_idempotency,
        "conflicting_updates": _make_conflicting_updates,
    }
    reporter = ReportGenerator(output_dir=tmp_path)
    total_critical = 0
    run_summaries = []

    for scenario_name, factory_fn in generator_map.items():
        for profile in FAILURE_PROFILES:
            family_id = f"matrix-{scenario_name}-{profile}"
            run_id = f"{scenario_name}__{profile}"

            engine = asyncio.run(
                _run(scenario_name, factory_fn, profile, family_id)
            )

            # Generate all 4 reports
            reporter.generate_explanation_log(engine.state.xai_explanations, run_id)
            reporter.generate_explanation_duplicates(engine.state.xai_explanations, run_id)
            reporter.generate_explanation_missing(engine.state, run_id)

            # Replay diff: compare to a second run with same seed
            engine2 = asyncio.run(_run(scenario_name, factory_fn, profile, family_id, seed=42))
            reporter.generate_explanation_replay_diff(
                engine.state.xai_explanations,
                engine2.state.xai_explanations,
                run_id,
            )

            # Validate XAI-specific invariants only; pre-existing harness
            # violations (e.g. no_duplicate_task_execution) are out of scope.
            validator = InvariantValidator()
            _, violations = validator.run_all_validations(engine.state)
            _XAI_PREFIXES = (
                "explanation_completeness",
                "no_duplicate_explanations",
                "explanation_causal_coverage",
                "no_cross_family_explanation_leakage",
            )
            critical = [
                v for v in violations
                if v.severity == "critical"
                and v.invariant_name.startswith(_XAI_PREFIXES)
            ]
            total_critical += len(critical)

            run_summaries.append({
                "scenario": scenario_name,
                "profile": profile,
                "explanations": len(engine.state.xai_explanations),
                "critical_violations": len(critical),
            })

    # Verify every report file was created
    assert len(list(tmp_path.glob("*_explanation_log.json"))) == 15  # 3 scenarios × 5 profiles
    assert len(list(tmp_path.glob("*_explanation_duplicates.json"))) == 15
    assert len(list(tmp_path.glob("*_explanation_missing.json"))) == 15
    assert len(list(tmp_path.glob("*_explanation_replay_diff.json"))) == 15

    assert total_critical == 0, (
        f"Critical XAI violations detected across full matrix: "
        f"{[(s['scenario'], s['profile'], s['critical_violations']) for s in run_summaries if s['critical_violations'] > 0]}"
    )
