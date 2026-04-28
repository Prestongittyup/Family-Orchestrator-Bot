"""
Scenario Runner - Orchestrates end-to-end simulation runs

Manages:
  - Multiple concurrent family simulations
  - Failure injection orchestration
  - Invariant validation
  - Deterministic replay capability
  - State convergence verification
  - Comprehensive reporting
"""

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Callable
import hashlib

from .simulation_engine import (
    SimulationEngine,
    FamilyMember,
    PersonRole,
    CommandType,
)
from .failure_injector import FailureInjector
from .invariant_validator import InvariantValidator, InvariantViolation
from .report_generator import ReportGenerator


@dataclass
class ScenarioRunResult:
    """Result of a single scenario run"""
    scenario_name: str
    run_id: str
    success: bool
    duration_seconds: float
    state_hash: str
    event_count: int
    entity_count: int
    execution_stats: Dict
    violations: List[InvariantViolation]
    violation_summary: Dict
    failure_injection_summary: Dict
    event_log: List[Dict]
    # XAI: explanation records produced during this run
    xai_explanations: List = field(default_factory=list)


class ScenarioRunner:
    """Orchestrates simulation scenarios"""
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.runs: List[ScenarioRunResult] = []
        self.run_counter = 0

    def _default_scenario_mapping(self) -> Dict[str, Callable]:
        return {
            "concurrent_plan_creation_scenario": concurrent_plan_creation_scenario,
            "task_execution_idempotency_scenario": task_execution_idempotency_scenario,
            "conflicting_plan_updates_scenario": conflicting_plan_updates_scenario,
        }

    def _resolve_generator(
        self,
        *,
        engine: SimulationEngine,
        scenario_name: str,
        scenario_generator: Callable | None,
        scenario_kwargs: Dict,
    ) -> Callable:
        if scenario_generator is not None:
            return scenario_generator

        scenario_mapping = self._default_scenario_mapping()
        scenario_factory = scenario_mapping.get(scenario_name)
        if scenario_factory is None:
            for key, value in scenario_mapping.items():
                if scenario_name.startswith(f"{key}_"):
                    scenario_factory = value
                    break
        if scenario_factory is None:
            raise ValueError(f"Unknown scenario '{scenario_name}' and no generator supplied")

        return scenario_factory(engine, **scenario_kwargs)
    
    async def run_scenario(
        self,
        scenario_name: str,
        family_id: str | None = None,
        scenario_generator: Callable | None = None,
        failure_profile: str = "no_failures",
        random_seed: int = 42,
        **scenario_kwargs,
    ) -> ScenarioRunResult:
        """
        Run a single simulation scenario
        
        Args:
            scenario_name: Name of the scenario
            family_id: Family ID for simulation
            scenario_generator: Async generator that yields commands
            failure_profile: Name of failure injection profile
            random_seed: Random seed for determinism
        
        Returns:
            ScenarioRunResult with validation results
        """
        self.run_counter += 1
        run_id = f"{scenario_name}_{self.run_counter}_{datetime.utcnow().timestamp()}"
        
        if self.verbose:
            print(f"\n[SCENARIO] Starting: {scenario_name}")
        
        resolved_family_id = family_id or f"family-{self.run_counter}"

        # Create simulation
        engine = SimulationEngine(resolved_family_id, random_seed=random_seed)

        resolved_generator = self._resolve_generator(
            engine=engine,
            scenario_name=scenario_name,
            scenario_generator=scenario_generator,
            scenario_kwargs=scenario_kwargs,
        )
        
        # Create failure injector
        failure_injector = FailureInjector(
            profile=failure_profile,
            random_seed=random_seed,
            verbose=self.verbose,
        )
        
        # Run scenario
        sim_results = await engine.run_scenario(
            scenario_name=scenario_name,
            scenario_generator=resolved_generator,
            failure_injector=failure_injector,
        )
        
        # Validate invariants
        validator = InvariantValidator()
        all_passed, violations = validator.run_all_validations(engine.state)
        
        if self.verbose:
            print(f"[VALIDATION] Passed: {all_passed}")
            if violations:
                print(f"[VIOLATIONS] {len(violations)} detected")
        
        # Build result
        result = ScenarioRunResult(
            scenario_name=scenario_name,
            run_id=run_id,
            success=all_passed and not engine.state.quarantine_mode,
            duration_seconds=sim_results["duration_seconds"],
            state_hash=sim_results["state_hash"],
            event_count=sim_results["event_count"],
            entity_count=sim_results["entity_count"],
            execution_stats=sim_results["stats"],
            violations=violations,
            violation_summary=validator.get_violation_summary(),
            failure_injection_summary=failure_injector.get_injection_summary(),
            event_log=engine.event_log,
            xai_explanations=list(engine.state.xai_explanations),
        )
        
        self.runs.append(result)
        return result
    
    async def run_scenario_with_replay(
        self,
        scenario_name: str,
        family_id: str | None = None,
        scenario_generator: Callable | None = None,
        failure_profile: str = "no_failures",
        replay_count: int = 3,
        num_replays: int | None = None,
        **scenario_kwargs,
    ) -> tuple[bool, list[str]]:
        """
        Run scenario multiple times with same seed for determinism verification
        
        Returns:
            Dict with convergence analysis
        """
        run_results = []
        state_hashes = []
        total_replays = num_replays if num_replays is not None else replay_count
        
        for i in range(total_replays):
            result = await self.run_scenario(
                scenario_name=f"{scenario_name}_replay_{i}",
                family_id=family_id,
                scenario_generator=scenario_generator,
                failure_profile=failure_profile,
                random_seed=42,  # Same seed each time
                **scenario_kwargs,
            )
            run_results.append(result)
            state_hashes.append(result.state_hash)
        
        # Check convergence
        unique_hashes = set(state_hashes)
        converged = len(unique_hashes) == 1

        # XAI: compare explanation IDs and texts across replays
        explanation_id_sets = [
            sorted(e.explanation_id for e in r.xai_explanations)
            for r in run_results
        ]
        explanation_text_sets = [
            sorted(e.explanation_text for e in r.xai_explanations)
            for r in run_results
        ]
        xai_deterministic = (
            len(set(map(tuple, explanation_id_sets))) == 1
            and len(set(map(tuple, explanation_text_sets))) == 1
        )

        _ = xai_deterministic
        return converged, state_hashes
    
    async def run_test_matrix(
        self,
        scenario_generators: Dict[str, Callable] | None = None,
        family_id: str = "test-family",
        failure_profiles: Optional[List[str]] = None,
        random_seed: int = 42,
    ) -> List[ScenarioRunResult]:
        """
        Run multiple scenarios across multiple failure profiles
        
        Creates a matrix of scenarios × failure profiles
        
        Returns:
            Comprehensive test matrix results
        """
        if failure_profiles is None:
            failure_profiles = ["no_failures", "light_transient", "moderate_network", "high_chaos"]

        if scenario_generators is None:
            scenario_generators = {
                "concurrent_plan_creation_scenario": None,
                "task_execution_idempotency_scenario": None,
                "conflicting_plan_updates_scenario": None,
            }
        
        matrix_run_results: List[ScenarioRunResult] = []
        
        for scenario_name, generator in scenario_generators.items():
            
            for failure_profile in failure_profiles:
                if self.verbose:
                    print(f"\n[MATRIX] {scenario_name} + {failure_profile}")
                
                result = await self.run_scenario(
                    scenario_name=f"{scenario_name}_{failure_profile}",
                    family_id=family_id,
                    scenario_generator=generator,
                    failure_profile=failure_profile,
                    random_seed=random_seed,
                )
                matrix_run_results.append(result)

        return matrix_run_results
    
    def get_results_summary(self) -> Dict:
        """Get summary of all runs"""
        if not self.runs:
            return {"total_runs": 0}
        
        successful_runs = len([r for r in self.runs if r.success])
        total_violations = sum(len(r.violations) for r in self.runs)
        critical_violations = sum(
            r.violation_summary.get("critical_count", 0)
            for r in self.runs
        )
        
        avg_duration = sum(r.duration_seconds for r in self.runs) / len(self.runs)
        
        return {
            "total_runs": len(self.runs),
            "successful_runs": successful_runs,
            "success_count": successful_runs,
            "failure_count": len(self.runs) - successful_runs,
            "success_rate": successful_runs / len(self.runs),
            "total_violations": total_violations,
            "critical_violations": critical_violations,
            "avg_duration_seconds": avg_duration,
            "by_scenario": self._summarize_by_scenario(),
        }
    
    def _summarize_by_scenario(self) -> Dict:
        """Summarize results by scenario name"""
        by_scenario = {}
        
        for run in self.runs:
            if run.scenario_name not in by_scenario:
                by_scenario[run.scenario_name] = {
                    "count": 0,
                    "successful": 0,
                    "violations": 0,
                }
            
            by_scenario[run.scenario_name]["count"] += 1
            if run.success:
                by_scenario[run.scenario_name]["successful"] += 1
            by_scenario[run.scenario_name]["violations"] += len(run.violations)
        
        return by_scenario


# Pre-defined test scenarios

def concurrent_plan_creation_scenario(
    engine: SimulationEngine,
    num_members: int = 3,
    plans_per_member: int = 5,
) -> Callable:
    """
    Scenario: Multiple family members concurrently creating plans
    
    Tests:
      - No lost updates under concurrent creation
      - Correct entity counts
      - Watermark consistency
    """
    # Setup family members
    for i in range(num_members):
        engine.add_family_member(
            person_id=f"member_{i}",
            name=f"Member {i}",
            role=PersonRole.PARENT if i == 0 else PersonRole.TEENAGER,
        )
    
    async def generate():
        tasks = []
        for member_id, member in engine.family_members.items():
            for j in range(plans_per_member):
                cmd = member.issue_command(
                    command_type=CommandType.CREATE_PLAN,
                    payload={"title": f"Plan {member_id}_{j}"},
                )
                tasks.append(cmd)
        
        for cmd in tasks:
            yield cmd
    
    return generate


def task_execution_idempotency_scenario(
    engine: SimulationEngine,
    num_members: int = 2,
    tasks_per_member: int = 10,
) -> Callable:
    """
    Scenario: Task execution with idempotency key replay
    
    Tests:
      - No duplicate task execution
      - Idempotency cache working
      - Deterministic results on replay
    """
    # Setup
    for i in range(num_members):
        engine.add_family_member(
            person_id=f"member_{i}",
            name=f"Member {i}",
            role=PersonRole.PARENT if i == 0 else PersonRole.TEENAGER,
        )
    
    async def generate():
        # First pass: create tasks
        tasks = []
        for member_id, member in engine.family_members.items():
            for j in range(tasks_per_member):
                cmd = member.issue_command(
                    command_type=CommandType.CREATE_TASK,
                    payload={
                        "title": f"Task {member_id}_{j}",
                        "plan_id": "test-plan",
                    },
                    delay_ms=random.randint(10, 100),
                )
                tasks.append(cmd)
                yield cmd
        
        # Second pass: mark complete with retries
        for task_id in [cmd.target_entity_id for cmd in tasks if cmd.target_entity_id]:
            member = engine.family_members[f"member_0"]
            cmd = member.issue_command(
                command_type=CommandType.MARK_TASK_COMPLETE,
                target_entity_id=task_id,
            )
            yield cmd
            
            # Simulate retry with same idempotency key
            for _ in range(2):
                retry_cmd = member.retry_command(cmd)
                # Keep same idempotency key
                retry_cmd.idempotency_key = cmd.idempotency_key
                yield retry_cmd
    
    return generate


def conflicting_plan_updates_scenario(
    engine: SimulationEngine,
    num_conflicting_updates: int = 5,
) -> Callable:
    """
    Scenario: Multiple concurrent updates to same plan
    
    Tests:
      - No lost updates with concurrent modifications
      - Version monotonicity
      - Last-write-wins or conflict detection
    """
    # Setup
    engine.add_family_member("member_1", "Parent 1", PersonRole.PARENT)
    engine.add_family_member("member_2", "Parent 2", PersonRole.PARENT)
    
    async def generate():
        member1 = engine.family_members["member_1"]
        member2 = engine.family_members["member_2"]
        
        # Create base plan
        plan_cmd = member1.issue_command(
            command_type=CommandType.CREATE_PLAN,
            payload={"title": "Shared Plan"},
        )
        yield plan_cmd
        
        # Wait for plan creation (simulated by yielding a dummy)
        await asyncio.sleep(0.1)
        plan_id = plan_cmd.target_entity_id
        
        # Concurrent updates from both members
        for i in range(num_conflicting_updates):
            cmd1 = member1.issue_command(
                command_type=CommandType.UPDATE_PLAN,
                target_entity_id=plan_id,
                payload={"status": f"state_from_member1_{i}"},
            )
            cmd2 = member2.issue_command(
                command_type=CommandType.UPDATE_PLAN,
                target_entity_id=plan_id,
                payload={"status": f"state_from_member2_{i}"},
            )
            yield cmd1
            yield cmd2
    
    return generate


import random
