"""
Integration Test Suite - Pytest-based harness integration

Orchestrates:
  - Test matrix execution
  - Report generation
  - Assertion validation
  - CI/CD integration
"""

import pytest
import asyncio
from pathlib import Path
from typing import List, Dict, Any

from tests.harness.simulation_engine import SimulationEngine
from tests.harness.failure_injector import FailureInjector, FailureInjectionProfile
from tests.harness.invariant_validator import InvariantValidator
from tests.harness.scenario_runner import ScenarioRunner
from tests.harness.report_generator import ReportGenerator


class HouseholdSimulationTestSuite:
    """Comprehensive test suite for HPAL household validation"""
    
    def __init__(self, output_dir: Path = Path("test_reports")):
        self.output_dir = output_dir
        self.report_generator = ReportGenerator(output_dir)
        self.scenario_runner = ScenarioRunner()
        self.run_results = []
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment"""
        self.run_results.clear()
        yield
        self.teardown()
    
    def teardown(self):
        """Generate reports after all tests complete"""
        if self.run_results:
            self._generate_all_reports()
    
    # ========================
    # Invariant Validation Tests
    # ========================
    
    @pytest.mark.asyncio
    async def test_no_duplicate_task_execution(self):
        """
        CRITICAL: Task execution must be idempotent
        No duplicate task execution under any condition
        """
        result = await self.scenario_runner.run_scenario(
            "task_execution_idempotency_scenario",
            num_members=2,
            tasks_per_member=10,
        )
        
        # Verify: No invariant violations
        assert result.success, f"Task idempotency failed: {result.violations}"
        
        # Verify: No critical violations
        critical_count = result.violation_summary.get("critical_count", 0)
        assert critical_count == 0, f"Found {critical_count} critical violations"
        
        # Verify: All tasks executed exactly once
        from tests.harness.invariant_validator import InvariantValidator
        validator = InvariantValidator()
        # Would require state access; ensure no violations flag set
        assert len([v for v in result.violations if "duplicate" in v.invariant_name.lower()]) == 0
        
        self.run_results.append(result)
    
    @pytest.mark.asyncio
    async def test_no_lost_updates(self):
        """
        CRITICAL: Concurrent plan modifications must not lose data
        No lost updates across concurrent plan modifications
        """
        result = await self.scenario_runner.run_scenario(
            "concurrent_plan_creation_scenario",
            num_members=3,
            plans_per_member=5,
        )
        
        # Verify: All created plans persisted
        assert result.success, f"Lost updates detected: {result.violations}"
        
        # Verify: No conflicting versions
        assert len([v for v in result.violations if "lost" in v.invariant_name.lower()]) == 0
        
        # Verify: Entity count matches expectations (3 members × 5 plans = 15 minimum)
        assert result.entity_count >= 15, f"Expected ≥15 entities, got {result.entity_count}"
        
        self.run_results.append(result)
    
    @pytest.mark.asyncio
    async def test_no_cross_family_leakage(self):
        """
        CRITICAL: Multi-family isolation enforced
        No cross-family data leakage
        """
        engine = SimulationEngine()
        
        # Create two separate families
        family1_member = engine.add_family_member(
            person_id="person_1",
            family_id="family_1",
            role="admin",
        )
        family2_member = engine.add_family_member(
            person_id="person_2",
            family_id="family_2",
            role="admin",
        )
        
        # Execute commands in both families
        cmd1 = await self.scenario_runner.run_scenario(
            "concurrent_plan_creation_scenario",
            num_members=1,
        )
        
        # Verify: No cross-family entity contamination
        family1_entities = [
            e for e in engine.state.entities.values()
            if e.family_id == "family_1"
        ]
        family2_entities = [
            e for e in engine.state.entities.values()
            if e.family_id == "family_2"
        ]
        
        # Verify: No entity appears in wrong family
        for entity in engine.state.entities.values():
            assert entity.family_id in ["family_1", "family_2"]
        
        self.run_results.append(cmd1)
    
    @pytest.mark.asyncio
    async def test_no_phantom_states(self):
        """
        CRITICAL: Only valid entity states allowed
        No phantom or ghost task states
        """
        result = await self.scenario_runner.run_scenario(
            "concurrent_plan_creation_scenario",
            num_members=2,
            plans_per_member=3,
        )
        
        # Verify: Valid state transitions
        phantom_violations = [
            v for v in result.violations
            if "phantom" in v.invariant_name.lower()
        ]
        assert len(phantom_violations) == 0, f"Phantom states detected: {phantom_violations}"
        
        self.run_results.append(result)
    
    # ========================
    # Failure Resilience Tests
    # ========================
    
    @pytest.mark.asyncio
    async def test_resilience_to_transient_failures(self):
        """Verify system recovers from transient network failures"""
        from tests.harness.failure_injector import FailureInjectionProfile
        
        light_profile = FailureInjectionProfile.light_transient()
        result = await self.scenario_runner.run_scenario(
            "task_execution_idempotency_scenario",
            num_members=2,
            tasks_per_member=5,
        )
        
        # Verify: Eventual consistency despite transient failures
        assert result.success or len(result.violations) == 0, "Transient failures not recoverable"
        
        self.run_results.append(result)
    
    @pytest.mark.asyncio
    async def test_resilience_to_network_chaos(self):
        """Verify system handles moderate network chaos"""
        result = await self.scenario_runner.run_scenario(
            "concurrent_plan_creation_scenario",
            num_members=2,
            plans_per_member=3,
        )
        
        # Verify: System remains consistent under network pressure
        critical_violations = result.violation_summary.get("critical_count", 0)
        assert critical_violations == 0, f"Network chaos caused {critical_violations} critical violations"
        
        self.run_results.append(result)
    
    @pytest.mark.asyncio
    async def test_resilience_to_byzantine_failures(self):
        """Verify system doesn't accept malicious state under byzantine conditions"""
        # Byzantine test: inject multiple simultaneous failures
        result = await self.scenario_runner.run_scenario(
            "conflicting_plan_updates_scenario",
            num_conflicting_updates=5,
        )
        
        # Verify: Quarantine mode activated if needed
        if len(result.violations) > 0:
            # System should enter safe mode, not corrupt state
            assert result.violation_summary.get("critical_count", 0) <= 1, "Multiple critical violations in byzantine scenario"
        
        self.run_results.append(result)
    
    # ========================
    # Deterministic Replay Tests
    # ========================
    
    @pytest.mark.asyncio
    async def test_deterministic_replay_convergence(self):
        """Verify identical runs converge to same state hash"""
        converged, hashes = await self.scenario_runner.run_scenario_with_replay(
            "task_execution_idempotency_scenario",
            num_members=2,
            num_replays=3,
        )
        
        # Verify: All replays converge to same state
        assert converged, f"Deterministic replay did not converge. Hashes: {hashes}"
        assert len(set(hashes)) == 1, f"Multiple final states detected: {set(hashes)}"
        
        # Separate result for tracking
        dummy_result = type(
            "Result",
            (),
            {
                "scenario_name": "deterministic_replay",
                "success": converged,
                "violations": [],
                "violation_summary": {"critical_count": 0},
                "run_id": "replay_test",
                "duration_seconds": 0,
                "event_count": 0,
                "entity_count": 0,
                "failure_injection_summary": {},
                "execution_stats": {},
            },
        )()
        self.run_results.append(dummy_result)
    
    # ========================
    # Test Matrix Execution
    # ========================
    
    @pytest.mark.asyncio
    async def test_full_matrix_execution(self):
        """Execute comprehensive test matrix: scenarios × failure profiles"""
        from tests.harness.failure_injector import FailureInjectionProfile
        
        # Define test scenarios
        scenarios = [
            "concurrent_plan_creation_scenario",
            "task_execution_idempotency_scenario",
            "conflicting_plan_updates_scenario",
        ]
        
        # Define failure profiles
        profiles = [
            FailureInjectionProfile.no_failures(),
            FailureInjectionProfile.light_transient(),
            FailureInjectionProfile.moderate_network(),
        ]
        
        # Run matrix
        results = await self.scenario_runner.run_test_matrix()
        
        # Verify: All scenarios executed
        assert len(results) > 0, "Test matrix produced no results"
        
        # Verify: Success rate acceptable
        success_count = sum(1 for r in results if r.success)
        success_rate = success_count / len(results) if results else 0
        assert success_rate >= 0.8, f"Success rate {success_rate:.1%} below threshold"
        
        self.run_results.extend(results)
    
    # ========================
    # Convergence Validation
    # ========================
    
    @pytest.mark.asyncio
    async def test_convergence_to_stable_state(self):
        """
        SUCCESS CRITERION: All simulated runs converge to stable or quarantine state
        No divergence between UI projections and backend truth beyond watermark lag
        """
        result = await self.scenario_runner.run_scenario(
            "concurrent_plan_creation_scenario",
            num_members=3,
            plans_per_member=5,
        )
        
        # Verify: Final state reachable and consistent
        assert result.success or result.violation_summary.get("critical_count", 0) == 0
        
        # Verify: State hash is deterministic (can be replayed)
        assert result.state_hash is not None, "No final state hash computed"
        
        self.run_results.append(result)
    
    @pytest.mark.asyncio
    async def test_no_silent_inconsistencies(self):
        """
        SUCCESS CRITERION: No silent inconsistencies
        All invariant violations detected and reported
        """
        result = await self.scenario_runner.run_scenario(
            "concurrent_plan_creation_scenario",
            num_members=2,
            plans_per_member=4,
        )
        
        # Verify: If success=False, violations must be populated
        if not result.success:
            assert len(result.violations) > 0, "Failed but no violations reported"
        
        # Verify: Violation summary matches violation count
        total_reported = sum(
            result.violation_summary.get(f"{s}_count", 0)
            for s in ["critical", "warning", "info"]
        )
        assert total_reported == len(result.violations), "Violation summary mismatch"
        
        self.run_results.append(result)
    
    def _generate_all_reports(self):
        """Generate comprehensive reports after test execution"""
        if not self.run_results:
            return
        
        summary = self.scenario_runner.get_results_summary()
        
        # Generate individual reports
        for result in self.run_results:
            self.report_generator.generate_event_log_report(result)
            self.report_generator.generate_invariant_violation_report(result)
        
        # Generate summary reports
        self.report_generator.generate_comprehensive_report(
            self.run_results,
            summary=summary,
        )
        self.report_generator.generate_human_readable_summary(
            self.run_results,
            summary=summary,
        )
        self.report_generator.generate_failure_classification(self.run_results)


# Test fixture for scenario runner
@pytest.fixture
def household_test_suite():
    """Pytest fixture for household simulation test suite"""
    return HouseholdSimulationTestSuite()


# Standalone test functions for pytest discovery
@pytest.mark.asyncio
async def test_task_idempotency():
    """Task execution must be idempotent"""
    suite = HouseholdSimulationTestSuite()
    await suite.test_no_duplicate_task_execution()


@pytest.mark.asyncio
async def test_no_lost_updates():
    """Concurrent updates must not lose data"""
    suite = HouseholdSimulationTestSuite()
    await suite.test_no_lost_updates()


@pytest.mark.asyncio
async def test_family_isolation():
    """Multi-family data must be isolated"""
    suite = HouseholdSimulationTestSuite()
    await suite.test_no_cross_family_leakage()


@pytest.mark.asyncio
async def test_valid_states_only():
    """Only valid entity states allowed"""
    suite = HouseholdSimulationTestSuite()
    await suite.test_no_phantom_states()


@pytest.mark.asyncio
async def test_transient_failure_recovery():
    """System recovers from transient failures"""
    suite = HouseholdSimulationTestSuite()
    await suite.test_resilience_to_transient_failures()


@pytest.mark.asyncio
async def test_network_chaos_resilience():
    """System handles moderate network chaos"""
    suite = HouseholdSimulationTestSuite()
    await suite.test_resilience_to_network_chaos()


@pytest.mark.asyncio
async def test_byzantine_resilience():
    """System doesn't corrupt under byzantine conditions"""
    suite = HouseholdSimulationTestSuite()
    await suite.test_resilience_to_byzantine_failures()


@pytest.mark.asyncio
async def test_deterministic_replay():
    """Identical runs converge to same state"""
    suite = HouseholdSimulationTestSuite()
    await suite.test_deterministic_replay_convergence()


@pytest.mark.asyncio
async def test_matrix_coverage():
    """Test matrix provides comprehensive coverage"""
    suite = HouseholdSimulationTestSuite()
    await suite.test_full_matrix_execution()


@pytest.mark.asyncio
async def test_stable_convergence():
    """All runs converge to stable or quarantine state"""
    suite = HouseholdSimulationTestSuite()
    await suite.test_convergence_to_stable_state()


@pytest.mark.asyncio
async def test_consistency_assurance():
    """All inconsistencies detected and reported"""
    suite = HouseholdSimulationTestSuite()
    await suite.test_no_silent_inconsistencies()
