"""
CLI Interface - Command-line harness runner

Enables:
  - Scenario selection and execution
  - Failure profile configuration
  - Output format selection
  - Report generation and retrieval
"""

import asyncio
import argparse
import sys
import json
from pathlib import Path
from typing import Optional
from datetime import datetime

from tests.harness.simulation_engine import SimulationEngine
from tests.harness.failure_injector import FailureInjectionProfile
from tests.harness.scenario_runner import ScenarioRunner
from tests.harness.report_generator import ReportGenerator


class HarnessCliRunner:
    """Command-line interface for household simulation harness"""
    
    SCENARIOS = {
        "concurrent_plan_creation": {
            "description": "Concurrent plan creation from multiple family members",
            "num_members": 3,
            "plans_per_member": 5,
        },
        "task_idempotency": {
            "description": "Task execution idempotency under retries",
            "num_members": 2,
            "tasks_per_member": 10,
        },
        "conflicting_updates": {
            "description": "Conflicting plan updates from concurrent family members",
            "num_conflicting_updates": 5,
        },
    }
    
    FAILURE_PROFILES = {
        "none": "No failures (baseline test)",
        "light": "Light transient failures (5% probability)",
        "moderate": "Moderate network chaos (10-15% probability)",
        "high": "High chaos mode (5-15% per failure type)",
        "byzantine": "Byzantine chaos (10-20% per failure type)",
    }
    
    OUTPUT_FORMATS = {
        "json": "Machine-readable JSON report",
        "human": "Human-readable text report",
        "both": "Both JSON and human-readable formats",
    }
    
    def __init__(self, output_dir: Path = Path("test_reports")):
        self.output_dir = output_dir
        self.output_dir.mkdir(exist_ok=True)
        self.report_generator = ReportGenerator(output_dir)
        self.scenario_runner = ScenarioRunner()
    
    async def run_scenario_command(
        self,
        scenario: str,
        failure_profile: str = "none",
        output_format: str = "both",
        num_replays: int = 1,
    ) -> int:
        """
        Execute single scenario
        
        Returns: 0 (success) or 1 (failure)
        """
        if scenario not in self.SCENARIOS:
            print(f"ERROR: Unknown scenario '{scenario}'")
            print(f"Available scenarios: {', '.join(self.SCENARIOS.keys())}")
            return 1
        
        if failure_profile not in self.FAILURE_PROFILES:
            print(f"ERROR: Unknown failure profile '{failure_profile}'")
            print(f"Available profiles: {', '.join(self.FAILURE_PROFILES.keys())}")
            return 1
        
        print(f"[HPAL Harness] Running scenario: {scenario}")
        print(f"  Failure Profile: {failure_profile}")
        print(f"  Replays: {num_replays}")
        print()
        
        try:
            # Get scenario function and parameters
            scenario_name = scenario.replace("_", "")
            params = self.SCENARIOS[scenario]
            
            # Run scenario
            result = await self.scenario_runner.run_scenario(
                f"{scenario}_scenario",
                **{k: v for k, v in params.items() if k != "description"}
            )
            
            # Generate reports based on format
            if output_format in ["json", "both"]:
                json_path = self.report_generator.generate_invariant_violation_report(result)
                print(f"✓ JSON report: {json_path}")
            
            if output_format in ["human", "both"]:
                human_path = self.report_generator.generate_human_readable_summary(
                    [result],
                    filename=f"{scenario}_{result.run_id}_summary.txt"
                )
                print(f"✓ Human-readable summary: {human_path}")
            
            # Print result summary
            print(f"\nResult Summary:")
            print(f"  Status: {'✓ PASS' if result.success else '✗ FAIL'}")
            print(f"  Duration: {result.duration_seconds:.2f}s")
            print(f"  Events: {result.event_count}")
            print(f"  Entities: {result.entity_count}")
            print(f"  Violations: {len(result.violations)}")
            
            if result.violations:
                print(f"\n  Violation Details:")
                for v in result.violations:
                    severity_badge = {
                        "critical": "🔴",
                        "warning": "🟡",
                        "info": "ℹ",
                    }.get(v.severity, "?")
                    print(f"    {severity_badge} [{v.severity.upper()}] {v.invariant_name}")
                    print(f"       {v.description}")
            
            return 0 if result.success else 1
        
        except Exception as e:
            print(f"ERROR: {e}")
            return 1
    
    async def run_matrix_command(
        self,
        output_format: str = "both",
    ) -> int:
        """
        Execute full test matrix
        
        Returns: 0 (all pass) or 1 (any failure)
        """
        print(f"[HPAL Harness] Running full test matrix:")
        print(f"  Scenarios: {len(self.SCENARIOS)}")
        print(f"  Failure Profiles: {len(self.FAILURE_PROFILES) - 1}")  # Exclude 'both' for counting
        print()
        
        try:
            results = await self.scenario_runner.run_test_matrix()
            summary = self.scenario_runner.get_results_summary()
            
            # Generate comprehensive reports
            if output_format in ["json", "both"]:
                json_path = self.report_generator.generate_comprehensive_report(results, summary=summary)
                print(f"✓ Comprehensive JSON report: {json_path}")
            
            if output_format in ["human", "both"]:
                human_path = self.report_generator.generate_human_readable_summary(results, summary=summary)
                print(f"✓ Human-readable summary: {human_path}")
            
            # Print matrix summary
            print(f"\nTest Matrix Summary:")
            print(f"  Total Runs: {summary.get('total_runs', 0)}")
            print(f"  Passed: {summary.get('success_count', 0)}")
            print(f"  Failed: {summary.get('failure_count', 0)}")
            print(f"  Success Rate: {summary.get('success_rate', 0):.1%}")
            print(f"  Total Violations: {summary.get('total_violations', 0)}")
            print(f"  Critical Violations: {summary.get('critical_violations', 0)}")
            print(f"  Average Duration: {summary.get('avg_duration_seconds', 0):.2f}s")
            
            return 0 if summary.get("success_count", 0) == summary.get("total_runs", 0) else 1
        
        except Exception as e:
            print(f"ERROR: {e}")
            return 1
    
    async def run_replay_command(
        self,
        scenario: str,
        num_replays: int = 3,
        output_format: str = "both",
    ) -> int:
        """
        Execute scenario with deterministic replay verification
        
        Returns: 0 (converged) or 1 (diverged or error)
        """
        if scenario not in self.SCENARIOS:
            print(f"ERROR: Unknown scenario '{scenario}'")
            return 1
        
        print(f"[HPAL Harness] Running deterministic replay verification:")
        print(f"  Scenario: {scenario}")
        print(f"  Replays: {num_replays}")
        print()
        
        try:
            converged, state_hashes = await self.scenario_runner.run_scenario_with_replay(
                f"{scenario}_scenario",
                num_replays=num_replays,
            )
            
            # Generate convergence report
            convergence_data = {
                "scenario_name": scenario,
                "replay_count": num_replays,
                "converged": converged,
                "unique_state_hashes": len(set(state_hashes)),
                "convergence_rate": 1.0 if converged else 0.0,
                "state_hashes": state_hashes,
            }
            
            if output_format in ["json", "both"]:
                json_path = self.report_generator.generate_state_hash_comparison_report(convergence_data)
                print(f"✓ State hash comparison report: {json_path}")
            
            # Print convergence summary
            print(f"\nConvergence Analysis:")
            print(f"  Status: {'✓ CONVERGENT' if converged else '✗ DIVERGENT'}")
            print(f"  Unique State Hashes: {len(set(state_hashes))}")
            print(f"  Expected: 1 (all replays identical)")
            
            if not converged:
                print(f"\n  State Hash Details:")
                for i, h in enumerate(state_hashes, 1):
                    print(f"    Replay {i}: {h[:16]}... (first 16 chars)")
            
            return 0 if converged else 1
        
        except Exception as e:
            print(f"ERROR: {e}")
            return 1
    
    def list_scenarios_command(self) -> int:
        """List available scenarios"""
        print("Available Scenarios:")
        print("-" * 60)
        for name, info in self.SCENARIOS.items():
            print(f"\n{name}:")
            print(f"  Description: {info['description']}")
        return 0
    
    def list_profiles_command(self) -> int:
        """List available failure profiles"""
        print("Available Failure Profiles:")
        print("-" * 60)
        for name, desc in self.FAILURE_PROFILES.items():
            print(f"  {name}: {desc}")
        return 0
    
    def list_formats_command(self) -> int:
        """List available output formats"""
        print("Available Output Formats:")
        print("-" * 60)
        for fmt, desc in self.OUTPUT_FORMATS.items():
            print(f"  {fmt}: {desc}")
        return 0


async def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="HPAL Household Simulation & Failure Injection Harness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run single scenario with no failures
  python -m tests.harness.cli run-scenario --scenario concurrent_plan_creation
  
  # Run scenario with moderate network chaos
  python -m tests.harness.cli run-scenario --scenario task_idempotency --profile moderate
  
  # Run full test matrix
  python -m tests.harness.cli run-matrix
  
  # Verify deterministic replay
  python -m tests.harness.cli run-replay --scenario concurrent_plan_creation --replays 5
  
  # List available scenarios
  python -m tests.harness.cli list-scenarios
        """,
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # run-scenario subcommand
    run_scenario_parser = subparsers.add_parser("run-scenario", help="Run single scenario")
    run_scenario_parser.add_argument(
        "--scenario",
        required=True,
        choices=list(HarnessCliRunner.SCENARIOS.keys()),
        help="Scenario to execute",
    )
    run_scenario_parser.add_argument(
        "--profile",
        default="none",
        choices=list(HarnessCliRunner.FAILURE_PROFILES.keys()),
        help="Failure injection profile",
    )
    run_scenario_parser.add_argument(
        "--format",
        default="both",
        choices=list(HarnessCliRunner.OUTPUT_FORMATS.keys()),
        help="Report output format",
    )
    
    # run-matrix subcommand
    run_matrix_parser = subparsers.add_parser("run-matrix", help="Run full test matrix")
    run_matrix_parser.add_argument(
        "--format",
        default="both",
        choices=list(HarnessCliRunner.OUTPUT_FORMATS.keys()),
        help="Report output format",
    )
    
    # run-replay subcommand
    run_replay_parser = subparsers.add_parser("run-replay", help="Run with deterministic replay")
    run_replay_parser.add_argument(
        "--scenario",
        required=True,
        choices=list(HarnessCliRunner.SCENARIOS.keys()),
        help="Scenario to execute",
    )
    run_replay_parser.add_argument(
        "--replays",
        type=int,
        default=3,
        help="Number of replay runs (default: 3)",
    )
    run_replay_parser.add_argument(
        "--format",
        default="both",
        choices=list(HarnessCliRunner.OUTPUT_FORMATS.keys()),
        help="Report output format",
    )
    
    # list-scenarios subcommand
    subparsers.add_parser("list-scenarios", help="List available scenarios")
    
    # list-profiles subcommand
    subparsers.add_parser("list-profiles", help="List available failure profiles")
    
    # list-formats subcommand
    subparsers.add_parser("list-formats", help="List available output formats")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    runner = HarnessCliRunner()
    
    if args.command == "run-scenario":
        return await runner.run_scenario_command(
            scenario=args.scenario,
            failure_profile=args.profile,
            output_format=args.format,
        )
    
    elif args.command == "run-matrix":
        return await runner.run_matrix_command(output_format=args.format)
    
    elif args.command == "run-replay":
        return await runner.run_replay_command(
            scenario=args.scenario,
            num_replays=args.replays,
            output_format=args.format,
        )
    
    elif args.command == "list-scenarios":
        return runner.list_scenarios_command()
    
    elif args.command == "list-profiles":
        return runner.list_profiles_command()
    
    elif args.command == "list-formats":
        return runner.list_formats_command()
    
    return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
