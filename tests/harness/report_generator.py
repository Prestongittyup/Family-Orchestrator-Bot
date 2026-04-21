"""
Report Generator - Comprehensive analysis and reporting

Produces:
  - Simulation event log
  - Failure injection timeline
  - Invariant violation report
  - Final state hash comparison report
  - Pass/fail classification per scenario
  - JSON and human-readable formats
"""

import json
from dataclasses import asdict
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path


class ReportGenerator:
    """Generates comprehensive test reports"""
    
    def __init__(self, output_dir: Optional[Path] = None):
        self.output_dir = output_dir or Path("test_reports")
        self.output_dir.mkdir(exist_ok=True)
    
    def generate_event_log_report(
        self,
        run_result,
        filename: Optional[str] = None,
    ) -> str:
        """
        Generate simulation event log report
        
        Output: Detailed log of all simulation events in chronological order
        """
        if filename is None:
            filename = f"{run_result.run_id}_event_log.json"
        
        report = {
            "run_id": run_result.run_id,
            "scenario_name": run_result.scenario_name,
            "generated_at": datetime.utcnow().isoformat(),
            "event_count": len(run_result.event_log),
            "events": run_result.event_log,
        }
        
        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        return str(filepath)
    
    def generate_failure_injection_report(
        self,
        run_result,
        failure_summary: Dict,
        filename: Optional[str] = None,
    ) -> str:
        """
        Generate failure injection timeline report
        
        Output: Timeline of all injected failures and their impact
        """
        if filename is None:
            filename = f"{run_result.run_id}_failure_timeline.json"
        
        report = {
            "run_id": run_result.run_id,
            "scenario_name": run_result.scenario_name,
            "generated_at": datetime.utcnow().isoformat(),
            "failure_summary": failure_summary,
            "execution_stats": run_result.execution_stats,
            "success_rate": (
                run_result.execution_stats["successful_commands"] / 
                run_result.execution_stats["total_commands"]
                if run_result.execution_stats["total_commands"] > 0
                else 0.0
            ),
        }
        
        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        return str(filepath)
    
    def generate_invariant_violation_report(
        self,
        run_result,
        filename: Optional[str] = None,
    ) -> str:
        """
        Generate invariant violation report
        
        Output: Detailed analysis of all violations detected
        """
        if filename is None:
            filename = f"{run_result.run_id}_invariant_violations.json"
        
        violations_data = [v.to_dict() for v in run_result.violations]
        
        report = {
            "run_id": run_result.run_id,
            "scenario_name": run_result.scenario_name,
            "generated_at": datetime.utcnow().isoformat(),
            "summary": run_result.violation_summary,
            "violations": violations_data,
            "pass": run_result.success,
        }
        
        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        return str(filepath)
    
    def generate_state_hash_comparison_report(
        self,
        convergence_results: Dict,
        filename: Optional[str] = None,
    ) -> str:
        """
        Generate state hash comparison report
        
        Output: Verify deterministic convergence across replay runs
        """
        if filename is None:
            filename = "state_hash_comparison.json"
        
        report = {
            "generated_at": datetime.utcnow().isoformat(),
            "convergence_analysis": {
                "scenario": convergence_results.get("scenario_name"),
                "total_replays": convergence_results.get("replay_count"),
                "converged": convergence_results.get("converged"),
                "unique_hashes": convergence_results.get("unique_state_hashes"),
                "convergence_rate": convergence_results.get("convergence_rate"),
            },
            "state_hashes": convergence_results.get("state_hashes", []),
        }
        
        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        return str(filepath)
    
    def generate_comprehensive_report(
        self,
        run_results: List,
        matrix_results: Optional[Dict] = None,
        summary: Optional[Dict] = None,
        filename: str = "comprehensive_test_report.json",
    ) -> str:
        """
        Generate comprehensive test report combining all results
        
        Output: Full summary of test matrix execution
        """
        # Prepare run data
        runs_data = []
        for result in run_results:
            runs_data.append({
                "run_id": result.run_id,
                "scenario_name": result.scenario_name,
                "success": result.success,
                "duration_seconds": result.duration_seconds,
                "event_count": result.event_count,
                "entity_count": result.entity_count,
                "violations_count": len(result.violations),
                "violation_summary": result.violation_summary,
                "failure_injection_summary": result.failure_injection_summary,
                "execution_stats": result.execution_stats,
            })
        
        report = {
            "generated_at": datetime.utcnow().isoformat(),
            "summary": summary or {},
            "test_matrix": matrix_results or {},
            "run_results": runs_data,
            "statistics": {
                "total_runs": len(run_results),
                "successful_runs": sum(1 for r in run_results if r.success),
                "failed_runs": sum(1 for r in run_results if not r.success),
                "total_violations": sum(len(r.violations) for r in run_results),
                "critical_violations": sum(
                    r.violation_summary.get("critical_count", 0)
                    for r in run_results
                ),
            },
        }
        
        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        return str(filepath)
    
    def generate_human_readable_summary(
        self,
        run_results: List,
        summary: Optional[Dict] = None,
        filename: str = "test_summary.txt",
    ) -> str:
        """
        Generate human-readable test summary
        
        Output: Plain text format for easy reading
        """
        lines = []
        lines.append("=" * 80)
        lines.append("HPAL SYSTEM VALIDATION TEST REPORT")
        lines.append("=" * 80)
        lines.append(f"\nGenerated: {datetime.utcnow().isoformat()}")
        lines.append(f"Total Runs: {len(run_results)}")
        
        if summary:
            lines.append(f"\nSUMMARY:")
            lines.append(f"  Success Rate: {summary.get('success_rate', 0):.1%}")
            lines.append(f"  Total Violations: {summary.get('total_violations', 0)}")
            lines.append(f"  Critical Violations: {summary.get('critical_violations', 0)}")
            lines.append(f"  Avg Duration: {summary.get('avg_duration_seconds', 0):.2f}s")
        
        lines.append(f"\nDETAILED RESULTS:")
        lines.append("-" * 80)
        
        for result in run_results:
            lines.append(f"\nScenario: {result.scenario_name}")
            lines.append(f"  Run ID: {result.run_id}")
            lines.append(f"  Status: {'✓ PASS' if result.success else '✗ FAIL'}")
            lines.append(f"  Duration: {result.duration_seconds:.2f}s")
            lines.append(f"  Entities Created: {result.entity_count}")
            lines.append(f"  Events Logged: {result.event_count}")
            lines.append(f"  Violations: {len(result.violations)}")
            
            if result.violations:
                lines.append(f"    Violation Details:")
                for v in result.violations:
                    lines.append(f"      - [{v.severity.upper()}] {v.invariant_name}")
                    lines.append(f"        {v.description}")
            
            lines.append(f"  Failure Injections: {result.failure_injection_summary.get('total_injected', 0)}")
            lines.append(f"  Command Stats:")
            lines.append(f"    - Total: {result.execution_stats.get('total_commands', 0)}")
            lines.append(f"    - Successful: {result.execution_stats.get('successful_commands', 0)}")
            lines.append(f"    - Failed: {result.execution_stats.get('failed_commands', 0)}")
            lines.append(f"    - Retried: {result.execution_stats.get('retried_commands', 0)}")
            lines.append(f"    - Duplicate Detections: {result.execution_stats.get('duplicate_detections', 0)}")
        
        lines.append("\n" + "=" * 80)
        lines.append("END OF REPORT")
        lines.append("=" * 80)
        
        content = "\n".join(lines)
        
        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            f.write(content)
        
        return str(filepath)
    
    # ------------------------------------------------------------------
    # XAI explanation reports
    # ------------------------------------------------------------------

    def generate_explanation_log(
        self,
        explanations: List,
        run_id: str,
        filename: Optional[str] = None,
    ) -> str:
        """
        Generate explanation_log.json — full serialised list of every
        ExplanationSchema produced during the run, in emission order.
        """
        if filename is None:
            filename = f"{run_id}_explanation_log.json"

        report = {
            "run_id": run_id,
            "generated_at": datetime.utcnow().isoformat(),
            "explanation_count": len(explanations),
            "explanations": [
                {
                    "explanation_id": e.explanation_id,
                    "entity_id": e.entity_id,
                    "entity_name": e.entity_name,
                    "entity_type": e.entity_type.value,
                    "reason_code": e.reason_code.value,
                    "explanation_text": e.explanation_text,
                    "change_type": e.change_type.value,
                    "trigger_type": e.trigger_type.value,
                    "initiated_by": e.initiated_by.value,
                    "family_id": e.family_id,
                    "idempotency_key": e.idempotency_key,
                    "timestamp": e.timestamp.isoformat() if e.timestamp else None,
                }
                for e in explanations
            ],
        }

        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            json.dump(report, f, indent=2, default=str)

        return str(filepath)

    def generate_explanation_duplicates(
        self,
        explanations: List,
        run_id: str,
        filename: Optional[str] = None,
    ) -> str:
        """
        Generate explanation_duplicates.json — any explanations that share
        an idempotency_key (should be empty in a passing run).
        """
        if filename is None:
            filename = f"{run_id}_explanation_duplicates.json"

        seen: Dict[str, str] = {}
        duplicates = []
        for exp in explanations:
            if exp.idempotency_key in seen:
                duplicates.append({
                    "idempotency_key": exp.idempotency_key,
                    "first_explanation_id": seen[exp.idempotency_key],
                    "duplicate_explanation_id": exp.explanation_id,
                    "entity_id": exp.entity_id,
                    "reason_code": exp.reason_code.value,
                })
            else:
                seen[exp.idempotency_key] = exp.explanation_id

        report = {
            "run_id": run_id,
            "generated_at": datetime.utcnow().isoformat(),
            "duplicate_count": len(duplicates),
            "duplicates": duplicates,
            "pass": len(duplicates) == 0,
        }

        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            json.dump(report, f, indent=2, default=str)

        return str(filepath)

    def generate_explanation_missing(
        self,
        state,
        run_id: str,
        filename: Optional[str] = None,
    ) -> str:
        """
        Generate explanation_missing.json — compares state mutation count
        against explanation count to surface gaps.
        """
        if filename is None:
            filename = f"{run_id}_explanation_missing.json"

        mutations = [
            m for m in getattr(state, "state_mutations", [])
            if m.get("type") in {"entity_created", "entity_updated"}
        ]
        xai_explanations = getattr(state, "xai_explanations", [])
        mutation_count = len(mutations)
        explanation_count = len(xai_explanations)
        missing_count = max(0, mutation_count - explanation_count)

        report = {
            "run_id": run_id,
            "generated_at": datetime.utcnow().isoformat(),
            "mutation_count": mutation_count,
            "explanation_count": explanation_count,
            "missing_count": missing_count,
            "pass": missing_count == 0,
        }

        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            json.dump(report, f, indent=2, default=str)

        return str(filepath)

    def generate_explanation_replay_diff(
        self,
        explanations_run1: List,
        explanations_run2: List,
        run_id: str,
        filename: Optional[str] = None,
    ) -> str:
        """
        Generate explanation_replay_diff.json — diff of explanation IDs and
        texts across two replay runs.  Both lists should be empty for a
        deterministic system (identical IDs + texts in every replay).
        """
        if filename is None:
            filename = f"{run_id}_explanation_replay_diff.json"

        ids1 = sorted(e.explanation_id for e in explanations_run1)
        ids2 = sorted(e.explanation_id for e in explanations_run2)
        texts1 = sorted(e.explanation_text for e in explanations_run1)
        texts2 = sorted(e.explanation_text for e in explanations_run2)

        ids1_set = set(ids1)
        ids2_set = set(ids2)
        added_ids   = [i for i in ids2 if i not in ids1_set]
        removed_ids = [i for i in ids1 if i not in ids2_set]
        text_diffs  = [t for t in texts2 if t not in set(texts1)]

        deterministic = (ids1 == ids2) and (texts1 == texts2)

        report = {
            "run_id": run_id,
            "generated_at": datetime.utcnow().isoformat(),
            "run1_explanation_count": len(explanations_run1),
            "run2_explanation_count": len(explanations_run2),
            "added_ids": added_ids,
            "removed_ids": removed_ids,
            "text_diffs": text_diffs,
            "deterministic": deterministic,
            "pass": deterministic,
        }

        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            json.dump(report, f, indent=2, default=str)

        return str(filepath)

    def generate_failure_classification(
        self,
        run_results: List,
        filename: str = "failure_classification.json",
    ) -> str:
        """
        Generate pass/fail classification per scenario
        
        Output: Detailed categorization of failures
        """
        classifications = {
            "pass": [],
            "fail_invariant": [],
            "fail_quarantine": [],
            "fail_undefined": [],
        }
        
        for result in run_results:
            classification = {
                "scenario_name": result.scenario_name,
                "run_id": result.run_id,
                "violations": len(result.violations),
                "critical_violations": result.violation_summary.get("critical_count", 0),
            }
            
            if result.success:
                classifications["pass"].append(classification)
            elif result.violation_summary.get("critical_count", 0) > 0:
                classifications["fail_invariant"].append(classification)
            elif result.success == False and "quarantine_mode" in result:
                classifications["fail_quarantine"].append(classification)
            else:
                classifications["fail_undefined"].append(classification)
        
        report = {
            "generated_at": datetime.utcnow().isoformat(),
            "classifications": classifications,
            "summary": {
                "total_scenarios": len(run_results),
                "passed": len(classifications["pass"]),
                "failed_invariant": len(classifications["fail_invariant"]),
                "failed_quarantine": len(classifications["fail_quarantine"]),
                "failed_undefined": len(classifications["fail_undefined"]),
            },
        }
        
        filepath = self.output_dir / filename
        with open(filepath, "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        return str(filepath)
