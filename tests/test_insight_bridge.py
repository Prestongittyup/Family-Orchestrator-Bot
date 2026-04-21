from __future__ import annotations

import ast
from pathlib import Path

from insights.insight_engine import InsightEngine
from insights.pattern_analyzer import detect_patterns


def _structure_of(value):
    if isinstance(value, dict):
        return {key: _structure_of(value[key]) for key in sorted(value.keys())}
    if isinstance(value, list):
        if not value:
            return []
        return [_structure_of(value[0])]
    return type(value).__name__


def test_artifact_parsing_integrity() -> None:
    engine = InsightEngine()

    evaluation = engine.load_evaluation_artifact()
    simulation = engine.load_simulation_artifact()
    operational = engine.load_operational_artifact()

    assert len(evaluation.scenarios) > 0
    assert simulation.simulation_id
    assert operational.artifact == "operational_mode_report"


def test_pattern_detection() -> None:
    engine = InsightEngine()
    patterns = detect_patterns(
        engine.load_evaluation_artifact(),
        engine.load_simulation_artifact(),
        engine.load_operational_artifact(),
    )

    assert any(pattern.frequency > 0 and "evaluation" in pattern.evidence_sources for pattern in patterns)
    assert any(pattern.frequency > 0 and "simulation" in pattern.evidence_sources for pattern in patterns)
    assert any(
        pattern.frequency > 0 and {"evaluation", "simulation", "operational"}.issubset(set(pattern.evidence_sources))
        for pattern in patterns
    )


def test_isolation_guarantee() -> None:
    insights_dir = Path(__file__).resolve().parent.parent / "insights"
    forbidden_prefixes = (
        "apps.api.integration_core",
        "tests.simulation",
        "tests.evaluation",
    )

    for file_path in insights_dir.glob("*.py"):
        tree = ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue

            for name in names:
                assert not name.startswith(forbidden_prefixes), f"Forbidden import found in {file_path.name}: {name}"


def test_deterministic_output_structure() -> None:
    engine = InsightEngine()

    first = engine.build_response().model_dump()
    second = engine.build_response().model_dump()

    assert _structure_of(first) == _structure_of(second)
    assert set(first.keys()) == {"timestamp", "insights", "system_health_summary", "recommendations"}