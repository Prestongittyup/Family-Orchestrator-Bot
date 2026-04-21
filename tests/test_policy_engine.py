from __future__ import annotations

import ast
from pathlib import Path

from policy_engine.itinerary_generator import generate_daily_itinerary
from policy_engine.memory_store import PolicyMemoryStore
from policy_engine.policy_engine import PolicyRecommendationEngine


def _parse_time_block(value: str) -> tuple[str, str]:
    start, end = value.split("-", 1)
    return start, end


def test_memory_determinism() -> None:
    store = PolicyMemoryStore()
    left = store.build_memory_snapshot().model_dump()
    right = store.build_memory_snapshot().model_dump()

    assert left == right


def test_policy_generation_validity() -> None:
    engine = PolicyRecommendationEngine()
    memory_snapshot = engine.memory_store.build_memory_snapshot()
    policy_summary = engine.build_policy_summary(memory_snapshot)

    policy_types = {policy.policy_type for policy in policy_summary.policies}
    assert any("priority" in policy_type for policy_type in policy_types)
    assert any("scheduling" in policy_type for policy_type in policy_types)
    assert any("conflict" in policy_type for policy_type in policy_types)


def test_itinerary_structure_integrity() -> None:
    store = PolicyMemoryStore()
    itinerary = generate_daily_itinerary(store.build_memory_snapshot())

    blocks = itinerary.recommended_itinerary
    assert len(blocks) > 0

    previous_end = None
    starts = []
    for block in blocks:
        assert set(block.model_dump().keys()) == {"time_block", "event", "reason", "priority"}
        start, end = _parse_time_block(block.time_block)
        starts.append(start)
        assert start < end
        if previous_end is not None:
            assert start >= previous_end
        previous_end = end

    assert starts == sorted(starts)


def test_isolation_guarantee() -> None:
    policy_dir = Path(__file__).resolve().parent.parent / "policy_engine"
    forbidden_prefixes = (
        "apps.api.integration_core",
        "integration_core",
        "tests.simulation",
        "tests.evaluation",
    )

    for file_path in policy_dir.glob("*.py"):
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


def test_non_execution_guarantee() -> None:
    policy_dir = Path(__file__).resolve().parent.parent / "policy_engine"
    forbidden_network_imports = {"requests", "httpx", "urllib", "subprocess", "socket"}
    forbidden_calendar_terms = (
        "calendar.events.insert",
        "calendar.events.update",
        "calendar.events.delete",
        "write_calendar",
        "create_calendar_event",
    )

    for file_path in policy_dir.glob("*.py"):
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports = [alias.name.split(".", 1)[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                imports = [(node.module or "").split(".", 1)[0]]
            else:
                imports = []
            for name in imports:
                assert name not in forbidden_network_imports, f"Forbidden network import in {file_path.name}: {name}"

        for term in forbidden_calendar_terms:
            assert term not in source, f"Forbidden calendar write term in {file_path.name}: {term}"

        if file_path.name != "memory_store.py":
            assert "write_text(" not in source, f"State mutation outside memory_store in {file_path.name}"