from __future__ import annotations

import ast
import inspect

import pytest

from app.api import notifications as notifications_api
from app.api import read_model_shared as shared
from app.api import reminders as reminders_api
from app.api import schedule as schedule_api
from app.api import tasks as tasks_api
from tests.governance.contract_kernel import READ_MODEL_DOMAIN_SPECS, READ_MODEL_SHARED_MODULE


_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate, pytest.mark.integration]


MODULES = {
    "app.api.read_model_shared": shared,
    "app.api.tasks": tasks_api,
    "app.api.schedule": schedule_api,
    "app.api.reminders": reminders_api,
    "app.api.notifications": notifications_api,
}
SHARED_MODULE = READ_MODEL_SHARED_MODULE
DOMAIN_MODULES = {f"app.api.{module_name}" for module_name, *_ in READ_MODEL_DOMAIN_SPECS}


def _module_edges(module_name: str) -> set[str]:
    source = inspect.getsource(MODULES[module_name])
    tree = ast.parse(source)

    edges: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for imported in node.names:
                if imported.name in MODULES:
                    edges.add(imported.name)

        if isinstance(node, ast.ImportFrom) and node.module:
            base_module = node.module
            if base_module in MODULES:
                edges.add(base_module)
            if base_module == "app.api":
                for imported in node.names:
                    candidate = f"app.api.{imported.name}"
                    if candidate in MODULES:
                        edges.add(candidate)

    return edges


def _build_graph() -> dict[str, set[str]]:
    return {module_name: _module_edges(module_name) for module_name in MODULES}


def _detect_cycle(graph: dict[str, set[str]]) -> list[str] | None:
    visited: set[str] = set()
    visiting: set[str] = set()
    stack: list[str] = []

    def _dfs(node: str) -> list[str] | None:
        if node in visiting:
            cycle_start = stack.index(node)
            return stack[cycle_start:] + [node]
        if node in visited:
            return None

        visiting.add(node)
        stack.append(node)

        for neighbor in graph[node]:
            cycle = _dfs(neighbor)
            if cycle is not None:
                return cycle

        stack.pop()
        visiting.remove(node)
        visited.add(node)
        return None

    for node in graph:
        cycle = _dfs(node)
        if cycle is not None:
            return cycle
    return None


def _reachable(start: str, graph: dict[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    frontier = [start]

    while frontier:
        current = frontier.pop()
        for neighbor in graph[current]:
            if neighbor not in seen:
                seen.add(neighbor)
                frontier.append(neighbor)

    return seen


def test_shared_helpers_do_not_import_domains() -> None:
    edges = _module_edges(SHARED_MODULE)
    assert edges.isdisjoint(DOMAIN_MODULES)


def test_domains_can_import_shared_helpers_but_not_each_other() -> None:
    for domain in DOMAIN_MODULES:
        edges = _module_edges(domain)
        assert SHARED_MODULE in edges

        cross_domain_edges = {edge for edge in edges if edge in DOMAIN_MODULES and edge != domain}
        assert not cross_domain_edges, f"Cross-domain read-model coupling detected: {domain} -> {cross_domain_edges}"


def test_shared_dependency_direction_is_acyclic_and_one_directional() -> None:
    graph = _build_graph()

    cycle = _detect_cycle(graph)
    assert cycle is None, f"Circular import path detected: {' -> '.join(cycle or [])}"

    reachable_from_shared = _reachable(SHARED_MODULE, graph)
    assert reachable_from_shared.isdisjoint(DOMAIN_MODULES)

    for domain in DOMAIN_MODULES:
        assert graph[domain] <= {SHARED_MODULE}
