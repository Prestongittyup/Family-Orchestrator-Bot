from __future__ import annotations

import ast
import inspect

import pytest

from app.api import read_model_shared as shared
from tests.governance.contract_kernel import (
    EXPECTED_SHARED_HELPER_EXPORTS,
    SHARED_LAYER_FORBIDDEN_DOMAIN_TOKENS,
    SHARED_LAYER_FORBIDDEN_ENFORCEMENT_TOKENS,
    SHARED_LAYER_FORBIDDEN_INTERPRETATION_TOKENS,
    SHARED_LAYER_FORBIDDEN_POLICY_TOKENS,
    SHARED_LAYER_FORBIDDEN_UTILITY_TOKENS,
)


_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.ci_gate, pytest.mark.integration]


_ALLOWED_TOP_LEVEL_NODES = (ast.Import, ast.ImportFrom, ast.FunctionDef)
_CACHE_HELPER_NAMES = {"cache_get", "cache_set"}
_SHARED_FUNCTIONS = EXPECTED_SHARED_HELPER_EXPORTS
_ALLOWED_LOCAL_CALLS: dict[str, set[str]] = {
    "safe_int": set(),
    "normalized_search": set(),
    "normalized_limit": {"safe_int"},
    "normalized_offset": {"safe_int"},
    "_normalized_text": set(),
    "normalized_filter_state": {"_normalized_text"},
    "projection_cache_state": {"_normalized_text", "safe_int"},
    "query_cache_state": {"normalized_filter_state", "_normalized_text"},
    "sort_records_with_tie_break": set(),
    "paginate_records": set(),
    "cache_get": set(),
    "cache_set": set(),
}


def _module_tree() -> ast.Module:
    return ast.parse(inspect.getsource(shared))


def _function_nodes() -> dict[str, ast.FunctionDef]:
    tree = _module_tree()
    return {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    }


def _local_calls(node: ast.FunctionDef) -> set[str]:
    calls: set[str] = set()
    for current in ast.walk(node):
        if isinstance(current, ast.Call) and isinstance(current.func, ast.Name):
            if current.func.id in _SHARED_FUNCTIONS:
                calls.add(current.func.id)
    return calls


def test_shared_layer_contains_stateless_functions_only() -> None:
    tree = _module_tree()

    for node in tree.body:
        assert isinstance(node, _ALLOWED_TOP_LEVEL_NODES), (
            "read_model_shared must stay utility-only with import/function definitions only"
        )

    assert not any(isinstance(node, (ast.Assign, ast.AnnAssign, ast.ClassDef)) for node in tree.body)
    assert not any(isinstance(node, (ast.Global, ast.Nonlocal)) for node in ast.walk(tree))


def test_shared_layer_forbids_orchestration_and_domain_specific_conditionals() -> None:
    source = inspect.getsource(shared).lower()

    for token in SHARED_LAYER_FORBIDDEN_UTILITY_TOKENS:
        assert token not in source
    for token in SHARED_LAYER_FORBIDDEN_DOMAIN_TOKENS:
        assert token not in source
    for token in SHARED_LAYER_FORBIDDEN_POLICY_TOKENS:
        assert token not in source
    for token in SHARED_LAYER_FORBIDDEN_ENFORCEMENT_TOKENS:
        assert token not in source
    for token in SHARED_LAYER_FORBIDDEN_INTERPRETATION_TOKENS:
        assert token not in source


def test_shared_layer_concern_boundaries_are_isolated_per_function() -> None:
    for name, function in inspect.getmembers(shared, inspect.isfunction):
        if getattr(function, "__module__", "") != shared.__name__:
            continue

        source = inspect.getsource(function)

        if name not in _CACHE_HELPER_NAMES:
            assert "move_to_end" not in source
            assert "popitem(" not in source
            assert "max_entries" not in source

        if name != "sort_records_with_tie_break":
            assert "source_event_field" not in source
            assert "decorated.sort" not in source

        if name != "paginate_records":
            assert "offset + limit" not in source


def test_shared_layer_functions_follow_single_responsibility_transform_pattern() -> None:
    function_nodes = _function_nodes()
    assert set(function_nodes.keys()) == _SHARED_FUNCTIONS

    decision_nodes = (ast.If, ast.For, ast.While, ast.Try, ast.BoolOp, ast.Match)

    for name, node in function_nodes.items():
        local_calls = _local_calls(node)
        assert local_calls <= _ALLOWED_LOCAL_CALLS[name], (
            f"Unexpected helper coupling in shared function {name}: {sorted(local_calls)}"
        )

        complexity = sum(1 for current in ast.walk(node) if isinstance(current, decision_nodes))
        assert complexity <= 8, f"Function {name} exceeded single-responsibility complexity budget"
