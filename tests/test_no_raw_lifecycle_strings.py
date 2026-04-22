from __future__ import annotations

import ast
import pathlib

FORBIDDEN = {
    "committed",
    "approved",
    "rejected",
    "failed",
    "executed",
    "ignored",
    "proposed",
    "pending_approval",
}


def _is_lifecycle_string_constant(node: ast.AST) -> bool:
    if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
        return False
    return node.value.lower() in FORBIDDEN


def _looks_like_state_expression(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return "state" in node.id.lower()
    if isinstance(node, ast.Attribute):
        return "state" in node.attr.lower()
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        return node.func.id in {"reduce_state", "parse_lifecycle_state", "assert_lifecycle_state"}
    return False


def test_no_raw_lifecycle_strings() -> None:
    """Guard against reintroducing string-literal lifecycle comparisons in logic paths."""
    candidate_roots = [
        pathlib.Path("source"),
        pathlib.Path("tests"),
        pathlib.Path("fixtures"),
        pathlib.Path("scripts"),
        pathlib.Path("household_os"),
        pathlib.Path("apps"),
    ]
    roots = [root for root in candidate_roots if root.exists()]

    for root in roots:
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

            for node in ast.walk(tree):
                # Block comparisons like: state == "approved", x in {"rejected", ...}
                if isinstance(node, ast.Compare):
                    state_like = any(_looks_like_state_expression(target) for target in [node.left, *node.comparators])
                    if not state_like:
                        continue

                    targets = [node.left, *node.comparators]
                    for target in targets:
                        if _is_lifecycle_string_constant(target):
                            raise AssertionError(
                                f"Raw lifecycle string '{target.value}' found in comparison in {path}"
                            )
                        if isinstance(target, (ast.Set, ast.Tuple, ast.List)):
                            for element in target.elts:
                                if _is_lifecycle_string_constant(element):
                                    raise AssertionError(
                                        f"Raw lifecycle string '{element.value}' found in comparison container in {path}"
                                    )
