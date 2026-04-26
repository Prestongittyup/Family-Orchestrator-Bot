from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCAN_ROOTS = [
    ROOT / "apps" / "api" / "services",
    ROOT / "apps" / "api" / "identity",
]
ALLOWED_DECORATORS = {"internal_only", "audit_only"}


@dataclass(frozen=True)
class Violation:
    file_path: str
    line_number: int
    function_name: str
    reason: str


class _FunctionCallScanner(ast.NodeVisitor):
    """Collect commit/emit calls inside one function body, excluding nested defs."""

    def __init__(self, target_function: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self._target = target_function
        self.commit_calls: list[ast.Call] = []
        self.emit_calls: list[ast.Call] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        if node is self._target:
            self.generic_visit(node)
            return
        # Ignore nested functions.
        return

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        if node is self._target:
            self.generic_visit(node)
            return
        # Ignore nested async functions.
        return

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        # Ignore nested classes in function scope.
        return

    def visit_Lambda(self, node: ast.Lambda) -> None:
        # Ignore lambda internals for parent function accounting.
        return

    def visit_Call(self, node: ast.Call) -> None:
        if _is_commit_call(node):
            self.commit_calls.append(node)
        if _is_router_emit_call(node):
            self.emit_calls.append(node)
        self.generic_visit(node)


def _decorator_name(decorator: ast.expr) -> str | None:
    if isinstance(decorator, ast.Name):
        return decorator.id
    if isinstance(decorator, ast.Attribute):
        return decorator.attr
    if isinstance(decorator, ast.Call):
        return _decorator_name(decorator.func)
    return None


def _has_allowed_decorator(func: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    for decorator in func.decorator_list:
        name = _decorator_name(decorator)
        if name in ALLOWED_DECORATORS:
            return True
    return False


def _is_name(node: ast.AST, value: str) -> bool:
    return isinstance(node, ast.Name) and node.id == value


def _is_commit_call(call: ast.Call) -> bool:
    # Match session.commit() and db_session.commit()
    fn = call.func
    return (
        isinstance(fn, ast.Attribute)
        and fn.attr == "commit"
        and (_is_name(fn.value, "session") or _is_name(fn.value, "db_session"))
    )


def _is_router_emit_call(call: ast.Call) -> bool:
    # Match router.emit(...)
    fn = call.func
    return (
        isinstance(fn, ast.Attribute)
        and fn.attr == "emit"
        and _is_name(fn.value, "router")
    )


def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for root in SCAN_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            rel = path.relative_to(ROOT).as_posix()
            if rel.startswith("tests/"):
                continue
            files.append(path)
    return sorted(files)


def _collect_violations() -> list[Violation]:
    violations: list[Violation] = []

    for file_path in _iter_python_files():
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))

        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue

            if _has_allowed_decorator(node):
                continue

            scanner = _FunctionCallScanner(node)
            scanner.visit(node)

            has_commit = bool(scanner.commit_calls)
            has_emit = bool(scanner.emit_calls)

            if has_commit and not has_emit:
                commit_line = scanner.commit_calls[0].lineno
                rel = file_path.relative_to(ROOT).as_posix()
                violations.append(
                    Violation(
                        file_path=rel,
                        line_number=commit_line,
                        function_name=node.name,
                        reason="session/db_session commit without router.emit in same function",
                    )
                )

    return violations


def test_no_silent_mutations() -> None:
    violations = _collect_violations()

    if violations:
        details = "\n".join(
            f"- {v.file_path}:{v.line_number} function '{v.function_name}' -> {v.reason}"
            for v in violations
        )
        raise AssertionError(
            "Silent mutation(s) detected. Functions calling session.commit() or db_session.commit() "
            "must also call router.emit(...) unless decorated with @internal_only or @audit_only.\n"
            f"{details}"
        )
