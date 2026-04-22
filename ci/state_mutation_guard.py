from __future__ import annotations

import argparse
import ast
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

# ────────────────────────────────────────────────────────────────────────────────
# LIFECYCLE TYPE REGISTRY
#
# These classes are lifecycle-managed and subject to state mutation enforcement.
# ────────────────────────────────────────────────────────────────────────────────

LIFECYCLE_CLASSES = frozenset({
    "LifecycleAction",
    "Action",
    "Task",
    "Workflow",
    "ActionPipeline",
    "TaskConnector",
    # Add more as lifecycle scope expands
})

# Fields that are protected on lifecycle classes
LIFECYCLE_STATE_FIELDS = frozenset({
    "state",
    "current_state",
    "lifecycle_state",
})

# Modules that are fully excluded from enforcement (no false positives)
EXCLUDED_MODULE_PATTERNS = {
    "logging",
    "log_",
    "dlq",
    "metrics",
    "telemetry",
    "utils",
    "infrastructure",
    "infra",
}

# State mutation is only allowed in the authoritative lifecycle FSM module.
ALLOWED_MUTATION_FILES = {
    "apps/api/core/state_machine.py",
}

EXCLUDED_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    "node_modules",
}


@dataclass(frozen=True)
class Violation:
    file_path: str
    line: int
    violation_type: str
    message: str
    variable_name: str | None = None
    detected_type: str | None = None


def _should_exclude_module(file_path: str) -> bool:
    """Check if file is in an excluded module that should never trigger violations."""
    normalized = file_path.replace("\\", "/").lower()
    for pattern in EXCLUDED_MODULE_PATTERNS:
        if f"/{pattern}" in normalized or normalized.startswith(pattern):
            return True
    return False


class ScopedMutationGuardVisitor(ast.NodeVisitor):
    """
    Enhanced AST visitor that tracks lifecycle types and only enforces restrictions
    on lifecycle-managed domain objects.

    Tracks per-file:
    - Local class definitions
    - Variable assignments to lifecycle types
    - Constructor calls creating lifecycle instances
    """

    def __init__(self, file_path: str, allowed_file: bool) -> None:
        self.file_path = file_path
        self.allowed_file = allowed_file
        self.violations: list[Violation] = []

        # Symbol table: variable name → lifecycle class name
        self.variable_types: dict[str, str] = {}

        # Imported lifecycle classes: short_name → qualified_name
        # (e.g., "LifecycleAction" → "household_os.runtime.action_pipeline.LifecycleAction")
        self.imported_lifecycle_classes: dict[str, str] = {}

        # Local class definitions in this file
        self.local_classes: set[str] = set()

    def _add_violation(
        self,
        node: ast.AST,
        violation_type: str,
        message: str,
        variable_name: str | None = None,
        detected_type: str | None = None,
    ) -> None:
        """Record a violation with optional variable and type context."""
        if self.allowed_file:
            return
        self.violations.append(
            Violation(
                file_path=self.file_path,
                line=getattr(node, "lineno", 0),
                violation_type=violation_type,
                message=message,
                variable_name=variable_name,
                detected_type=detected_type,
            )
        )

    def _is_lifecycle_type(self, name: str) -> bool:
        """Check if a name refers to a lifecycle class."""
        return name in LIFECYCLE_CLASSES

    def _get_variable_type(self, name: str) -> str | None:
        """Get the inferred lifecycle type of a variable, if any."""
        return self.variable_types.get(name)

    def _extract_class_name_from_call(self, func: ast.expr) -> str | None:
        """Extract class name from a Call node (e.g., LifecycleAction() or module.Action())."""
        if isinstance(func, ast.Name):
            return func.id
        if isinstance(func, ast.Attribute):
            # Handle module.Action() pattern
            return func.attr
        return None

    def _track_assignment(self, target: str, value: ast.expr) -> None:
        """Track variable assignments to lifecycle types."""
        # Direct constructor call: x = LifecycleAction()
        if isinstance(value, ast.Call):
            class_name = self._extract_class_name_from_call(value.func)
            if class_name and self._is_lifecycle_type(class_name):
                self.variable_types[target] = class_name

        # Variable assignment: x = y (copy type from y)
        elif isinstance(value, ast.Name):
            if value.id in self.variable_types:
                self.variable_types[target] = self.variable_types[value.id]

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Track local class definitions."""
        self.local_classes.add(node.name)
        if self._is_lifecycle_type(node.name):
            # Local definition of a lifecycle class
            self.variable_types[node.name] = node.name
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        """Track imports of lifecycle classes."""
        for alias in node.names:
            module_name = alias.name
            import_as = alias.asname if alias.asname else alias.name.split(".")[-1]
            # Check if importing a lifecycle class
            if import_as in LIFECYCLE_CLASSES:
                self.imported_lifecycle_classes[import_as] = module_name

        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Track from-imports of lifecycle classes."""
        if node.names:
            for alias in node.names:
                import_name = alias.name
                import_as = alias.asname if alias.asname else import_name
                if import_as in LIFECYCLE_CLASSES or import_name in LIFECYCLE_CLASSES:
                    self.imported_lifecycle_classes[import_as] = import_name
                    # Track the imported class as available in this module
                    if import_as in LIFECYCLE_CLASSES:
                        self.variable_types[import_as] = import_as

        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        """Track variable assignments (symbol table) and check mutation patterns."""
        # Update symbol table
        for target in node.targets:
            if isinstance(target, ast.Name):
                self._track_assignment(target.id, node.value)

        # Check for forbidden mutations
        self._check_assignment_targets(node.targets, node)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        """Track annotated assignments and check mutation patterns."""
        if isinstance(node.target, ast.Name):
            if node.value:
                self._track_assignment(node.target.id, node.value)

        self._check_assignment_target(node.target, node)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        """Check for setattr() and __dict__.update() mutations."""
        # setattr(obj, "state", value)
        if isinstance(node.func, ast.Name) and node.func.id == "setattr":
            if len(node.args) >= 2:
                obj_arg = node.args[0]
                field_name = self._const_str(node.args[1])

                if field_name in LIFECYCLE_STATE_FIELDS:
                    # Determine if obj is a lifecycle type
                    obj_var = self._extract_name(obj_arg)
                    obj_type = self._get_variable_type(obj_var) if obj_var else None

                    if obj_type:
                        self._add_violation(
                            node,
                            "SETATTR_BYPASS",
                            f"setattr(..., '{field_name}', ...) on lifecycle object is forbidden.",
                            variable_name=obj_var,
                            detected_type=obj_type,
                        )

        # obj.__dict__.update(...)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "update":
            if self._is_dunder_dict_attr(node.func.value):
                obj_var = self._extract_name(node.func.value)
                obj_type = self._get_variable_type(obj_var) if obj_var else None

                if obj_type:
                    self._add_violation(
                        node,
                        "DICT_UPDATE_BYPASS",
                        "Mutation via obj.__dict__.update(...) on lifecycle object is forbidden.",
                        variable_name=obj_var,
                        detected_type=obj_type,
                    )

        self.generic_visit(node)

    def _check_assignment_targets(self, targets: list[ast.expr], node: ast.AST) -> None:
        """Check all assignment targets recursively."""
        for target in targets:
            self._check_assignment_target(target, node)

    def _check_assignment_target(self, target: ast.expr, node: ast.AST) -> None:
        """Check a single assignment target for forbidden mutations."""
        if isinstance(target, ast.Attribute):
            if target.attr in LIFECYCLE_STATE_FIELDS:
                # Determine if target object is a lifecycle type
                obj_var = self._extract_name(target.value)
                obj_type = self._get_variable_type(obj_var) if obj_var else None

                if obj_type:
                    self._add_violation(
                        node,
                        "ATTR_ASSIGN",
                        f"Direct assignment to '{target.attr}' on lifecycle object is forbidden.",
                        variable_name=obj_var,
                        detected_type=obj_type,
                    )
            return

        if isinstance(target, ast.Subscript):
            if self._is_dict_state_subscript(target):
                obj_var = self._extract_name(target.value)
                obj_type = self._get_variable_type(obj_var) if obj_var else None

                if obj_type:
                    self._add_violation(
                        node,
                        "DICT_SUBSCRIPT_BYPASS",
                        "Dictionary-style state mutation via __dict__ on lifecycle object is forbidden.",
                        variable_name=obj_var,
                        detected_type=obj_type,
                    )
            return

        if isinstance(target, (ast.Tuple, ast.List)):
            for elt in target.elts:
                self._check_assignment_target(elt, node)

    @staticmethod
    def _extract_name(node: ast.expr) -> str | None:
        """Extract simple name from expression (e.g., 'x' from 'x.field')."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return ScopedMutationGuardVisitor._extract_name(node.value)
        return None

    @staticmethod
    def _const_str(node: ast.AST | None) -> str | None:
        """Extract string constant from node."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return None

    @classmethod
    def _slice_str(cls, node: ast.Subscript) -> str | None:
        """Extract string key from subscript."""
        return cls._const_str(node.slice)

    @classmethod
    def _is_dunder_dict_attr(cls, node: ast.AST) -> bool:
        """Check if node is .__dict__ access."""
        return isinstance(node, ast.Attribute) and node.attr == "__dict__"

    @classmethod
    def _is_dict_state_subscript(cls, node: ast.Subscript) -> bool:
        """Check if node is __dict__["state"] pattern."""
        key = cls._slice_str(node)
        if key not in LIFECYCLE_STATE_FIELDS:
            return False
        return cls._is_dunder_dict_attr(node.value)


def _normalize_path(path: str) -> str:
    """Normalize path for consistent comparison."""
    return path.replace("\\", "/")


def _is_allowed_mutation_file(path: str) -> bool:
    """Check if file is in the allowed mutation list."""
    normalized = _normalize_path(path)
    return any(normalized.endswith(suffix) for suffix in ALLOWED_MUTATION_FILES)


def scan_file(path: str) -> list[Violation]:
    """Scan a single file for lifecycle state mutation violations."""
    normalized_path = _normalize_path(path)

    # Skip excluded modules
    if _should_exclude_module(normalized_path):
        return []

    # Skip allowed files (FSM control plane)
    allowed_file = _is_allowed_mutation_file(normalized_path)

    try:
        source = Path(path).read_text(encoding="utf-8")
    except OSError as exc:
        return [
            Violation(
                file_path=normalized_path,
                line=0,
                violation_type="READ_ERROR",
                message=f"Unable to read file: {exc}",
            )
        ]

    try:
        tree = ast.parse(source, filename=normalized_path)
    except SyntaxError as exc:
        return [
            Violation(
                file_path=normalized_path,
                line=exc.lineno or 0,
                violation_type="PARSE_ERROR",
                message=f"Unable to parse file: {exc.msg}",
            )
        ]

    visitor = ScopedMutationGuardVisitor(file_path=normalized_path, allowed_file=allowed_file)
    visitor.visit(tree)
    return visitor.violations


def _iter_python_files(root: str) -> Iterable[str]:
    """Iterate all Python files in root directory."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDED_DIRS]
        for filename in filenames:
            if filename.endswith(".py"):
                yield os.path.join(dirpath, filename)


def scan_directory(root: str) -> list[Violation]:
    """Scan directory recursively for violations, or scan a single file if given."""
    violations: list[Violation] = []
    
    # Handle both files and directories
    if os.path.isfile(root):
        violations.extend(scan_file(root))
    elif os.path.isdir(root):
        for file_path in _iter_python_files(root):
            violations.extend(scan_file(file_path))
    
    return violations


def _print_report(violations: list[Violation]) -> None:
    """Print structured report of violations."""
    print("Scoped Lifecycle State Mutation Guard Report")
    print("=" * 80)
    if not violations:
        print("✓ No lifecycle state mutations detected on lifecycle-managed objects.")
        return

    for violation in sorted(violations, key=lambda v: (v.file_path, v.line, v.violation_type)):
        type_info = f" ({violation.detected_type})" if violation.detected_type else ""
        var_info = f" [{violation.variable_name}]" if violation.variable_name else ""
        print(
            f"{violation.file_path}:{violation.line}: {violation.violation_type}: "
            f"{violation.message}{type_info}{var_info}"
        )

    print("-" * 80)
    print(f"Total violations: {len(violations)}")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description=(
            "Scoped AST guard: blocks lifecycle state mutations only on "
            "lifecycle-managed domain objects."
        )
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=".",
        help="Root directory to scan (default: current directory).",
    )
    args = parser.parse_args()

    root = args.root
    violations = scan_directory(root)
    _print_report(violations)

    return 1 if violations else 0


if __name__ == "__main__":
    raise SystemExit(main())

