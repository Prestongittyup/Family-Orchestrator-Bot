from __future__ import annotations

import ast
import re
import sys
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]

EXCLUDED_DIRECTORY_NAMES = {
    ".git",
    ".venv",
    "archive",
    "node_modules",
    "dist",
    "build",
    "legacy",
    "tests",
    "__pycache__",
}

PYTHON_SCAN_ROOTS = (
    REPO_ROOT / "app",
    REPO_ROOT / "apps",
    REPO_ROOT / "assistant",
    REPO_ROOT / "core",
    REPO_ROOT / "household_os",
    REPO_ROOT / "household_state",
    REPO_ROOT / "decision_card_system",
)

FRONTEND_SCAN_ROOT = REPO_ROOT / "hpal-frontend" / "src"

DECISION_SUBSYSTEM_ROOT = REPO_ROOT / "decision_card_system"
DECISION_REGISTRY_PATH = DECISION_SUBSYSTEM_ROOT / "registry.py"
DECISION_CONTRACT_PY_PATH = DECISION_SUBSYSTEM_ROOT / "decision_card_contract.py"
DECISION_CONTRACT_TS_PATH = FRONTEND_SCAN_ROOT / "runtime" / "decision_card_contract.ts"
LEGACY_DECISION_REGISTRY_SHIM = REPO_ROOT / "core" / "replay" / "decision_card_registry.py"

RUNTIME_ALLOWED_IMPLEMENTATION_IMPORTERS = {
    REPO_ROOT / "app" / "services" / "commands" / "runtime.py",
    REPO_ROOT / "app" / "services" / "rules_engine" / "command_rules.py",
    REPO_ROOT / "core" / "replay" / "event_replay_engine.py",
    LEGACY_DECISION_REGISTRY_SHIM,
}

AGENT_IMPORT_ROOTS = (
    REPO_ROOT / "assistant",
    REPO_ROOT / "app" / "services" / "agents",
    REPO_ROOT / "apps" / "api" / "assistant_runtime_router.py",
)

FORBIDDEN_DECISION_LOGIC_PATTERN = re.compile(
    r"^\s*def\s+(createDecisionCard|create_decision_card|reduce_decision_card_projection)\s*\(",
    re.MULTILINE,
)

FORBIDDEN_DECISION_TYPE_DUPLICATION_PATTERN_PY = re.compile(
    r"^\s*(class\s+DecisionCard(?:Record|State|EventType|EventPayload)\b|DecisionCard(?:State|EventType|ContractVersion)\s*=\s*Literal\[)",
    re.MULTILINE,
)

FORBIDDEN_DECISION_TYPE_DUPLICATION_PATTERN_TS = re.compile(
    r"^\s*(interface|type|enum)\s+DecisionCard\w*",
    re.MULTILINE,
)

FORBIDDEN_FRONTEND_IMPLEMENTATION_TOKENS = (
    "decision_card_system.registry",
    "core.replay.decision_card_registry",
    "createDecisionCard(",
    "reduce_decision_card_projection(",
)


def _iter_python_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return
    for path in root.rglob("*.py"):
        if any(part in EXCLUDED_DIRECTORY_NAMES for part in path.parts):
            continue
        yield path


def _iter_frontend_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".ts", ".tsx", ".js", ".jsx"}:
            continue
        if any(part in EXCLUDED_DIRECTORY_NAMES for part in path.parts):
            continue
        yield path


def _is_agent_path(path: Path) -> bool:
    for prefix in AGENT_IMPORT_ROOTS:
        if prefix.is_dir() and prefix in path.parents:
            return True
        if prefix.is_file() and path == prefix:
            return True
    return False


def _resolve_imports(module_name: str, node: ast.AST) -> list[str]:
    imports: list[str] = []

    if isinstance(node, ast.Import):
        for alias in node.names:
            imports.append(alias.name)
        return imports

    if not isinstance(node, ast.ImportFrom):
        return imports

    base = node.module or ""
    if node.level:
        parts = module_name.split(".")[:-1]
        if node.level <= len(parts):
            prefix = parts[:-node.level + 1] if node.level > 1 else parts
            base = ".".join(prefix + ([base] if base else []))

    if base:
        imports.append(base)
        for alias in node.names:
            imports.append(f"{base}.{alias.name}")

    return imports


def _module_name_for_path(path: Path, root: Path) -> str:
    rel = path.relative_to(root)
    return rel.with_suffix("").as_posix().replace("/", ".")


def collect_dependency_graph_violations(root: Path | None = None) -> list[str]:
    workspace_root = (root or REPO_ROOT).resolve()
    violations: list[str] = []

    if not DECISION_REGISTRY_PATH.exists():
        violations.append("decision_card_system/registry.py missing")
    if not DECISION_CONTRACT_PY_PATH.exists():
        violations.append("decision_card_system/decision_card_contract.py missing")
    if not DECISION_CONTRACT_TS_PATH.exists():
        violations.append("hpal-frontend/src/runtime/decision_card_contract.ts missing")

    saw_runtime_to_subsystem = False
    saw_replay_to_subsystem = False

    for scan_root in PYTHON_SCAN_ROOTS:
        for path in _iter_python_files(scan_root):
            source = path.read_text(encoding="utf-8", errors="ignore")
            rel_path = path.relative_to(workspace_root).as_posix()

            if path == LEGACY_DECISION_REGISTRY_SHIM:
                if "from decision_card_system.registry import *" not in source:
                    violations.append(
                        f"{rel_path}: legacy shim must only re-export decision_card_system.registry"
                    )
                if "def createDecisionCard(" in source or "def reduce_decision_card_projection(" in source:
                    violations.append(
                        f"{rel_path}: legacy shim must not implement decision-card logic"
                    )
                continue

            if path != DECISION_REGISTRY_PATH and FORBIDDEN_DECISION_LOGIC_PATTERN.search(source):
                violations.append(
                    f"{rel_path}: decision-card implementation logic exists outside decision_card_system/registry.py"
                )

            if path != DECISION_CONTRACT_PY_PATH and FORBIDDEN_DECISION_TYPE_DUPLICATION_PATTERN_PY.search(source):
                violations.append(
                    f"{rel_path}: DecisionCard contract type duplicated outside decision_card_contract.py"
                )

            try:
                tree = ast.parse(source)
            except SyntaxError as exc:
                violations.append(f"{rel_path}: syntax error: {exc}")
                continue

            module_name = _module_name_for_path(path, workspace_root)
            imports: list[tuple[int, str]] = []
            for node in ast.walk(tree):
                if not isinstance(node, (ast.Import, ast.ImportFrom)):
                    continue
                line = getattr(node, "lineno", 1)
                for imported in _resolve_imports(module_name, node):
                    imports.append((line, imported))

            for line, imported in imports:
                if imported.startswith("core.replay.decision_card_registry"):
                    violations.append(
                        f"{rel_path}:{line}: forbidden legacy import root core.replay.decision_card_registry"
                    )

                if imported.startswith("decision_card_system.registry"):
                    if path in RUNTIME_ALLOWED_IMPLEMENTATION_IMPORTERS:
                        if path == REPO_ROOT / "app" / "services" / "commands" / "runtime.py":
                            saw_runtime_to_subsystem = True
                        if path == REPO_ROOT / "core" / "replay" / "event_replay_engine.py":
                            saw_replay_to_subsystem = True
                    elif path == DECISION_REGISTRY_PATH:
                        continue
                    elif path.parent == DECISION_SUBSYSTEM_ROOT:
                        continue
                    elif _is_agent_path(path):
                        violations.append(
                            f"{rel_path}:{line}: agent layer must not import decision implementation module"
                        )
                    else:
                        violations.append(
                            f"{rel_path}:{line}: decision registry import not allowed from this module"
                        )

                if imported.startswith("decision_card_system") and not imported.startswith("decision_card_system.decision_card_contract") and not imported.startswith("decision_card_system.registry"):
                    violations.append(
                        f"{rel_path}:{line}: import decision_card_system via explicit registry/contract module only"
                    )

                if _is_agent_path(path) and imported.startswith("decision_card_system") and not imported.startswith("decision_card_system.decision_card_contract"):
                    violations.append(
                        f"{rel_path}:{line}: agent layer may only import decision_card_contract"
                    )

            if path.parent == DECISION_SUBSYSTEM_ROOT or path == DECISION_REGISTRY_PATH:
                for line, imported in imports:
                    if imported.startswith(("assistant", "apps", "app.services", "app.api", "household_os", "household_state")):
                        violations.append(
                            f"{rel_path}:{line}: decision_card_system must be dependency-leaf and cannot import {imported}"
                        )

    for path in _iter_frontend_files(FRONTEND_SCAN_ROOT):
        source = path.read_text(encoding="utf-8", errors="ignore")
        rel_path = path.relative_to(workspace_root).as_posix()

        for token in FORBIDDEN_FRONTEND_IMPLEMENTATION_TOKENS:
            if token in source:
                violations.append(
                    f"{rel_path}: frontend must not reference decision implementation token '{token}'"
                )

        if path != DECISION_CONTRACT_TS_PATH and FORBIDDEN_DECISION_TYPE_DUPLICATION_PATTERN_TS.search(source):
            violations.append(
                f"{rel_path}: DecisionCard contract type duplicated outside decision_card_contract.ts"
            )

    if not saw_runtime_to_subsystem:
        violations.append("app/services/commands/runtime.py must import decision_card_system.registry")
    if not saw_replay_to_subsystem:
        violations.append("core/replay/event_replay_engine.py must import decision_card_system.registry")

    # Deterministic ordering for stable CI output.
    return sorted(set(violations))


def run_decision_card_dependency_graph_guard(root: Path | None = None) -> int:
    violations = collect_dependency_graph_violations(root)
    if not violations:
        print("decision-card dependency graph guard: PASS")
        return 0

    print("decision-card dependency graph guard: FAIL")
    for violation in violations:
        print(violation)
    return 1


if __name__ == "__main__":
    raise SystemExit(run_decision_card_dependency_graph_guard())
