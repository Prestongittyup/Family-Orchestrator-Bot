from __future__ import annotations

import ast
import logging
import os
import sys
from pathlib import Path
from typing import Any

from core.architecture.contract_loader import (
    ArchitectureContract,
    ArchitectureContractError,
    load_architecture_contract,
)

logger = logging.getLogger(__name__)

_RUNTIME_SCOPE_ROOTS = (
    Path("app/services"),
    Path("household_os/runtime"),
    Path("apps/api"),
)
_RUNTIME_SCOPE_PREFIXES = (
    "app.services",
    "household_os.runtime",
    "apps.api",
)
_EXECUTION_LAYER_NAME = "Execution Runtime Layer (Single Mutation Surface)"
_PROVIDER_IMPORT_PREFIXES = (
    "app.adapters.llm.providers",
    "app.adapters.external",
    "app.adapters.providers",
)
_PROVIDER_SDK_PREFIXES = (
    "openai",
    "anthropic",
)
_PROVIDER_ALLOWED_CALLERS = (
    "app.services.llm_gateway",
    "app.services.provider_sync",
    "app.adapters.llm.gateway",
    "app.adapters.llm.providers",
)
_EXECUTION_SURFACE_PREFIXES = (
    "app.services.execution_gateway",
    "app.services.saga",
    "household_os.runtime",
)
_SHADOW_EXECUTION_PREFIXES = (
    "apps.api.services.decision_engine",
    "apps.api.services.synthesis_engine",
    "apps.api.services.worker",
    "modules.core.services.orchestrator_lite",
)

_DEFAULT_DIAGNOSTIC: dict[str, Any] = {
    "architecture_compliant": True,
    "layer_map_version": "RFC-001-derived",
    "violations": [],
}

_LAST_DIAGNOSTIC: dict[str, Any] = dict(_DEFAULT_DIAGNOSTIC)


class ArchitectureViolationError(RuntimeError):
    """Raised when runtime architecture contract checks fail in strict mode."""


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _normalize_module(value: str) -> str:
    module = str(value or "").strip().replace("\\", "/")
    if module.endswith(".py"):
        module = module[:-3]
    module = module.strip("/")
    if module.startswith("./"):
        module = module[2:]
    module = module.replace("/", ".")
    if module.endswith(".__init__"):
        module = module[: -len(".__init__")]
    return module.strip(".")


def _is_ci_environment() -> bool:
    return os.getenv("CI", "").strip().lower() in {"1", "true", "yes"}


def _strict_mode_enabled(strict: bool | None) -> bool:
    if _is_ci_environment():
        return True
    if strict is not None:
        return bool(strict)
    return os.getenv("ARCHITECTURE_GUARD_STRICT", "0").strip().lower() in {"1", "true", "yes"}


def _iter_scope_files(repo_root: Path) -> list[Path]:
    files: list[Path] = []
    for relative_root in _RUNTIME_SCOPE_ROOTS:
        absolute_root = repo_root / relative_root
        if not absolute_root.exists():
            continue
        files.extend(path for path in absolute_root.rglob("*.py") if path.is_file())
    return sorted(files)


def _module_from_file(repo_root: Path, file_path: Path) -> str:
    relative = file_path.relative_to(repo_root)
    return _normalize_module(str(relative))


def _resolve_imports(current_module: str, node: ast.AST) -> list[str]:
    imports: list[str] = []

    if isinstance(node, ast.Import):
        for alias in node.names:
            imports.append(alias.name)
        return imports

    if not isinstance(node, ast.ImportFrom):
        return imports

    base = node.module or ""
    if node.level:
        parts = current_module.split(".")[:-1]
        if node.level <= len(parts):
            prefix = parts[:-node.level + 1] if node.level > 1 else parts
            base = ".".join(prefix + ([base] if base else []))

    if base:
        imports.append(base)
        for alias in node.names:
            imports.append(f"{base}.{alias.name}")

    return imports


def _execution_layer_name(contract: ArchitectureContract) -> str:
    for layer_name in contract.get_allowed_layers().keys():
        if _EXECUTION_LAYER_NAME in layer_name:
            return layer_name
    return _EXECUTION_LAYER_NAME


def _find_layer(contract: ArchitectureContract, module_name: str) -> str | None:
    return contract.find_layer(module_name)


def _append_unique(violations: list[str], candidate: str) -> None:
    if candidate not in violations:
        violations.append(candidate)


def _module_is_provider_allowed(module_name: str) -> bool:
    return any(
        module_name == prefix or module_name.startswith(f"{prefix}.")
        for prefix in _PROVIDER_ALLOWED_CALLERS
    )


def _is_execution_surface(module_name: str) -> bool:
    return any(
        module_name == prefix or module_name.startswith(f"{prefix}.")
        for prefix in _EXECUTION_SURFACE_PREFIXES
    )


def _collect_loaded_module_violations(contract: ArchitectureContract) -> list[str]:
    violations: list[str] = []
    for module_name in sorted(sys.modules.keys()):
        normalized = _normalize_module(module_name)
        if not normalized:
            continue
        if not any(normalized == prefix or normalized.startswith(f"{prefix}.") for prefix in _RUNTIME_SCOPE_PREFIXES):
            continue
        if not contract.validate_module_path(normalized):
            _append_unique(
                violations,
                f"unknown service boundary loaded at runtime: {normalized}",
            )
        if any(
            normalized == forbidden or normalized.startswith(f"{forbidden}.")
            for forbidden in _SHADOW_EXECUTION_PREFIXES
        ):
            _append_unique(
                violations,
                f"shadow execution path loaded at runtime: {normalized}",
            )
    return violations


def _evaluate_module_import_edges(
    contract: ArchitectureContract,
    module_name: str,
    source: str,
    *,
    current_file: Path,
) -> list[str]:
    violations: list[str] = []
    execution_layer = _execution_layer_name(contract)

    source_layer = _find_layer(contract, module_name)

    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        _append_unique(
            violations,
            f"{current_file.as_posix()}: unable to parse module for architecture edge validation: {exc}",
        )
        return violations

    for node in ast.walk(tree):
        if not isinstance(node, (ast.Import, ast.ImportFrom)):
            continue
        line = getattr(node, "lineno", 1)
        imports = _resolve_imports(module_name, node)

        for imported in imports:
            normalized_import = _normalize_module(imported)
            if not normalized_import:
                continue

            if any(
                normalized_import == forbidden or normalized_import.startswith(f"{forbidden}.")
                for forbidden in _SHADOW_EXECUTION_PREFIXES
            ):
                _append_unique(
                    violations,
                    f"{current_file.as_posix()}:{line}: shadow execution path import detected: {normalized_import}",
                )

            if normalized_import.split(".")[0] in _PROVIDER_SDK_PREFIXES and not _module_is_provider_allowed(module_name):
                _append_unique(
                    violations,
                    f"{current_file.as_posix()}:{line}: direct provider SDK import outside llm_gateway/provider_sync: {normalized_import}",
                )

            if any(
                normalized_import == provider_prefix or normalized_import.startswith(f"{provider_prefix}.")
                for provider_prefix in _PROVIDER_IMPORT_PREFIXES
            ) and not _module_is_provider_allowed(module_name):
                _append_unique(
                    violations,
                    f"{current_file.as_posix()}:{line}: direct provider access outside llm_gateway/provider_sync: {normalized_import}",
                )

            if not (
                normalized_import.startswith("app.")
                or normalized_import.startswith("apps.")
                or normalized_import.startswith("household_os.")
            ):
                continue

            if normalized_import.startswith("app.services.") and not contract.validate_module_path(normalized_import):
                _append_unique(
                    violations,
                    f"{current_file.as_posix()}:{line}: unknown service boundary import: {normalized_import}",
                )

            target_layer = _find_layer(contract, normalized_import)

            if _is_execution_surface(normalized_import) and source_layer != execution_layer:
                _append_unique(
                    violations,
                    f"{current_file.as_posix()}:{line}: execution gateway bypass attempt from layer '{source_layer or 'unknown'}' to '{normalized_import}'",
                )

            if source_layer and target_layer:
                if source_layer != execution_layer and target_layer == execution_layer:
                    _append_unique(
                        violations,
                        f"{current_file.as_posix()}:{line}: cross-layer unauthorized import into execution runtime from '{source_layer}'",
                    )

    return violations


def _collect_scope_violations(contract: ArchitectureContract) -> list[str]:
    repo_root = _repo_root()
    violations: list[str] = []

    for file_path in _iter_scope_files(repo_root):
        module_name = _module_from_file(repo_root, file_path)
        if not contract.validate_module_path(module_name):
            _append_unique(
                violations,
                f"module exists outside declared layers: {module_name}",
            )

        source = file_path.read_text(encoding="utf-8", errors="ignore")
        for issue in _evaluate_module_import_edges(
            contract,
            module_name,
            source,
            current_file=file_path,
        ):
            _append_unique(violations, issue)

        if module_name.startswith("app.services.") and "execution_gateway" in module_name:
            if not module_name.startswith("app.services.execution_gateway"):
                _append_unique(
                    violations,
                    f"duplicate execution gateway detected: {module_name}",
                )

        if module_name.startswith("app.services.") and "saga" in module_name and not module_name.startswith("app.services.saga"):
            _append_unique(
                violations,
                f"alternative saga pipeline detected outside canonical saga boundary: {module_name}",
            )

    return violations


def _collect_forbidden_pattern_violations(contract: ArchitectureContract) -> list[str]:
    repo_root = _repo_root()
    violations: list[str] = []
    patterns = contract.get_forbidden_patterns()
    if not patterns:
        return violations

    for file_path in _iter_scope_files(repo_root):
        source = file_path.read_text(encoding="utf-8", errors="ignore")
        lower_source = source.lower()
        for pattern in patterns:
            normalized = pattern.lower().strip()
            if not normalized:
                continue
            if normalized in lower_source:
                if "forbidden" in normalized or "must not" in normalized:
                    _append_unique(
                        violations,
                        f"{file_path.as_posix()}: forbidden architecture pattern token detected: {pattern}",
                    )
    return violations


def _handle_violations(
    violations: list[str],
    *,
    strict: bool,
    context: str,
) -> None:
    if not violations:
        return

    message = (
        "Architecture contract validation failed "
        f"during {context} with {len(violations)} violation(s). "
        f"Sample: {violations[0]}"
    )

    if strict:
        raise ArchitectureViolationError(message)

    logger.warning(message)


def evaluate_architecture_contract(
    *,
    strict: bool | None = None,
    context: str = "runtime",
) -> dict[str, Any]:
    global _LAST_DIAGNOSTIC

    violations: list[str] = []

    try:
        contract = load_architecture_contract()
    except ArchitectureContractError as exc:
        strict_enabled = _strict_mode_enabled(strict)
        _append_unique(violations, str(exc))
        diagnostic = {
            "architecture_compliant": False,
            "layer_map_version": "RFC-001-derived",
            "violations": list(violations),
        }
        _LAST_DIAGNOSTIC = diagnostic
        _handle_violations(violations, strict=strict_enabled, context=context)
        return diagnostic

    for issue in _collect_loaded_module_violations(contract):
        _append_unique(violations, issue)
    for issue in _collect_scope_violations(contract):
        _append_unique(violations, issue)
    for issue in _collect_forbidden_pattern_violations(contract):
        _append_unique(violations, issue)

    diagnostic = {
        "architecture_compliant": len(violations) == 0,
        "layer_map_version": contract.layer_map_version,
        "violations": list(violations),
    }
    _LAST_DIAGNOSTIC = diagnostic

    strict_enabled = _strict_mode_enabled(strict)
    _handle_violations(violations, strict=strict_enabled, context=context)
    return diagnostic


def enforce_architecture_on_startup(
    *,
    strict: bool | None = None,
    context: str = "startup",
) -> dict[str, Any]:
    return evaluate_architecture_contract(strict=strict, context=context)


def enforce_import_boundary(
    *,
    module_path: str,
    module_file: str | None = None,
    strict: bool | None = None,
) -> bool:
    contract = load_architecture_contract()
    module_name = _normalize_module(module_path)

    violations: list[str] = []

    if not contract.validate_module_path(module_name):
        _append_unique(
            violations,
            f"module belongs to unknown layer boundary: {module_name}",
        )

    if module_file:
        file_path = Path(module_file)
        source = file_path.read_text(encoding="utf-8", errors="ignore")
        for issue in _evaluate_module_import_edges(
            contract,
            module_name,
            source,
            current_file=file_path,
        ):
            _append_unique(violations, issue)

    strict_enabled = _strict_mode_enabled(strict)
    _handle_violations(violations, strict=strict_enabled, context=f"import:{module_name}")
    return len(violations) == 0


def get_architecture_diagnostic() -> dict[str, Any]:
    return {
        "architecture_compliant": bool(_LAST_DIAGNOSTIC.get("architecture_compliant", True)),
        "layer_map_version": str(_LAST_DIAGNOSTIC.get("layer_map_version", "RFC-001-derived")),
        "violations": list(_LAST_DIAGNOSTIC.get("violations", [])),
    }


__all__ = [
    "ArchitectureViolationError",
    "enforce_architecture_on_startup",
    "enforce_import_boundary",
    "evaluate_architecture_contract",
    "get_architecture_diagnostic",
]
