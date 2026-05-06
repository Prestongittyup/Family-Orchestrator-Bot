from __future__ import annotations

import ast
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = ROOT / "app"
RUNTIME_SCOPE_ROOTS = (
    ROOT / "app",
    ROOT / "household_os",
    ROOT / "household_state",
)

PROVIDER_ALLOWLIST_PREFIXES = (
    ROOT / "app" / "adapters" / "llm" / "providers",
    ROOT / "app" / "adapters" / "llm" / "gateway.py",
    ROOT / "app" / "adapters" / "llm" / "__init__.py",
)

COMMAND_RUNTIME_ALLOWLIST = {
    ROOT / "app" / "api" / "command.py",
    ROOT / "app" / "api" / "assistant_runtime.py",
    ROOT / "app" / "api" / "tasks.py",
}

SENSITIVE_STATE_WRITE_ALLOWLIST = {
    ROOT / "household_os" / "runtime" / "orchestrator.py",
}

EVENT_LOG_REPOSITORY_IMPORT_ALLOWLIST = {
    ROOT / "app" / "adapters" / "db" / "__init__.py",
    ROOT / "app" / "services" / "events" / "event_log_service.py",
    ROOT / "app" / "services" / "events" / "canonical_router_service.py",
}

ACTION_PIPELINE_DIRECT_CALL_ALLOWLIST = {
    ROOT / "household_os" / "runtime" / "orchestrator.py",
    ROOT / "household_os" / "runtime" / "action_pipeline.py",
}

RFC_ARCHITECTURE_ROOT = ROOT / "docs" / "architecture"
RFC_FILE = RFC_ARCHITECTURE_ROOT / "RFC-001.md"
RFC_REQUIRED_REFERENCE_DOCS = (
    RFC_ARCHITECTURE_ROOT / "Architecture Implementation Plan.md",
    RFC_ARCHITECTURE_ROOT / "Runtime Flow Spec.md",
    RFC_ARCHITECTURE_ROOT / "Enforcement Checklist.md",
    RFC_ARCHITECTURE_ROOT / "LAYER_MAP.md",
)
RFC_REQUIRED_REFERENCE_TOKEN = "RFC-001.md"
RFC_IMMUTABILITY_TOKEN = "IMMUTABLE CONTRACT"

PREMERGE_ENFORCEMENT_FILES = (
    ROOT / ".githooks" / "pre-commit",
    ROOT / ".github" / "workflows" / "governance-gate.yml",
)
PREMERGE_REQUIRED_TOKEN = "python scripts/architecture_layer_guard.py"

FORBIDDEN_ARCHIVE_IMPORT_PATTERN = re.compile(r"from\s+archive\.|import\s+archive\.")
FORBIDDEN_PROVIDER_USAGE_PATTERNS = (
    re.compile(r"(^|\s)import\s+openai\b"),
    re.compile(r"from\s+openai\b"),
    re.compile(r"(^|\s)import\s+anthropic\b"),
    re.compile(r"from\s+anthropic\b"),
    re.compile(r"generativelanguage\.googleapis\.com"),
    re.compile(r"streamGenerateContent\?alt=sse"),
)

BANNED_SERVICE_EXTERNAL_IMPORTS = {
    "httpx",
    "redis",
    "redis.asyncio",
    "sqlalchemy",
    "requests",
    "aiohttp",
}


def _collect_python_files() -> list[Path]:
    return sorted(APP_ROOT.rglob("*.py"))


def _collect_runtime_python_files() -> list[Path]:
    files: set[Path] = set()
    for scope in RUNTIME_SCOPE_ROOTS:
        if not scope.exists():
            continue
        files.update(path for path in scope.rglob("*.py") if path.is_file())
    return sorted(files)


def _module_path(path: Path) -> str:
    rel = path.relative_to(ROOT)
    return rel.with_suffix("").as_posix().replace("/", ".")


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


def _role_for(path: Path) -> str:
    rel = path.relative_to(ROOT).as_posix()
    if rel.startswith("app/api/"):
        return "router"
    if rel == "app/adapters/llm/gateway.py":
        return "gateway"
    if rel.startswith("app/adapters/llm/providers/"):
        return "provider"
    if rel.startswith("app/adapters/"):
        return "adapter"
    if rel.startswith("app/services/"):
        return "service"
    if rel.startswith("app/schemas/"):
        return "core"
    return "other"


def _is_internal(name: str) -> bool:
    return name == "app" or name.startswith("app.")


def _is_path_allowlisted(path: Path, allowlist: set[Path] | tuple[Path, ...]) -> bool:
    return any(path == allowed or allowed in path.parents for allowed in allowlist)


def _first_import_segment(name: str) -> str:
    if not name:
        return ""
    pieces = name.split(".")
    if len(pieces) >= 2:
        return ".".join(pieces[:2])
    return pieces[0]


def _check_rules() -> list[str]:
    violations: list[str] = []

    # RFC immutability and root-of-truth checks.
    if not RFC_FILE.exists():
        violations.append("docs/architecture/RFC-001.md: missing canonical RFC artifact")
    else:
        rfc_source = RFC_FILE.read_text(encoding="utf-8")
        if RFC_IMMUTABILITY_TOKEN not in rfc_source:
            violations.append(
                "docs/architecture/RFC-001.md: missing immutability marker 'IMMUTABLE CONTRACT'"
            )

    for doc in RFC_REQUIRED_REFERENCE_DOCS:
        rel = doc.relative_to(ROOT).as_posix()
        if not doc.exists():
            violations.append(f"{rel}: required architecture document missing")
            continue
        source = doc.read_text(encoding="utf-8")
        if RFC_REQUIRED_REFERENCE_TOKEN not in source:
            violations.append(f"{rel}: missing RFC reference token '{RFC_REQUIRED_REFERENCE_TOKEN}'")
        if "source of truth" not in source.lower():
            violations.append(f"{rel}: missing explicit source-of-truth declaration")

    for hook_file in PREMERGE_ENFORCEMENT_FILES:
        rel = hook_file.relative_to(ROOT).as_posix()
        if not hook_file.exists():
            violations.append(f"{rel}: required pre-merge enforcement file missing")
            continue
        source = hook_file.read_text(encoding="utf-8")
        if PREMERGE_REQUIRED_TOKEN not in source:
            violations.append(f"{rel}: missing architecture guard invocation")

    files = _collect_python_files()
    for path in files:
        module = _module_path(path)
        role = _role_for(path)

        try:
            source = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            source = path.read_text(errors="ignore")

        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            violations.append(f"{path.as_posix()}: syntax error: {exc}")
            continue

        for node in ast.walk(tree):
            if not isinstance(node, (ast.Import, ast.ImportFrom)):
                continue

            imports = _resolve_imports(module, node)
            line = getattr(node, "lineno", 1)

            for imported in imports:
                if not imported:
                    continue

                segment = _first_import_segment(imported)

                if role == "router" and _is_internal(imported):
                    allowed = (
                        imported == "app"
                        or imported.startswith("app.services")
                        or imported.startswith("app.schemas")
                    )
                    if not allowed:
                        violations.append(
                            f"{path.as_posix()}:{line}: router import not allowed -> {imported}"
                        )

                if role == "service" and _is_internal(imported):
                    if imported.startswith("app.api"):
                        violations.append(
                            f"{path.as_posix()}:{line}: service must not import router -> {imported}"
                        )
                    if imported.startswith("app.adapters.llm.providers"):
                        violations.append(
                            f"{path.as_posix()}:{line}: service must not bypass gateway providers -> {imported}"
                        )

                if role == "service" and segment in BANNED_SERVICE_EXTERNAL_IMPORTS:
                    violations.append(
                        f"{path.as_posix()}:{line}: service external import forbidden -> {imported}"
                    )

                if role == "gateway" and _is_internal(imported):
                    allowed = (
                        imported.startswith("app.adapters.llm.providers")
                        or imported.startswith("app.adapters.external")
                    )
                    if not allowed:
                        violations.append(
                            f"{path.as_posix()}:{line}: gateway internal import restricted -> {imported}"
                        )

                if role == "provider" and _is_internal(imported):
                    if imported.startswith("app.services") or imported.startswith("app.api"):
                        violations.append(
                            f"{path.as_posix()}:{line}: provider import forbidden -> {imported}"
                        )
                    if imported.startswith("app.adapters.llm.gateway"):
                        violations.append(
                            f"{path.as_posix()}:{line}: provider must not import gateway -> {imported}"
                        )

                if role == "adapter" and _is_internal(imported):
                    if imported.startswith("app.services") or imported.startswith("app.api"):
                        violations.append(
                            f"{path.as_posix()}:{line}: adapter import forbidden -> {imported}"
                        )

                if segment == "httpx" and not path.as_posix().startswith(
                    (
                        ROOT / "app/adapters/external"
                    ).as_posix()
                ) and not path.as_posix().startswith((ROOT / "app/adapters/providers").as_posix()):
                    violations.append(
                        f"{path.as_posix()}:{line}: httpx must stay in app/adapters/external"
                    )

                if imported.startswith("redis.asyncio") and path.as_posix() != (ROOT / "app/adapters/cache/redis_client.py").as_posix():
                    violations.append(
                        f"{path.as_posix()}:{line}: redis.asyncio must stay in app/adapters/cache/redis_client.py"
                    )

        if role != "gateway" and "stream_json_text(" in source and "app/adapters/llm/providers" not in path.as_posix():
            if "app/adapters/llm/gateway.py" not in path.as_posix():
                violations.append(
                    f"{path.as_posix()}: direct stream_json_text call path not allowed outside gateway/providers"
                )

    # Shadow module guards.
    shadow_pairs = [
        (ROOT / "app/services/usage.py", ROOT / "app/services/usage"),
        (ROOT / "app/services/cache.py", ROOT / "app/services/cache"),
    ]
    for file_path, dir_path in shadow_pairs:
        if file_path.exists() and dir_path.exists():
            violations.append(
                f"shadow conflict: both {file_path.relative_to(ROOT).as_posix()} and {dir_path.relative_to(ROOT).as_posix()} exist"
            )

    runtime_files = _collect_runtime_python_files()
    for path in runtime_files:
        rel_path = path.relative_to(ROOT).as_posix()
        module = _module_path(path)

        try:
            source = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            source = path.read_text(errors="ignore")

        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            violations.append(f"{rel_path}: syntax error: {exc}")
            continue

        if FORBIDDEN_ARCHIVE_IMPORT_PATTERN.search(source):
            violations.append(f"{rel_path}: runtime import forbidden -> archive namespace")

        provider_allowlisted = _is_path_allowlisted(path, PROVIDER_ALLOWLIST_PREFIXES)

        for node in ast.walk(tree):
            if not isinstance(node, (ast.Import, ast.ImportFrom)):
                continue
            line = getattr(node, "lineno", 1)
            imports = _resolve_imports(module, node)
            for imported in imports:
                if imported.startswith("app.adapters.llm.providers") and not provider_allowlisted:
                    violations.append(
                        f"{rel_path}:{line}: direct provider import forbidden outside gateway/providers -> {imported}"
                    )
                if imported.startswith("app.adapters.db.event_log_repository") and path not in EVENT_LOG_REPOSITORY_IMPORT_ALLOWLIST:
                    violations.append(
                        f"{rel_path}:{line}: direct event log repository import forbidden outside event services -> {imported}"
                    )

        if not provider_allowlisted:
            if "stream_json_text(" in source:
                violations.append(
                    f"{rel_path}: direct stream_json_text call forbidden outside gateway/providers"
                )
            for pattern in FORBIDDEN_PROVIDER_USAGE_PATTERNS:
                if pattern.search(source):
                    violations.append(
                        f"{rel_path}: direct provider SDK/network usage forbidden outside gateway/providers"
                    )
                    break

        if rel_path.startswith("app/api/"):
            uses_command_runtime = (
                "get_command_runtime_service(" in source or "handle_command(" in source
            )
            if uses_command_runtime and path not in COMMAND_RUNTIME_ALLOWLIST:
                violations.append(
                    f"{rel_path}: command runtime usage is only allowed in app/api/command.py, app/api/assistant_runtime.py, or app/api/tasks.py"
                )

        writes_sensitive_state = (
            "RequestActionType.WRITE_SENSITIVE_STATE" in source
            or "_write_sensitive_state(" in source
        )
        if writes_sensitive_state and path not in SENSITIVE_STATE_WRITE_ALLOWLIST:
            violations.append(
                f"{rel_path}: sensitive state write path is restricted to household_os/runtime/orchestrator.py"
            )

        direct_action_pipeline_call = (
            ".execute_approved_actions(" in source or ".reject_actions(" in source
        )
        if rel_path.startswith("household_os/") and direct_action_pipeline_call and path not in ACTION_PIPELINE_DIRECT_CALL_ALLOWLIST:
            violations.append(
                f"{rel_path}: direct action pipeline invocation is restricted to household_os/runtime/orchestrator.py"
            )

    return violations


def main() -> int:
    violations = _check_rules()
    if violations:
        print("LAYER_GUARD_FAIL")
        for item in violations:
            print(item)
        return 1

    print("LAYER_GUARD_PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
