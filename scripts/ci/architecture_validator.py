from __future__ import annotations

import ast
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[2]
RFC_PATH = ROOT / "docs" / "architecture" / "RFC-001.md"
LAYER_MAP_PATH = ROOT / "docs" / "architecture" / "LAYER_MAP.md"
SCAN_ROOTS = (
    ROOT / "app",
    ROOT / "household_os" / "runtime",
    ROOT / "household_os" / "security",
)
SERVICE_ROOT = ROOT / "app" / "services"

LAYER_HEADING_PATTERN = re.compile(r"^###\s+\d+\.\s+(?P<name>.+)$")
BULLET_PATTERN = re.compile(r"^-\s+(?P<item>.+)$")
RUNTIME_FLOW_PATTERN = re.compile(r"User Request\s*->\s*.+?->\s*Response")

PROVIDER_SDK_PREFIXES = (
    "openai",
    "anthropic",
    "google.generativeai",
    "googleapiclient",
    "langchain_openai",
)
PROVIDER_ALLOWED_PREFIXES = (
    "app.adapters.llm",
    "app.services.llm_gateway",
    "app.services.provider_sync",
    "app.adapters.llm.gateway",
    "app.adapters.llm.providers",
)
MUTATION_ALLOWED_PREFIXES = (
    "app.services.commands",
    "app.services.events.event_log_service",
    "app.services.execution_gateway",
    "app.services.saga",
    "household_os.runtime",
    "app.adapters.db.event_log_repository",
)
EVENT_REPOSITORY_ALLOWED_PREFIXES = (
    "app.adapters.db.event_log_repository",
    "app.services.commands",
    "app.services.events.event_log_service",
    "app.services.execution_gateway",
    "app.services.saga",
    "household_os.runtime",
)
LLM_ALLOWED_PREFIXES = (
    "app.services.llm_gateway",
    "app.services.provider_sync",
    "app.adapters.llm.gateway",
    "app.adapters.llm.providers",
)

STATE_WRITE_CALL_MARKERS = (
    ".commit",
    ".append_event",
    ".commit_event",
    ".save_graph",
    ".save_state",
    ".write_state",
    ".persist",
    ".write_text",
    "json.dump",
)
SIDE_EFFECT_IMPORT_PREFIXES = (
    "requests",
    "httpx",
    "aiohttp",
    "subprocess",
    "sqlite3",
    "sqlalchemy",
    "openai",
    "anthropic",
)
SIDE_EFFECT_CALL_MARKERS = (
    "requests.",
    "httpx.",
    "aiohttp.",
    "subprocess.",
    "os.system",
    "openai.",
    "anthropic.",
)
LLM_CALL_MARKERS = (
    "stream_json_text",
    "chat.completions.create",
    "responses.create",
    "generativelanguage",
)
DB_WRITE_CALL_MARKERS = (
    ".commit",
)
EVENT_COMMIT_MARKERS = (
    "append_event",
    "commit_event",
    "event_log_repository",
    "event_log_service",
)
RFC_REQUIRED_SERVICE_BOUNDARIES = (
    "execution_gateway",
    "rules_engine",
    "risk_engine",
    "saga",
    "projections",
    "llm_gateway",
    "provider_sync",
    "auth",
    "permissions",
)
AUTH_SYSTEM_KEYWORDS = (
    "/auth/",
    "/permissions/",
    "/authorization/",
    "/identity/",
    "household_os/security/",
)
CANONICAL_AUTH_ROOTS = {
    "app/services/auth",
    "app/services/permissions",
    "household_os/security",
}

EXPECTED_RUNTIME_FLOW = (
    "user request",
    "command validation",
    "execution gateway",
    "rules engine",
    "llm advisory",
    "risk engine",
    "decision",
    "saga execution",
    "event commit",
    "projection update",
    "response",
)
EXPECTED_WRITE_CHAIN = (
    "command",
    "gateway",
    "rules",
    "risk",
    "saga",
    "event",
    "projection",
)

RFC_REQUIRED_BOUNDARY_ALIASES: dict[str, tuple[str, ...]] = {
    "execution_gateway": ("app/services/execution_gateway", "app/services/commands"),
    "rules_engine": ("app/services/rules_engine",),
    "risk_engine": ("app/services/risk_engine", "app/services/policy_engine"),
    "saga": ("app/services/saga", "app/services/events"),
    "projections": ("app/services/projections", "app/services/runtime"),
    "llm_gateway": ("app/services/llm_gateway",),
    "provider_sync": ("app/services/provider_sync",),
    "auth": ("app/services/auth", "household_os/security"),
    "permissions": ("app/services/permissions", "household_os/security"),
}

EXECUTION_GATEWAY_IMPORT_PREFIXES = (
    "app.services.execution_gateway",
    "app.services.commands",
)


@dataclass(frozen=True)
class Violation:
    type: str
    module: str
    rule: str
    severity: str = "critical"

    def as_dict(self) -> dict[str, str]:
        return {
            "type": self.type,
            "module": self.module,
            "rule": self.rule,
            "severity": self.severity,
        }


@dataclass(frozen=True)
class LayerPattern:
    layer_name: str
    raw: str
    normalized: str

    def matches(self, module_key: str) -> bool:
        if module_key == self.normalized:
            return True
        return module_key.startswith(f"{self.normalized}/")


@dataclass
class ModuleRecord:
    path: Path
    rel_path: str
    import_path: str
    module_key: str
    imports: list[str]
    calls: list[str]
    source: str
    layers: list[str]


def _normalize_flow_step(value: str) -> str:
    text = value.strip().lower()
    text = re.sub(r"\(.*?\)", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _path_to_import(rel_path: str) -> str:
    module = rel_path.replace("\\", "/")
    if module.endswith(".py"):
        module = module[:-3]
    if module.endswith("/__init__"):
        module = module[: -len("/__init__")]
    return module.replace("/", ".")


def _module_key(rel_path: str) -> str:
    key = rel_path.replace("\\", "/")
    if key.endswith(".py"):
        key = key[:-3]
    if key.endswith("/__init__"):
        key = key[: -len("/__init__")]
    return key


def _resolve_imports(current_module: str, node: ast.AST) -> list[str]:
    imports: list[str] = []

    if isinstance(node, ast.Import):
        for alias in node.names:
            imports.append(alias.name)
        return imports

    if not isinstance(node, ast.ImportFrom):
        return imports

    base_module = node.module or ""
    if node.level:
        parent_parts = current_module.split(".")[:-1]
        keep_count = len(parent_parts) - (node.level - 1)
        if keep_count < 0:
            keep_count = 0
        prefix = parent_parts[:keep_count]
        if base_module:
            prefix.extend(base_module.split("."))
        base_module = ".".join(part for part in prefix if part)

    if base_module:
        imports.append(base_module)
        for alias in node.names:
            if alias.name == "*":
                continue
            imports.append(f"{base_module}.{alias.name}")

    return imports


def _extract_call_name(expr: ast.AST) -> str:
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        left = _extract_call_name(expr.value)
        if left:
            return f"{left}.{expr.attr}"
        return expr.attr
    return ""


def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for scan_root in SCAN_ROOTS:
        if not scan_root.exists():
            continue
        for path in scan_root.rglob("*.py"):
            if path.is_file() and "__pycache__" not in path.parts:
                files.append(path)
    return sorted(files)


def _parse_layer_map_patterns(layer_map_text: str) -> list[LayerPattern]:
    patterns: list[LayerPattern] = []
    current_layer: str | None = None
    collecting_paths = False

    for line in layer_map_text.splitlines():
        stripped = line.strip()
        heading_match = LAYER_HEADING_PATTERN.match(stripped)
        if heading_match:
            current_layer = heading_match.group("name").strip()
            collecting_paths = False
            continue

        if current_layer is None:
            continue

        if stripped == "Path:":
            collecting_paths = True
            continue

        if not collecting_paths:
            continue

        bullet_match = BULLET_PATTERN.match(stripped)
        if bullet_match:
            raw_path = bullet_match.group("item").strip().strip("`").replace("\\", "/")
            normalized = raw_path.rstrip("/")
            if normalized.endswith("/*"):
                normalized = normalized[:-2]
            normalized = normalized.rstrip("/")
            if normalized:
                patterns.append(
                    LayerPattern(
                        layer_name=current_layer,
                        raw=raw_path,
                        normalized=normalized,
                    )
                )
            continue

        if stripped == "":
            continue

        if stripped.endswith(":") or stripped.startswith("##"):
            collecting_paths = False

    return patterns


def _record_layers(module_key: str, patterns: Iterable[LayerPattern]) -> list[str]:
    layers = [pattern.layer_name for pattern in patterns if pattern.matches(module_key)]
    unique: list[str] = []
    for layer in layers:
        if layer not in unique:
            unique.append(layer)
    return unique


def _build_module_records(patterns: list[LayerPattern]) -> list[ModuleRecord]:
    records: list[ModuleRecord] = []
    for path in _iter_python_files():
        rel_path = path.relative_to(ROOT).as_posix()
        import_path = _path_to_import(rel_path)
        module_key = _module_key(rel_path)
        source = path.read_text(encoding="utf-8", errors="ignore")

        try:
            tree = ast.parse(source)
        except SyntaxError:
            tree = ast.Module(body=[], type_ignores=[])

        imports: list[str] = []
        calls: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                imports.extend(_resolve_imports(import_path, node))
            if isinstance(node, ast.Call):
                call_name = _extract_call_name(node.func)
                if call_name:
                    calls.append(call_name)

        records.append(
            ModuleRecord(
                path=path,
                rel_path=rel_path,
                import_path=import_path,
                module_key=module_key,
                imports=imports,
                calls=calls,
                source=source,
                layers=_record_layers(module_key, patterns),
            )
        )

    return records


def _starts_with_module_prefix(module_name: str, prefixes: Iterable[str]) -> bool:
    return any(module_name == prefix or module_name.startswith(f"{prefix}.") for prefix in prefixes)


def _source_contains_any(source: str, markers: Iterable[str]) -> bool:
    lowered = source.lower()
    return any(marker.lower() in lowered for marker in markers)


def _call_matches_marker(call: str, marker: str) -> bool:
    normalized_call = call.lower().strip()
    normalized_marker = marker.lower().strip()
    if not normalized_call or not normalized_marker:
        return False
    if normalized_marker.startswith("."):
        return normalized_call.endswith(normalized_marker[1:])
    return normalized_call == normalized_marker or normalized_call.endswith(f".{normalized_marker}")


def _calls_contain_any(calls: Iterable[str], markers: Iterable[str]) -> bool:
    return any(_call_matches_marker(call, marker) for call in calls for marker in markers)


def _has_state_write(module: ModuleRecord) -> bool:
    return _calls_contain_any(module.calls, STATE_WRITE_CALL_MARKERS)


def _has_db_write(module: ModuleRecord) -> bool:
    if _calls_contain_any(module.calls, DB_WRITE_CALL_MARKERS):
        return True
    return bool(
        re.search(r"execute\(\s*['\"]\s*(insert|update|delete)\b", module.source, flags=re.IGNORECASE)
    )


def _has_side_effects(module: ModuleRecord) -> bool:
    if _has_state_write(module):
        return True
    for imported in module.imports:
        if imported.split(".")[0] in SIDE_EFFECT_IMPORT_PREFIXES:
            return True
    return _calls_contain_any(module.calls, SIDE_EFFECT_CALL_MARKERS)


def _has_provider_sdk_import(module: ModuleRecord) -> bool:
    for imported in module.imports:
        if _starts_with_module_prefix(imported, PROVIDER_SDK_PREFIXES):
            return True
    if _source_contains_any(module.source, ("generativelanguage.googleapis.com", "streamGenerateContent")):
        return True
    return False


def _has_llm_call(module: ModuleRecord) -> bool:
    return _source_contains_any(module.source, LLM_CALL_MARKERS)


def _flow_steps_from_rfc(rfc_text: str) -> list[str]:
    match = RUNTIME_FLOW_PATTERN.search(rfc_text)
    if not match:
        return []
    raw_flow = match.group(0)
    steps = [step.strip() for step in raw_flow.split("->") if step.strip()]
    return [_normalize_flow_step(step) for step in steps]


def _collect_roots(records: Iterable[ModuleRecord], predicate) -> set[str]:
    roots: set[str] = set()
    for module in records:
        if not predicate(module):
            continue
        rel = module.rel_path
        if rel.startswith("app/services/"):
            pieces = rel.split("/")
            if len(pieces) >= 3:
                roots.add(f"app/services/{pieces[2]}")
                continue
        if rel.startswith("household_os/"):
            pieces = rel.split("/")
            if len(pieces) >= 2:
                roots.add(f"household_os/{pieces[1]}")
                continue
        if rel.startswith("scripts/"):
            pieces = rel.split("/")
            if len(pieces) >= 2:
                roots.add(f"scripts/{pieces[1]}")
                continue
        roots.add(rel)
    return roots


def _service_folder_has_python(path: Path) -> bool:
    return any(candidate.is_file() for candidate in path.rglob("*.py"))


def _validate() -> list[Violation]:
    violations: list[Violation] = []
    seen: set[tuple[str, str, str, str]] = set()

    def add_violation(v_type: str, module: str, rule: str, severity: str = "critical") -> None:
        key = (v_type, module, rule, severity)
        if key in seen:
            return
        seen.add(key)
        violations.append(Violation(type=v_type, module=module, rule=rule, severity=severity))

    if not RFC_PATH.exists():
        add_violation("rfc_compliance", "docs/architecture/RFC-001.md", "RFC-001.md input missing")
        return violations
    if not LAYER_MAP_PATH.exists():
        add_violation("layer_violation", "docs/architecture/LAYER_MAP.md", "LAYER_MAP.md input missing")
        return violations

    rfc_text = RFC_PATH.read_text(encoding="utf-8", errors="ignore")
    layer_map_text = LAYER_MAP_PATH.read_text(encoding="utf-8", errors="ignore")

    patterns = _parse_layer_map_patterns(layer_map_text)
    if not patterns:
        add_violation("layer_violation", "docs/architecture/LAYER_MAP.md", "No layer path declarations found")
        return violations

    records = _build_module_records(patterns)

    # RULE A — Layer existence validation.
    for module in records:
        if not module.layers:
            add_violation(
                "layer_violation",
                module.rel_path,
                "module path not mapped to declared layer in LAYER_MAP",
            )

    declared_service_folders: set[str] = set()
    for pattern in patterns:
        if not pattern.normalized.startswith("app/services/"):
            continue
        remainder = pattern.normalized[len("app/services/") :]
        service_folder = remainder.split("/", 1)[0].strip()
        if service_folder:
            declared_service_folders.add(service_folder)

    if SERVICE_ROOT.exists():
        for child in SERVICE_ROOT.iterdir():
            if not child.is_dir() or child.name == "__pycache__":
                continue
            if not _service_folder_has_python(child):
                continue
            if child.name not in declared_service_folders:
                add_violation(
                    "layer_violation",
                    f"app/services/{child.name}/",
                    "new service folder not declared in LAYER_MAP",
                )

    # RULE B — Cross-layer import rules.
    for module in records:
        is_provider_allowed = _starts_with_module_prefix(module.import_path, PROVIDER_ALLOWED_PREFIXES)
        is_mutation_allowed = _starts_with_module_prefix(module.import_path, MUTATION_ALLOWED_PREFIXES)

        if not is_mutation_allowed and _has_state_write(module):
            add_violation(
                "boundary_violation",
                module.rel_path,
                "execution_gateway bypass detected via state mutation outside execution_gateway/saga/runtime",
            )

        for imported in module.imports:
            if imported.startswith("app.adapters.llm.providers") and not is_provider_allowed:
                add_violation(
                    "boundary_violation",
                    module.rel_path,
                    "direct provider access outside llm_gateway/provider_sync",
                )

        if _has_provider_sdk_import(module) and not is_provider_allowed:
            add_violation(
                "boundary_violation",
                module.rel_path,
                "direct provider SDK import outside llm_gateway/provider_sync",
            )

        if _starts_with_module_prefix(module.import_path, ("app.services.rules_engine",)) and _has_state_write(module):
            add_violation(
                "boundary_violation",
                module.rel_path,
                "rules_engine writing state",
            )

        if _starts_with_module_prefix(module.import_path, ("app.services.risk_engine",)) and _has_side_effects(module):
            add_violation(
                "boundary_violation",
                module.rel_path,
                "risk_engine executing side effects",
            )

        if _starts_with_module_prefix(module.import_path, ("app.services.projections",)) and _has_state_write(module):
            add_violation(
                "boundary_violation",
                module.rel_path,
                "projections writing state",
            )

        if _starts_with_module_prefix(module.import_path, ("app.services.saga",)):
            imports_execution_gateway = any(
                imported == "app.services.execution_gateway"
                or imported.startswith("app.services.execution_gateway.")
                for imported in module.imports
            )
            if _has_state_write(module) and not imports_execution_gateway:
                add_violation(
                    "boundary_violation",
                    module.rel_path,
                    "saga bypassing execution_gateway",
                )

    # RULE C — Architecture duplication detection.
    execution_gateway_roots = _collect_roots(
        records,
        lambda module: module.rel_path.startswith("app/services/execution_gateway/")
        or module.rel_path.startswith("app/services/commands/")
        or module.rel_path.endswith("execution_gateway.py"),
    )
    if len(execution_gateway_roots) > 1:
        add_violation(
            "duplication_violation",
            ", ".join(sorted(execution_gateway_roots)),
            "more than one execution gateway exists",
        )

    saga_roots = _collect_roots(
        records,
        lambda module: "/saga/" in module.rel_path or module.rel_path.endswith("saga.py"),
    )
    if len(saga_roots) > 1:
        add_violation(
            "duplication_violation",
            ", ".join(sorted(saga_roots)),
            "multiple saga systems exist",
        )

    event_commit_roots = _collect_roots(
        records,
        lambda module: module.rel_path.startswith("app/services/") and _source_contains_any(module.source, EVENT_COMMIT_MARKERS),
    )
    canonical_event_root_sets = (
        {"app/services/commands", "app/services/events"},
        {"app/services/execution_gateway", "app/services/saga"},
        {"app/services/execution_gateway"},
        {"app/services/saga"},
    )
    if event_commit_roots and not any(event_commit_roots.issubset(allowed_roots) for allowed_roots in canonical_event_root_sets):
        add_violation(
            "duplication_violation",
            ", ".join(sorted(event_commit_roots)),
            "multiple event commit pipelines exist",
        )

    auth_roots = _collect_roots(
        records,
        lambda module: any(token in module.rel_path for token in AUTH_SYSTEM_KEYWORDS),
    )
    non_canonical_auth_roots = {root for root in auth_roots if root not in CANONICAL_AUTH_ROOTS}
    if non_canonical_auth_roots:
        add_violation(
            "duplication_violation",
            ", ".join(sorted(non_canonical_auth_roots)),
            "multiple authorization systems exist",
        )

    # RULE D — Forbidden anti-pattern scan.
    for module in records:
        is_event_repo_allowed = _starts_with_module_prefix(module.import_path, EVENT_REPOSITORY_ALLOWED_PREFIXES)
        is_llm_allowed = _starts_with_module_prefix(module.import_path, LLM_ALLOWED_PREFIXES)
        is_mutation_allowed = _starts_with_module_prefix(module.import_path, MUTATION_ALLOWED_PREFIXES)

        if _has_db_write(module) and not is_event_repo_allowed:
            add_violation(
                "anti_pattern",
                module.rel_path,
                "direct DB writes outside event repository",
            )

        if _has_provider_sdk_import(module) and not is_llm_allowed:
            add_violation(
                "anti_pattern",
                module.rel_path,
                "provider SDK imports outside gateway",
            )

        if _has_llm_call(module) and not is_llm_allowed:
            add_violation(
                "anti_pattern",
                module.rel_path,
                "LLM calls outside llm_gateway",
            )

        if _has_state_write(module) and not is_mutation_allowed:
            add_violation(
                "anti_pattern",
                module.rel_path,
                "state mutation outside execution_gateway/saga/runtime",
            )

    # RULE E — RFC-001 compliance enforcement.
    runtime_flow_steps = _flow_steps_from_rfc(rfc_text)
    if not runtime_flow_steps:
        add_violation(
            "rfc_compliance",
            "docs/architecture/RFC-001.md",
            "runtime flow order missing from RFC-001 Section 2",
        )
    elif runtime_flow_steps != list(EXPECTED_RUNTIME_FLOW):
        add_violation(
            "rfc_compliance",
            "docs/architecture/RFC-001.md",
            "runtime flow order does not match canonical RFC-001 sequence",
        )

    for boundary, aliases in RFC_REQUIRED_BOUNDARY_ALIASES.items():
        boundary_exists = any((ROOT / alias).exists() and (ROOT / alias).is_dir() for alias in aliases)
        if not boundary_exists:
            add_violation(
                "rfc_compliance",
                aliases[0],
                "required RFC-001 service boundary missing",
            )

    gateway_records = [
        module
        for module in records
        if _starts_with_module_prefix(module.import_path, EXECUTION_GATEWAY_IMPORT_PREFIXES)
    ]

    write_chain_verified = False
    for module in gateway_records:
        text = module.source.lower()
        has_command_phase = "handle_command" in text and "command_type" in text
        has_rules_phase = "rules_engine" in text or "rules" in text
        has_risk_phase = "risk" in text or "policy_resolution" in text or "policy" in text
        has_saga_phase = "saga" in text
        has_event_phase = any(marker in text for marker in ("event_log", "append_event", "commit_event", "log_system_event"))
        has_projection_phase = "projection" in text or "get_projection" in text

        if (
            has_command_phase
            and has_rules_phase
            and has_risk_phase
            and has_saga_phase
            and has_event_phase
            and has_projection_phase
        ):
            write_chain_verified = True
            break

    if not write_chain_verified:
        add_violation(
            "rfc_compliance",
            "app/services/execution_gateway",
            "write flow not enforced as Command -> Gateway -> Rules -> Risk -> Saga -> Event -> Projection",
        )

    return violations


def main() -> int:
    violations = _validate()

    payload = {
        "architecture_valid": len(violations) == 0,
        "violations": [violation.as_dict() for violation in violations],
    }

    print(json.dumps(payload, indent=2))

    return 0 if payload["architecture_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
