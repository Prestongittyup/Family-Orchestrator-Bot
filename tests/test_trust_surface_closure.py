from __future__ import annotations

import ast
import inspect
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from apps.api.core.state_machine import ActionState, StateMachine
from household_os.core.lifecycle_state import LifecycleState
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.runtime.action_pipeline import ActionPipeline
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.event_store import InMemoryEventStore
from household_os.runtime.orchestrator import HouseholdOSOrchestrator, OrchestratorRequest
from household_os.security.trust_boundary_enforcer import SecurityViolation


REPO_ROOT = Path(__file__).resolve().parents[1]

SENSITIVE_IMPORT_PATTERNS = {
    "household_os.runtime.action_pipeline",
    "household_os.core.household_state_graph",
    "household_os.runtime.event_store",
    "apps.api.core.state_machine",
    "household_os.core.decision_engine",
}

SENSITIVE_CALL_PATTERNS = {
    "approve_actions",
    "reject_actions",
    "execute_approved_actions",
    "reject_action_timeout",
    "load_graph",
    "save_graph",
    "append",
    "transition_to",
    "run",
}

AUTHORIZED_INTERNAL_SURFACE = {
    "household_os.runtime.orchestrator",
    "household_os.security.authorization_gate",
    "household_os.runtime.daily_cycle",
    "apps.api.hpal.orchestration_adapter",
    "apps.api.assistant_runtime_router",
    "household_os.runtime.lifecycle_migration",
    "legacy.lifecycle.execution_state_machine",
    "scripts.migrate_event_stream",
    "scripts.migrate_lifecycle_states",
    "scripts.phase1_breakpoint_validation",
    "scripts.production_torture_audit",
    "scripts.verify_worker_hardening",
}


@dataclass
class ModuleAnalysis:
    path: Path
    module: str
    direct_sensitive_imports: list[str] = field(default_factory=list)
    sensitive_calls: list[str] = field(default_factory=list)


@dataclass
class EntryPointAnalysis:
    module: str
    qualname: str
    entry_type: str
    trace: list[str]
    classification: str
    reason: str


def _module_name_for_path(path: Path) -> str:
    rel = path.relative_to(REPO_ROOT).as_posix()
    if rel.endswith(".py"):
        rel = rel[:-3]
    return rel.replace("/", ".")


def _iter_project_python_files() -> list[Path]:
    files: list[Path] = []
    for path in REPO_ROOT.rglob("*.py"):
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel.startswith((".venv/", "tests/", "archive/", "docs/")):
            continue
        files.append(path)
    return sorted(files)


def _is_route_decorator(dec: ast.expr) -> bool:
    if isinstance(dec, ast.Call):
        dec = dec.func
    return isinstance(dec, ast.Attribute) and dec.attr in {"get", "post", "put", "patch", "delete"}


def _collect_calls(node: ast.AST) -> list[str]:
    calls: list[str] = []
    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            fn = child.func
            if isinstance(fn, ast.Attribute):
                owner = ""
                if isinstance(fn.value, ast.Name):
                    owner = fn.value.id
                calls.append(f"{owner}.{fn.attr}".strip("."))
            elif isinstance(fn, ast.Name):
                calls.append(fn.id)
    return calls


def _scan_module(path: Path) -> tuple[ModuleAnalysis, list[EntryPointAnalysis]]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    module = _module_name_for_path(path)

    module_analysis = ModuleAnalysis(path=path, module=module)
    entry_points: list[EntryPointAnalysis] = []

    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            imported = (node.module or "").strip()
            if imported in SENSITIVE_IMPORT_PATTERNS:
                module_analysis.direct_sensitive_imports.append(imported)

        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in SENSITIVE_IMPORT_PATTERNS:
                    module_analysis.direct_sensitive_imports.append(alias.name)

    all_calls = _collect_calls(tree)
    module_analysis.sensitive_calls = [
        call for call in all_calls if call.split(".")[-1] in SENSITIVE_CALL_PATTERNS
    ]

    candidates: list[tuple[str, str, ast.AST, str]] = []

    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            is_route = any(_is_route_decorator(dec) for dec in node.decorator_list)
            is_script_entry = node.name == "main" and "scripts" in module
            if is_route:
                candidates.append((module, node.name, node, "http_route"))
            elif is_script_entry:
                candidates.append((module, node.name, node, "cli_script"))

        if isinstance(node, ast.ClassDef):
            lower_name = node.name.lower()
            is_service_like = any(token in lower_name for token in ("adapter", "service", "cycle", "scheduler"))
            for item in node.body:
                if not isinstance(item, ast.FunctionDef):
                    continue
                if item.name.startswith("_"):
                    continue
                if is_service_like:
                    if "adapter" in lower_name:
                        etype = "adapter"
                    elif "cycle" in lower_name or "scheduler" in lower_name:
                        etype = "scheduler_job"
                    else:
                        etype = "service_module"
                    candidates.append((module, f"{node.name}.{item.name}", item, etype))

    module_has_sensitive_imports = bool(module_analysis.direct_sensitive_imports)

    for mod_name, qualname, node, entry_type in candidates:
        calls = _collect_calls(node)
        has_handle_request = any(call.endswith("handle_request") for call in calls)
        sensitive_direct = [call for call in calls if call.split(".")[-1] in SENSITIVE_CALL_PATTERNS]
        touches_sensitive_runtime = bool(sensitive_direct) or module_has_sensitive_imports

        if has_handle_request:
            classification = "SAFE"
            reason = "Routes through orchestrator.handle_request"
        elif mod_name in AUTHORIZED_INTERNAL_SURFACE:
            classification = "SAFE"
            reason = "Explicitly allowed internal surface"
        elif sensitive_direct:
            classification = "DIRECT_MUTATION_SURFACE"
            reason = "Sensitive direct calls without orchestrator.handle_request"
        elif touches_sensitive_runtime and entry_type in {
            "http_route",
            "adapter",
            "scheduler_job",
            "cli_script",
            "service_module",
        }:
            classification = "UNROUTED"
            reason = "Entry point touches sensitive runtime surface but does not prove routing"
        elif entry_type in {"http_route", "adapter", "scheduler_job", "cli_script", "service_module"}:
            classification = "SAFE"
            reason = "No sensitive runtime mutation surface detected"
        else:
            classification = "UNKNOWN"
            reason = "Unable to classify entry point"

        entry_points.append(
            EntryPointAnalysis(
                module=mod_name,
                qualname=qualname,
                entry_type=entry_type,
                trace=calls,
                classification=classification,
                reason=reason,
            )
        )

    return module_analysis, entry_points


def _render_report(modules: list[ModuleAnalysis], entries: list[EntryPointAnalysis]) -> str:
    lines: list[str] = []
    lines.append("=== TRUST SURFACE CLOSURE REPORT ===")
    lines.append("")
    lines.append("1) PER-MODULE ANALYSIS")
    for mod in modules:
        lines.append(f"- module: {mod.module}")
        lines.append(f"  path: {mod.path.relative_to(REPO_ROOT).as_posix()}")
        lines.append(f"  direct_sensitive_imports: {sorted(set(mod.direct_sensitive_imports))}")
        lines.append(f"  sensitive_calls: {sorted(set(mod.sensitive_calls))}")

    lines.append("")
    lines.append("2) ENTRY POINT TRACES")
    for ep in sorted(entries, key=lambda x: (x.module, x.qualname)):
        lines.append(f"- [{ep.classification}] {ep.entry_type} {ep.module}:{ep.qualname}")
        lines.append(f"  reason: {ep.reason}")
        lines.append(f"  trace: {ep.trace}")

    bypassable = [
        ep for ep in entries if ep.classification in {"UNROUTED", "DIRECT_MUTATION_SURFACE", "UNKNOWN"}
    ]
    lines.append("")
    lines.append("3) DETECTED BYPASSABLE ENTRY POINTS")
    if bypassable:
        for ep in bypassable:
            lines.append(f"- {ep.classification}: {ep.module}:{ep.qualname}")
    else:
        lines.append("- none")

    counts = {"SAFE": 0, "UNROUTED": 0, "DIRECT_MUTATION_SURFACE": 0, "UNKNOWN": 0}
    for ep in entries:
        counts[ep.classification] = counts.get(ep.classification, 0) + 1

    lines.append("")
    lines.append("4) FINAL RISK SUMMARY")
    lines.append(f"- SAFE: {counts['SAFE']}")
    lines.append(f"- UNROUTED: {counts['UNROUTED']}")
    lines.append(f"- DIRECT_MUTATION_SURFACE: {counts['DIRECT_MUTATION_SURFACE']}")
    lines.append(f"- UNKNOWN: {counts['UNKNOWN']}")

    return "\n".join(lines)


@pytest.fixture
def runtime_guard():
    """Built-in trust-boundary guards are already active in sensitive modules."""
    return {"active": True}


def test_static_trust_surface_closure_report():
    modules: list[ModuleAnalysis] = []
    entries: list[EntryPointAnalysis] = []

    for py_file in _iter_project_python_files():
        mod_analysis, eps = _scan_module(py_file)
        modules.append(mod_analysis)
        entries.extend(eps)

    report = _render_report(modules, entries)
    print(report)

    bypassable = [
        ep for ep in entries if ep.classification in {"UNROUTED", "DIRECT_MUTATION_SURFACE", "UNKNOWN"}
    ]

    # For closure enforcement, UNROUTED/DIRECT surfaces are not acceptable unless explicitly allowed.
    blocked = [ep for ep in bypassable if ep.classification in {"UNROUTED", "DIRECT_MUTATION_SURFACE"}]
    assert not blocked, (
        "Trust surface closure failed. Detected bypassable entry points:\n"
        + "\n".join(f"- {ep.classification}: {ep.module}:{ep.qualname}" for ep in blocked)
    )


def test_negative_direct_pipeline_call_must_fail(runtime_guard):
    pipeline = ActionPipeline()
    with pytest.raises(SecurityViolation, match="blocked|provenance|unauthorized"):
        pipeline.execute_approved_actions(graph={"household_id": "h1", "action_lifecycle": {"actions": {}}}, now="2026-04-22T08:00:00Z")


def test_negative_direct_event_store_append_must_fail(runtime_guard):
    store = InMemoryEventStore()
    event = DomainEvent.create(
        aggregate_id="a1",
        event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
        payload={"state": LifecycleState.PROPOSED},
        metadata={"actor_type": "api_user"},
    )
    with pytest.raises(SecurityViolation, match="blocked|provenance|unauthorized"):
        store.append(event, provenance_token=object(), test_mode=True)


def test_negative_direct_state_store_save_must_fail(tmp_path: Path, runtime_guard):
    store = HouseholdStateGraphStore(graph_path=tmp_path / "state.json")
    with pytest.raises(SecurityViolation, match="blocked|provenance|unauthorized"):
        store.save_graph({"household_id": "h1", "action_lifecycle": {"actions": {}, "transition_log": []}})


def test_negative_fsm_transition_outside_orchestrator_must_fail(runtime_guard):
    fsm = StateMachine(action_id="a1")
    with pytest.raises(SecurityViolation, match="blocked|provenance|unauthorized"):
        fsm.transition_to(ActionState.PENDING_APPROVAL, reason="external")


def test_authorized_path_via_handle_request_is_allowed(tmp_path: Path, runtime_guard):
    store = HouseholdStateGraphStore(graph_path=tmp_path / "auth.json")
    store.verify_household_owner = lambda hid, uid: True
    orch = HouseholdOSOrchestrator(state_store=store)

    result = orch.handle_request(
        OrchestratorRequest(
            action_type="RUN",
            household_id="h1",
            actor={
                "actor_type": "system_worker",
                "subject_id": "system",
                "session_id": None,
                "verified": True,
            },
            user_input="schedule dentist appointment tomorrow",
            context={"system_worker_verified": True},
        )
    )
    assert result is not None
