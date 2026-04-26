from __future__ import annotations

import ast
import inspect
from dataclasses import dataclass
from pathlib import Path

import pytest

from apps.api.core.state_machine import ActionState, StateMachine
from household_os.core.decision_engine import HouseholdOSDecisionEngine
from household_os.core.lifecycle_state import LifecycleState
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.runtime.action_pipeline import ActionPipeline
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.event_store import InMemoryEventStore
from household_os.runtime.orchestrator import HouseholdOSOrchestrator, OrchestratorRequest, RequestActionType
from household_os.security.trust_boundary_enforcer import SecurityViolation
from household_os.security.trust_surface_registry import (
    ALLOWED_IMPORTERS_BY_MODULE,
    AUTHORIZED_ENTRYPOINTS,
    FORBIDDEN_DIRECT_SURFACES,
)


REPO_ROOT = Path(__file__).resolve().parents[1]

ENTRYPOINT_FOLDERS = ("apps/", "household_os/", "scripts/", "ui/")
ENTRYPOINT_TOKENS = ("router", "adapter", "service", "scheduler", "cycle")
MUTATION_CALLS = {
    "refresh_graph",
    "store_response",
    "apply_approval",
    "load_graph",
    "save_graph",
    "append",
    "transition_to",
    "execute_approved_actions",
    "approve_actions",
    "reject_actions",
    "reject_action_timeout",
}

MUTATION_OWNER_HINTS = {
    "state_store",
    "graph_store",
    "event_store",
    "state_machine",
    "fsm",
    "action_pipeline",
    "decision_engine",
    "os_state_store",
    "self._graph_store",
    "self._decision_engine",
}


@dataclass(frozen=True)
class SurfaceFinding:
    kind: str
    module: str
    symbol: str
    reason: str


def _module_name(path: Path) -> str:
    rel = path.relative_to(REPO_ROOT).as_posix()
    return rel[:-3].replace("/", ".") if rel.endswith(".py") else rel.replace("/", ".")


def _iter_project_files() -> list[Path]:
    files: list[Path] = []
    for path in REPO_ROOT.rglob("*.py"):
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel.startswith((".venv/", "archive/")):
            continue
        files.append(path)
    return sorted(files)


def _allowed_importer(importer: str, allowed: tuple[str, ...]) -> bool:
    return any(importer == item or importer.startswith(f"{item}.") for item in allowed)


def _collect_calls(node: ast.AST) -> list[str]:
    calls: list[str] = []
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        fn = child.func
        if isinstance(fn, ast.Attribute):
            owner = ""
            if isinstance(fn.value, ast.Name):
                owner = fn.value.id
            elif isinstance(fn.value, ast.Attribute) and isinstance(fn.value.value, ast.Name):
                owner = f"{fn.value.value.id}.{fn.value.attr}"
            calls.append(f"{owner}.{fn.attr}".strip("."))
        elif isinstance(fn, ast.Name):
            calls.append(fn.id)
    return calls


def _is_entrypoint(rel: str, fn_name: str, class_name: str | None, decorators: list[ast.expr]) -> bool:
    if rel.startswith("scripts/") and fn_name == "main":
        return True
    if rel.startswith("ui/") and fn_name.startswith("run_"):
        return True
    if any(tok in rel for tok in ENTRYPOINT_FOLDERS) and class_name:
        lower = class_name.lower()
        if any(token in lower for token in ENTRYPOINT_TOKENS):
            return not fn_name.startswith("_")
    for dec in decorators:
        if isinstance(dec, ast.Call):
            dec = dec.func
        if isinstance(dec, ast.Attribute) and dec.attr in {"get", "post", "put", "patch", "delete"}:
            return True
    return False


def _scan_static_surfaces() -> tuple[list[SurfaceFinding], list[SurfaceFinding], list[str]]:
    resolved: list[SurfaceFinding] = []
    remaining: list[SurfaceFinding] = []
    call_graph: list[str] = []

    for path in _iter_project_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        module = _module_name(path)
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))

        forbidden_imports: list[str] = []
        for node in tree.body:
            if isinstance(node, ast.ImportFrom):
                imported = (node.module or "").strip()
                if imported in ALLOWED_IMPORTERS_BY_MODULE:
                    allowed = ALLOWED_IMPORTERS_BY_MODULE[imported]
                    if not _allowed_importer(module, allowed):
                        forbidden_imports.append(imported)
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported = alias.name
                    if imported in ALLOWED_IMPORTERS_BY_MODULE:
                        allowed = ALLOWED_IMPORTERS_BY_MODULE[imported]
                        if not _allowed_importer(module, allowed):
                            forbidden_imports.append(imported)

        if forbidden_imports:
            for imported in sorted(set(forbidden_imports)):
                remaining.append(
                    SurfaceFinding(
                        kind="DIRECT_MUTATION_SURFACE",
                        module=module,
                        symbol=imported,
                        reason="Forbidden direct import outside trust registry",
                    )
                )
        else:
            resolved.append(
                SurfaceFinding(
                    kind="RESOLVED",
                    module=module,
                    symbol="imports",
                    reason="No forbidden direct sensitive imports",
                )
            )

        for node in tree.body:
            functions: list[tuple[str, ast.FunctionDef, str | None]] = []
            if isinstance(node, ast.FunctionDef):
                functions.append((node.name, node, None))
            if isinstance(node, ast.ClassDef):
                for child in node.body:
                    if isinstance(child, ast.FunctionDef):
                        functions.append((child.name, child, node.name))

            for fn_name, fn_node, cls_name in functions:
                if not _is_entrypoint(rel, fn_name, cls_name, list(fn_node.decorator_list)):
                    continue

                calls = _collect_calls(fn_node)
                has_handle_request = any(call.endswith("handle_request") for call in calls)
                qualname = f"{cls_name}.{fn_name}" if cls_name else fn_name
                fq_entry = f"{module}.{qualname}"

                touches_mutation = False
                for call in calls:
                    parts = call.split(".")
                    attr = parts[-1]
                    owner = ".".join(parts[:-1]).lower()
                    if attr in MUTATION_CALLS and any(hint in owner for hint in MUTATION_OWNER_HINTS):
                        touches_mutation = True
                        break

                if fq_entry in AUTHORIZED_ENTRYPOINTS:
                    call_graph.append(f"{module}:{qualname} -> authorized_internal")
                    continue

                if touches_mutation and not has_handle_request:
                    remaining.append(
                        SurfaceFinding(
                            kind="UNROUTED",
                            module=module,
                            symbol=qualname,
                            reason="Mutation path does not route through orchestrator.handle_request",
                        )
                    )
                    call_graph.append(f"{module}:{qualname} -> NOT_ROUTED")
                else:
                    route = "orchestrator.handle_request" if has_handle_request else "read-only/no-mutation"
                    call_graph.append(f"{module}:{qualname} -> {route}")

    return resolved, remaining, sorted(call_graph)


def _trusted_actor() -> dict[str, object]:
    return {
        "actor_type": "system_worker",
        "subject_id": "system",
        "session_id": None,
        "verified": True,
    }


def _assert_orchestrator_provenance() -> None:
    for frame in inspect.stack()[2:]:
        module_name = str(frame.frame.f_globals.get("__name__", ""))
        if module_name.startswith("household_os.runtime.orchestrator"):
            return
    raise SecurityViolation("orchestrator provenance token required")


@pytest.mark.parametrize(
    ("owner", "method_name"),
    [
        (InMemoryEventStore, "append"),
        (HouseholdStateGraphStore, "load_graph"),
        (HouseholdStateGraphStore, "save_graph"),
        (StateMachine, "transition_to"),
        (ActionPipeline, "execute_approved_actions"),
        (HouseholdOSDecisionEngine, "run"),
    ],
)
def test_runtime_enforcement_requires_orchestrator_provenance(monkeypatch: pytest.MonkeyPatch, owner: type, method_name: str) -> None:
    original = getattr(owner, method_name)

    def _guard(*args, **kwargs):
        _assert_orchestrator_provenance()
        return original(*args, **kwargs)

    monkeypatch.setattr(owner, method_name, _guard)

    if owner is InMemoryEventStore:
        store = InMemoryEventStore()
        store.bind_internal_gate_token(object())
        event = DomainEvent.create(
            aggregate_id="a1",
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            payload={"state": LifecycleState.PROPOSED},
            metadata={"actor_type": "system_worker", "subject_id": "system", "request_id": "r1"},
        )
        with pytest.raises(SecurityViolation, match="orchestrator provenance token required"):
            store.append(event, provenance_token=object())
        return

    if owner is HouseholdStateGraphStore and method_name == "load_graph":
        with pytest.raises(SecurityViolation, match="orchestrator provenance token required"):
            HouseholdStateGraphStore().load_graph("h1")
        return

    if owner is HouseholdStateGraphStore and method_name == "save_graph":
        with pytest.raises(SecurityViolation, match="orchestrator provenance token required"):
            HouseholdStateGraphStore().save_graph({"household_id": "h1"})
        return

    if owner is StateMachine:
        with pytest.raises(SecurityViolation, match="orchestrator provenance token required"):
            StateMachine(action_id="a1").transition_to(ActionState.PENDING_APPROVAL, reason="direct")
        return

    if owner is ActionPipeline:
        with pytest.raises(SecurityViolation, match="orchestrator provenance token required"):
            ActionPipeline().execute_approved_actions(graph={"household_id": "h1", "action_lifecycle": {"actions": {}}}, now="2026-04-22T00:00:00Z")
        return

    if owner is HouseholdOSDecisionEngine:
        with pytest.raises(SecurityViolation, match="orchestrator provenance token required"):
            HouseholdOSDecisionEngine().run(
                household_id="h1",
                query="test",
                graph={"household_id": "h1", "reference_time": "2026-04-22T00:00:00Z"},
                request_id="r1",
            )


def test_runtime_authorized_orchestrator_path_is_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    original = InMemoryEventStore.append

    def _guard(*args, **kwargs):
        _assert_orchestrator_provenance()
        return original(*args, **kwargs)

    monkeypatch.setattr(InMemoryEventStore, "append", _guard)

    orchestrator = HouseholdOSOrchestrator()
    result = orchestrator.handle_request(
        OrchestratorRequest(
            action_type=RequestActionType.RUN,
            household_id="h1",
            actor=_trusted_actor(),
            user_input="schedule dentist appointment tomorrow",
            context={"system_worker_verified": True},
        )
    )
    assert result is not None


def test_trust_surface_final_closure_report() -> None:
    resolved, remaining, call_graph = _scan_static_surfaces()
    remaining_direct = [f for f in remaining if f.kind == "DIRECT_MUTATION_SURFACE"]
    remaining_unrouted = [f for f in remaining if f.kind == "UNROUTED"]

    print("### A. Closure Report")
    print(f"- RESOLVED surfaces: {len(resolved)}")
    print(f"- REMAINING DIRECT_MUTATION_SURFACE: {len(remaining_direct)}")
    print(f"- UNROUTED entrypoints: {len(remaining_unrouted)}")
    if remaining:
        for finding in sorted(remaining, key=lambda f: (f.kind, f.module, f.symbol)):
            print(f"- {finding.kind}: {finding.module}:{finding.symbol} ({finding.reason})")

    print("\n### B. Call graph summary")
    print(f"- AUTHORIZED_ENTRYPOINTS: {len(AUTHORIZED_ENTRYPOINTS)}")
    for entry in sorted(AUTHORIZED_ENTRYPOINTS):
        print(f"- {entry}")
    for edge in call_graph:
        print(f"- {edge}")

    verdict = "CLOSED" if not remaining_direct and not remaining_unrouted else "NOT CLOSED"
    print("\n### C. Final verdict")
    print(f"- {verdict}")

    assert not remaining_direct, "DIRECT_MUTATION_SURFACE must be 0"
    assert not remaining_unrouted, "UNROUTED entrypoints must be 0"
