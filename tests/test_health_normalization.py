from __future__ import annotations

import ast
import inspect
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from app.services.commands.runtime import CommandActor, CommandRuntimeService
from core.control import ControlPlane
from core.health import derive_system_health, system_health_inputs_from_projection
from core.replay import replay
from household_os.runtime.event_router import CanonicalEventEnvelope


_REPO_ROOT = Path(__file__).resolve().parents[1]
_CANONICAL_HEALTH_PATH = _REPO_ROOT / "core" / "health" / "system_health.py"


@dataclass
class _FakeEventRow:
    event_id: str
    household_id: str
    timestamp: datetime
    type: str
    payload: dict[str, Any]
    source: str


class _InMemoryEventLogService:
    def __init__(self) -> None:
        self.insert_order: list[_FakeEventRow] = []

    def append_envelope(self, envelope: CanonicalEventEnvelope) -> None:
        self.insert_order.append(
            _FakeEventRow(
                event_id=envelope.event_id,
                household_id=envelope.household_id,
                timestamp=envelope.timestamp,
                type=envelope.event_type,
                payload=dict(envelope.payload),
                source=envelope.source,
            )
        )

    def get_event_logs(
        self,
        *,
        household_id: str,
        user_id: str | None = None,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[_FakeEventRow]:
        rows = [row for row in self.insert_order if row.household_id == household_id]
        if event_type:
            rows = [row for row in rows if row.type == event_type]

        ordered = sorted(rows, key=lambda row: (row.timestamp, row.event_id), reverse=True)
        return ordered[: max(1, int(limit))]


class _InMemoryRouterService:
    def __init__(self, event_log: _InMemoryEventLogService) -> None:
        self._event_log = event_log
        self._idempotency_keys: set[str] = set()

    def route(
        self,
        envelope: CanonicalEventEnvelope,
        *,
        persist: bool = True,
        dispatch: bool = True,
    ) -> dict[str, Any] | None:
        if envelope.idempotency_key and envelope.idempotency_key in self._idempotency_keys:
            return {
                "status": "duplicate",
                "event_id": envelope.event_id,
            }

        if envelope.idempotency_key:
            self._idempotency_keys.add(envelope.idempotency_key)

        if persist:
            self._event_log.append_envelope(envelope)
        if not dispatch:
            return None
        return {
            "status": "persisted",
            "event_id": envelope.event_id,
        }


def _build_runtime(*, control_plane: ControlPlane | None = None) -> tuple[CommandRuntimeService, _InMemoryEventLogService]:
    event_log = _InMemoryEventLogService()
    runtime = CommandRuntimeService(
        router_service=_InMemoryRouterService(event_log),
        event_log_service=event_log,
        control_plane=control_plane,
    )
    return runtime, event_log


def _actor() -> CommandActor:
    return CommandActor(actor_type="api_user", user_id="health-user", session_id="health-session")


def _iter_repo_python_files() -> list[Path]:
    files: list[Path] = []
    for path in _REPO_ROOT.rglob("*.py"):
        normalized = path.as_posix()
        if "/.venv/" in normalized or "/__pycache__/" in normalized:
            continue
        files.append(path)
    return files


def _iter_subsystem_python_files() -> list[Path]:
    targeted_prefixes = (
        "app/",
        "assistant/",
        "core/control/",
        "core/replay/",
        "core/sagas/",
    )
    files: list[Path] = []
    for path in _iter_repo_python_files():
        rel = path.relative_to(_REPO_ROOT).as_posix()
        if path == _CANONICAL_HEALTH_PATH:
            continue
        if rel.startswith("tests/"):
            continue
        if rel.startswith(targeted_prefixes):
            files.append(path)
    return files


def _module_tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


@pytest.mark.ci_gate
def test_single_canonical_health_function_exists() -> None:
    locations: list[str] = []
    for path in _iter_repo_python_files():
        try:
            tree = _module_tree(path)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "derive_system_health":
                locations.append(path.relative_to(_REPO_ROOT).as_posix())

    assert locations == ["core/health/system_health.py"]


@pytest.mark.migration
def test_no_duplicate_health_implementations() -> None:
    duplicates: list[str] = []
    for path in _iter_subsystem_python_files():
        try:
            tree = _module_tree(path)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef):
                continue
            name = node.name.lower()
            if "health" in name and any(token in name for token in ("derive", "compute", "score")):
                duplicates.append(f"{path.relative_to(_REPO_ROOT).as_posix()}:{node.lineno}")

    assert duplicates == []


@pytest.mark.migration
def test_no_inline_health_computation_in_subsystems() -> None:
    forbidden_tokens = (
        "event_log_integrity=",
        "replay_consistency=",
        "policy_determinism=",
        "control_plane_consistency=",
        "saga_validity=",
        "saga_completion_validity=",
    )

    offenders: list[str] = []
    for path in _iter_subsystem_python_files():
        text = path.read_text(encoding="utf-8")
        if any(token in text for token in forbidden_tokens):
            offenders.append(path.relative_to(_REPO_ROOT).as_posix())

    assert offenders == []


@pytest.mark.ci_gate
def test_system_health_import_only_usage() -> None:
    invalid_usage: list[str] = []
    for path in _iter_repo_python_files():
        if path == _CANONICAL_HEALTH_PATH:
            continue

        text = path.read_text(encoding="utf-8")
        if "derive_system_health(" not in text:
            continue

        imported = (
            "from core.health import derive_system_health" in text
            or "from core.health.system_health import derive_system_health" in text
        )
        if not imported:
            invalid_usage.append(path.relative_to(_REPO_ROOT).as_posix())

    assert invalid_usage == []


@pytest.mark.reliability
def test_health_consistency_across_runtime_replay_control() -> None:
    runtime, event_log = _build_runtime(control_plane=ControlPlane())

    result = runtime.handle_command(
        command_type="saga.execute",
        household_id="household-health",
        actor=_actor(),
        payload={
            "saga_id": "health-consistency-1",
            "idempotency_key": "health-consistency:1",
            "steps": [
                {
                    "step_id": "step-1",
                    "event_emitted": "workflow.health.step",
                    "payload": {"message": "ok"},
                }
            ],
            "compensation_steps": [],
            "metadata": {
                "saga_type": "health-consistency",
                "resource_keys": ["calendar:family"],
                "risk_level": "low",
            },
        },
        source="tests.health",
    )

    projection = runtime.get_projection("household-health", force_replay=True)
    replay_projection = replay(event_log.insert_order)["derived_state"]

    runtime_health = derive_system_health(**system_health_inputs_from_projection(projection))
    replay_health = derive_system_health(**system_health_inputs_from_projection(replay_projection))

    assert result["response"]["control"]["replay_validation"]["matches"] is True
    assert runtime_health == replay_health

    tampered_replay_projection = dict(replay_projection)
    tampered_replay_projection["drift"] = {
        "structural": False,
        "integrity": True,
        "causal": False,
    }
    tampered_health = derive_system_health(**system_health_inputs_from_projection(tampered_replay_projection))
    assert tampered_health == "red"


@pytest.mark.ci_gate
def test_health_derivation_is_pure_function() -> None:
    import core.health.system_health as health_module

    signature = inspect.signature(derive_system_health)
    assert list(signature.parameters.keys()) == [
        "event_log_integrity",
        "replay_consistency",
        "policy_determinism",
        "control_plane_consistency",
        "saga_validity",
    ]

    before_drift_keys = tuple(health_module._DRIFT_KEYS)
    outputs = {
        derive_system_health(
            event_log_integrity=True,
            replay_consistency=True,
            policy_determinism=True,
            control_plane_consistency=True,
            saga_validity=True,
        )
        for _ in range(8)
    }
    assert outputs == {"green"}
    assert tuple(health_module._DRIFT_KEYS) == before_drift_keys

    source = inspect.getsource(health_module.derive_system_health)
    tree = ast.parse(source)
    function_node = next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef))
    has_state_assignment = any(
        isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)) for node in ast.walk(function_node)
    )
    assert has_state_assignment is False
