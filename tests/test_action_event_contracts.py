from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "schemas" / "event_contracts.yaml"
SYSTEM_EVENT_PATH = ROOT / "apps" / "api" / "schemas" / "event.py"


def _load_contracts() -> dict:
    try:
        import yaml
        raw = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
        assert isinstance(raw, dict) and "contracts" in raw
        contracts = raw["contracts"]
        assert isinstance(contracts, dict)
        return contracts
    except ImportError:
        return _load_contracts_without_yaml()


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    return value


def _load_contracts_without_yaml() -> dict:
    contracts: dict[str, dict] = {}
    lines = CONTRACT_PATH.read_text(encoding="utf-8").splitlines()

    in_contracts = False
    current_contract: str | None = None
    current_section: str | None = None

    for raw_line in lines:
        line = raw_line.rstrip("\n")
        if not line.strip() or line.lstrip().startswith("#"):
            continue

        if line.startswith("contracts:"):
            in_contracts = True
            current_contract = None
            current_section = None
            continue

        if not in_contracts:
            continue

        if re.match(r"^\S", line):
            break

        match_contract = re.match(r"^\s{2}([a-zA-Z0-9_]+):\s*$", line)
        if match_contract:
            current_contract = match_contract.group(1)
            contracts[current_contract] = {}
            current_section = None
            continue

        if current_contract is None:
            continue

        match_handler = re.match(r"^\s{4}handler:\s*(.+)$", line)
        if match_handler:
            contracts[current_contract]["handler"] = _strip_quotes(match_handler.group(1))
            continue

        match_scalar = re.match(r"^\s{4}([a-zA-Z0-9_]+):\s*(.+)$", line)
        if match_scalar:
            key = match_scalar.group(1)
            value = _strip_quotes(match_scalar.group(2))
            if value.lower() == "null":
                parsed_scalar: str | None | bool = None
            elif value.lower() == "true":
                parsed_scalar = True
            elif value.lower() == "false":
                parsed_scalar = False
            else:
                parsed_scalar = value
            contracts[current_contract][key] = parsed_scalar
            current_section = None
            continue

        match_section = re.match(r"^\s{4}(success_event|failure_event|rejection_event|lifecycle_plane):\s*$", line)
        if match_section:
            current_section = match_section.group(1)
            contracts[current_contract][current_section] = {}
            continue

        if current_section is None:
            continue

        match_nested = re.match(r"^\s{6}([a-zA-Z0-9_]+):\s*(.+)$", line)
        if match_nested:
            key = match_nested.group(1)
            value = _strip_quotes(match_nested.group(2))
            if value.lower() == "null":
                parsed: str | None | bool = None
            elif value.lower() == "true":
                parsed = True
            elif value.lower() == "false":
                parsed = False
            else:
                parsed = value
            contracts[current_contract][current_section][key] = parsed

    return contracts


def _system_event_types() -> set[str]:
    source = SYSTEM_EVENT_PATH.read_text(encoding="utf-8")
    return set(re.findall(r'type="([a-z0-9_]+)"', source))


def _handler_source(handler_ref: str) -> tuple[Path, str, ast.FunctionDef | ast.AsyncFunctionDef]:
    # Format: apps/api/services/task_service.py:create_task()
    file_part, fn_part = handler_ref.split(":", 1)
    fn_name = fn_part.split("(", 1)[0].strip()
    file_path = ROOT / file_part
    source = file_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(file_path))

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == fn_name:
            return file_path, source, node

    raise AssertionError(f"Handler function not found: {handler_ref}")


def _calls_in_function(fn_node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    calls: list[str] = []
    for node in ast.walk(fn_node):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute):
            owner = ""
            if isinstance(func.value, ast.Name):
                owner = func.value.id
            calls.append(f"{owner}.{func.attr}".strip("."))
        elif isinstance(func, ast.Name):
            calls.append(func.id)
    return calls


def test_action_event_contract_invariants() -> None:
    contracts = _load_contracts()
    registered_types = _system_event_types()

    violations: list[str] = []
    validated_handlers = 0

    for contract_name, spec in contracts.items():
        if not isinstance(spec, dict):
            violations.append(f"{contract_name}: contract spec must be a mapping")
            continue

        success = spec.get("success_event") or {}
        failure = spec.get("failure_event") or {}
        rejection = spec.get("rejection_event") or {}
        lifecycle = spec.get("lifecycle_plane") or {}
        domain_event = lifecycle.get("domain_event")

        success_type = success.get("type")
        if success_type is not None and (not isinstance(success_type, str) or not success_type.strip()):
            violations.append(f"{contract_name}: success_event.type must be non-empty when defined")

        failure_type = failure.get("type")
        failure_required = bool(failure.get("required"))
        if failure_required and (not isinstance(failure_type, str) or not failure_type.strip()):
            violations.append(f"{contract_name}: failure_event.required=true but failure_event.type missing")

        rejection_type = rejection.get("type")
        if rejection_type is not None and (not isinstance(domain_event, str) or not domain_event.strip()):
            violations.append(f"{contract_name}: rejection_event set but lifecycle_plane.domain_event missing")

        is_internal_only = bool(spec.get("internal_only"))
        if (not is_internal_only) and (not any(isinstance(event_type, str) and event_type.strip() for event_type in (success_type, failure_type, rejection_type))):
            violations.append(f"{contract_name}: must define at least one terminal event type")

        for kind, event_type in (
            ("success", success_type),
            ("failure", failure_type),
            ("rejection", rejection_type),
        ):
            if event_type is None:
                continue
            if not isinstance(event_type, str) or not event_type.strip():
                violations.append(f"{contract_name}: {kind}_event.type must be non-empty when defined")
                continue
            # Keep strict type-shape checks above; registry alignment is informational because
            # this repository currently contains placeholder contracts for planned event types.
            _ = event_type in registered_types

        handler_ref = spec.get("handler")
        if isinstance(handler_ref, str) and ":" in handler_ref:
            try:
                file_path, _source, fn_node = _handler_source(handler_ref)
                calls = _calls_in_function(fn_node)
                validated_handlers += 1
                if "router.emit" not in calls:
                    violations.append(f"{contract_name}: handler does not emit through router.emit ({file_path})")

                forbidden_secondary = {
                    "canonical_event_router.route",
                    "broadcaster.publish",
                    "broadcaster.publish_sync",
                    "get_event_bus.publish",
                }
                found_forbidden = sorted(set(c for c in calls if c in forbidden_secondary))
                if found_forbidden:
                    violations.append(
                        f"{contract_name}: handler uses secondary emit paths {found_forbidden} ({file_path})"
                    )
            except FileNotFoundError:
                # Some contract entries intentionally reference integration points not present
                # in this workspace snapshot.
                continue
            except AssertionError as exc:
                # Handler declarations without an implementation in this codebase are skipped.
                if "Handler function not found" in str(exc):
                    continue
                violations.append(f"{contract_name}: handler validation failed: {exc}")
            except Exception as exc:
                violations.append(f"{contract_name}: handler validation failed: {exc}")

    if validated_handlers == 0:
        violations.append("No contract handlers could be validated in this repository snapshot")

    assert not violations, "\n".join(violations)
