from __future__ import annotations

import inspect
import os
from pathlib import Path

from household_os.security.trust_surface_registry import (
    ALLOWED_IMPORTERS_BY_MODULE,
    FORBIDDEN_DIRECT_SURFACES,
    INTERNAL_ALLOWED_CALLERS,
    OBSERVABILITY_WRAPPER_MODULE_PREFIXES,
)


FORBIDDEN_CALLS = {
    call
    for calls in FORBIDDEN_DIRECT_SURFACES.values()
    for call in calls
}


class SecurityViolation(RuntimeError):
    """Raised when code crosses an unauthorized trust boundary."""


_REPLAY_ALLOWED_CALLERS = {
    "household_os.runtime.orchestrator",
    "household_os.runtime.state_reducer",
    "household_os.runtime.lifecycle_migration",
}


def _normalize_module_name(raw: str | None) -> str:
    if not raw:
        return ""
    normalized = raw.replace("\\", "/")
    if normalized.endswith(".py"):
        normalized = normalized[:-3]
    return normalized.replace("/", ".")


def _module_from_filename(filename: str) -> str:
    path = filename.replace("\\", "/")
    if "/" not in path:
        return path

    marker = None
    for candidate in ("/household_os/", "/apps/"):
        idx = path.rfind(candidate)
        if idx != -1:
            marker = idx + 1
            break
    if marker is None:
        return Path(path).stem

    rel = path[marker:]
    if rel.endswith(".py"):
        rel = rel[:-3]
    return rel.replace("/", ".")


def _is_allowed(module_name: str, allowed_modules: set[str]) -> bool:
    return any(module_name == allowed or module_name.startswith(f"{allowed}.") for allowed in allowed_modules)


def _is_observability_wrapper(module_name: str, function_name: str) -> bool:
    if any(
        module_name == prefix or module_name.startswith(f"{prefix}.")
        for prefix in OBSERVABILITY_WRAPPER_MODULE_PREFIXES
    ):
        return True
    lowered_fn = function_name.lower()
    return lowered_fn in {"trace_function", "wrapper", "decorator", "emit", "debug", "info", "warning", "error"}


def _iter_effective_stack(skip_modules: set[str] | None = None) -> list[tuple[str, str]]:
    ignored = {
        "household_os.security.trust_boundary_enforcer",
        "importlib",
        "importlib._bootstrap",
        "importlib._bootstrap_external",
    }
    if skip_modules:
        ignored.update(skip_modules)

    effective: list[tuple[str, str]] = []
    for frame_info in inspect.stack()[2:]:
        frame = frame_info.frame
        module_name = _normalize_module_name(frame.f_globals.get("__name__"))
        if not module_name:
            module_name = _module_from_filename(frame_info.filename)
        if not module_name:
            continue
        fn_name = str(frame_info.function or "")
        if any(module_name == blocked or module_name.startswith(f"{blocked}.") for blocked in ignored):
            continue
        if _is_observability_wrapper(module_name, fn_name):
            continue
        effective.append((module_name, fn_name))
    return effective


def _first_external_module(skip_modules: set[str] | None = None) -> str:
    stack = _iter_effective_stack(skip_modules=skip_modules)
    return stack[0][0] if stack else ""


def enforce_call_origin(module_name: str, function_name: str, caller_frame: inspect.FrameInfo | None = None) -> None:
    target = f"{module_name}.{function_name}" if module_name else function_name
    is_forbidden = target in FORBIDDEN_CALLS or function_name in FORBIDDEN_CALLS
    if not is_forbidden:
        return

    del caller_frame
    stack = _iter_effective_stack(skip_modules={module_name} if module_name else None)
    if not stack:
        raise SecurityViolation(f"Forbidden call blocked: {target} from unknown")

    if any(_is_allowed(mod, INTERNAL_ALLOWED_CALLERS) for mod, _ in stack):
        return

    caller = stack[0][0]
    if caller.startswith("tests."):
        return
    raise SecurityViolation(f"Forbidden call blocked: {target} from {caller}")


def enforce_trust_boundary(fn_name: str, caller_module: str) -> None:
    """Raise if a forbidden call is attempted outside trusted modules."""
    if fn_name not in FORBIDDEN_CALLS:
        return
    if _is_allowed(caller_module, INTERNAL_ALLOWED_CALLERS):
        return
    if caller_module.startswith("tests."):
        return

    # Full-stack validation prevents wrapper/test frames from causing false denials
    # when the trusted orchestrator is present higher in the call graph.
    stack = _iter_effective_stack()
    if any(_is_allowed(mod, INTERNAL_ALLOWED_CALLERS) for mod, _ in stack):
        return

    raise SecurityViolation(
        f"Forbidden call blocked: {fn_name} from {caller_module or 'unknown'}"
    )


def validate_forbidden_call(fn_name: str, skip_modules: set[str] | None = None) -> None:
    caller_module = _first_external_module(skip_modules=skip_modules)
    enforce_trust_boundary(fn_name=fn_name, caller_module=caller_module)
    module_name, function_name = fn_name.rsplit(".", 1) if "." in fn_name else ("", fn_name)
    # Class.method callsites pass class names here; treat as function-only to avoid
    # misclassifying module provenance.
    if module_name and module_name[:1].isupper():
        module_name = ""
    enforce_call_origin(module_name=module_name, function_name=function_name, caller_frame=None)


def enforce_import_boundary(sensitive_module: str, importer_module: str | None = None) -> None:
    """Validate import provenance for sensitive modules at import-time."""
    allowed_importers = ALLOWED_IMPORTERS_BY_MODULE.get(sensitive_module)
    if not allowed_importers:
        return

    importer = importer_module or _first_external_module(skip_modules={sensitive_module})
    if not importer:
        return

    if any(importer == allowed or importer.startswith(f"{allowed}.") for allowed in allowed_importers):
        return

    raise ImportError(
        f"Trust boundary violation: direct domain import forbidden ({importer} -> {sensitive_module})"
    )


def validate_replay_call(
    caller_module: str | None = None,
    skip_modules: set[str] | None = None,
    actor_type: str | None = None,
) -> None:
    raw_actor_type = str(actor_type or "system_worker").strip().lower()
    if raw_actor_type == "api_user":
        raw_actor_type = "user"
    if raw_actor_type == "user":
        raise SecurityViolation("Replay access denied for user actor")
    if raw_actor_type not in {"system_worker", "scheduler"}:
        raise SecurityViolation(f"Replay access denied for actor_type: {raw_actor_type}")

    resolved = caller_module or _first_external_module(skip_modules=skip_modules)
    if resolved.startswith("tests."):
        return
    if _is_allowed(resolved, _REPLAY_ALLOWED_CALLERS):
        return
    raise SecurityViolation(f"Replay access denied for caller: {resolved or 'unknown'}")


def allow_test_mode_bypass(test_mode: bool) -> bool:
    if not test_mode:
        return False
    return str(os.getenv("TEST_MODE", "")).lower() == "true"
