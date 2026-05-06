from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping


SurfaceCommandHandler = Callable[..., "SurfaceCommandDispatch"]
SurfaceReducer = Callable[..., tuple[Any, Mapping[str, Mapping[str, Any]]]]
SurfaceValidator = Callable[..., Any]
SurfaceProjectionBuilder = Callable[..., Mapping[str, Any]]


@dataclass(frozen=True)
class SurfaceCommandDispatch:
    status: str
    event_type: str
    event_payload: Mapping[str, Any]
    response_payload: Mapping[str, Any]
    effects: tuple[Mapping[str, Any], ...]
    idempotency_key: str | None = None


@dataclass(frozen=True)
class SurfaceReducerRegistration:
    reducer: SurfaceReducer
    validator: SurfaceValidator | None = None


@dataclass(frozen=True)
class ResolvedSurfaceCommand:
    surface_id: str
    event_type: str
    command_handler: SurfaceCommandHandler


__all__ = [
    "ResolvedSurfaceCommand",
    "SurfaceCommandDispatch",
    "SurfaceCommandHandler",
    "SurfaceProjectionBuilder",
    "SurfaceReducer",
    "SurfaceReducerRegistration",
    "SurfaceValidator",
]
