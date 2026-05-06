from __future__ import annotations

from collections import OrderedDict
from types import MappingProxyType
from typing import Any, Mapping

from core.replay.reducer_factory import (
    resolve_command_dispatch_symbol,
    resolve_projection_symbol,
    resolve_reducer_symbol,
    resolve_validator_symbol,
)
from core.replay.surface_descriptors import SurfaceDescriptor, get_surface_descriptors
from core.replay.surface_types import (
    ResolvedSurfaceCommand,
    SurfaceProjectionBuilder,
    SurfaceReducerRegistration,
)


def _normalize_surface_id(surface_id: str) -> str:
    normalized = str(surface_id or "").strip()
    if not normalized:
        raise ValueError("surface_id is required")
    return normalized


def _normalize_event_type(event_type: str) -> str:
    normalized = str(event_type or "").strip()
    if not normalized:
        raise ValueError("event_type is required")
    return normalized


def _normalize_command_type(command_type: str) -> str:
    normalized = str(command_type or "").strip()
    if not normalized:
        raise ValueError("command_type is required")
    return normalized


def _event_type_from_value(event_or_type: Mapping[str, Any] | str) -> str:
    if isinstance(event_or_type, Mapping):
        candidate = event_or_type.get("event_type") or event_or_type.get("type")
        return _normalize_event_type(str(candidate or ""))
    return _normalize_event_type(str(event_or_type or ""))


def _descriptors() -> Mapping[str, SurfaceDescriptor]:
    return get_surface_descriptors()


def get_surface(surface_id: str) -> SurfaceDescriptor:
    normalized_surface_id = _normalize_surface_id(surface_id)
    descriptor = _descriptors().get(normalized_surface_id)
    if descriptor is None:
        raise KeyError(f"surface not found: {normalized_surface_id}")
    return descriptor


def iter_surfaces() -> tuple[SurfaceDescriptor, ...]:
    return tuple(_descriptors().values())


def resolve_command(command_type: str, surface_id: str | None = None) -> ResolvedSurfaceCommand | None:
    normalized_command_type = _normalize_command_type(command_type)

    descriptor_iterable: tuple[SurfaceDescriptor, ...]
    if surface_id:
        descriptor_iterable = (get_surface(_normalize_surface_id(surface_id)),)
    else:
        descriptor_iterable = iter_surfaces()

    for descriptor in descriptor_iterable:
        command_descriptor = descriptor.command_spec.get(normalized_command_type)
        if command_descriptor is None:
            continue

        command_handler = resolve_command_dispatch_symbol(command_descriptor.command_dispatch_symbol)
        return ResolvedSurfaceCommand(
            surface_id=descriptor.id,
            event_type=command_descriptor.event_type,
            command_handler=command_handler,
        )

    return None


def resolve_surface_id_for_event(event_type: str) -> str | None:
    normalized_event_type = _normalize_event_type(event_type)

    for descriptor in iter_surfaces():
        if normalized_event_type in descriptor.reducer_spec:
            return descriptor.id

    return None


def resolve_surface_for_event(event: Mapping[str, Any] | str) -> str | None:
    return resolve_surface_id_for_event(_event_type_from_value(event))


def resolve_reducer(surface_id: str, event_type: str) -> SurfaceReducerRegistration | None:
    normalized_surface_id = _normalize_surface_id(surface_id)
    normalized_event_type = _normalize_event_type(event_type)

    descriptor = get_surface(normalized_surface_id)
    reducer_descriptor = descriptor.reducer_spec.get(normalized_event_type)
    if reducer_descriptor is None:
        return None

    reducer_builder = resolve_reducer_symbol(reducer_descriptor.reducer_symbol)

    def reducer(
        *,
        payload: Mapping[str, Any],
        recorded_at: str,
        surface_state: Mapping[str, Any] | None,
        responses: Mapping[str, Mapping[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        return reducer_builder(
            event_type=normalized_event_type,
            payload=payload,
            recorded_at=recorded_at,
            surface_state=surface_state,
            responses=responses,
        )

    validator = None
    if reducer_descriptor.validator_symbol:
        validator_builder = resolve_validator_symbol(reducer_descriptor.validator_symbol)

        def _validator(
            *,
            event_payload: Mapping[str, Any],
            validation_state: Mapping[str, Any] | None,
        ) -> dict[str, Any]:
            return validator_builder(
                event_type=normalized_event_type,
                event_payload=event_payload,
                validation_state=validation_state,
            )

        validator = _validator

    return SurfaceReducerRegistration(reducer=reducer, validator=validator)


def resolve_projection(
    surface_id: str,
    projection_type: str | None = None,
) -> Mapping[str, SurfaceProjectionBuilder]:
    normalized_surface_id = _normalize_surface_id(surface_id)
    descriptor = get_surface(normalized_surface_id)

    resolved_projection_map: OrderedDict[str, SurfaceProjectionBuilder] = OrderedDict()

    for projection_name, projection_descriptor in descriptor.projection_spec.items():
        normalized_projection_name = str(projection_name)
        if projection_type and normalized_projection_name != str(projection_type).strip():
            continue

        projection_builder = resolve_projection_symbol(projection_descriptor.projection_symbol)

        def _projection(
            *,
            partition_id: str,
            surface_state: Mapping[str, Any],
            reference_timestamp: str,
            _projection_name: str = normalized_projection_name,
            _projection_builder: Any = projection_builder,
        ) -> Mapping[str, Any]:
            return _projection_builder(
                projection_key=_projection_name,
                partition_id=partition_id,
                surface_state=surface_state,
                reference_timestamp=reference_timestamp,
            )

        resolved_projection_map[normalized_projection_name] = _projection

    return MappingProxyType(resolved_projection_map)


__all__ = [
    "get_surface",
    "iter_surfaces",
    "resolve_command",
    "resolve_projection",
    "resolve_reducer",
    "resolve_surface_for_event",
    "resolve_surface_id_for_event",
]
