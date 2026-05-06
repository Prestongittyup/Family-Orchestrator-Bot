from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from core.replay.reducer_factory import (
    resolve_command_dispatch_symbol,
    resolve_projection_symbol,
    resolve_reducer_symbol,
    resolve_validator_symbol,
)
from core.replay.surface_registry import get_surface


SURFACE_1_ID = "surface_1"


@dataclass(frozen=True)
class SurfaceEventRegistration:
    event_type: str
    command_handler: Any


@dataclass(frozen=True)
class SurfaceReducerRegistration:
    reducer: Any
    validator: Any | None = None


@dataclass(frozen=True)
class SurfaceConfig:
    surface_id: str
    event_map: Mapping[str, SurfaceEventRegistration]
    reducer_map: Mapping[str, SurfaceReducerRegistration]
    projection_map: Mapping[str, Any]


def build_surface_1_config() -> SurfaceConfig:
    descriptor = get_surface(SURFACE_1_ID)

    event_map: dict[str, SurfaceEventRegistration] = {}
    for command_type, event_descriptor in descriptor.event_spec.items():
        command_handler = resolve_command_dispatch_symbol(event_descriptor.command_dispatch_symbol)
        event_map[str(command_type)] = SurfaceEventRegistration(
            event_type=str(event_descriptor.event_type),
            command_handler=command_handler,
        )

    reducer_map: dict[str, SurfaceReducerRegistration] = {}
    for event_type, reducer_descriptor in descriptor.reducer_spec.items():
        reducer_builder = resolve_reducer_symbol(reducer_descriptor.reducer_symbol)

        def _reducer(
            *,
            payload: Mapping[str, Any],
            recorded_at: str,
            surface_state: Mapping[str, Any] | None,
            responses: Mapping[str, Mapping[str, Any]],
            _event_type: str = str(event_type),
            _builder: Any = reducer_builder,
        ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
            return _builder(
                event_type=_event_type,
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
                _event_type: str = str(event_type),
                _builder: Any = validator_builder,
            ) -> dict[str, Any]:
                return _builder(
                    event_type=_event_type,
                    event_payload=event_payload,
                    validation_state=validation_state,
                )

            validator = _validator

        reducer_map[str(event_type)] = SurfaceReducerRegistration(
            reducer=_reducer,
            validator=validator,
        )

    projection_map: dict[str, Any] = {}
    for projection_key, projection_descriptor in descriptor.projection_spec.items():
        projection_builder = resolve_projection_symbol(projection_descriptor.projection_symbol)

        def _projection(
            *,
            partition_id: str,
            surface_state: Mapping[str, Any],
            reference_timestamp: str,
            _projection_key: str = str(projection_key),
            _builder: Any = projection_builder,
        ) -> Mapping[str, Any]:
            return _builder(
                projection_key=_projection_key,
                partition_id=partition_id,
                surface_state=surface_state,
                reference_timestamp=reference_timestamp,
            )

        projection_map[str(projection_key)] = _projection

    return SurfaceConfig(
        surface_id=SURFACE_1_ID,
        event_map=event_map,
        reducer_map=reducer_map,
        projection_map=projection_map,
    )


__all__ = [
    "SURFACE_1_ID",
    "SurfaceConfig",
    "SurfaceEventRegistration",
    "SurfaceReducerRegistration",
    "build_surface_1_config",
]
