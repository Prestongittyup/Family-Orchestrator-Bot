from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class CommandDescriptor:
    event_type: str
    command_dispatch_symbol: str


@dataclass(frozen=True)
class ReducerDescriptor:
    reducer_symbol: str
    validator_symbol: str | None = None


@dataclass(frozen=True)
class ProjectionDescriptor:
    projection_symbol: str


@dataclass(frozen=True)
class SurfaceDescriptor:
    id: str
    event_spec: Mapping[str, CommandDescriptor]
    reducer_spec: Mapping[str, ReducerDescriptor]
    projection_spec: Mapping[str, ProjectionDescriptor]

    @property
    def command_spec(self) -> Mapping[str, CommandDescriptor]:
        return self.event_spec


# Backward-compatible aliases for legacy imports.
SurfaceEventDescriptor = CommandDescriptor
SurfaceReducerDescriptor = ReducerDescriptor
SurfaceProjectionDescriptor = ProjectionDescriptor


_SURFACE_1_COMMAND_EVENT_SPEC = OrderedDict(
    {
        "household.member.add": "HouseholdMemberAdded",
        "household.member.update": "HouseholdMemberUpdated",
        "household.responsibility.create": "ResponsibilityCreated",
        "household.responsibility.assign": "ResponsibilityAssigned",
        "household.responsibility.update": "ResponsibilityUpdated",
        "household.event.schedule": "EventScheduled",
        "household.event.reschedule": "EventRescheduled",
        "household.event.cancel": "EventCancelled",
        "household.execution.change": "ExecutionStateChanged",
        "household.conflict.detect": "ConflictDetected",
        "household.conflict.resolve": "ConflictResolved",
    }
)


def _surface_1_descriptor() -> SurfaceDescriptor:
    event_spec: OrderedDict[str, CommandDescriptor] = OrderedDict()
    reducer_spec: OrderedDict[str, ReducerDescriptor] = OrderedDict()

    for command_type, event_type in _SURFACE_1_COMMAND_EVENT_SPEC.items():
        event_spec[str(command_type)] = CommandDescriptor(
            event_type=str(event_type),
            command_dispatch_symbol="command_dispatch.family_coordination",
        )
        reducer_spec[str(event_type)] = ReducerDescriptor(
            reducer_symbol="reducer.family_coordination",
            validator_symbol="validator.family_coordination",
        )

    projection_spec: OrderedDict[str, ProjectionDescriptor] = OrderedDict(
        {
            "family_coordination_surface": ProjectionDescriptor(
                projection_symbol="projection.family_coordination_surface"
            )
        }
    )

    return SurfaceDescriptor(
        id="surface_1",
        event_spec=MappingProxyType(event_spec),
        reducer_spec=MappingProxyType(reducer_spec),
        projection_spec=MappingProxyType(projection_spec),
    )


def get_surface_descriptors() -> Mapping[str, SurfaceDescriptor]:
    descriptors = OrderedDict({"surface_1": _surface_1_descriptor()})
    return MappingProxyType(descriptors)


__all__ = [
    "CommandDescriptor",
    "ProjectionDescriptor",
    "ReducerDescriptor",
    "SurfaceDescriptor",
    "SurfaceEventDescriptor",
    "SurfaceProjectionDescriptor",
    "SurfaceReducerDescriptor",
    "get_surface_descriptors",
]
