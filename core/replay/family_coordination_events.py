from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, ClassVar, Mapping


SURFACE1_COMMAND_EVENT_TYPE: Mapping[str, str] = MappingProxyType(
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
SURFACE1_COMMAND_TYPES = frozenset(SURFACE1_COMMAND_EVENT_TYPE.keys())
SURFACE1_EVENT_TYPES = frozenset(SURFACE1_COMMAND_EVENT_TYPE.values())


def _utc_iso(value: datetime) -> str:
    resolved = value.astimezone(UTC)
    return resolved.isoformat().replace("+00:00", "Z")


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {
                str(key): _freeze_value(item)
                for key, item in sorted(value.items(), key=lambda entry: str(entry[0]))
            }
        )
    if isinstance(value, list):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, datetime):
        resolved = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return _utc_iso(resolved)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _thaw_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _thaw_value(item)
            for key, item in sorted(value.items(), key=lambda entry: str(entry[0]))
        }
    if isinstance(value, tuple):
        return [_thaw_value(item) for item in value]
    return value


def _freeze_payload(payload: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        return MappingProxyType({})

    return MappingProxyType(
        {
            str(key): _freeze_value(item)
            for key, item in sorted(payload.items(), key=lambda entry: str(entry[0]))
        }
    )


@dataclass(frozen=True)
class FamilyCoordinationEvent:
    partition_id: str
    timestamp: datetime
    actor: str
    source: str
    payload: Mapping[str, Any]

    event_type: ClassVar[str] = "FamilyCoordinationEvent"

    def __post_init__(self) -> None:
        partition_id = str(self.partition_id or "").strip()
        if not partition_id:
            raise ValueError("partition_id is required")

        actor = str(self.actor or "").strip()
        if not actor:
            raise ValueError("actor is required")

        source = str(self.source or "").strip()
        if not source:
            raise ValueError("source is required")

        timestamp = self.timestamp
        if not isinstance(timestamp, datetime):
            raise ValueError("timestamp must be a datetime")
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)

        object.__setattr__(self, "partition_id", partition_id)
        object.__setattr__(self, "actor", actor)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "timestamp", timestamp.astimezone(UTC))
        object.__setattr__(self, "payload", _freeze_payload(self.payload))

    def payload_dict(self) -> dict[str, Any]:
        return {
            str(key): _thaw_value(value)
            for key, value in sorted(self.payload.items(), key=lambda entry: str(entry[0]))
        }

    def as_runtime_payload(self) -> dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "timestamp": _utc_iso(self.timestamp),
            "actor": self.actor,
            "source": self.source,
            **self.payload_dict(),
        }


@dataclass(frozen=True)
class HouseholdMemberAdded(FamilyCoordinationEvent):
    event_type: ClassVar[str] = "HouseholdMemberAdded"


@dataclass(frozen=True)
class HouseholdMemberUpdated(FamilyCoordinationEvent):
    event_type: ClassVar[str] = "HouseholdMemberUpdated"


@dataclass(frozen=True)
class ResponsibilityCreated(FamilyCoordinationEvent):
    event_type: ClassVar[str] = "ResponsibilityCreated"


@dataclass(frozen=True)
class ResponsibilityAssigned(FamilyCoordinationEvent):
    event_type: ClassVar[str] = "ResponsibilityAssigned"


@dataclass(frozen=True)
class ResponsibilityUpdated(FamilyCoordinationEvent):
    event_type: ClassVar[str] = "ResponsibilityUpdated"


@dataclass(frozen=True)
class EventScheduled(FamilyCoordinationEvent):
    event_type: ClassVar[str] = "EventScheduled"


@dataclass(frozen=True)
class EventRescheduled(FamilyCoordinationEvent):
    event_type: ClassVar[str] = "EventRescheduled"


@dataclass(frozen=True)
class EventCancelled(FamilyCoordinationEvent):
    event_type: ClassVar[str] = "EventCancelled"


@dataclass(frozen=True)
class ExecutionStateChanged(FamilyCoordinationEvent):
    event_type: ClassVar[str] = "ExecutionStateChanged"


@dataclass(frozen=True)
class ConflictDetected(FamilyCoordinationEvent):
    event_type: ClassVar[str] = "ConflictDetected"


@dataclass(frozen=True)
class ConflictResolved(FamilyCoordinationEvent):
    event_type: ClassVar[str] = "ConflictResolved"


SURFACE1_EVENT_CLASS_BY_TYPE: Mapping[str, type[FamilyCoordinationEvent]] = MappingProxyType(
    {
        "HouseholdMemberAdded": HouseholdMemberAdded,
        "HouseholdMemberUpdated": HouseholdMemberUpdated,
        "ResponsibilityCreated": ResponsibilityCreated,
        "ResponsibilityAssigned": ResponsibilityAssigned,
        "ResponsibilityUpdated": ResponsibilityUpdated,
        "EventScheduled": EventScheduled,
        "EventRescheduled": EventRescheduled,
        "EventCancelled": EventCancelled,
        "ExecutionStateChanged": ExecutionStateChanged,
        "ConflictDetected": ConflictDetected,
        "ConflictResolved": ConflictResolved,
    }
)


def event_type_for_surface1_command(command_type: str) -> str | None:
    return SURFACE1_COMMAND_EVENT_TYPE.get(str(command_type or "").strip())


def build_family_coordination_event(
    *,
    event_type: str,
    partition_id: str,
    actor: str,
    source: str,
    payload: Mapping[str, Any],
    timestamp: datetime | None = None,
) -> FamilyCoordinationEvent:
    event_cls = SURFACE1_EVENT_CLASS_BY_TYPE.get(str(event_type or "").strip())
    if event_cls is None:
        raise ValueError(f"unsupported family coordination event_type: {event_type}")

    return event_cls(
        partition_id=partition_id,
        timestamp=timestamp or datetime.now(UTC),
        actor=actor,
        source=source,
        payload=payload,
    )


def build_family_coordination_event_from_command(
    *,
    command_type: str,
    partition_id: str,
    actor: str,
    source: str,
    payload: Mapping[str, Any],
    timestamp: datetime | None = None,
) -> FamilyCoordinationEvent:
    resolved_event_type = event_type_for_surface1_command(command_type)
    if resolved_event_type is None:
        raise ValueError(f"unsupported family coordination command_type: {command_type}")

    return build_family_coordination_event(
        event_type=resolved_event_type,
        partition_id=partition_id,
        actor=actor,
        source=source,
        payload=payload,
        timestamp=timestamp,
    )


__all__ = [
    "ConflictDetected",
    "ConflictResolved",
    "EventCancelled",
    "EventRescheduled",
    "EventScheduled",
    "ExecutionStateChanged",
    "FamilyCoordinationEvent",
    "HouseholdMemberAdded",
    "HouseholdMemberUpdated",
    "ResponsibilityAssigned",
    "ResponsibilityCreated",
    "ResponsibilityUpdated",
    "SURFACE1_COMMAND_EVENT_TYPE",
    "SURFACE1_COMMAND_TYPES",
    "SURFACE1_EVENT_CLASS_BY_TYPE",
    "SURFACE1_EVENT_TYPES",
    "build_family_coordination_event",
    "build_family_coordination_event_from_command",
    "event_type_for_surface1_command",
]
