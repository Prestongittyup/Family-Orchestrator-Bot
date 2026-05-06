from __future__ import annotations

from collections import OrderedDict
from datetime import UTC, datetime
from typing import Any, Callable, Mapping

from core.replay.domain_projection_helpers import (
    apply_family_coordination_event_projection,
    derive_family_coordination_surface_projections,
)
from core.replay.family_coordination_events import build_family_coordination_event_from_command
from core.replay.surface_types import SurfaceCommandDispatch


CommandDispatchBuilder = Callable[..., SurfaceCommandDispatch]
ReducerBuilder = Callable[..., tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]]
ValidatorBuilder = Callable[..., dict[str, Any]]
ProjectionBuilder = Callable[..., Mapping[str, Any]]


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_iso(value: datetime | None = None) -> str:
    resolved = value or _utc_now()
    return resolved.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _normalize_schedule_timestamp(raw_timestamp: str) -> str:
    normalized = raw_timestamp.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("schedule timestamps must be ISO-8601") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return _utc_iso(parsed.astimezone(UTC))


def _normalize_family_coordination_payload(
    *,
    command_type: str,
    payload: Mapping[str, Any],
    request_id: str,
    household_id: str,
) -> dict[str, Any]:
    normalized_payload: dict[str, Any] = {
        "request_id": request_id,
        "partition_id": household_id,
    }

    if command_type in {"household.member.add", "household.member.update"}:
        member_payload = payload.get("member")
        raw_member = dict(member_payload) if isinstance(member_payload, Mapping) else dict(payload)
        member_id = str(raw_member.get("member_id") or payload.get("member_id") or "").strip()
        if command_type == "household.member.add":
            member_id = member_id or f"member-{request_id}"
        if not member_id:
            raise ValueError(f"{command_type} requires member_id")

        normalized_member = {
            "member_id": member_id,
            "display_name": str(raw_member.get("display_name") or raw_member.get("name") or "").strip(),
            "role": str(raw_member.get("role") or "").strip(),
            "timezone": str(raw_member.get("timezone") or "").strip(),
            "status": str(raw_member.get("status") or "active").strip() or "active",
        }
        normalized_payload.update(
            {
                "member_id": member_id,
                "member": {
                    key: value
                    for key, value in normalized_member.items()
                    if key in {"member_id", "status"} or value
                },
            }
        )
        return normalized_payload

    if command_type == "household.responsibility.create":
        responsibility_payload = payload.get("responsibility")
        raw_responsibility = (
            dict(responsibility_payload) if isinstance(responsibility_payload, Mapping) else dict(payload)
        )
        responsibility_id = str(
            raw_responsibility.get("responsibility_id") or payload.get("responsibility_id") or ""
        ).strip() or f"responsibility-{request_id}"
        title = str(raw_responsibility.get("title") or "").strip()
        if not title:
            raise ValueError("household.responsibility.create requires title")

        assigned_member_ids = sorted(
            {
                str(member_id).strip()
                for member_id in raw_responsibility.get("assigned_member_ids") or []
                if str(member_id).strip()
            }
        )
        fallback_assignee = str(
            raw_responsibility.get("assignee_member_id")
            or raw_responsibility.get("owner_member_id")
            or ""
        ).strip()
        if fallback_assignee:
            assigned_member_ids = sorted({*assigned_member_ids, fallback_assignee})

        normalized_payload.update(
            {
                "responsibility_id": responsibility_id,
                "responsibility": {
                    "responsibility_id": responsibility_id,
                    "title": title,
                    "description": str(raw_responsibility.get("description") or "").strip(),
                    "assigned_member_ids": assigned_member_ids,
                    "status": str(raw_responsibility.get("status") or "active").strip() or "active",
                    "priority": str(raw_responsibility.get("priority") or "medium").strip() or "medium",
                    "next_due_at": str(raw_responsibility.get("next_due_at") or "").strip() or None,
                },
            }
        )
        return normalized_payload

    if command_type == "household.responsibility.assign":
        responsibility_id = str(payload.get("responsibility_id") or "").strip()
        if not responsibility_id:
            raise ValueError("household.responsibility.assign requires responsibility_id")

        assigned_member_ids = sorted(
            {
                str(member_id).strip()
                for member_id in payload.get("assigned_member_ids") or []
                if str(member_id).strip()
            }
        )
        fallback_assignee = str(payload.get("assignee_member_id") or payload.get("member_id") or "").strip()
        if fallback_assignee:
            assigned_member_ids = sorted({*assigned_member_ids, fallback_assignee})
        if not assigned_member_ids:
            raise ValueError("household.responsibility.assign requires assigned_member_ids")

        normalized_payload.update(
            {
                "responsibility_id": responsibility_id,
                "assigned_member_ids": assigned_member_ids,
                "assignee_member_id": assigned_member_ids[0],
            }
        )
        return normalized_payload

    if command_type == "household.responsibility.update":
        responsibility_payload = payload.get("responsibility")
        raw_responsibility = (
            dict(responsibility_payload) if isinstance(responsibility_payload, Mapping) else dict(payload)
        )
        responsibility_id = str(
            raw_responsibility.get("responsibility_id") or payload.get("responsibility_id") or ""
        ).strip()
        if not responsibility_id:
            raise ValueError("household.responsibility.update requires responsibility_id")

        normalized_responsibility: dict[str, Any] = {
            "responsibility_id": responsibility_id,
            "title": str(raw_responsibility.get("title") or "").strip(),
            "description": str(raw_responsibility.get("description") or "").strip(),
            "priority": str(raw_responsibility.get("priority") or "").strip(),
            "status": str(raw_responsibility.get("status") or "").strip(),
            "next_due_at": str(raw_responsibility.get("next_due_at") or "").strip(),
        }
        assigned_member_ids = sorted(
            {
                str(member_id).strip()
                for member_id in raw_responsibility.get("assigned_member_ids") or []
                if str(member_id).strip()
            }
        )
        if assigned_member_ids:
            normalized_responsibility["assigned_member_ids"] = assigned_member_ids

        normalized_payload.update(
            {
                "responsibility_id": responsibility_id,
                "responsibility": {
                    key: value
                    for key, value in normalized_responsibility.items()
                    if key == "responsibility_id" or (value is not None and value != "")
                },
            }
        )
        return normalized_payload

    if command_type in {"household.event.schedule", "household.event.reschedule"}:
        event_payload = payload.get("coordination_event")
        raw_event = dict(event_payload) if isinstance(event_payload, Mapping) else dict(payload)
        coordination_event_id = str(
            raw_event.get("coordination_event_id")
            or raw_event.get("event_id")
            or payload.get("coordination_event_id")
            or ""
        ).strip()
        if command_type == "household.event.schedule":
            coordination_event_id = coordination_event_id or f"coordination-event-{request_id}"
        if not coordination_event_id:
            raise ValueError(f"{command_type} requires coordination_event_id")

        start_at_raw = str(raw_event.get("start_at") or payload.get("start_at") or "").strip()
        end_at_raw = str(raw_event.get("end_at") or payload.get("end_at") or "").strip()
        if not start_at_raw or not end_at_raw:
            raise ValueError(f"{command_type} requires start_at and end_at")

        start_at = _normalize_schedule_timestamp(start_at_raw)
        end_at = _normalize_schedule_timestamp(end_at_raw)
        if end_at <= start_at:
            raise ValueError(f"{command_type} requires end_at > start_at")

        normalized_payload.update(
            {
                "coordination_event_id": coordination_event_id,
                "responsibility_id": str(
                    raw_event.get("responsibility_id") or payload.get("responsibility_id") or ""
                ).strip(),
                "coordination_event": {
                    "coordination_event_id": coordination_event_id,
                    "title": str(raw_event.get("title") or "").strip(),
                    "responsibility_id": str(
                        raw_event.get("responsibility_id") or payload.get("responsibility_id") or ""
                    ).strip(),
                    "start_at": start_at,
                    "end_at": end_at,
                    "location": str(raw_event.get("location") or "").strip(),
                },
            }
        )
        return normalized_payload

    if command_type == "household.event.cancel":
        coordination_event_id = str(
            payload.get("coordination_event_id") or payload.get("event_id") or ""
        ).strip()
        if not coordination_event_id:
            raise ValueError("household.event.cancel requires coordination_event_id")

        normalized_payload.update(
            {
                "coordination_event_id": coordination_event_id,
                "cancelled_at": _utc_iso(),
            }
        )
        return normalized_payload

    if command_type == "household.execution.change":
        target_type = str(payload.get("target_type") or "").strip().lower()
        target_id = str(payload.get("target_id") or "").strip()
        execution_state = str(payload.get("execution_state") or payload.get("state") or "").strip().lower()
        if not target_type or not target_id or not execution_state:
            raise ValueError(
                "household.execution.change requires target_type, target_id, and execution_state"
            )

        normalized_payload.update(
            {
                "target_type": target_type,
                "target_id": target_id,
                "execution_state": execution_state,
                "reason": str(payload.get("reason") or "").strip(),
            }
        )
        return normalized_payload

    if command_type == "household.conflict.detect":
        conflict_id = str(payload.get("conflict_id") or "").strip() or f"conflict-{request_id}"
        conflict_type = str(payload.get("conflict_type") or "unspecified").strip().lower() or "unspecified"
        severity = str(payload.get("severity") or "medium").strip().lower() or "medium"
        if severity not in {"low", "medium", "high"}:
            severity = "medium"

        related_entity_ids = sorted(
            {
                str(entity_id).strip()
                for entity_id in payload.get("related_entity_ids") or []
                if str(entity_id).strip()
            }
        )

        normalized_payload.update(
            {
                "conflict_id": conflict_id,
                "conflict": {
                    "conflict_id": conflict_id,
                    "conflict_type": conflict_type,
                    "severity": severity,
                    "message": str(payload.get("message") or "").strip(),
                    "related_entity_ids": related_entity_ids,
                },
            }
        )
        return normalized_payload

    if command_type == "household.conflict.resolve":
        conflict_id = str(payload.get("conflict_id") or "").strip()
        if not conflict_id:
            raise ValueError("household.conflict.resolve requires conflict_id")

        normalized_payload.update(
            {
                "conflict_id": conflict_id,
                "resolution": str(payload.get("resolution") or payload.get("reason") or "resolved").strip()
                or "resolved",
                "resolved_at": _utc_iso(),
            }
        )
        return normalized_payload

    raise ValueError(f"Unsupported Surface command_type: {command_type}")


def _dispatch_family_coordination_command(
    *,
    command_type: str,
    event_type: str,
    household_id: str,
    actor_user_id: str,
    request_id: str,
    payload: Mapping[str, Any],
) -> SurfaceCommandDispatch:
    _ = event_type
    normalized_payload = _normalize_family_coordination_payload(
        command_type=command_type,
        payload=payload,
        request_id=request_id,
        household_id=household_id,
    )

    surface_event = build_family_coordination_event_from_command(
        command_type=command_type,
        partition_id=household_id,
        actor=actor_user_id,
        source="runtime.action_pipeline",
        payload=normalized_payload,
        timestamp=_utc_now(),
    )
    runtime_payload = surface_event.as_runtime_payload()

    response_payload = {
        "request_id": request_id,
        "surface": "family_coordination",
        "event_type": surface_event.event_type,
        "partition_id": household_id,
        "payload": dict(runtime_payload),
    }

    entity_id = str(
        normalized_payload.get("member_id")
        or normalized_payload.get("responsibility_id")
        or normalized_payload.get("coordination_event_id")
        or normalized_payload.get("target_id")
        or normalized_payload.get("conflict_id")
        or ""
    ).strip()
    effects_payload: dict[str, Any] = {
        "event_type": surface_event.event_type,
        "partition_id": household_id,
    }
    if entity_id:
        effects_payload["entity_id"] = entity_id

    return SurfaceCommandDispatch(
        status="accepted",
        event_type=surface_event.event_type,
        event_payload=runtime_payload,
        response_payload=response_payload,
        effects=(effects_payload,),
        idempotency_key=f"{request_id}:{surface_event.event_type}",
    )


def _reduce_family_coordination_event(
    *,
    event_type: str,
    payload: Mapping[str, Any],
    recorded_at: str,
    surface_state: Mapping[str, Any] | None,
    responses: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    return apply_family_coordination_event_projection(
        event_type=event_type,
        payload=payload,
        recorded_at=recorded_at,
        surface_state=surface_state,
        responses=responses,
    )


def _validate_family_coordination_event(
    *,
    event_type: str,
    event_payload: Mapping[str, Any],
    validation_state: Mapping[str, Any] | None,
) -> dict[str, Any]:
    current = dict(validation_state) if isinstance(validation_state, Mapping) else {}
    household_members = {
        str(item)
        for item in current.get("household_members") or set()
        if str(item).strip()
    }
    created_responsibilities = {
        str(item)
        for item in current.get("created_responsibilities") or set()
        if str(item).strip()
    }
    scheduled_coordination_events = {
        str(item)
        for item in current.get("scheduled_coordination_events") or set()
        if str(item).strip()
    }
    open_conflicts = {
        str(item)
        for item in current.get("open_conflicts") or set()
        if str(item).strip()
    }

    payload = dict(event_payload)

    if event_type == "HouseholdMemberAdded":
        member_payload = payload.get("member")
        member_id = ""
        if isinstance(member_payload, Mapping):
            member_id = str(member_payload.get("member_id") or "").strip()
        if not member_id:
            member_id = str(payload.get("member_id") or "").strip()
        if not member_id:
            raise ValueError("HouseholdMemberAdded missing member_id")
        if member_id in household_members:
            raise ValueError(f"Duplicate HouseholdMemberAdded for member_id={member_id}")
        household_members.add(member_id)

    elif event_type == "HouseholdMemberUpdated":
        member_payload = payload.get("member")
        member_id = ""
        if isinstance(member_payload, Mapping):
            member_id = str(member_payload.get("member_id") or "").strip()
        if not member_id:
            member_id = str(payload.get("member_id") or "").strip()
        if not member_id:
            raise ValueError("HouseholdMemberUpdated missing member_id")
        if member_id not in household_members:
            raise ValueError(f"Missing prerequisite HouseholdMemberAdded for member_id={member_id}")

    elif event_type == "ResponsibilityCreated":
        responsibility_payload = payload.get("responsibility")
        responsibility_id = ""
        if isinstance(responsibility_payload, Mapping):
            responsibility_id = str(responsibility_payload.get("responsibility_id") or "").strip()
        if not responsibility_id:
            responsibility_id = str(payload.get("responsibility_id") or "").strip()
        if not responsibility_id:
            raise ValueError("ResponsibilityCreated missing responsibility_id")
        if responsibility_id in created_responsibilities:
            raise ValueError(
                f"Duplicate ResponsibilityCreated for responsibility_id={responsibility_id}"
            )
        created_responsibilities.add(responsibility_id)

    elif event_type in {"ResponsibilityAssigned", "ResponsibilityUpdated"}:
        responsibility_payload = payload.get("responsibility")
        responsibility_id = ""
        if isinstance(responsibility_payload, Mapping):
            responsibility_id = str(responsibility_payload.get("responsibility_id") or "").strip()
        if not responsibility_id:
            responsibility_id = str(payload.get("responsibility_id") or "").strip()
        if not responsibility_id:
            raise ValueError(f"{event_type} missing responsibility_id")
        if responsibility_id not in created_responsibilities:
            raise ValueError(
                f"Missing prerequisite ResponsibilityCreated for responsibility_id={responsibility_id}"
            )

        if event_type == "ResponsibilityAssigned":
            assigned_member_ids = [
                str(item).strip()
                for item in payload.get("assigned_member_ids") or []
                if str(item).strip()
            ]
            assignee_member_id = str(payload.get("assignee_member_id") or "").strip()
            if assignee_member_id:
                assigned_member_ids.append(assignee_member_id)
            for member_id in sorted(set(assigned_member_ids)):
                if member_id not in household_members:
                    raise ValueError(
                        f"Missing prerequisite HouseholdMemberAdded for member_id={member_id}"
                    )

    elif event_type == "EventScheduled":
        coordination_event_payload = payload.get("coordination_event")
        coordination_event_id = ""
        if isinstance(coordination_event_payload, Mapping):
            coordination_event_id = str(
                coordination_event_payload.get("coordination_event_id")
                or coordination_event_payload.get("event_id")
                or ""
            ).strip()
        if not coordination_event_id:
            coordination_event_id = str(payload.get("coordination_event_id") or "").strip()
        if not coordination_event_id:
            raise ValueError("EventScheduled missing coordination_event_id")
        if coordination_event_id in scheduled_coordination_events:
            raise ValueError(
                f"Duplicate EventScheduled for coordination_event_id={coordination_event_id}"
            )

        responsibility_id = str(payload.get("responsibility_id") or "").strip()
        if isinstance(coordination_event_payload, Mapping):
            responsibility_id = str(
                coordination_event_payload.get("responsibility_id") or responsibility_id
            ).strip()
        if responsibility_id and responsibility_id not in created_responsibilities:
            raise ValueError(
                f"Missing prerequisite ResponsibilityCreated for responsibility_id={responsibility_id}"
            )
        scheduled_coordination_events.add(coordination_event_id)

    elif event_type in {"EventRescheduled", "EventCancelled"}:
        coordination_event_payload = payload.get("coordination_event")
        coordination_event_id = ""
        if isinstance(coordination_event_payload, Mapping):
            coordination_event_id = str(
                coordination_event_payload.get("coordination_event_id")
                or coordination_event_payload.get("event_id")
                or ""
            ).strip()
        if not coordination_event_id:
            coordination_event_id = str(payload.get("coordination_event_id") or "").strip()
        if not coordination_event_id:
            raise ValueError(f"{event_type} missing coordination_event_id")
        if coordination_event_id not in scheduled_coordination_events:
            raise ValueError(
                f"Missing prerequisite EventScheduled for coordination_event_id={coordination_event_id}"
            )

    elif event_type == "ExecutionStateChanged":
        target_type = str(payload.get("target_type") or "").strip().lower()
        target_id = str(payload.get("target_id") or "").strip()
        execution_state = str(payload.get("execution_state") or payload.get("state") or "").strip()
        if not target_type or not target_id or not execution_state:
            raise ValueError("ExecutionStateChanged missing target_type, target_id, or execution_state")
        if target_type == "responsibility" and target_id not in created_responsibilities:
            raise ValueError(
                f"Missing prerequisite ResponsibilityCreated for responsibility_id={target_id}"
            )
        if target_type in {"event", "coordination_event"} and target_id not in scheduled_coordination_events:
            raise ValueError(
                f"Missing prerequisite EventScheduled for coordination_event_id={target_id}"
            )

    elif event_type == "ConflictDetected":
        conflict_payload = payload.get("conflict")
        conflict_id = ""
        if isinstance(conflict_payload, Mapping):
            conflict_id = str(conflict_payload.get("conflict_id") or "").strip()
        if not conflict_id:
            conflict_id = str(payload.get("conflict_id") or "").strip()
        if not conflict_id:
            raise ValueError("ConflictDetected missing conflict_id")
        if conflict_id in open_conflicts:
            raise ValueError(f"Duplicate ConflictDetected for conflict_id={conflict_id}")
        open_conflicts.add(conflict_id)

    elif event_type == "ConflictResolved":
        conflict_payload = payload.get("conflict")
        conflict_id = ""
        if isinstance(conflict_payload, Mapping):
            conflict_id = str(conflict_payload.get("conflict_id") or "").strip()
        if not conflict_id:
            conflict_id = str(payload.get("conflict_id") or "").strip()
        if not conflict_id:
            raise ValueError("ConflictResolved missing conflict_id")
        if conflict_id not in open_conflicts:
            raise ValueError(f"Missing prerequisite ConflictDetected for conflict_id={conflict_id}")
        open_conflicts.remove(conflict_id)

    return {
        "household_members": sorted(household_members),
        "created_responsibilities": sorted(created_responsibilities),
        "scheduled_coordination_events": sorted(scheduled_coordination_events),
        "open_conflicts": sorted(open_conflicts),
    }


def _project_family_coordination_surface(
    *,
    projection_key: str,
    partition_id: str,
    surface_state: Mapping[str, Any],
    reference_timestamp: str,
) -> Mapping[str, Any]:
    return {
        projection_key: derive_family_coordination_surface_projections(
            partition_id=partition_id,
            state=surface_state,
            reference_timestamp=reference_timestamp,
        )
    }


_COMMAND_DISPATCH_SYMBOLS: "OrderedDict[str, CommandDispatchBuilder]" = OrderedDict(
    {
        "command_dispatch.family_coordination": _dispatch_family_coordination_command,
    }
)
_REDUCER_SYMBOLS: "OrderedDict[str, ReducerBuilder]" = OrderedDict(
    {
        "reducer.family_coordination": _reduce_family_coordination_event,
    }
)
_VALIDATOR_SYMBOLS: "OrderedDict[str, ValidatorBuilder]" = OrderedDict(
    {
        "validator.family_coordination": _validate_family_coordination_event,
    }
)
_PROJECTION_SYMBOLS: "OrderedDict[str, ProjectionBuilder]" = OrderedDict(
    {
        "projection.family_coordination_surface": _project_family_coordination_surface,
    }
)


class CommandDispatchFactory:
    @staticmethod
    def resolve(symbol: str) -> CommandDispatchBuilder:
        resolved = _COMMAND_DISPATCH_SYMBOLS.get(str(symbol or "").strip())
        if resolved is None:
            raise KeyError(f"Unknown command dispatch symbol: {symbol}")
        return resolved


class ReducerFactory:
    @staticmethod
    def resolve(symbol: str) -> ReducerBuilder:
        resolved = _REDUCER_SYMBOLS.get(str(symbol or "").strip())
        if resolved is None:
            raise KeyError(f"Unknown reducer symbol: {symbol}")
        return resolved

    @staticmethod
    def resolve_validator(symbol: str) -> ValidatorBuilder:
        resolved = _VALIDATOR_SYMBOLS.get(str(symbol or "").strip())
        if resolved is None:
            raise KeyError(f"Unknown validator symbol: {symbol}")
        return resolved


class ProjectionFactory:
    @staticmethod
    def resolve(symbol: str) -> ProjectionBuilder:
        resolved = _PROJECTION_SYMBOLS.get(str(symbol or "").strip())
        if resolved is None:
            raise KeyError(f"Unknown projection symbol: {symbol}")
        return resolved


def resolve_command_dispatch_symbol(symbol: str) -> CommandDispatchBuilder:
    return CommandDispatchFactory.resolve(symbol)


def resolve_reducer_symbol(symbol: str) -> ReducerBuilder:
    return ReducerFactory.resolve(symbol)


def resolve_validator_symbol(symbol: str) -> ValidatorBuilder:
    return ReducerFactory.resolve_validator(symbol)


def resolve_projection_symbol(symbol: str) -> ProjectionBuilder:
    return ProjectionFactory.resolve(symbol)


__all__ = [
    "CommandDispatchFactory",
    "ProjectionFactory",
    "ReducerFactory",
    "resolve_command_dispatch_symbol",
    "resolve_projection_symbol",
    "resolve_reducer_symbol",
    "resolve_validator_symbol",
]
