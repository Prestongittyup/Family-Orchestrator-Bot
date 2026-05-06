from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

from app.services.commands.runtime import CommandActor, CommandRuntimeService
from app.services.events import EventLogService


@dataclass(frozen=True)
class ValidatedCommand:
    household_id: str
    command_type: str
    target_type: str
    target_id: str
    metadata_source: str
    metadata_timestamp: str


@dataclass(frozen=True)
class TargetContext:
    projection: dict[str, Any]
    target: dict[str, Any]


@dataclass(frozen=True)
class RuntimeDispatch:
    runtime_command_type: str
    runtime_payload: dict[str, Any]
    canonical_event_type: str


class MinimalCommandExecutionLayer:
    _ALLOWED_COMMAND_TYPES = frozenset({"resolve", "defer", "complete", "ignore"})
    _ALLOWED_TARGET_TYPES = frozenset({"decision", "action", "calendar_event"})

    def __init__(
        self,
        *,
        runtime_service: CommandRuntimeService,
        event_log_service: EventLogService | None = None,
    ) -> None:
        self._runtime = runtime_service
        self._event_log = event_log_service or EventLogService()

    def execute(
        self,
        *,
        household_id: str,
        command_type: str,
        target_type: str,
        target_id: str,
        metadata_source: str,
        metadata_timestamp: str,
        actor: CommandActor,
    ) -> dict[str, Any]:
        validated = self.validate_command(
            household_id=household_id,
            command_type=command_type,
            target_type=target_type,
            target_id=target_id,
            metadata_source=metadata_source,
            metadata_timestamp=metadata_timestamp,
        )
        target_context = self.load_target_from_projection(command=validated)
        dispatch = self.apply_command(command=validated, target_context=target_context)
        command_id = self._command_id(command=validated)
        return self.emit_event(
            command=validated,
            dispatch=dispatch,
            command_id=command_id,
            actor=actor,
        )

    def validate_command(
        self,
        *,
        household_id: str,
        command_type: str,
        target_type: str,
        target_id: str,
        metadata_source: str,
        metadata_timestamp: str,
    ) -> ValidatedCommand:
        resolved_household_id = str(household_id or "").strip()
        resolved_command_type = str(command_type or "").strip().lower()
        resolved_target_type = str(target_type or "").strip().lower()
        resolved_target_id = str(target_id or "").strip()
        resolved_source = str(metadata_source or "").strip()
        resolved_timestamp = self._normalize_timestamp(str(metadata_timestamp or "").strip())

        if not resolved_household_id:
            raise ValueError("household_id is required")
        if resolved_command_type not in self._ALLOWED_COMMAND_TYPES:
            allowed = ", ".join(sorted(self._ALLOWED_COMMAND_TYPES))
            raise ValueError(f"command_type must be one of: {allowed}")
        if resolved_target_type not in self._ALLOWED_TARGET_TYPES:
            allowed = ", ".join(sorted(self._ALLOWED_TARGET_TYPES))
            raise ValueError(f"target_type must be one of: {allowed}")
        if not resolved_target_id:
            raise ValueError("target_id is required")
        if not resolved_source:
            raise ValueError("metadata.source is required")

        return ValidatedCommand(
            household_id=resolved_household_id,
            command_type=resolved_command_type,
            target_type=resolved_target_type,
            target_id=resolved_target_id,
            metadata_source=resolved_source,
            metadata_timestamp=resolved_timestamp,
        )

    def load_target_from_projection(self, *, command: ValidatedCommand) -> TargetContext:
        projection = self._runtime.get_projection(command.household_id)

        if command.target_type == "decision":
            target = self._decision_target(projection=projection, target_id=command.target_id)
            if target is None:
                raise ValueError(
                    f"target_id not found for target_type=decision: {command.target_id}"
                )
            return TargetContext(projection=projection, target=target)

        if command.target_type == "action":
            tasks = projection.get("tasks")
            task = tasks.get(command.target_id) if isinstance(tasks, Mapping) else None
            if not isinstance(task, Mapping):
                raise ValueError(
                    f"target_id not found for target_type=action: {command.target_id}"
                )
            return TargetContext(projection=projection, target=dict(task))

        schedules = projection.get("schedules")
        schedule = schedules.get(command.target_id) if isinstance(schedules, Mapping) else None
        if not isinstance(schedule, Mapping):
            raise ValueError(
                f"target_id not found for target_type=calendar_event: {command.target_id}"
            )
        return TargetContext(projection=projection, target=dict(schedule))

    def apply_command(self, *, command: ValidatedCommand, target_context: TargetContext) -> RuntimeDispatch:
        _ = target_context

        if command.target_type == "decision":
            if command.command_type == "resolve":
                return RuntimeDispatch(
                    runtime_command_type="decision.complete",
                    runtime_payload={"decision_id": command.target_id},
                    canonical_event_type="DecisionCompleted",
                )
            if command.command_type == "defer":
                defer_to_date = self._defer_to_date(command.metadata_timestamp)
                return RuntimeDispatch(
                    runtime_command_type="decision.defer",
                    runtime_payload={
                        "decision_id": command.target_id,
                        "defer_to_date": defer_to_date,
                    },
                    canonical_event_type="DecisionDeferred",
                )
            if command.command_type == "ignore":
                return RuntimeDispatch(
                    runtime_command_type="decision.ignore",
                    runtime_payload={"decision_id": command.target_id},
                    canonical_event_type="DecisionIgnored",
                )
            raise ValueError(
                "Unsupported command combination for decision target: complete is not allowed"
            )

        if command.target_type == "action":
            if command.command_type != "complete":
                raise ValueError(
                    "Unsupported command combination for action target: only complete is allowed"
                )
            return RuntimeDispatch(
                runtime_command_type="task_completed",
                runtime_payload={"task_id": command.target_id},
                canonical_event_type="TaskCompleted",
            )

        if command.command_type != "ignore":
            raise ValueError(
                "Unsupported command combination for calendar_event target: only ignore is allowed"
            )
        return RuntimeDispatch(
            runtime_command_type="schedule.cancel",
            runtime_payload={"schedule_id": command.target_id},
            canonical_event_type="ScheduleCancelled",
        )

    def emit_event(
        self,
        *,
        command: ValidatedCommand,
        dispatch: RuntimeDispatch,
        command_id: str,
        actor: CommandActor,
    ) -> dict[str, Any]:
        if self._event_log.idempotency_key_exists(command_id):
            projection = self._runtime.get_projection(command.household_id)
            return {
                "status": "duplicate",
                "command_id": command_id,
                "request_id": command_id,
                "event_id": str(projection.get("last_event_id") or ""),
                "event_type": dispatch.canonical_event_type,
                "response": self._response_for_request(projection=projection, request_id=command_id),
                "effects": [],
                "projection": projection,
            }

        runtime_payload = {
            **dispatch.runtime_payload,
            "request_id": command_id,
            "command_id": command_id,
            "metadata": {
                "source": command.metadata_source,
                "timestamp": command.metadata_timestamp,
            },
        }

        result = self._runtime.handle_command(
            command_type=dispatch.runtime_command_type,
            household_id=command.household_id,
            actor=actor,
            payload=runtime_payload,
            source="api.command.execution",
            idempotency_key=command_id,
        )

        return {
            "status": str(result.get("status") or "accepted"),
            "command_id": command_id,
            "request_id": str(result.get("request_id") or command_id),
            "event_id": str(result.get("event_id") or ""),
            "event_type": dispatch.canonical_event_type,
            "response": result.get("response"),
            "effects": list(result.get("effects") or []),
            "projection": result.get("projection"),
        }

    def _command_id(self, *, command: ValidatedCommand) -> str:
        raw = f"{command.household_id}:{command.target_id}:{command.command_type}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _normalize_timestamp(self, raw_timestamp: str) -> str:
        if not raw_timestamp:
            raise ValueError("metadata.timestamp is required")

        timestamp = raw_timestamp
        if raw_timestamp.endswith("Z"):
            timestamp = f"{raw_timestamp[:-1]}+00:00"

        try:
            parsed = datetime.fromisoformat(timestamp)
        except ValueError as exc:
            raise ValueError("metadata.timestamp must be ISO-8601") from exc

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")

    def _defer_to_date(self, timestamp: str) -> str:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return (parsed + timedelta(days=1)).date().isoformat()

    def _decision_target(self, *, projection: Mapping[str, Any], target_id: str) -> dict[str, Any] | None:
        decisions = projection.get("decisions")
        if isinstance(decisions, Mapping):
            decision = decisions.get(target_id)
            if isinstance(decision, Mapping):
                return dict(decision)

        decision_cards = projection.get("decision_cards")
        if isinstance(decision_cards, Mapping):
            card = decision_cards.get(target_id)
            if isinstance(card, Mapping):
                return dict(card)

        tasks = projection.get("tasks")
        if isinstance(tasks, Mapping):
            task = tasks.get(target_id)
            if isinstance(task, Mapping):
                return dict(task)

        return None

    def _response_for_request(self, *, projection: Mapping[str, Any], request_id: str) -> dict[str, Any]:
        responses = projection.get("responses")
        if isinstance(responses, Mapping):
            response = responses.get(request_id)
            if isinstance(response, Mapping):
                return dict(response)
        return {"request_id": request_id}


__all__ = ["MinimalCommandExecutionLayer"]
