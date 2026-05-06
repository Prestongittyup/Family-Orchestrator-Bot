from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from app.api import tasks as tasks_api
from app.services.commands import CommandActor, get_command_runtime_service
from app.services.commands.command_execution_layer import MinimalCommandExecutionLayer
from app.services.events.event_log_service import EventLogService


router = APIRouter(prefix="/command", tags=["command"])
ingest_router = APIRouter(prefix="/ingest", tags=["ingest"])

_HOME_DELTA_CATEGORIES = ("needs_decision", "actions", "calendar")
_TARGET_TYPE_VALUES = frozenset({"decision", "action", "calendar_event"})


class CommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command_type: str = Field(min_length=1)
    household_id: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None


class CommandMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1)
    timestamp: str = Field(min_length=1)


class StrictCommandExecutionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    household_id: str = Field(min_length=1)
    command_type: Literal["resolve", "defer", "complete", "ignore"]
    target_type: Literal["decision", "action", "calendar_event"]
    target_id: str = Field(min_length=1)
    metadata: CommandMetadata


CommandEndpointPayload = CommandRequest | StrictCommandExecutionRequest


class EmailIngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    household_id: str = Field(min_length=1)
    email: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None


class CalendarIngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    household_id: str = Field(min_length=1)
    events: list[dict[str, Any]] = Field(default_factory=list)
    calendar_id: str | None = None
    idempotency_key: str | None = None


class HouseholdMessageIngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    household_id: str = Field(min_length=1)
    raw_content: str = Field(min_length=1)
    source: str | None = None
    created_at: str | None = None
    member_id: str | None = None
    idempotency_key: str | None = None


def _mapping_rows(raw_value: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_value, list):
        return []
    return [dict(row) for row in raw_value if isinstance(row, dict)]


def _stable_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        ordered_pairs = sorted(
            ((str(key), _stable_json_value(item)) for key, item in value.items()),
            key=lambda item: item[0],
        )
        return {key: item for key, item in ordered_pairs}
    if isinstance(value, list):
        return [_stable_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [_stable_json_value(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _normalized_home_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[tuple[str, dict[str, Any]]] = []
    for row in rows:
        stable_row = _stable_json_value(dict(row))
        row_id = str(stable_row.get("id") or "").strip()
        if not row_id:
            encoded = json.dumps(stable_row, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
            row_id = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        normalized.append((row_id, stable_row))
    normalized.sort(key=lambda item: item[0])
    return [row for _, row in normalized]


def _derive_home_surface_snapshot(*, household_id: str, projection: dict[str, Any], target_date: str) -> dict[str, Any]:
    raw_emails = tasks_api._projection_email_input(projection)
    raw_calendar_events = tasks_api._projection_calendar_input(projection, target_date=target_date)

    email_items = tasks_api.email_agent(raw_emails)
    calendar_items = tasks_api.calendar_agent(raw_calendar_events)
    ordered_payload = tasks_api.orchestrator(email_items, calendar_items)

    base_decisions = _mapping_rows(ordered_payload.get("needs_decision"))
    base_actions = _mapping_rows(ordered_payload.get("actions"))
    base_calendar = _mapping_rows(ordered_payload.get("calendar"))

    merged_decisions = tasks_api._merge_home_decisions_with_projection_cards(
        base_decisions=base_decisions,
        projection=projection,
    )
    merged_actions = tasks_api._merge_home_actions_with_projection_tasks(
        base_actions=base_actions,
        projection=projection,
        target_date=target_date,
    )
    merged_calendar = tasks_api._merge_home_calendar_with_projection_schedule(
        base_calendar=base_calendar,
        projection=projection,
        target_date=target_date,
    )

    summary_signature = {
        "household_id": household_id,
        "decision_count": len(merged_decisions),
        "action_count": len(merged_actions),
        "calendar_count": len(merged_calendar),
        "top_decision_id": str(merged_decisions[0].get("id") or "") if merged_decisions else "",
        "top_action_id": str(merged_actions[0].get("id") or "") if merged_actions else "",
        "top_calendar_id": str(merged_calendar[0].get("id") or "") if merged_calendar else "",
        "latest_change_at": tasks_api._latest_projection_change_timestamp(projection),
    }

    return {
        "needs_decision": _normalized_home_rows(merged_decisions),
        "actions": _normalized_home_rows(merged_actions),
        "calendar": _normalized_home_rows(merged_calendar),
        "summary": _stable_json_value(summary_signature),
    }


def _fingerprint_for_home_surface(home_surface: dict[str, Any]) -> str:
    canonical_surface = _stable_json_value(home_surface)
    encoded = json.dumps(canonical_surface, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def compute_projection_fingerprint(projection: dict[str, Any]) -> str:
    household_id = str(projection.get("household_id") or "").strip()
    target_date = tasks_api._normalized_today_date(None)
    home_surface = _derive_home_surface_snapshot(
        household_id=household_id,
        projection=projection,
        target_date=target_date,
    )
    return _fingerprint_for_home_surface(home_surface)


def _event_rows_for_household(runtime: Any, *, household_id: str) -> list[Any]:
    event_log = getattr(runtime, "_event_log", None)
    if event_log is None or not hasattr(event_log, "get_event_logs"):
        event_log = EventLogService()

    try:
        rows = event_log.get_event_logs(household_id=household_id, limit=5000)
    except TypeError:
        rows = event_log.get_event_logs(household_id=household_id)
    return list(rows) if isinstance(rows, list) else []


def _event_row_sort_key(row: Any) -> tuple[str, str]:
    raw_timestamp = getattr(row, "timestamp", None)
    if isinstance(raw_timestamp, datetime):
        timestamp = raw_timestamp.isoformat()
    else:
        timestamp = str(raw_timestamp or "")
    return timestamp, str(getattr(row, "event_id", "") or "")


def _events_emitted_since(*, before_rows: list[Any], after_rows: list[Any]) -> list[str]:
    before_ids = {
        str(getattr(row, "event_id", "") or "").strip()
        for row in before_rows
        if str(getattr(row, "event_id", "") or "").strip()
    }

    emitted_rows = [
        row
        for row in after_rows
        if str(getattr(row, "event_id", "") or "").strip()
        and str(getattr(row, "event_id", "") or "").strip() not in before_ids
    ]
    emitted_rows.sort(key=_event_row_sort_key)
    return [str(getattr(row, "event_id", "") or "") for row in emitted_rows]


def _rows_by_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows_by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_id = str(row.get("id") or "").strip()
        if row_id:
            rows_by_id[row_id] = row
    return rows_by_id


def _derive_home_delta(*, before_home: dict[str, Any], after_home: dict[str, Any]) -> dict[str, Any]:
    added: list[dict[str, str]] = []
    removed: list[dict[str, str]] = []
    updated: list[dict[str, str]] = []

    for category in _HOME_DELTA_CATEGORIES:
        before_rows = _rows_by_id(_mapping_rows(before_home.get(category)))
        after_rows = _rows_by_id(_mapping_rows(after_home.get(category)))

        for row_id in sorted(set(after_rows).difference(before_rows)):
            added.append({"category": category, "id": row_id})

        for row_id in sorted(set(before_rows).difference(after_rows)):
            removed.append({"category": category, "id": row_id})

        for row_id in sorted(set(before_rows).intersection(after_rows)):
            if _stable_json_value(before_rows[row_id]) != _stable_json_value(after_rows[row_id]):
                updated.append({"category": category, "id": row_id})

    summary_changed = _stable_json_value(before_home.get("summary") or {}) != _stable_json_value(after_home.get("summary") or {})
    changed = bool(added or removed or updated or summary_changed)

    return {
        "changed": changed,
        "added": added,
        "removed": removed,
        "updated": updated,
        "summary_changed": summary_changed,
    }


def _feedback_status(raw_status: Any) -> str:
    normalized = str(raw_status or "").strip().lower()
    if normalized == "duplicate":
        return "duplicate"
    if normalized == "rejected":
        return "rejected"
    return "applied"


def _resolved_command_id(*, request_payload: CommandEndpointPayload, execution_result: dict[str, Any]) -> str:
    response_command_id = str(execution_result.get("command_id") or "").strip()
    if response_command_id:
        return response_command_id

    response_request_id = str(execution_result.get("request_id") or "").strip()
    if response_request_id:
        return response_request_id

    if isinstance(request_payload, CommandRequest):
        idempotency_key = str(request_payload.idempotency_key or "").strip()
        if idempotency_key:
            return idempotency_key

    if isinstance(request_payload, StrictCommandExecutionRequest):
        raw = f"{request_payload.household_id}:{request_payload.target_id}:{request_payload.command_type}"
    else:
        payload_json = json.dumps(
            _stable_json_value(request_payload.payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        raw = f"{request_payload.household_id}:{request_payload.command_type}:{payload_json}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _response_mapping(execution_result: dict[str, Any]) -> dict[str, Any]:
    response_payload = execution_result.get("response")
    if isinstance(response_payload, dict):
        return response_payload
    return {}


def _legacy_target_descriptor(
    *,
    request_payload: CommandRequest,
    execution_result: dict[str, Any],
    command_id: str,
) -> tuple[str, str]:
    response_payload = _response_mapping(execution_result)

    task_payload = response_payload.get("task")
    if isinstance(task_payload, dict):
        task_id = str(task_payload.get("task_id") or "").strip()
        if task_id:
            return "action", task_id

    schedule_payload = response_payload.get("schedule")
    if isinstance(schedule_payload, dict):
        schedule_id = str(schedule_payload.get("schedule_id") or "").strip()
        if schedule_id:
            return "calendar_event", schedule_id

    decision_payload = response_payload.get("decision")
    if isinstance(decision_payload, dict):
        decision_id = str(decision_payload.get("decision_id") or "").strip()
        if decision_id:
            return "decision", decision_id

    body = dict(request_payload.payload)
    command_type = str(request_payload.command_type or "").strip().lower()

    if command_type.startswith("decision."):
        decision_id = str(body.get("decision_id") or "").strip()
        if decision_id:
            return "decision", decision_id

    if command_type.startswith("schedule."):
        schedule_id = str(body.get("schedule_id") or body.get("event_id") or "").strip()
        if schedule_id:
            return "calendar_event", schedule_id

    task_id = str(body.get("task_id") or "").strip()
    if task_id:
        return "action", task_id

    decision_id = str(body.get("decision_id") or "").strip()
    if decision_id:
        return "decision", decision_id

    schedule_id = str(body.get("schedule_id") or body.get("event_id") or "").strip()
    if schedule_id:
        return "calendar_event", schedule_id

    return "action", command_id


def _resolved_target_descriptor(
    *,
    request_payload: CommandEndpointPayload,
    execution_result: dict[str, Any],
    command_id: str,
) -> tuple[str, str]:
    if isinstance(request_payload, StrictCommandExecutionRequest):
        return request_payload.target_type, request_payload.target_id

    target_type, target_id = _legacy_target_descriptor(
        request_payload=request_payload,
        execution_result=execution_result,
        command_id=command_id,
    )
    if target_type not in _TARGET_TYPE_VALUES:
        target_type = "action"
    if not target_id.strip():
        target_id = command_id
    return target_type, target_id


def _augment_execution_feedback(
    *,
    request_payload: CommandEndpointPayload,
    execution_result: dict[str, Any],
    runtime: Any,
    before_home: dict[str, Any],
    before_fingerprint: str,
    before_event_rows: list[Any],
    target_date: str,
) -> dict[str, Any]:
    household_id = str(request_payload.household_id).strip()

    projection = execution_result.get("projection")
    if isinstance(projection, dict):
        after_projection = projection
    else:
        after_projection = runtime.get_projection(household_id, force_replay=True)

    after_home = _derive_home_surface_snapshot(
        household_id=household_id,
        projection=after_projection,
        target_date=target_date,
    )
    after_fingerprint = _fingerprint_for_home_surface(after_home)

    after_event_rows = _event_rows_for_household(runtime, household_id=household_id)
    events_emitted = _events_emitted_since(before_rows=before_event_rows, after_rows=after_event_rows)

    command_id = _resolved_command_id(
        request_payload=request_payload,
        execution_result=execution_result,
    )
    target_type, target_id = _resolved_target_descriptor(
        request_payload=request_payload,
        execution_result=execution_result,
        command_id=command_id,
    )

    response_payload = dict(execution_result)
    if isinstance(request_payload, StrictCommandExecutionRequest):
        response_payload["status"] = _feedback_status(response_payload.get("status"))

    response_payload.update(
        {
            "command_id": command_id,
            "target_id": target_id,
            "target_type": target_type,
            "events_emitted": events_emitted,
            "home_delta": _derive_home_delta(before_home=before_home, after_home=after_home),
            "projection_fingerprint_before": before_fingerprint,
            "projection_fingerprint_after": after_fingerprint,
        }
    )
    return response_payload


def _actor_from_request(request: Request, fallback_user_id: str | None = None) -> CommandActor:
    actor_type = str(getattr(request.state, "actor_type", "api_user") or "api_user").strip().lower()
    if actor_type in {"", "unknown"}:
        actor_type = "api_user"

    user_claims = getattr(request.state, "user", None)
    claim_subject = ""
    if isinstance(user_claims, dict):
        claim_subject = str(user_claims.get("sub") or user_claims.get("user_id") or "").strip()

    resolved_user_id = claim_subject or str(fallback_user_id or "system").strip() or "system"
    session_id = str(getattr(request.state, "session_id", "") or "").strip() or None

    return CommandActor(
        actor_type=actor_type,
        user_id=resolved_user_id,
        session_id=session_id,
    )


@router.post("")
async def execute_command(payload: CommandEndpointPayload, request: Request) -> dict[str, Any]:
    runtime = get_command_runtime_service()
    target_date = tasks_api._normalized_today_date(None)
    before_projection = runtime.get_projection(payload.household_id)
    before_home = _derive_home_surface_snapshot(
        household_id=payload.household_id,
        projection=before_projection,
        target_date=target_date,
    )
    before_fingerprint = _fingerprint_for_home_surface(before_home)
    before_event_rows = _event_rows_for_household(runtime, household_id=payload.household_id)

    try:
        if isinstance(payload, StrictCommandExecutionRequest):
            actor = _actor_from_request(request, fallback_user_id="system")
            layer = MinimalCommandExecutionLayer(runtime_service=runtime)
            execution_result = layer.execute(
                household_id=payload.household_id,
                command_type=payload.command_type,
                target_type=payload.target_type,
                target_id=payload.target_id,
                metadata_source=payload.metadata.source,
                metadata_timestamp=payload.metadata.timestamp,
                actor=actor,
            )
            return _augment_execution_feedback(
                request_payload=payload,
                execution_result=execution_result,
                runtime=runtime,
                before_home=before_home,
                before_fingerprint=before_fingerprint,
                before_event_rows=before_event_rows,
                target_date=target_date,
            )

        actor = _actor_from_request(request, fallback_user_id=str(payload.payload.get("user_id") or "system"))
        execution_result = runtime.handle_command(
            command_type=payload.command_type,
            household_id=payload.household_id,
            actor=actor,
            payload=payload.payload,
            source="api.command",
            idempotency_key=payload.idempotency_key,
        )
        return _augment_execution_feedback(
            request_payload=payload,
            execution_result=execution_result,
            runtime=runtime,
            before_home=before_home,
            before_fingerprint=before_fingerprint,
            before_event_rows=before_event_rows,
            target_date=target_date,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@ingest_router.post("/email")
async def ingest_email(payload: EmailIngestRequest, request: Request) -> dict[str, Any]:
    runtime = get_command_runtime_service()
    actor = _actor_from_request(request, fallback_user_id="system")

    try:
        return runtime.handle_command(
            command_type="email.ingest",
            household_id=payload.household_id,
            actor=actor,
            payload={"email": payload.email},
            source="api.ingest.email",
            idempotency_key=payload.idempotency_key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@ingest_router.post("/calendar")
async def ingest_calendar(payload: CalendarIngestRequest, request: Request) -> dict[str, Any]:
    runtime = get_command_runtime_service()
    actor = _actor_from_request(request, fallback_user_id="system")

    try:
        return runtime.handle_command(
            command_type="calendar.ingest",
            household_id=payload.household_id,
            actor=actor,
            payload={
                "calendar_id": payload.calendar_id,
                "events": payload.events,
            },
            source="api.ingest.calendar",
            idempotency_key=payload.idempotency_key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@ingest_router.post("/message")
async def ingest_household_message(payload: HouseholdMessageIngestRequest, request: Request) -> dict[str, Any]:
    runtime = get_command_runtime_service()
    actor = _actor_from_request(request, fallback_user_id=str(payload.member_id or "system"))

    try:
        return runtime.handle_command(
            command_type="household.message.ingest",
            household_id=payload.household_id,
            actor=actor,
            payload={
                "source": payload.source,
                "raw_content": payload.raw_content,
                "created_at": payload.created_at,
                "member_id": payload.member_id,
            },
            source="api.ingest.message",
            idempotency_key=payload.idempotency_key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


