from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Header, HTTPException

from apps.api.hpal.command_gateway import HpalCommandGateway
from apps.api.hpal.contracts import (
    CreateEventRequest,
    CreateFamilyRequest,
    CreatePlanRequest,
    InternalTaskRescheduleRequest,
    InternalTaskStatusRequest,
    LinkEventPlanRequest,
    RecomputePlanRequest,
    UpdateMemberRequest,
    UpdatePlanRequest,
)


router = APIRouter(prefix="/v1", tags=["hpal"])
gateway = HpalCommandGateway()
_INTERNAL_TOKEN = os.getenv("HPAL_INTERNAL_TOKEN")


def _to_http_error(exc: Exception) -> HTTPException:
    message = str(exc).strip() or "request failed"
    if "not found" in message.lower():
        return HTTPException(status_code=404, detail=message)
    if "cross-family" in message.lower() or "idempotency" in message.lower() or "unknown participant" in message.lower():
        return HTTPException(status_code=409, detail=message)
    return HTTPException(status_code=400, detail=message)


@router.post("/families")
def create_family(request: CreateFamilyRequest) -> dict[str, Any]:
    try:
        family = gateway.create_family(request)
        return family.model_dump()
    except Exception as exc:  # pragma: no cover - transport mapping
        raise _to_http_error(exc)


@router.get("/families/{family_id}")
def get_family_state(family_id: str) -> dict[str, Any]:
    try:
        return gateway.get_family_state(family_id=family_id).model_dump()
    except Exception as exc:
        raise _to_http_error(exc)


@router.patch("/families/{family_id}/members/{person_id}")
def update_member(family_id: str, person_id: str, request: UpdateMemberRequest) -> dict[str, Any]:
    try:
        return gateway.update_member(family_id=family_id, person_id=person_id, request=request).model_dump()
    except Exception as exc:
        raise _to_http_error(exc)


@router.get("/families/{family_id}/overview")
def get_household_overview(family_id: str) -> dict[str, Any]:
    try:
        return gateway.get_household_overview(family_id=family_id).model_dump()
    except Exception as exc:
        raise _to_http_error(exc)


@router.post("/families/{family_id}/plans")
def create_plan_from_intent(family_id: str, request: CreatePlanRequest) -> dict[str, Any]:
    try:
        plan, command = gateway.create_plan_from_intent(family_id=family_id, request=request)
        return {"plan": plan.model_dump(), "command": command.model_dump()}
    except Exception as exc:
        raise _to_http_error(exc)


@router.patch("/families/{family_id}/plans/{plan_id}")
def update_plan(family_id: str, plan_id: str, request: UpdatePlanRequest) -> dict[str, Any]:
    try:
        plan, command = gateway.update_plan(family_id=family_id, plan_id=plan_id, request=request)
        return {"plan": plan.model_dump(), "command": command.model_dump()}
    except Exception as exc:
        raise _to_http_error(exc)


@router.post("/families/{family_id}/plans/{plan_id}/recompute")
def recompute_plan(family_id: str, plan_id: str, request: RecomputePlanRequest) -> dict[str, Any]:
    try:
        command = gateway.recompute_plan(family_id=family_id, plan_id=plan_id, request=request)
        return command.model_dump()
    except Exception as exc:
        raise _to_http_error(exc)


@router.get("/families/{family_id}/plans/{plan_id}")
def get_plan_status(family_id: str, plan_id: str) -> dict[str, Any]:
    try:
        return gateway.get_plan_status(family_id=family_id, plan_id=plan_id).model_dump()
    except Exception as exc:
        raise _to_http_error(exc)


@router.get("/families/{family_id}/tasks")
def get_tasks_by_family(family_id: str) -> dict[str, Any]:
    try:
        return {"tasks": gateway.get_tasks_by_family(family_id=family_id)}
    except Exception as exc:
        raise _to_http_error(exc)


@router.get("/families/{family_id}/people/{person_id}/tasks")
def get_tasks_by_person(family_id: str, person_id: str) -> dict[str, Any]:
    try:
        return {"tasks": gateway.get_tasks_by_person(family_id=family_id, person_id=person_id)}
    except Exception as exc:
        raise _to_http_error(exc)


@router.post("/internal/families/{family_id}/tasks/{task_id}/status")
def update_task_status_system_only(
    family_id: str,
    task_id: str,
    request: InternalTaskStatusRequest,
    x_hpal_system_token: str | None = Header(default=None),
) -> dict[str, Any]:
    if not _INTERNAL_TOKEN:
        raise HTTPException(status_code=503, detail="internal task controls disabled")
    if x_hpal_system_token != _INTERNAL_TOKEN:
        raise HTTPException(status_code=403, detail="forbidden")
    try:
        return gateway.system_override_task_status(
            family_id=family_id,
            task_id=task_id,
            target_status=request.target_status,
            reason_code=request.reason_code,
        )
    except Exception as exc:
        raise _to_http_error(exc)


@router.post("/internal/families/{family_id}/tasks/{task_id}/reschedule")
def reschedule_task_system_only(
    family_id: str,
    task_id: str,
    request: InternalTaskRescheduleRequest,
    x_hpal_system_token: str | None = Header(default=None),
) -> dict[str, Any]:
    if not _INTERNAL_TOKEN:
        raise HTTPException(status_code=503, detail="internal task controls disabled")
    if x_hpal_system_token != _INTERNAL_TOKEN:
        raise HTTPException(status_code=403, detail="forbidden")
    try:
        return gateway.system_reschedule_task(
            family_id=family_id,
            task_id=task_id,
            due_time=request.due_time,
            reason_code=request.reason_code,
        )
    except Exception as exc:
        raise _to_http_error(exc)


@router.post("/families/{family_id}/events")
def create_event(family_id: str, request: CreateEventRequest) -> dict[str, Any]:
    try:
        event, command = gateway.create_event(family_id=family_id, request=request)
        return {"event": event.model_dump(), "command": command.model_dump()}
    except Exception as exc:
        raise _to_http_error(exc)


@router.get("/families/{family_id}/calendar")
def get_calendar_view(family_id: str) -> dict[str, Any]:
    try:
        return {"events": gateway.get_calendar_view(family_id=family_id)}
    except Exception as exc:
        raise _to_http_error(exc)


@router.post("/families/{family_id}/events/{event_id}/link-plan")
def link_event_to_plan(family_id: str, event_id: str, request: LinkEventPlanRequest) -> dict[str, Any]:
    try:
        event, command = gateway.link_event_to_plan(family_id=family_id, event_id=event_id, request=request)
        return {"event": event.model_dump(), "command": command.model_dump()}
    except Exception as exc:
        raise _to_http_error(exc)
