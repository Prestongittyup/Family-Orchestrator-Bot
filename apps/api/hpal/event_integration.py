from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any

from apps.api.hpal.contracts import CreateEventRequest, EventModel, LinkEventPlanRequest


class EventIntegrationLayer:
    def create_event(self, *, family_id: str, request: CreateEventRequest, graph: dict[str, Any]) -> EventModel:
        events = graph.setdefault("hpal", {}).setdefault("events", {})
        event_id = f"evt-{self._digest(f'{family_id}:{request.idempotency_key}')[:12]}"
        existing = events.get(event_id)
        if existing is not None:
            return EventModel.model_validate(existing)

        model = EventModel(
            event_id=event_id,
            family_id=family_id,
            title=request.title,
            time_window=request.time_window,
            participants=request.participants,
            linked_plans=[],
            source=request.source,
        )
        payload = model.model_dump()
        payload["linked_plan_revisions"] = []
        events[event_id] = payload
        return model

    def link_event_to_plan(
        self,
        *,
        family_id: str,
        event_id: str,
        request: LinkEventPlanRequest,
        plan_revision: int,
        graph: dict[str, Any],
    ) -> EventModel:
        events = graph.setdefault("hpal", {}).setdefault("events", {})
        payload = events.get(event_id)
        if payload is None:
            raise ValueError("event not found")
        if payload.get("family_id") != family_id:
            raise ValueError("cross-family event linkage is not allowed")

        linked = list(payload.get("linked_plans", []))
        linked_revisions = set(payload.get("linked_plan_revisions", []))
        revision_key = f"{request.plan_id}:r{plan_revision}"
        if revision_key in linked_revisions:
            return EventModel.model_validate(payload)

        if request.plan_id not in linked:
            linked.append(request.plan_id)
        linked_revisions.add(revision_key)
        payload["linked_plans"] = sorted(linked)
        payload["linked_plan_revisions"] = sorted(linked_revisions)
        events[event_id] = payload
        return EventModel.model_validate(payload)

    def should_trigger_recompute(self, *, before: dict[str, Any] | None, after: dict[str, Any]) -> bool:
        if before is None:
            return False
        before_window = before.get("time_window", {})
        after_window = after.get("time_window", {})
        before_people = sorted(before.get("participants", []))
        after_people = sorted(after.get("participants", []))
        return before_window != after_window or before_people != after_people

    def _digest(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def now_iso(self) -> str:
        return datetime.utcnow().isoformat() + "Z"
