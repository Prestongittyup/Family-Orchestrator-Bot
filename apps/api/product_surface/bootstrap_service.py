from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from threading import RLock

from apps.api.hpal.command_gateway import HpalCommandGateway
from apps.api.product_surface.contracts import (
    CalendarEventSummary,
    CalendarState,
    FamilySummary,
    Notification,
    PlanSummary,
    SystemHealthSnapshot,
    TaskBoardState,
    TaskSummary,
    TodayOverview,
    UIBootstrapState,
    XAIExplanationSummary,
)
from apps.api.xai.store import ExplanationStore


class UIBootstrapService:
    """Deterministic projection aggregator for a UI-safe bootstrap contract."""

    def __init__(
        self,
        *,
        hpal_gateway: HpalCommandGateway | None = None,
        xai_store: ExplanationStore | None = None,
    ) -> None:
        self._gateway = hpal_gateway or HpalCommandGateway()
        self._xai_store = xai_store or ExplanationStore()
        self._cache_lock = RLock()
        self._cache: dict[str, UIBootstrapState] = {}

    def get_state(self, *, family_id: str) -> UIBootstrapState:
        if not family_id or not family_id.strip():
            raise ValueError("family_id is required")

        family = self._gateway.get_family_state(family_id=family_id)
        plans_raw = self._gateway.get_plans_by_family(family_id=family_id)
        tasks_raw = self._gateway.get_tasks_by_family(family_id=family_id)
        events_raw = self._gateway.get_calendar_view(family_id=family_id)
        explanations_raw = self._xai_store.get_recent(family_id=family_id, limit=20)

        system_state = dict(family.system_state_summary)
        source_watermark = self._source_watermark(system_state)
        cache_key = f"{family_id}:{source_watermark}"

        with self._cache_lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

        member_names = sorted(person.name for person in family.members)
        family_summary = FamilySummary(
            family_id=family.family_id,
            member_count=len(family.members),
            member_names=member_names,
            default_time_zone=family.default_time_zone,
        )

        active_plans = sorted(
            [
                PlanSummary(
                    plan_id=row["plan_id"],
                    title=row["title"],
                    status=row["status"],
                    revision=int(row.get("revision", 0)),
                    linked_task_count=len(row.get("linked_tasks", [])),
                )
                for row in plans_raw
            ],
            key=lambda item: (item.title.lower(), item.plan_id),
        )

        task_rows = [
            TaskSummary(
                task_id=row["task_id"],
                title=row["title"],
                plan_id=row["plan_id"],
                assigned_to=row["assigned_to"],
                status=row["status"],
                priority=row.get("priority", "medium"),
                due_time=row.get("due_time"),
            )
            for row in tasks_raw
        ]
        task_rows.sort(key=lambda item: (item.status, item.priority, item.task_id))

        task_board = TaskBoardState(
            pending=[t for t in task_rows if t.status == "pending"],
            in_progress=[t for t in task_rows if t.status == "in_progress"],
            completed=[t for t in task_rows if t.status == "completed"],
            failed=[t for t in task_rows if t.status == "failed"],
        )

        anchor = _parse_iso(str(system_state.get("last_projection_at", "")))
        window_start = (anchor - timedelta(days=1)).replace(microsecond=0)
        window_end = (anchor + timedelta(days=30)).replace(microsecond=0)

        calendar_events = []
        for row in events_raw:
            start = _parse_iso(row["time_window"]["start"])
            if window_start <= start <= window_end:
                calendar_events.append(
                    CalendarEventSummary(
                        event_id=row["event_id"],
                        title=row["title"],
                        start=row["time_window"]["start"],
                        end=row["time_window"]["end"],
                        participants=sorted(row.get("participants", [])),
                    )
                )
        calendar_events.sort(key=lambda item: (item.start, item.event_id))

        calendar = CalendarState(
            window_start=window_start.isoformat().replace("+00:00", "Z"),
            window_end=window_end.isoformat().replace("+00:00", "Z"),
            events=calendar_events,
        )

        notifications = self._build_notifications(
            family_id=family_id,
            task_rows=task_rows,
            stale_projection=bool(system_state.get("stale_projection", False)),
        )

        explanation_digest = []
        for row in explanations_raw:
            explanation_digest.append(
                XAIExplanationSummary(
                    explanation_id=row.explanation_id,
                    entity_type=row.entity_type.value,
                    entity_id=row.entity_id,
                    summary=row.explanation_text,
                    timestamp=row.timestamp.isoformat(),
                )
            )
        explanation_digest.sort(key=lambda item: (item.timestamp, item.explanation_id), reverse=True)

        health = SystemHealthSnapshot(
            status="degraded" if bool(system_state.get("stale_projection", False)) else "healthy",
            pending_actions=int(system_state.get("pending_actions", 0)),
            stale_projection=bool(system_state.get("stale_projection", False)),
            state_version=int(system_state.get("state_version", 0)),
            last_updated=str(system_state.get("last_projection_at", "")),
        )

        today = anchor.date().isoformat()
        today_events = [e for e in calendar.events if e.start.startswith(today)]
        open_tasks = [t for t in task_rows if t.status not in {"completed", "failed"}]

        today_overview = TodayOverview(
            date=today,
            open_task_count=len(open_tasks),
            scheduled_event_count=len(today_events),
            active_plan_count=len(active_plans),
            notification_count=len(notifications),
        )

        snapshot_version = self._snapshot_version(
            family=family_summary,
            active_plans=active_plans,
            task_board=task_board,
            calendar=calendar,
            notifications=notifications,
            explanation_digest=explanation_digest,
            system_health=health,
            today_overview=today_overview,
            source_watermark=source_watermark,
        )

        state = UIBootstrapState(
            snapshot_version=snapshot_version,
            source_watermark=source_watermark,
            family=family_summary,
            today_overview=today_overview,
            active_plans=active_plans,
            task_board=task_board,
            calendar=calendar,
            notifications=notifications,
            explanation_digest=explanation_digest,
            system_health=health,
        )

        with self._cache_lock:
            self._cache[cache_key] = state

        return state

    @staticmethod
    def _source_watermark(system_state: dict[str, object]) -> str:
        epoch = int(system_state.get("projection_epoch", 0))
        version = int(system_state.get("state_version", 0))
        last_projection_at = str(system_state.get("last_projection_at", ""))
        return f"{epoch}:{version}:{last_projection_at}"

    @staticmethod
    def _snapshot_version(**payload: object) -> int:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=_json_default)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return int(digest[:12], 16)

    @staticmethod
    def _build_notifications(
        *,
        family_id: str,
        task_rows: list[TaskSummary],
        stale_projection: bool,
    ) -> list[Notification]:
        notifications: list[Notification] = []

        failed_tasks = [row for row in task_rows if row.status == "failed"]
        failed_tasks.sort(key=lambda row: row.task_id)
        for row in failed_tasks:
            notifications.append(
                Notification(
                    notification_id=f"notif:task_failed:{row.task_id}",
                    title="Task needs attention",
                    message=f"{row.title} is marked as failed.",
                    level="warning",
                    related_entity=row.task_id,
                )
            )

        if stale_projection:
            notifications.append(
                Notification(
                    notification_id=f"notif:stale:{family_id}",
                    title="View refresh pending",
                    message="A refreshed snapshot is pending.",
                    level="info",
                    related_entity=family_id,
                )
            )

        notifications.sort(key=lambda row: row.notification_id)
        return notifications


def _parse_iso(value: str) -> datetime:
    fallback = "1970-01-01T00:00:00+00:00"
    raw = (value or fallback).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
