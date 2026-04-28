from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from threading import RLock
from typing import Any
from urllib import parse as urlparse
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError

from archive.apps.assistant_core.meal_planner import RECIPES, default_inventory, default_recipe_history
from archive.apps.api.hpal.command_gateway import HpalCommandGateway
from archive.apps.api.integration_core.models.household_state import HouseholdState
from archive.apps.api.integration_core.orchestrator import create_orchestrator
from archive.apps.api.integration_core.credentials import OAuthCredential
from archive.apps.api.integration_core.google_oauth_config import GoogleOAuthClientConfig, refresh_access_token
from archive.apps.api.product_surface.contracts import (
    CalendarEventSummary,
    CalendarState,
    FamilySummary,
    Notification,
    PantryIngredientRequirement,
    PantryInventoryItem,
    PantryRecipeSuggestion,
    PantryState,
    PlanSummary,
    SystemHealthSnapshot,
    TaskBoardState,
    TaskSummary,
    TodayOverview,
    UIBootstrapState,
    XAIExplanationSummary,
)
from archive.apps.api.services.calendar_service import get_events_by_household
from archive.apps.api.services.event_log_service import get_event_logs
from archive.apps.api.xai.store import ExplanationStore


_INTEGRATION_PROVIDER_NAME = "google_calendar"
_RECIPE_URL_CACHE_LOCK = RLock()
_RECIPE_URL_CACHE: dict[str, str] = {}
_RECIPE_URL_TIMEOUT_SECONDS = 2.5


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

    def get_state(
        self,
        *,
        family_id: str,
        user_id: str | None = None,
        credential_store: Any | None = None,
        http_client: Any = None,
    ) -> UIBootstrapState:
        if not family_id or not family_id.strip():
            raise ValueError("family_id is required")

        family = self._gateway.get_family_state(family_id=family_id)
        plans_raw = self._gateway.get_plans_by_family(family_id=family_id)
        tasks_raw = self._gateway.get_tasks_by_family(family_id=family_id)
        events_raw = self._gateway.get_calendar_view(family_id=family_id)
        runtime_graph = self._safe_load_runtime_graph(family_id=family_id)
        runtime_task_rows = runtime_graph.get("tasks", []) if isinstance(runtime_graph.get("tasks"), list) else []
        runtime_event_rows = runtime_graph.get("calendar_events", []) if isinstance(runtime_graph.get("calendar_events"), list) else []
        merged_task_rows = _merge_task_sources(gateway_rows=tasks_raw, runtime_rows=runtime_task_rows)
        calendar_db_rows = self._safe_load_calendar_db_events(family_id=family_id)
        integration_rows = self._safe_load_integration_events(
            user_id=user_id,
            credential_store=credential_store,
            http_client=http_client,
        )
        normalized_events = _merge_calendar_sources(
            family_id=family_id,
            gateway_rows=events_raw,
            db_rows=calendar_db_rows,
            runtime_rows=runtime_event_rows,
            integration_rows=integration_rows,
        )
        explanations_raw = self._xai_store.get_recent(family_id=family_id, limit=20)

        system_state = dict(family.system_state_summary)
        source_watermark = self._source_watermark(system_state)
        task_watermark = _task_row_watermark(merged_task_rows)
        calendar_watermark = _calendar_event_watermark(normalized_events)
        anchor = _parse_iso(str(system_state.get("last_projection_at", "")))
        member_names = sorted(person.name for person in family.members)
        member_count = max(1, len(member_names))
        pantry = _build_pantry_state(runtime_graph=runtime_graph, anchor=anchor, member_count=member_count)
        pantry_watermark = _pantry_state_watermark(pantry)
        email_summary_rows = self._safe_load_recent_email_summaries(family_id=family_id)
        email_summary_watermark = _email_summary_watermark(email_summary_rows)
        cache_key = (
            f"{family_id}:{source_watermark}:{task_watermark}:{calendar_watermark}:"
            f"{pantry_watermark}:{email_summary_watermark}"
        )

        with self._cache_lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

        family_summary = FamilySummary(
            family_id=family.family_id,
            member_count=member_count,
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
            for row in merged_task_rows
        ]
        task_rows.sort(key=lambda item: (item.status, item.priority, item.task_id))

        task_board = TaskBoardState(
            pending=[t for t in task_rows if t.status == "pending"],
            in_progress=[t for t in task_rows if t.status == "in_progress"],
            completed=[t for t in task_rows if t.status == "completed"],
            failed=[t for t in task_rows if t.status == "failed"],
        )

        window_start = (anchor - timedelta(days=1)).replace(microsecond=0)
        window_end = (anchor + timedelta(days=30)).replace(microsecond=0)

        calendar_events = []
        for row in normalized_events:
            start = _parse_iso(row["start"])
            if window_start <= start <= window_end:
                calendar_events.append(
                    CalendarEventSummary(
                        event_id=row["event_id"],
                        title=row["title"],
                        start=row["start"],
                        end=row["end"],
                        participants=row["participants"],
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
            email_summary_rows=email_summary_rows,
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
            pantry=pantry,
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
            pantry=pantry,
            notifications=notifications,
            explanation_digest=explanation_digest,
            system_health=health,
        )

        with self._cache_lock:
            self._cache[cache_key] = state

        return state

    def get_email_detail(self, *, family_id: str, email_id: str) -> dict[str, Any]:
        normalized_family_id = (family_id or "").strip()
        normalized_email_id = (email_id or "").strip()
        if not normalized_family_id:
            raise ValueError("family_id is required")
        if not normalized_email_id:
            raise ValueError("email_id is required")

        parsed_payload = self._safe_find_email_event_payload(
            family_id=normalized_family_id,
            email_id=normalized_email_id,
            event_type="email_parsed",
        )
        received_payload = self._safe_find_email_event_payload(
            family_id=normalized_family_id,
            email_id=normalized_email_id,
            event_type="email_received",
        )

        if parsed_payload is None and received_payload is None:
            raise LookupError("email_not_found")

        parsed_fields = parsed_payload.get("fields", {}) if isinstance(parsed_payload, dict) else {}
        received_fields = received_payload.get("fields", {}) if isinstance(received_payload, dict) else {}

        def _pick(*values: object, default: str = "") -> str:
            for value in values:
                text = str(value or "").strip()
                if text:
                    return text
            return default

        def _pick_optional(*values: object) -> str | None:
            for value in values:
                text = str(value or "").strip()
                if text:
                    return text
            return None

        def _pick_float(*values: object, default: float = 0.0) -> float:
            for value in values:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
            return default

        def _dict_rows(value: object) -> list[dict[str, Any]]:
            if not isinstance(value, list):
                return []
            return [row for row in value if isinstance(row, dict)]

        action_items = _dict_rows(parsed_fields.get("action_items") or received_fields.get("action_items"))
        calendar_candidates = _dict_rows(
            parsed_fields.get("calendar_candidates") or received_fields.get("calendar_candidates")
        )
        informational_items = _dict_rows(
            parsed_fields.get("informational_items") or received_fields.get("informational_items")
        )

        summary = _pick(parsed_fields.get("summary"), received_fields.get("summary"), default="Email analysis is ready.")
        body = _pick(received_fields.get("body"), parsed_fields.get("body"), default="")
        if len(body) > 320:
            body_excerpt = f"{body[:317].rstrip()}..."
        else:
            body_excerpt = body

        triage_decision = _pick(parsed_fields.get("triage_decision"), received_fields.get("triage_decision"), default="")
        if not triage_decision:
            triage_decision = "task" if action_items or calendar_candidates else "informational"

        is_junk = bool(parsed_fields.get("is_junk") or received_fields.get("is_junk") or triage_decision == "junk")
        if is_junk:
            triage_decision = "junk"

        return {
            "email_id": normalized_email_id,
            "subject": _pick(parsed_fields.get("subject"), received_fields.get("subject"), default="Email update"),
            "sender": _pick(parsed_fields.get("sender"), received_fields.get("sender"), default="unknown"),
            "recipient": _pick(parsed_fields.get("recipient"), received_fields.get("recipient"), default=""),
            "provider": _pick(parsed_fields.get("provider"), received_fields.get("provider"), default="generic"),
            "received_at": _pick(parsed_fields.get("received_at"), received_fields.get("received_at"), default=""),
            "summary": summary,
            "importance_score": _pick_float(parsed_fields.get("importance_score"), received_fields.get("importance_score")),
            "importance_bucket": _pick(
                parsed_fields.get("importance_bucket"),
                received_fields.get("importance_bucket"),
                default="medium",
            ).lower(),
            "junk_score": _pick_float(parsed_fields.get("junk_score"), received_fields.get("junk_score")),
            "triage_decision": triage_decision,
            "is_junk": is_junk,
            "processing_status": _pick_optional(
                parsed_fields.get("processing_status"),
                received_fields.get("processing_status"),
            ),
            "task_id": _pick_optional(parsed_fields.get("task_id"), received_fields.get("task_id")),
            "task_title": _pick_optional(parsed_fields.get("task_title"), received_fields.get("task_title")),
            "priority": _pick_optional(parsed_fields.get("priority"), received_fields.get("priority")),
            "calendar_event_id": _pick_optional(
                parsed_fields.get("calendar_event_id"),
                received_fields.get("calendar_event_id"),
            ),
            "action_items": action_items,
            "calendar_candidates": calendar_candidates,
            "informational_items": informational_items,
            "body_excerpt": body_excerpt,
            "body": body,
            "parsed_event_id": str(parsed_payload.get("event_id") or "") if isinstance(parsed_payload, dict) else "",
            "received_event_id": str(received_payload.get("event_id") or "") if isinstance(received_payload, dict) else "",
        }

    @staticmethod
    def _safe_find_email_event_payload(
        *,
        family_id: str,
        email_id: str,
        event_type: str,
        limit: int = 250,
    ) -> dict[str, Any] | None:
        try:
            rows = get_event_logs(family_id, event_type=event_type, limit=limit)
        except Exception:
            return None

        needle = email_id.strip().lower()
        for row in rows:
            payload = getattr(row, "payload", None)
            if not isinstance(payload, dict):
                continue

            parsed_fields = payload.get("parsed_fields")
            fields = parsed_fields if isinstance(parsed_fields, dict) else payload
            if not isinstance(fields, dict):
                continue

            candidate_email_id = str(payload.get("email_id") or fields.get("email_id") or "").strip().lower()
            if candidate_email_id != needle:
                continue

            return {
                "event_id": str(getattr(row, "id", "") or ""),
                "fields": fields,
            }

        return None

    @staticmethod
    def _safe_load_calendar_db_events(*, family_id: str) -> list[dict[str, Any]]:
        try:
            return get_events_by_household(family_id, include_past=False)
        except Exception:
            return []

    @staticmethod
    def _safe_load_recent_email_summaries(
        *,
        family_id: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        scan_limit = max(limit * 20, 100)
        rows: list[Any] = []
        for event_type in ("email_parsed", "email_received"):
            try:
                candidate_rows = get_event_logs(family_id, event_type=event_type, limit=scan_limit)
            except Exception:
                candidate_rows = []

            if candidate_rows:
                rows.extend(candidate_rows)

        if not rows:
            return []

        summaries: list[dict[str, Any]] = []
        seen_email_ids: set[str] = set()
        for row in rows:
            payload = getattr(row, "payload", None)
            if not isinstance(payload, dict):
                continue

            parsed_fields = payload.get("parsed_fields")
            fields = parsed_fields if isinstance(parsed_fields, dict) else payload
            if not isinstance(fields, dict):
                continue

            triage_decision = str(fields.get("triage_decision") or "").strip().lower()
            if bool(fields.get("is_junk")) or triage_decision == "junk":
                continue

            action_items = fields.get("action_items")
            if not isinstance(action_items, list):
                action_items = []

            calendar_candidates = fields.get("calendar_candidates")
            if not isinstance(calendar_candidates, list):
                calendar_candidates = []

            email_id = str(
                payload.get("email_id")
                or fields.get("email_id")
                or getattr(row, "id", "")
            ).strip()
            if not email_id:
                continue
            normalized_email_id = email_id.lower()
            if normalized_email_id in seen_email_ids:
                continue
            seen_email_ids.add(normalized_email_id)

            summary = str(fields.get("summary") or "").strip()
            if not summary:
                if action_items or calendar_candidates:
                    summary = "New email analysis is ready."
                else:
                    summary = "New email parsed."

            calendar_event_id = str(fields.get("calendar_event_id") or "").strip()

            summaries.append(
                {
                    "email_id": email_id,
                    "subject": str(fields.get("subject") or "Email update"),
                    "summary": summary,
                    "importance_bucket": str(fields.get("importance_bucket") or "medium").lower(),
                    "action_item_count": len(action_items),
                    "calendar_candidate_count": len(calendar_candidates),
                    "calendar_event_id": calendar_event_id,
                }
            )

            if len(summaries) >= limit:
                break

        return summaries

    @staticmethod
    def _safe_load_integration_events(
        *,
        user_id: str | None,
        credential_store: Any | None,
        http_client: Any = None,
    ) -> list[dict[str, Any]]:
        normalized_user_id = (user_id or "").strip()
        if not normalized_user_id or credential_store is None:
            return []

        get_credentials = getattr(credential_store, "get_credentials", None)
        if not callable(get_credentials):
            return []

        try:
            credentials = get_credentials(
                user_id=normalized_user_id,
                provider_name=_INTEGRATION_PROVIDER_NAME,
            )
        except Exception:
            return []

        if credentials is None:
            return []

        if _is_token_expiring(getattr(credentials, "expires_at", None)):
            refresh_token = getattr(credentials, "refresh_token", None)
            save_credentials = getattr(credential_store, "save_credentials", None)

            if isinstance(refresh_token, str) and refresh_token.strip():
                try:
                    resolved_http_client = _resolve_google_http_client(http_client)
                    if resolved_http_client is not None:
                        refreshed = refresh_access_token(
                            refresh_token=refresh_token,
                            config=GoogleOAuthClientConfig.from_env(),
                            http_client=resolved_http_client,
                        )

                        scopes = tuple(getattr(credentials, "scopes", ()) or ())
                        refreshed_credentials = OAuthCredential(
                            user_id=str(getattr(credentials, "user_id", normalized_user_id) or normalized_user_id),
                            provider_name=str(getattr(credentials, "provider_name", _INTEGRATION_PROVIDER_NAME) or _INTEGRATION_PROVIDER_NAME),
                            access_token=refreshed.access_token,
                            refresh_token=refreshed.refresh_token or refresh_token,
                            scopes=scopes,
                            expires_at=(
                                datetime.now(UTC) + timedelta(seconds=int(refreshed.expires_in or 0))
                                if refreshed.expires_in is not None
                                else getattr(credentials, "expires_at", None)
                            ),
                        )

                        if callable(save_credentials):
                            try:
                                save_credentials(refreshed_credentials)
                            except Exception:
                                pass
                except Exception:
                    # Keep bootstrap resilient; integration events can still be sourced from local projections.
                    pass

        try:
            orchestrator = create_orchestrator(
                credential_store=credential_store,
                http_client=http_client,
                max_results=200,
            )
            state_result = orchestrator.build_household_state(normalized_user_id)
        except Exception:
            return []

        state: HouseholdState
        if isinstance(state_result, tuple):
            state = state_result[0]
        else:
            state = state_result

        rows: list[dict[str, Any]] = []
        for event in state.calendar_events:
            rows.append(
                {
                    "event_id": event.event_id,
                    "title": event.title,
                    "start": event.start,
                    "end": event.end,
                    "participants": [normalized_user_id],
                }
            )
        return rows

    def _safe_load_runtime_graph(self, *, family_id: str) -> dict[str, Any]:
        adapter = getattr(self._gateway, "adapter", None)
        loader = getattr(adapter, "load_graph", None)
        if not callable(loader):
            return {}

        try:
            graph = loader(family_id)
        except Exception:
            return {}

        return graph if isinstance(graph, dict) else {}

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
        email_summary_rows: list[dict[str, Any]],
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

        for row in email_summary_rows:
            email_id = str(row.get("email_id") or "").strip()
            if not email_id:
                continue

            bucket = str(row.get("importance_bucket") or "medium").lower()
            level = "critical" if bucket == "critical" else "warning" if bucket == "high" else "info"

            action_item_count = int(row.get("action_item_count", 0) or 0)
            calendar_candidate_count = int(row.get("calendar_candidate_count", 0) or 0)
            calendar_event_id = str(row.get("calendar_event_id") or "").strip()
            follow_up_parts: list[str] = []
            if action_item_count > 0:
                suffix = "" if action_item_count == 1 else "s"
                follow_up_parts.append(f"{action_item_count} action item{suffix}")
            if calendar_candidate_count > 0:
                suffix = "" if calendar_candidate_count == 1 else "s"
                follow_up_parts.append(f"{calendar_candidate_count} calendar candidate{suffix}")
            if calendar_event_id:
                follow_up_parts.append("already added to calendar")

            summary = str(row.get("summary") or "").strip()
            subject = str(row.get("subject") or "Email update").strip() or "Email update"
            if follow_up_parts:
                summary_suffix = f" ({', '.join(follow_up_parts)})."
            else:
                summary_suffix = ""
            message = f"{summary}{summary_suffix}".strip() or "New email parsed."

            notifications.append(
                Notification(
                    notification_id=f"notif:email_summary:{email_id}",
                    title=f"Email: {subject}",
                    message=message,
                    level=level,
                    related_entity=email_id,
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


def _is_token_expiring(expires_at: datetime | None, *, skew_seconds: int = 60) -> bool:
    if expires_at is None:
        return False

    timestamp = expires_at
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)

    return timestamp <= (datetime.now(UTC) + timedelta(seconds=skew_seconds))


def _resolve_google_http_client(http_client: Any) -> Any | None:
    if http_client is not None:
        return http_client

    try:
        import httpx  # noqa: PLC0415

        return httpx
    except Exception:
        try:
            import requests  # noqa: PLC0415

            return requests
        except Exception:
            return None


def _normalize_gateway_task(row: dict[str, Any]) -> dict[str, Any] | None:
    task_id = row.get("task_id")
    title = row.get("title")

    if not isinstance(task_id, str) or not task_id:
        return None
    if not isinstance(title, str):
        return None

    status = str(row.get("status") or "pending")
    if status not in {"pending", "in_progress", "completed", "failed"}:
        status = "pending"

    return {
        "task_id": task_id,
        "title": title,
        "plan_id": str(row.get("plan_id") or "runtime"),
        "assigned_to": str(row.get("assigned_to") or "household"),
        "status": status,
        "priority": str(row.get("priority") or "medium"),
        "due_time": row.get("due_time"),
    }


def _normalize_runtime_task(row: dict[str, Any]) -> dict[str, Any] | None:
    task_id = row.get("task_id") or row.get("id")
    title = row.get("title")

    if not isinstance(task_id, str) or not task_id:
        return None
    if not isinstance(title, str):
        return None

    status = str(row.get("status") or "pending")
    if status not in {"pending", "in_progress", "completed", "failed"}:
        status = "pending"

    return {
        "task_id": task_id,
        "title": title,
        "plan_id": str(row.get("plan_id") or "runtime"),
        "assigned_to": str(row.get("assigned_to") or "household"),
        "status": status,
        "priority": str(row.get("priority") or "medium"),
        "due_time": row.get("due_time"),
    }


def _merge_task_sources(*, gateway_rows: list[dict[str, Any]], runtime_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    for row in gateway_rows:
        normalized = _normalize_gateway_task(row)
        if normalized is None:
            continue
        merged[normalized["task_id"]] = normalized

    for row in runtime_rows:
        normalized = _normalize_runtime_task(row)
        if normalized is None:
            continue
        merged[normalized["task_id"]] = normalized

    return sorted(merged.values(), key=lambda item: (item["status"], item["priority"], item["task_id"]))


def _task_row_watermark(rows: list[dict[str, Any]]) -> str:
    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _normalize_gateway_event(row: dict[str, Any]) -> dict[str, Any] | None:
    time_window = row.get("time_window") if isinstance(row.get("time_window"), dict) else {}
    start = time_window.get("start")
    end = time_window.get("end")
    event_id = row.get("event_id")
    title = row.get("title")

    if not isinstance(start, str) or not isinstance(end, str):
        return None
    if not isinstance(event_id, str) or not event_id:
        return None
    if not isinstance(title, str):
        return None

    participants_raw = row.get("participants")
    participants = (
        sorted(str(p) for p in participants_raw if isinstance(p, str) and p)
        if isinstance(participants_raw, list)
        else []
    )

    return {
        "event_id": event_id,
        "title": title,
        "start": start,
        "end": end,
        "participants": participants,
    }


def _normalize_db_event(row: dict[str, Any]) -> dict[str, Any] | None:
    start = row.get("start_time")
    end = row.get("end_time")
    event_id = row.get("event_id")
    title = row.get("title")

    if not isinstance(start, str) or not isinstance(end, str):
        return None
    if not isinstance(event_id, str) or not event_id:
        return None
    if not isinstance(title, str):
        return None

    metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
    participants_raw = metadata.get("participants") if isinstance(metadata.get("participants"), list) else []
    participants = [str(p) for p in participants_raw if isinstance(p, str) and p]

    user_id = metadata.get("user_id")
    if not participants and isinstance(user_id, str) and user_id:
        participants = [user_id]

    return {
        "event_id": event_id,
        "title": title,
        "start": start,
        "end": end,
        "participants": sorted(participants),
    }


def _normalize_runtime_event(row: dict[str, Any]) -> dict[str, Any] | None:
    start = row.get("start")
    end = row.get("end")
    event_id = row.get("event_id")
    title = row.get("title")

    if not isinstance(start, str) or not isinstance(end, str):
        return None
    if not isinstance(event_id, str) or not event_id:
        return None
    if not isinstance(title, str):
        return None

    participants_raw = row.get("participants")
    participants = (
        sorted(str(item) for item in participants_raw if isinstance(item, str) and item)
        if isinstance(participants_raw, list)
        else []
    )

    return {
        "event_id": event_id,
        "title": title,
        "start": start,
        "end": end,
        "participants": participants,
    }


def _normalize_integration_event(row: dict[str, Any]) -> dict[str, Any] | None:
    start = row.get("start")
    end = row.get("end")
    event_id = row.get("event_id")
    title = row.get("title")

    if not isinstance(start, str) or not isinstance(end, str):
        return None
    if not isinstance(event_id, str) or not event_id:
        return None
    if not isinstance(title, str):
        return None

    participants_raw = row.get("participants")
    participants = (
        sorted(str(item) for item in participants_raw if isinstance(item, str) and item)
        if isinstance(participants_raw, list)
        else []
    )

    return {
        "event_id": event_id,
        "title": title,
        "start": start,
        "end": end,
        "participants": participants,
    }


def _merge_calendar_sources(
    *,
    family_id: str,
    gateway_rows: list[dict[str, Any]],
    db_rows: list[dict[str, Any]],
    runtime_rows: list[dict[str, Any]],
    integration_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    for row in gateway_rows:
        normalized = _normalize_gateway_event(row)
        if normalized is None:
            continue
        merged[normalized["event_id"]] = normalized

    for row in db_rows:
        household_id = row.get("household_id")
        if isinstance(household_id, str) and household_id and household_id != family_id:
            continue

        normalized = _normalize_db_event(row)
        if normalized is None:
            continue
        merged[normalized["event_id"]] = normalized

    for row in runtime_rows:
        normalized = _normalize_runtime_event(row)
        if normalized is None:
            continue
        merged[normalized["event_id"]] = normalized

    for row in integration_rows:
        normalized = _normalize_integration_event(row)
        if normalized is None:
            continue
        merged[normalized["event_id"]] = normalized

    return sorted(merged.values(), key=lambda item: (item["start"], item["event_id"]))


def _calendar_event_watermark(rows: list[dict[str, Any]]) -> str:
    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _email_summary_watermark(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "none"

    tokens: list[str] = []
    for row in rows:
        email_id = str(row.get("email_id") or "").strip().lower()
        subject = str(row.get("subject") or "").strip().lower()
        summary = str(row.get("summary") or "").strip().lower()
        importance_bucket = str(row.get("importance_bucket") or "").strip().lower()
        action_count = int(row.get("action_item_count", 0) or 0)
        calendar_count = int(row.get("calendar_candidate_count", 0) or 0)
        calendar_event_id = str(row.get("calendar_event_id") or "").strip().lower()
        tokens.append(
            f"{email_id}:{subject}:{importance_bucket}:{action_count}:{calendar_count}:"
            f"{calendar_event_id}:{summary}"
        )

    canonical = "|".join(sorted(tokens))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


_DEFAULT_INVENTORY_UNIT = "count"
_ALLOWED_INVENTORY_UNITS = {
    "count",
    "can",
    "pack",
    "oz",
    "lb",
    "g",
    "kg",
    "ml",
    "fl_oz",
    "l",
}
_INVENTORY_UNIT_ALIASES = {
    "": _DEFAULT_INVENTORY_UNIT,
    "ea": "count",
    "each": "count",
    "piece": "count",
    "pieces": "count",
    "pcs": "count",
    "cans": "can",
    "packs": "pack",
    "floz": "fl_oz",
    "fl oz": "fl_oz",
    "fluid ounce": "fl_oz",
    "fluid ounces": "fl_oz",
    "liter": "l",
    "liters": "l",
    "litre": "l",
    "litres": "l",
}


def _normalize_inventory(raw_inventory: object) -> dict[str, dict[str, float | str]]:
    if not isinstance(raw_inventory, dict):
        return {}

    normalized: dict[str, dict[str, float | str]] = {}
    for name, raw_entry in raw_inventory.items():
        item_name = str(name).strip().lower()
        if not item_name:
            continue

        quantity_value: float | None
        unit_value: str
        if isinstance(raw_entry, dict):
            quantity_value = _coerce_quantity(raw_entry.get("quantity"))
            unit_value = _normalize_inventory_unit(raw_entry.get("unit"))
        else:
            quantity_value = _coerce_quantity(raw_entry)
            unit_value = _DEFAULT_INVENTORY_UNIT

        if quantity_value is None:
            continue

        normalized[item_name] = {
            "quantity": max(0.0, float(quantity_value)),
            "unit": unit_value,
        }
    return normalized


def _coerce_quantity(raw_value: object) -> float | None:
    try:
        quantity = float(raw_value)
    except (TypeError, ValueError):
        return None

    if quantity != quantity:  # NaN
        return None
    if quantity == float("inf") or quantity == float("-inf"):
        return None
    return quantity


def _normalize_inventory_unit(raw_value: object) -> str:
    normalized = str(raw_value or "").strip().lower().replace("-", "_")
    normalized = " ".join(normalized.split())
    normalized = _INVENTORY_UNIT_ALIASES.get(normalized, normalized.replace(" ", "_"))

    if not normalized:
        return _DEFAULT_INVENTORY_UNIT

    if normalized not in _ALLOWED_INVENTORY_UNITS:
        return _DEFAULT_INVENTORY_UNIT

    return normalized


def _inventory_quantity(inventory: dict[str, dict[str, float | str]], ingredient: str) -> float:
    row = inventory.get(ingredient)
    if not isinstance(row, dict):
        return 0.0

    quantity = _coerce_quantity(row.get("quantity"))
    if quantity is None:
        return 0.0
    return max(0.0, quantity)


def _inventory_unit(inventory: dict[str, dict[str, float | str]], ingredient: str) -> str:
    row = inventory.get(ingredient)
    if not isinstance(row, dict):
        return _DEFAULT_INVENTORY_UNIT
    return _normalize_inventory_unit(row.get("unit"))


def _display_quantity(quantity: float) -> float:
    rounded = round(float(quantity), 3)
    if abs(rounded - round(rounded)) < 0.001:
        return float(int(round(rounded)))
    return rounded


def _normalize_meal_history(raw_history: object) -> list[dict[str, str]]:
    if not isinstance(raw_history, list):
        return []

    normalized: list[dict[str, str]] = []
    for row in raw_history:
        if not isinstance(row, dict):
            continue
        recipe_name = str(row.get("recipe_name") or "").strip()
        served_on = str(row.get("served_on") or "").strip()
        if not recipe_name:
            continue
        normalized.append(
            {
                "recipe_name": recipe_name,
                "served_on": served_on,
            }
        )
    return normalized


def _inventory_status(quantity: float, unit: str) -> str:
    if quantity <= 0:
        return "out_of_stock"

    normalized_unit = _normalize_inventory_unit(unit)
    low_threshold = 1.0
    if normalized_unit in {"oz", "fl_oz"}:
        low_threshold = 8.0
    elif normalized_unit in {"lb", "kg", "l"}:
        low_threshold = 0.5
    elif normalized_unit in {"g", "ml"}:
        low_threshold = 250.0

    if quantity <= low_threshold:
        return "low"
    return "in_stock"


def _recent_recipe_names_for_anchor(
    recipe_history: list[dict[str, str]],
    *,
    anchor: datetime,
    repeat_window_days: int,
) -> set[str]:
    cutoff = anchor.date() - timedelta(days=repeat_window_days)
    recent: set[str] = set()
    for row in recipe_history:
        recipe_name = str(row.get("recipe_name") or "").strip()
        if not recipe_name:
            continue

        served_on = str(row.get("served_on") or "").strip()
        if not served_on:
            continue

        try:
            served_date = datetime.fromisoformat(served_on.replace("Z", "+00:00")).date()
        except ValueError:
            continue

        if served_date >= cutoff:
            recent.add(recipe_name)
    return recent


def _recipe_score(
    *,
    recipe_name: str,
    ingredients: tuple[str, ...],
    nutrition_balance: tuple[str, ...],
    inventory: dict[str, dict[str, float | str]],
    recent_recipe_names: set[str],
    planned_recipe_counts: dict[str, int],
) -> tuple[int, int, int, int, str]:
    missing_count = sum(1 for ingredient in ingredients if _inventory_quantity(inventory, ingredient) <= 0)
    in_stock_count = sum(1 for ingredient in ingredients if _inventory_quantity(inventory, ingredient) > 0)
    repeat_penalty = 1 if recipe_name in recent_recipe_names else 0
    weekly_repeat_penalty = int(planned_recipe_counts.get(recipe_name, 0))
    nutrition_score = len(nutrition_balance)

    return (
        -missing_count,
        in_stock_count + nutrition_score,
        -repeat_penalty,
        -weekly_repeat_penalty,
        recipe_name,
    )


def _select_recipe_candidate(
    *,
    ranked_recipes: list[Any],
    planned_recipe_counts: dict[str, int],
    previous_recipe_name: str | None,
) -> Any:
    if not ranked_recipes:
        raise ValueError("ranked_recipes is required")

    # Strong variety pass: avoid immediate repeats and prefer recipes not yet
    # used this week.
    for candidate in ranked_recipes:
        candidate_name = str(getattr(candidate, "name", "")).strip()
        if not candidate_name:
            continue
        if previous_recipe_name and candidate_name == previous_recipe_name:
            continue
        if int(planned_recipe_counts.get(candidate_name, 0)) > 0:
            continue
        return candidate

    # Secondary pass: still avoid immediate repeat if possible.
    for candidate in ranked_recipes:
        candidate_name = str(getattr(candidate, "name", "")).strip()
        if not candidate_name:
            continue
        if previous_recipe_name and candidate_name == previous_recipe_name:
            continue
        return candidate

    # Tertiary cap: avoid planning any single recipe more than twice unless
    # all options are exhausted.
    for candidate in ranked_recipes:
        candidate_name = str(getattr(candidate, "name", "")).strip()
        if not candidate_name:
            continue
        if int(planned_recipe_counts.get(candidate_name, 0)) >= 2:
            continue
        return candidate

    return ranked_recipes[0]


def _recipe_link_healthcheck_enabled() -> bool:
    configured = str(os.getenv("RECIPE_URL_HEALTHCHECK_ENABLED", "1")).strip().lower()
    if configured in {"0", "false", "off", "no"}:
        return False
    # Skip network probes during pytest runs to keep tests deterministic and fast.
    if "PYTEST_CURRENT_TEST" in os.environ:
        return False
    return True


def _recipe_search_url(recipe_name: str, source_name: str) -> str:
    query = " ".join(part for part in [recipe_name.strip(), source_name.strip(), "recipe"] if part)
    return f"https://www.google.com/search?q={urlparse.quote_plus(query)}"


def _is_recipe_url_reachable(url: str) -> bool:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) FamilyOrchestrationBot/1.0",
    }
    request = urlrequest.Request(url, headers=headers, method="HEAD")
    try:
        with urlrequest.urlopen(request, timeout=_RECIPE_URL_TIMEOUT_SECONDS) as response:
            status = int(getattr(response, "status", 200))
            return 200 <= status < 400
    except HTTPError as exc:
        if int(exc.code) in {404, 410}:
            return False
        if int(exc.code) == 405:
            get_request = urlrequest.Request(url, headers=headers, method="GET")
            try:
                with urlrequest.urlopen(get_request, timeout=_RECIPE_URL_TIMEOUT_SECONDS) as response:
                    status = int(getattr(response, "status", 200))
                    return 200 <= status < 400
            except HTTPError as get_exc:
                return int(get_exc.code) not in {404, 410}
            except (TimeoutError, URLError, ValueError):
                return True
        return True
    except (TimeoutError, URLError, ValueError):
        return True


def _resolve_recipe_url(*, recipe_name: str, source_name: str, candidate_url: str) -> str:
    fallback = _recipe_search_url(recipe_name, source_name)
    normalized_url = str(candidate_url or "").strip()
    if normalized_url == "":
        return fallback

    parsed = urlparse.urlparse(normalized_url)
    if parsed.scheme not in {"http", "https"} or parsed.netloc.strip() == "":
        return fallback
    if parsed.path.strip() in {"", "/"}:
        return fallback

    if not _recipe_link_healthcheck_enabled():
        return normalized_url

    cache_key = f"{recipe_name.strip().lower()}|{source_name.strip().lower()}|{normalized_url}"
    with _RECIPE_URL_CACHE_LOCK:
        cached = _RECIPE_URL_CACHE.get(cache_key)
        if cached is not None:
            return cached

    resolved = normalized_url if _is_recipe_url_reachable(normalized_url) else fallback

    with _RECIPE_URL_CACHE_LOCK:
        _RECIPE_URL_CACHE[cache_key] = resolved

    return resolved


def _build_weekly_recipe_suggestions(
    *,
    inventory: dict[str, dict[str, float | str]],
    recipe_history: list[dict[str, str]],
    anchor: datetime,
    member_count: int,
    days: int = 7,
    repeat_window_days: int = 10,
) -> list[PantryRecipeSuggestion]:
    if days <= 0:
        return []

    working_inventory = dict(inventory)
    recent_recipe_names = _recent_recipe_names_for_anchor(
        recipe_history,
        anchor=anchor,
        repeat_window_days=repeat_window_days,
    )
    planned_recipe_counts: dict[str, int] = {}
    suggestions: list[PantryRecipeSuggestion] = []
    target_servings = max(1, min(8, int(member_count)))

    for offset in range(days):
        candidate_date = (anchor + timedelta(days=offset)).date()
        ranked_recipes = sorted(
            RECIPES,
            key=lambda recipe: _recipe_score(
                recipe_name=recipe.name,
                ingredients=recipe.ingredients,
                nutrition_balance=recipe.nutrition_balance,
                inventory=working_inventory,
                recent_recipe_names=recent_recipe_names,
                planned_recipe_counts=planned_recipe_counts,
            ),
            reverse=True,
        )
        previous_recipe_name = suggestions[-1].recipe_name if suggestions else None
        selected = _select_recipe_candidate(
            ranked_recipes=ranked_recipes,
            planned_recipe_counts=planned_recipe_counts,
            previous_recipe_name=previous_recipe_name,
        )

        ingredient_requirements: list[PantryIngredientRequirement] = []
        ingredients_used: list[str] = []
        missing_ingredients: list[str] = []
        fully_satisfied_count = 0

        for requirement in selected.ingredient_requirements:
            required_quantity = max(0.0, float(requirement.amount_per_serving)) * target_servings
            normalized_required_quantity = _display_quantity(required_quantity)
            ingredient_requirements.append(
                PantryIngredientRequirement(
                    item=requirement.item,
                    quantity=normalized_required_quantity,
                    unit=_normalize_inventory_unit(requirement.unit),
                )
            )

            available_quantity = _inventory_quantity(working_inventory, requirement.item)
            if available_quantity > 0:
                ingredients_used.append(requirement.item)
            if available_quantity >= required_quantity and required_quantity > 0:
                fully_satisfied_count += 1
            if available_quantity < required_quantity:
                missing_ingredients.append(requirement.item)

        ingredient_total = max(1, len(selected.ingredient_requirements))
        inventory_match_score = round((fully_satisfied_count / ingredient_total) * 100, 1)

        suggestions.append(
            PantryRecipeSuggestion(
                day=candidate_date.strftime("%A"),
                date=candidate_date.isoformat(),
                recipe_name=selected.name,
                meal_type=selected.meal_type,
                servings=target_servings,
                recipe_source=selected.source_name,
                recipe_url=_resolve_recipe_url(
                    recipe_name=selected.name,
                    source_name=selected.source_name,
                    candidate_url=selected.source_url,
                ),
                ingredient_requirements=ingredient_requirements,
                ingredients_used=ingredients_used,
                missing_ingredients=missing_ingredients,
                nutrition_balance=list(selected.nutrition_balance),
                inventory_match_score=inventory_match_score,
            )
        )

        for requirement in selected.ingredient_requirements:
            ingredient = requirement.item
            current_quantity = _inventory_quantity(working_inventory, ingredient)
            if current_quantity <= 0:
                continue

            required_quantity = max(0.0, float(requirement.amount_per_serving)) * target_servings
            working_inventory[ingredient] = {
                "quantity": max(0.0, current_quantity - required_quantity),
                "unit": _inventory_unit(working_inventory, ingredient),
            }

        recent_recipe_names.add(selected.name)
        planned_recipe_counts[selected.name] = int(planned_recipe_counts.get(selected.name, 0)) + 1

    return suggestions


def _build_pantry_state(*, runtime_graph: dict[str, Any], anchor: datetime, member_count: int) -> PantryState:
    inventory = _normalize_inventory(runtime_graph.get("inventory"))
    if not inventory:
        inventory = _normalize_inventory(runtime_graph.get("grocery_inventory"))
    if not inventory:
        inventory = _normalize_inventory(default_inventory())

    meal_history = _normalize_meal_history(runtime_graph.get("meal_history"))
    if not meal_history:
        meal_history = default_recipe_history()

    inventory_items = [
        PantryInventoryItem(
            name=name,
            quantity=_display_quantity(_inventory_quantity(inventory, name)),
            unit=_inventory_unit(inventory, name),
            status=_inventory_status(
                _inventory_quantity(inventory, name),
                _inventory_unit(inventory, name),
            ),
        )
        for name in sorted(inventory)
    ]
    inventory_items.sort(
        key=lambda item: (
            0 if item.status == "out_of_stock" else 1 if item.status == "low" else 2,
            item.name,
        )
    )

    weekly_suggestions = _build_weekly_recipe_suggestions(
        inventory=inventory,
        recipe_history=meal_history,
        anchor=anchor,
        member_count=member_count,
    )
    grocery_recommendations = sorted(
        {
            ingredient
            for suggestion in weekly_suggestions
            for ingredient in suggestion.missing_ingredients
        }
    )

    low_stock_count = sum(1 for item in inventory_items if item.status in {"low", "out_of_stock"})

    return PantryState(
        low_stock_count=low_stock_count,
        inventory_items=inventory_items,
        weekly_recipe_suggestions=weekly_suggestions,
        grocery_recommendations=grocery_recommendations,
    )


def _pantry_state_watermark(state: PantryState) -> str:
    canonical = json.dumps(state.model_dump(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
