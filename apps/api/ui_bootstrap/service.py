"""
Unified UI Bootstrap Service
============================

Single deterministic read surface assembler for:
  - Frontend hydration
  - COL chat UI binding
  - Observability panels

Non-goals enforced:
  - no orchestration triggering
  - no command execution
  - no mutation of domain state
"""
from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

from apps.api.conversation_orchestration.store import (
    ConversationSessionStore,
    DEFAULT_SESSION_STORE,
)
from apps.api.hpal.command_gateway import HpalCommandGateway
from apps.api.ui_bootstrap.cache import DEFAULT_UI_BOOTSTRAP_CACHE, UIBootstrapCache
from apps.api.ui_bootstrap.models import (
    BootstrapMetadata,
    ConversationSessionView,
    EventView,
    FamilyView,
    PartialIntentView,
    PlanView,
    SystemStateView,
    TaskView,
    UIBootstrapResponse,
    XAIExplanationView,
)
from apps.api.ui_bootstrap.system_state_store import (
    DEFAULT_SYSTEM_STATE_STORE,
    SystemStateProjectionStore,
)
from apps.api.xai.store import ExplanationStore


class UIBootstrapService:
    """Composes all read projections into a stable UI response contract."""

    EVENT_WINDOW_DAYS = 30
    XAI_RECENT_LIMIT = 50

    def __init__(
        self,
        *,
        hpal_gateway: HpalCommandGateway | None = None,
        session_store: ConversationSessionStore | None = None,
        xai_store: ExplanationStore | None = None,
        system_state_store: SystemStateProjectionStore | None = None,
        cache: UIBootstrapCache | None = None,
    ) -> None:
        self._hpal_gateway = hpal_gateway or HpalCommandGateway()
        self._session_store = session_store or DEFAULT_SESSION_STORE
        self._xai_store = xai_store or ExplanationStore()
        self._system_state_store = system_state_store or DEFAULT_SYSTEM_STATE_STORE
        self._cache = cache or DEFAULT_UI_BOOTSTRAP_CACHE

    def get_bootstrap(self, *, family_id: str, include_debug: bool = False) -> dict[str, Any]:
        if not family_id or not family_id.strip():
            raise ValueError("family_id is required")

        now = datetime.now(tz=UTC)
        degraded_components: list[str] = []

        family_model, family_summary = self._safe_load_family(family_id=family_id, degraded=degraded_components)
        plans = self._safe_load_plans(family_id=family_id, degraded=degraded_components)
        tasks = self._safe_load_tasks(family_id=family_id, now=now, degraded=degraded_components)
        events = self._safe_load_events(family_id=family_id, now=now, degraded=degraded_components)
        sessions, pending_intents, session_version = self._safe_load_conversations(
            family_id=family_id,
            degraded=degraded_components,
        )
        xai_recent, xai_watermark = self._safe_load_xai(family_id=family_id, degraded=degraded_components)
        system_state, system_version = self._safe_load_system_state(
            family_id=family_id,
            family_summary=family_summary,
            now=now,
            degraded=degraded_components,
        )

        projection_version = _compute_projection_version(
            family_summary=family_summary,
            session_version=session_version,
            xai_watermark=xai_watermark,
            system_version=system_version,
        )

        cache_key = f"{family_id}:{projection_version}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            payload = cached.to_dict()
            if include_debug:
                payload["debug"] = {
                    "cache": {"hit": True, "key": cache_key},
                }
            return payload

        projection_epoch = str(family_summary.get("projection_epoch", "0"))
        source_watermark = _build_source_watermark(
            family_summary=family_summary,
            session_version=session_version,
            xai_watermark=xai_watermark,
            system_version=system_version,
        )

        last_projection_at = _safe_parse_dt(str(family_summary.get("last_projection_at", "")))
        staleness_ms = max(0, int((now - last_projection_at).total_seconds() * 1000))

        response = UIBootstrapResponse(
            family=family_model,
            plans=plans,
            tasks=tasks,
            events=events,
            conversation_sessions=sessions,
            pending_intents=pending_intents,
            system_state=system_state,
            xai_recent=xai_recent,
            metadata=BootstrapMetadata(
                projection_version=projection_version,
                projection_epoch=projection_epoch,
                source_watermark=source_watermark,
                generated_at=now,
                staleness_ms=staleness_ms,
                degraded_components=sorted(set(degraded_components)),
            ),
        )

        self._cache.set(cache_key, response)
        payload = response.to_dict()
        if include_debug:
            payload["debug"] = {
                "cache": {"hit": False, "key": cache_key},
                "counts": {
                    "plans": len(plans),
                    "tasks": len(tasks),
                    "events": len(events),
                    "conversation_sessions": len(sessions),
                    "pending_intents": len(pending_intents),
                    "xai_recent": len(xai_recent),
                },
            }
        return payload

    def _safe_load_family(self, *, family_id: str, degraded: list[str]) -> tuple[FamilyView, dict[str, Any]]:
        try:
            family = self._hpal_gateway.get_family_state(family_id=family_id)
            summary = dict(family.system_state_summary)
            return (
                FamilyView(
                    family_id=family.family_id,
                    shared_calendar_ref=family.shared_calendar_ref,
                    default_time_zone=family.default_time_zone,
                    member_count=len(family.members),
                    active_plan_ids=list(family.active_plans),
                ),
                summary,
            )
        except Exception:
            degraded.append("family_projection")
            return (
                FamilyView(
                    family_id=family_id,
                    shared_calendar_ref="primary",
                    default_time_zone="UTC",
                    member_count=0,
                    active_plan_ids=[],
                ),
                {},
            )

    def _safe_load_plans(self, *, family_id: str, degraded: list[str]) -> list[PlanView]:
        try:
            rows = self._hpal_gateway.get_plans_by_family(family_id=family_id)
            rows.sort(key=lambda r: str(r.get("plan_id", "")))
            return [
                PlanView(
                    plan_id=str(r.get("plan_id", "")),
                    family_id=str(r.get("family_id", family_id)),
                    title=str(r.get("title", "")),
                    status=str(r.get("status", "")),
                    linked_tasks=list(r.get("linked_tasks", [])),
                    revision=int(r.get("revision", 1)),
                    stability_state=str(r.get("stability_state", "stable")),
                    last_recomputed_at=r.get("last_recomputed_at"),
                )
                for r in rows
            ]
        except Exception:
            degraded.append("plan_projection")
            return []

    def _safe_load_tasks(self, *, family_id: str, now: datetime, degraded: list[str]) -> list[TaskView]:
        try:
            rows = self._hpal_gateway.get_tasks_by_family(family_id=family_id)
            filtered: list[dict[str, Any]] = []
            for row in rows:
                status = str(row.get("status", ""))
                if status not in {"pending", "in_progress"}:
                    continue

                due_time_raw = row.get("due_time")
                if due_time_raw:
                    due_time = _safe_parse_dt(str(due_time_raw))
                    if due_time < now - timedelta(days=1):
                        continue
                filtered.append(row)

            filtered.sort(key=lambda r: (str(r.get("due_time", "")), str(r.get("task_id", ""))))
            return [
                TaskView(
                    task_id=str(r.get("task_id", "")),
                    plan_id=str(r.get("plan_id", "")),
                    assigned_to=str(r.get("assigned_to", "")),
                    status=str(r.get("status", "")),
                    due_time=r.get("due_time"),
                    priority=str(r.get("priority", "medium")),
                    title=str(r.get("title", "")),
                )
                for r in filtered
            ]
        except Exception:
            degraded.append("task_projection")
            return []

    def _safe_load_events(self, *, family_id: str, now: datetime, degraded: list[str]) -> list[EventView]:
        try:
            rows = self._hpal_gateway.get_calendar_view(family_id=family_id)
            low = now - timedelta(days=self.EVENT_WINDOW_DAYS)
            high = now + timedelta(days=self.EVENT_WINDOW_DAYS)

            in_window: list[dict[str, Any]] = []
            for row in rows:
                tw = row.get("time_window", {})
                start = _safe_parse_dt(str(tw.get("start", "")))
                if low <= start <= high:
                    in_window.append(row)

            in_window.sort(key=lambda r: (str(r.get("time_window", {}).get("start", "")), str(r.get("event_id", ""))))
            return [
                EventView(
                    event_id=str(r.get("event_id", "")),
                    family_id=str(r.get("family_id", family_id)),
                    title=str(r.get("title", "")),
                    start=str(r.get("time_window", {}).get("start", "")),
                    end=str(r.get("time_window", {}).get("end", "")),
                    participants=list(r.get("participants", [])),
                    source=str(r.get("source", "manual")),
                )
                for r in in_window
            ]
        except Exception:
            degraded.append("event_projection")
            return []

    def _safe_load_conversations(
        self,
        *,
        family_id: str,
        degraded: list[str],
    ) -> tuple[list[ConversationSessionView], list[PartialIntentView], int]:
        try:
            sessions = self._session_store.list_by_family(family_id)
            pending_sessions = self._session_store.list_pending_by_family(family_id)

            session_views = [
                ConversationSessionView(
                    session_id=s.session_id,
                    user_id=s.user_id,
                    state=s.state.value,
                    active_intent_summary={
                        "intent_type": s.active_intent.intent_type if s.active_intent else None,
                        "missing_fields": list(s.active_intent.missing_fields) if s.active_intent else [],
                        "confidence": s.active_intent.confidence if s.active_intent else 0.0,
                    },
                    last_user_message=_last_user_message(s.messages),
                    last_updated=s.last_updated,
                )
                for s in sessions
            ]

            pending_views = [
                PartialIntentView(
                    session_id=s.session_id,
                    intent_type=s.active_intent.intent_type if s.active_intent else None,
                    extracted_fields=dict(s.active_intent.extracted_fields) if s.active_intent else {},
                    missing_fields=list(s.active_intent.missing_fields) if s.active_intent else [],
                    ambiguous_fields=list(s.active_intent.ambiguous_fields) if s.active_intent else [],
                    confidence=s.active_intent.confidence if s.active_intent else 0.0,
                )
                for s in pending_sessions
                if s.active_intent is not None
            ]

            session_views.sort(key=lambda s: (s.last_updated.isoformat(), s.session_id))
            pending_views.sort(key=lambda p: p.session_id)
            return session_views, pending_views, self._session_store.version()
        except Exception:
            degraded.append("col_session_store")
            return [], [], 0

    def _safe_load_xai(self, *, family_id: str, degraded: list[str]) -> tuple[list[XAIExplanationView], str]:
        try:
            items = self._xai_store.get_recent(family_id=family_id, limit=self.XAI_RECENT_LIMIT)
            mapped = [
                XAIExplanationView(
                    explanation_id=x.explanation_id,
                    entity_type=x.entity_type.value,
                    entity_id=x.entity_id,
                    change_type=x.change_type.value,
                    reason_code=x.reason_code.value,
                    explanation_text=x.explanation_text,
                    timestamp=x.timestamp,
                )
                for x in items
            ]
            watermark = mapped[0].explanation_id if mapped else "none"
            return mapped, watermark
        except Exception:
            degraded.append("xai_store")
            return [], "none"

    def _safe_load_system_state(
        self,
        *,
        family_id: str,
        family_summary: dict[str, Any],
        now: datetime,
        degraded: list[str],
    ) -> tuple[SystemStateView, int]:
        projection = self._system_state_store.get(family_id)
        if projection is not None:
            return (
                SystemStateView(
                    mode=projection.mode,
                    health_score=projection.health_score,
                    active_repair_count=projection.active_repair_count,
                    last_reconciliation_at=projection.last_reconciliation_at,
                ),
                projection.version,
            )

        degraded.append("gscl_gre_projection")
        pending_actions = int(family_summary.get("pending_actions", 0))
        stale_projection = bool(family_summary.get("stale_projection", False))

        if stale_projection and pending_actions > 20:
            mode = "RECONCILIATION_HEAVY"
        elif stale_projection:
            mode = "DEGRADED"
        else:
            mode = "NORMAL"

        health = 1.0
        if stale_projection:
            health -= 0.25
        health -= min(pending_actions, 20) * 0.02
        health = max(0.0, min(1.0, round(health, 3)))

        last_recon = _safe_parse_dt(str(family_summary.get("last_projection_at", "")))
        if last_recon.year < 1971:
            last_recon = now

        return (
            SystemStateView(
                mode=mode,  # type: ignore[arg-type]
                health_score=health,
                active_repair_count=max(pending_actions, 0),
                last_reconciliation_at=last_recon,
            ),
            0,
        )


def _safe_parse_dt(raw: str) -> datetime:
    if not raw:
        return datetime.fromtimestamp(0, tz=UTC)
    text = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return datetime.fromtimestamp(0, tz=UTC)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _last_user_message(messages: list[Any]) -> str | None:
    for msg in reversed(messages):
        if getattr(msg, "role", None) == "user":
            return str(getattr(msg, "content", ""))
    return None


def _compute_projection_version(
    *,
    family_summary: dict[str, Any],
    session_version: int,
    xai_watermark: str,
    system_version: int,
) -> int:
    base_state_version = int(family_summary.get("state_version", 0))
    key = "|".join([
        str(base_state_version),
        str(session_version),
        str(xai_watermark),
        str(system_version),
    ])
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:8], 16)


def _build_source_watermark(
    *,
    family_summary: dict[str, Any],
    session_version: int,
    xai_watermark: str,
    system_version: int,
) -> str:
    return "|".join([
        f"hpal:{family_summary.get('state_version', 0)}",
        f"col:{session_version}",
        f"xai:{xai_watermark}",
        f"sys:{system_version}",
    ])
