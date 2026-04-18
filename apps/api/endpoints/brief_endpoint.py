from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends

from apps.api.core.feature_flags import resolve_feature_flags
from apps.api.endpoints.brief_invariants_v1 import validate_brief_v1
from apps.api.endpoints.integrations_router import get_credential_store, get_http_client
from apps.api.endpoints.brief_renderer_v1 import render_brief_v1
from apps.api.integration_core.brief_builder import BriefBuilder
from apps.api.integration_core.credentials import InMemoryOAuthCredentialStore
from apps.api.integration_core.decision_engine import DecisionEngine
from apps.api.integration_core.orchestrator import create_orchestrator
from apps.api.services.observability_service import build_brief_observability_snapshot

log = logging.getLogger(__name__)


router = APIRouter()

_CACHE_TTL_SECONDS = 600


@dataclass
class _BriefCacheEntry:
    brief: dict[str, Any]
    generated_at: datetime
    expires_at: datetime


_brief_cache: dict[str, _BriefCacheEntry] = {}
_last_known_good_brief: dict[str, dict[str, Any]] = {}
_brief_builder = BriefBuilder()


def _cache_key(household_id: str, iso_date: str) -> str:
    return f"{household_id}:{iso_date}"


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _iso_utc(ts: datetime) -> str:
    return ts.isoformat().replace("+00:00", "Z")


def _get_cached_brief(household_id: str) -> _BriefCacheEntry | None:
    today = _now_utc().date().isoformat()
    key = _cache_key(household_id, today)
    entry = _brief_cache.get(key)
    if entry is None:
        return None
    if _now_utc() >= entry.expires_at:
        _brief_cache.pop(key, None)
        return None
    return entry


def _set_cached_brief(household_id: str, brief: dict[str, Any]) -> _BriefCacheEntry:
    now = _now_utc()
    iso_date = str(brief.get("date", now.date().isoformat()))
    entry = _BriefCacheEntry(
        brief=brief,
        generated_at=now,
        expires_at=now + timedelta(seconds=_CACHE_TTL_SECONDS),
    )
    _brief_cache[_cache_key(household_id, iso_date)] = entry
    _last_known_good_brief[household_id] = brief
    return entry


def _clear_brief_cache(*, clear_last_known_good: bool = True) -> None:
    _brief_cache.clear()
    if clear_last_known_good:
        _last_known_good_brief.clear()


@router.get("/brief/{household_id}")
def get_daily_brief(
    household_id: str,
    user_id: str | None = None,
    include_trace: bool = False,
    include_observability: bool = False,
    validate_contract_v1: bool = False,
    render_human: bool = False,
    credential_store: InMemoryOAuthCredentialStore = Depends(get_credential_store),
    http_client: Any = Depends(get_http_client),
) -> dict[str, Any]:
    flags = resolve_feature_flags(household_id=household_id)
    target_user_id = user_id or household_id

    def _attach_debug(response: dict[str, Any], *, cache_state: str) -> None:
        if not flags.debug_mode:
            return
        response["debug"] = {
            "cache_state": cache_state,
            "feature_flags": {
                "ingestion_enabled": flags.ingestion_enabled,
                "tracing_enabled": flags.tracing_enabled,
                "debug_mode": flags.debug_mode,
            },
        }

    def _attach_rendered(response: dict[str, Any], brief: dict[str, Any]) -> None:
        if not render_human:
            return
        validation = validate_brief_v1(brief, enabled=True)
        brief_v1 = validation.get("brief_v1")
        if brief_v1 is None:
            return
        response["rendered"] = render_brief_v1(brief_v1)

    if target_user_id is not None:
        log.info("request_user", extra={"user_id": target_user_id, "household_id": household_id})
        orchestrator = create_orchestrator(
            credential_store=credential_store,
            http_client=http_client,
            max_results=50,
            decision_engine=DecisionEngine(),
        )
        result = orchestrator.build_household_state(target_user_id)
        if isinstance(result, tuple):
            state, decision_context = result
        else:
            state = result
            decision_context = None
        brief = _brief_builder.build(state, decision_context)
        if validate_contract_v1:
            validate_brief_v1(brief, enabled=True)
        response = {
            "status": "success",
            "brief": brief,
            "generated_at": _iso_utc(_now_utc()),
        }
        _attach_rendered(response, brief)
        if include_observability:
            response["observability"] = build_brief_observability_snapshot(household_id)
        _attach_debug(response, cache_state="orchestrated")
        return response
