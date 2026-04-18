from __future__ import annotations

import re
from datetime import datetime
from datetime import date
from typing import Any

from apps.api.services.decision_engine import run_decision_engine_v2
from modules.core.services.contract_registry import validate_brief_output_contract
from modules.core.services.orchestrator_lite import run_orchestrator


def _extract_decisions(decision_layer: dict[str, Any]) -> list[dict[str, Any]]:
    if "decisions" in decision_layer and isinstance(decision_layer["decisions"], list):
        return [dict(item) for item in decision_layer["decisions"]]

    if (
        "decision_layer" in decision_layer
        and isinstance(decision_layer["decision_layer"], dict)
        and isinstance(decision_layer["decision_layer"].get("decisions"), list)
    ):
        return [dict(item) for item in decision_layer["decision_layer"]["decisions"]]

    return []


def _proposal_item(
    proposal: dict[str, Any],
    decision: dict[str, Any],
    ordering_position: int | None,
) -> dict[str, Any]:
    return {
        "proposal_id": proposal.get("id", decision.get("proposal_id")),
        "title": proposal.get("title"),
        "description": proposal.get("description"),
        "source_module": decision.get("source_module"),
        "decision_type": decision.get("decision_type"),
        "reason": decision.get("reason"),
        "confidence": float(decision.get("confidence", 0.0)),
        "normalized_priority": proposal.get("normalized_priority"),
        "ordering_position": ordering_position,
    }


def _extract_start_hour_from_description(description: str | None) -> int | None:
    if not isinstance(description, str):
        return None

    match = re.search(r"time_window=([^;]+)", description)
    if not match:
        return None

    value = match.group(1).strip()
    if value == "none" or "->" not in value:
        return None

    start_raw, _ = value.split("->", 1)
    start = start_raw.strip()
    try:
        return datetime.fromisoformat(start.replace("Z", "+00:00")).hour
    except Exception:
        return None


def _time_bucket_for_hour(hour: int | None) -> str:
    if hour is None:
        return "afternoon"
    if hour < 12:
        return "morning"
    if hour < 18:
        return "afternoon"
    return "evening"


def _sanitize_brief_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "proposal_id": item.get("proposal_id"),
        "title": item.get("title"),
        "description": item.get("description"),
        "source_module": item.get("source_module"),
        "decision_type": item.get("decision_type"),
        "reason": item.get("reason"),
        "confidence": float(item.get("confidence", 0.0)),
        "normalized_priority": item.get("normalized_priority"),
        "ordering_position": item.get("ordering_position"),
        "time_bucket": item.get("time_bucket"),
        "score": float(item.get("score", 0.0)),
        "duration_units": int(item.get("duration_units", 1)),
        "duration": int(item.get("duration", item.get("duration_units", 1))),
        "start_time": item.get("start_time"),
        "end_time": item.get("end_time"),
    }


def _decision_row_to_item(
    row: dict[str, Any],
    proposal: dict[str, Any],
    *,
    decision_type: str,
    reason: str,
    ordering_position: int | None,
) -> dict[str, Any]:
    return {
        "proposal_id": proposal.get("id", row.get("proposal_id")),
        "title": proposal.get("title"),
        "description": proposal.get("description"),
        "source_module": proposal.get("source_module", row.get("source_module")),
        "decision_type": decision_type,
        "reason": reason,
        "confidence": float(row.get("score", 0.0)),
        "normalized_priority": proposal.get("normalized_priority"),
        "ordering_position": ordering_position,
        "time_bucket": row.get("bucket"),
        "score": float(row.get("score", 0.0)),
        "duration_units": int(row.get("duration_units", 1)),
        "duration": int(row.get("duration", row.get("duration_units", proposal.get("duration", 1)))),
        "start_time": row.get("start_time"),
        "end_time": row.get("end_time"),
    }


def build_daily_brief(
    household_id: str,
    decision_layer: dict[str, Any] | None = None,
    orchestrator_output: dict[str, Any] | None = None,
    include_trace: bool = False,
) -> dict[str, Any]:
    if orchestrator_output is None:
        orchestrator_output = run_orchestrator(household_id)

    proposals = [dict(item) for item in orchestrator_output.get("proposals", [])]
    signals = [dict(item) for item in orchestrator_output.get("signals", [])]
    semantic_layer = dict(orchestrator_output.get("semantic_layer", {}))
    decision_v2 = run_decision_engine_v2(
        {"proposals": proposals, "signals": signals},
        include_trace=include_trace,
    )
    scheduled_actions = [dict(item) for item in decision_v2.get("scheduled_actions", [])]
    unscheduled_actions = [dict(item) for item in decision_v2.get("unscheduled_actions", [])]
    ranked_priorities = [dict(item) for item in decision_v2.get("priorities", [])]

    proposal_by_id = {proposal.get("id"): proposal for proposal in proposals}

    ordering_index = list(semantic_layer.get("ordering_index", []))
    ordering_position_by_proposal_id: dict[str, int] = {}
    for fallback_pos, row in enumerate(ordering_index):
        proposal_id = row.get("proposal_id", row.get("id"))
        if proposal_id is None:
            continue
        ordering_position_by_proposal_id[proposal_id] = int(row.get("position", fallback_pos))

    brief: dict[str, Any] = {
        "household_id": household_id,
        "date": date.today().isoformat(),
        "schedule": [],
        "personal_agendas": {
            "tasks": [],
            "notifications": [],
        },
        "suggestions": [],
        "suggested_actions": [],
        "priorities": [],
        "warnings": [],
        "risks": [],
        "summary_text": "",
        "time_based_schedule": {
            "morning": [],
            "afternoon": [],
            "evening": [],
        },
        "financial": {
            "items": [],
        },
        "meals": {
            "items": [],
        },
        "interrupts": [],
    }

    for row in scheduled_actions:
        proposal_id = row.get("proposal_id")
        proposal = proposal_by_id.get(proposal_id, {"id": proposal_id})
        ordering_position = ordering_position_by_proposal_id.get(proposal_id)
        item = _decision_row_to_item(
            row,
            proposal,
            decision_type="scheduled",
            reason="scheduled_within_capacity",
            ordering_position=ordering_position,
        )
        safe_item = _sanitize_brief_item(item)

        brief["suggested_actions"].append(safe_item)

        source_module = safe_item.get("source_module")
        if source_module == "task_module":
            brief["personal_agendas"]["tasks"].append(safe_item)
        elif source_module == "calendar_module":
            brief["schedule"].append(safe_item)
        elif source_module == "meal_module":
            brief["meals"]["items"].append(safe_item)
        elif source_module == "budget_module":
            brief["financial"]["items"].append(safe_item)
        else:
            brief["suggestions"].append(safe_item)

        bucket = ""
        start_time = safe_item.get("start_time")
        if isinstance(start_time, str) and start_time:
            try:
                parsed = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                bucket = _time_bucket_for_hour(parsed.hour)
            except Exception:
                bucket = ""
        if bucket not in brief["time_based_schedule"]:
            bucket = str(row.get("bucket", "")).strip().lower()
        if bucket not in brief["time_based_schedule"]:
            hour = _extract_start_hour_from_description(safe_item.get("description"))
            bucket = _time_bucket_for_hour(hour)
        brief["time_based_schedule"][bucket].append(safe_item)

    for row in unscheduled_actions:
        proposal_id = row.get("proposal_id")
        proposal = proposal_by_id.get(proposal_id, {"id": proposal_id})
        ordering_position = ordering_position_by_proposal_id.get(proposal_id)
        unscheduled_reason = str(row.get("unscheduled_reason", "deferred")).strip() or "deferred"
        item = _decision_row_to_item(
            row,
            proposal,
            decision_type="deferred",
            reason=unscheduled_reason,
            ordering_position=ordering_position,
        )
        safe_item = _sanitize_brief_item(item)
        brief["suggestions"].append(safe_item)

    brief["schedule"] = sorted(
        brief["schedule"],
        key=lambda item: (
            item.get("ordering_position", 10**9),
            str(item.get("proposal_id", "")),
        ),
    )

    priority_rows: list[dict[str, Any]] = []
    for row in ranked_priorities:
        proposal_id = row.get("proposal_id")
        proposal = proposal_by_id.get(proposal_id, {})
        priority_rows.append(
            {
                "rank": int(row.get("rank", len(priority_rows) + 1)),
                "proposal_id": proposal_id,
                "title": proposal.get("title"),
                "source_module": row.get("source_module", proposal.get("source_module")),
                "normalized_priority": float(row.get("score", 0.0)),
                "score": float(row.get("score", 0.0)),
                "urgency_score": float(row.get("urgency_score", 0.0)),
                "context_score": float(row.get("context_score", 0.0)),
            }
        )

    brief["priorities"] = priority_rows
    brief["warnings"] = [dict(item) for item in decision_v2.get("warnings", [])]
    brief["risks"] = [dict(item) for item in decision_v2.get("risks", [])]

    brief["summary_text"] = (
        f"{len(scheduled_actions)} scheduled actions, "
        f"{len(unscheduled_actions)} deferred actions, "
        f"{len(brief['warnings'])} warnings, "
        f"{len(brief['risks'])} risks."
    )

    brief["meta"] = {
        "decision_count": len(ranked_priorities),
        "scheduled_count": len(scheduled_actions),
        "deferred_count": len(unscheduled_actions),
    }

    validate_brief_output_contract(brief)

    if include_trace:
        trace = decision_v2.get("_internal", {}).get("decision_trace")
        if trace is not None:
            brief["decision_trace"] = trace

    return brief

