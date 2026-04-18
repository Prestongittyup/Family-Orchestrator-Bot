from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any

from apps.api.endpoints.brief_endpoint import get_daily_brief
from apps.api.ingestion.adapters.adapter_governance import validate_adapter_output_contract
from apps.api.ingestion.adapters.manual_priority import partition_actions_by_visibility, score_manual_item
from apps.api.ingestion.adapters.time_normalizer import normalize_time_input
from apps.api.services.planning_boundary_contract import pre_os2_validation


def map_manual_to_brief(base_brief: dict[str, Any], manual_items: list[dict[str, Any]]) -> dict[str, Any]:
    if "scheduled_actions" not in base_brief:
        base_brief["scheduled_actions"] = []

    if "unscheduled_actions" not in base_brief:
        base_brief["unscheduled_actions"] = []

    # Ensure the payload remains projectable as brief_v1 for rendering.
    base_brief.setdefault("priorities", [])
    base_brief.setdefault("warnings", [])
    base_brief.setdefault("risks", [])
    base_brief.setdefault("summary", str(base_brief.get("summary_text", "")))

    # Use system date from brief context for time normalization
    reference_date = datetime.now()

    manual_actions: list[dict[str, Any]] = []

    for item in manual_items:
        if not isinstance(item, dict):
            continue

        action: dict[str, Any] = {
            "title": str(item.get("title", "")).strip(),
        }

        # Preserve raw time input for traceability
        raw_time = item.get("time")
        if raw_time:
            action["raw_time_input"] = str(raw_time).strip()

            # Normalize time input using normalizer utility
            normalized_iso = normalize_time_input(raw_time, reference_date=reference_date)
            if normalized_iso:
                action["start_time"] = normalized_iso
        action["priority_score"] = score_manual_item(
            title=action.get("title", ""),
            start_time=action.get("start_time"),
        )
        manual_actions.append(action)

    scheduled_actions, unscheduled_actions = partition_actions_by_visibility(manual_actions)
    base_brief["scheduled_actions"].extend(scheduled_actions)
    base_brief["unscheduled_actions"].extend(unscheduled_actions)

    # Soft governance only in adapter layer; no hard enforcement here.
    validate_adapter_output_contract(base_brief)

    return base_brief


def run_brief_pipeline(manual_items: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    payload = get_daily_brief(household_id="hh-001")
    if isinstance(payload, dict) and "brief" in payload:
        brief = payload.get("brief")
        if isinstance(brief, dict):
            base_brief = deepcopy(brief)
        else:
            base_brief = {}
    elif isinstance(payload, dict):
        base_brief = deepcopy(payload)
    else:
        base_brief = {}

    # Temporary developer bridge: map manual inputs into brief_v1-style action lists.
    adapter_output = map_manual_to_brief(base_brief, list(manual_items or []))

    # Single hard gate: planning boundary contract enforcement.
    validated = pre_os2_validation(adapter_output)
    return dict(validated)
