from __future__ import annotations

from typing import Any


_EFFORT_LEVELS = {"low", "medium", "high"}
_PROPOSAL_CATEGORIES = {"task", "event_prep", "maintenance", "health", "other"}
_SIGNAL_SEVERITIES = {"low", "medium", "high"}


def _fail(reason: str) -> None:
    raise ValueError(reason)


def _require_keys(payload: dict[str, Any], *, required: set[str], allowed: set[str], path: str) -> None:
    keys = set(payload.keys())
    missing = required - keys
    if missing:
        _fail(f"{path} missing required keys: {sorted(missing)}")

    extra = keys - allowed
    if extra:
        _fail(f"{path} has unexpected keys: {sorted(extra)}")


def _validate_iso_datetime(value: Any, path: str) -> None:
    if not isinstance(value, str) or value.strip() == "":
        _fail(f"{path} must be non-empty ISO datetime string")


def validate_proposal_contract(
    proposal: Any,
    *,
    path: str = "proposal",
    require_planning_fields: bool = True,
) -> dict[str, Any]:
    if not isinstance(proposal, dict):
        _fail(f"{path} must be dict")

    required = {"id", "type", "title", "description", "priority", "source_module"}
    planning_fields = {"duration", "effort", "category"}
    if require_planning_fields:
        required |= planning_fields
    allowed = set(required) | planning_fields | {"normalized_priority"}
    _require_keys(proposal, required=required, allowed=allowed, path=path)

    if not isinstance(proposal["id"], str):
        _fail(f"{path}.id must be str")
    if not isinstance(proposal["type"], str):
        _fail(f"{path}.type must be str")
    if not isinstance(proposal["title"], str):
        _fail(f"{path}.title must be str")
    if not isinstance(proposal["description"], str):
        _fail(f"{path}.description must be str")
    if not isinstance(proposal["source_module"], str):
        _fail(f"{path}.source_module must be str")

    if not isinstance(proposal["priority"], (int, float)):
        _fail(f"{path}.priority must be int or float")

    if "duration" in proposal:
        if not isinstance(proposal["duration"], int):
            _fail(f"{path}.duration must be int")
        if proposal["duration"] < 1:
            _fail(f"{path}.duration must be >= 1")

    if "effort" in proposal:
        effort = str(proposal["effort"]).strip().lower()
        if effort not in _EFFORT_LEVELS:
            _fail(f"{path}.effort must be one of: {sorted(_EFFORT_LEVELS)}")

    if "category" in proposal:
        category = str(proposal["category"]).strip().lower()
        if category not in _PROPOSAL_CATEGORIES:
            _fail(f"{path}.category must be one of: {sorted(_PROPOSAL_CATEGORIES)}")

    if "normalized_priority" in proposal and not isinstance(proposal["normalized_priority"], (int, float)):
        _fail(f"{path}.normalized_priority must be int or float when provided")

    return proposal


def validate_signal_contract(signal: Any, *, path: str = "signal") -> dict[str, Any]:
    if not isinstance(signal, dict):
        _fail(f"{path} must be dict")

    required = {"id", "type", "message", "severity", "source_module"}
    allowed = set(required) | {"correlation_id"}
    _require_keys(signal, required=required, allowed=allowed, path=path)

    if not isinstance(signal["id"], str):
        _fail(f"{path}.id must be str")
    if not isinstance(signal["type"], str):
        _fail(f"{path}.type must be str")
    if not isinstance(signal["message"], str):
        _fail(f"{path}.message must be str")
    if not isinstance(signal["source_module"], str):
        _fail(f"{path}.source_module must be str")

    severity = str(signal["severity"]).strip().lower()
    if severity not in _SIGNAL_SEVERITIES:
        _fail(f"{path}.severity must be one of: {sorted(_SIGNAL_SEVERITIES)}")

    if "correlation_id" in signal and not isinstance(signal["correlation_id"], str):
        _fail(f"{path}.correlation_id must be str when provided")

    return signal


def validate_module_output_contract(module_output: Any, *, path: str = "module_output") -> dict[str, Any]:
    if not isinstance(module_output, dict):
        _fail(f"{path} must be dict")

    required = {"module", "proposals", "signals", "confidence", "metadata"}
    allowed = set(required)
    _require_keys(module_output, required=required, allowed=allowed, path=path)

    if not isinstance(module_output["module"], str):
        _fail(f"{path}.module must be str")

    confidence = module_output["confidence"]
    if not isinstance(confidence, (int, float)):
        _fail(f"{path}.confidence must be int or float")
    if not (0.0 <= float(confidence) <= 1.0):
        _fail(f"{path}.confidence must be between 0.0 and 1.0")

    if not isinstance(module_output["metadata"], dict):
        _fail(f"{path}.metadata must be dict")
    if not isinstance(module_output["proposals"], list):
        _fail(f"{path}.proposals must be list")
    if not isinstance(module_output["signals"], list):
        _fail(f"{path}.signals must be list")

    for index, proposal in enumerate(module_output["proposals"]):
        validate_proposal_contract(
            proposal,
            path=f"{path}.proposals[{index}]",
            require_planning_fields=True,
        )

    for index, signal in enumerate(module_output["signals"]):
        validate_signal_contract(signal, path=f"{path}.signals[{index}]")

    return module_output


def validate_decision_input_contract(payload: Any, *, path: str = "decision_input") -> dict[str, Any]:
    if not isinstance(payload, dict):
        _fail(f"{path} must be dict")

    required = {"proposals", "signals"}
    allowed = {"proposals", "signals", "calendar_events"}
    _require_keys(payload, required=required, allowed=allowed, path=path)

    proposals = payload["proposals"]
    signals = payload["signals"]
    if not isinstance(proposals, list):
        _fail(f"{path}.proposals must be list")
    if not isinstance(signals, list):
        _fail(f"{path}.signals must be list")

    for index, proposal in enumerate(proposals):
        validate_proposal_contract(
            proposal,
            path=f"{path}.proposals[{index}]",
            require_planning_fields=False,
        )

    for index, signal in enumerate(signals):
        validate_signal_contract(signal, path=f"{path}.signals[{index}]")

    calendar_events = payload.get("calendar_events", [])
    if not isinstance(calendar_events, list):
        _fail(f"{path}.calendar_events must be list when provided")

    for index, row in enumerate(calendar_events):
        if not isinstance(row, dict):
            _fail(f"{path}.calendar_events[{index}] must be dict")
        required_event = {"start_time", "end_time", "source"}
        allowed_event = set(required_event)
        _require_keys(
            row,
            required=required_event,
            allowed=allowed_event,
            path=f"{path}.calendar_events[{index}]",
        )
        _validate_iso_datetime(row["start_time"], f"{path}.calendar_events[{index}].start_time")
        _validate_iso_datetime(row["end_time"], f"{path}.calendar_events[{index}].end_time")
        if not isinstance(row["source"], str):
            _fail(f"{path}.calendar_events[{index}].source must be str")

    return payload


def _validate_priority_row(item: Any, path: str) -> None:
    if not isinstance(item, dict):
        _fail(f"{path} must be dict")
    required = {
        "rank",
        "proposal_id",
        "source_module",
        "score",
        "priority_hint",
        "urgency_score",
        "context_score",
        "duration",
        "effort",
        "category",
    }
    _require_keys(item, required=required, allowed=set(required), path=path)


def _validate_calendar_event_row(item: Any, path: str) -> None:
    if not isinstance(item, dict):
        _fail(f"{path} must be dict")
    required = {"start_time", "end_time", "source"}
    _require_keys(item, required=required, allowed=set(required), path=path)
    _validate_iso_datetime(item["start_time"], f"{path}.start_time")
    _validate_iso_datetime(item["end_time"], f"{path}.end_time")
    if not isinstance(item["source"], str):
        _fail(f"{path}.source must be str")


def _validate_scheduled_action_row(item: Any, path: str) -> None:
    if not isinstance(item, dict):
        _fail(f"{path} must be dict")

    required = {
        "proposal_id",
        "source_module",
        "type",
        "priority_hint",
        "urgency_score",
        "context_score",
        "effort",
        "effort_weight",
        "category",
        "score",
        "duration_units",
        "preferred_bucket",
        "bucket",
        "start_time",
        "end_time",
    }
    allowed = set(required) | {"scheduled_day", "unscheduled_reason"}
    _require_keys(item, required=required, allowed=allowed, path=path)

    _validate_iso_datetime(item["start_time"], f"{path}.start_time")
    _validate_iso_datetime(item["end_time"], f"{path}.end_time")


def _validate_unscheduled_action_row(item: Any, path: str) -> None:
    if not isinstance(item, dict):
        _fail(f"{path} must be dict")

    required = {
        "proposal_id",
        "source_module",
        "type",
        "priority_hint",
        "urgency_score",
        "context_score",
        "effort",
        "effort_weight",
        "category",
        "score",
        "duration_units",
        "preferred_bucket",
        "unscheduled_reason",
    }
    _require_keys(item, required=required, allowed=set(required), path=path)


def _validate_daily_load_balancer(payload: Any, path: str) -> None:
    if not isinstance(payload, dict):
        _fail(f"{path} must be dict")

    required = {"day_1", "day_2", "day_3"}
    _require_keys(payload, required=required, allowed=set(required), path=path)

    for day_key in sorted(required):
        row = payload[day_key]
        day_path = f"{path}.{day_key}"
        if not isinstance(row, dict):
            _fail(f"{day_path} must be dict")
        required_row = {"total_capacity_used", "remaining_slack", "overload_penalty"}
        _require_keys(row, required=required_row, allowed=set(required_row), path=day_path)
        for metric in sorted(required_row):
            if not isinstance(row[metric], (int, float)):
                _fail(f"{day_path}.{metric} must be int or float")


def _validate_decision_trace(payload: Any, path: str) -> None:
    if not isinstance(payload, dict):
        _fail(f"{path} must be dict")

    required = {"scheduled", "unscheduled", "summary"}
    _require_keys(payload, required=required, allowed=set(required), path=path)

    if not isinstance(payload["scheduled"], list):
        _fail(f"{path}.scheduled must be list")
    if not isinstance(payload["unscheduled"], list):
        _fail(f"{path}.unscheduled must be list")
    if not isinstance(payload["summary"], dict):
        _fail(f"{path}.summary must be dict")

    scheduled_required = {
        "proposal_id",
        "computed_score",
        "priority_hint",
        "urgency_score",
        "duration_cost",
        "effort_weight",
        "final_score",
        "assigned_bucket",
        "assigned_time_window",
        "reason_assigned",
    }
    for index, row in enumerate(payload["scheduled"]):
        if not isinstance(row, dict):
            _fail(f"{path}.scheduled[{index}] must be dict")
        _require_keys(
            row,
            required=scheduled_required,
            allowed=set(scheduled_required),
            path=f"{path}.scheduled[{index}]",
        )

    unscheduled_required = {
        "proposal_id",
        "rejection_reason",
        "failed_constraints",
        "computed_score",
        "priority_hint",
        "urgency_score",
        "duration_cost",
        "effort_weight",
        "final_score",
    }
    for index, row in enumerate(payload["unscheduled"]):
        if not isinstance(row, dict):
            _fail(f"{path}.unscheduled[{index}] must be dict")
        _require_keys(
            row,
            required=unscheduled_required,
            allowed=set(unscheduled_required),
            path=f"{path}.unscheduled[{index}]",
        )
        if not isinstance(row["failed_constraints"], list):
            _fail(f"{path}.unscheduled[{index}].failed_constraints must be list")

    summary_required = {
        "total_proposals_evaluated",
        "total_scheduled",
        "total_unscheduled",
        "capacity_utilization_per_bucket",
        "conflict_count",
        "scheduling_pass_count",
    }
    _require_keys(payload["summary"], required=summary_required, allowed=set(summary_required), path=f"{path}.summary")


def validate_decision_output_contract(payload: Any, *, path: str = "decision_output") -> dict[str, Any]:
    if not isinstance(payload, dict):
        _fail(f"{path} must be dict")

    required = {
        "priorities",
        "calendar_events",
        "scheduled_actions",
        "unscheduled_actions",
        "warnings",
        "risks",
        "day_1_schedule",
        "day_2_schedule",
        "day_3_schedule",
        "backlog",
        "_internal",
    }
    _require_keys(payload, required=required, allowed=set(required), path=path)

    list_fields = [
        "priorities",
        "calendar_events",
        "scheduled_actions",
        "unscheduled_actions",
        "warnings",
        "risks",
        "day_1_schedule",
        "day_2_schedule",
        "day_3_schedule",
        "backlog",
    ]
    for field_name in list_fields:
        if not isinstance(payload[field_name], list):
            _fail(f"{path}.{field_name} must be list")

    for index, item in enumerate(payload["priorities"]):
        _validate_priority_row(item, f"{path}.priorities[{index}]")

    for index, item in enumerate(payload["calendar_events"]):
        _validate_calendar_event_row(item, f"{path}.calendar_events[{index}]")

    for index, item in enumerate(payload["scheduled_actions"]):
        _validate_scheduled_action_row(item, f"{path}.scheduled_actions[{index}]")

    for index, item in enumerate(payload["unscheduled_actions"]):
        _validate_unscheduled_action_row(item, f"{path}.unscheduled_actions[{index}]")

    for index, item in enumerate(payload["day_1_schedule"]):
        _validate_scheduled_action_row(item, f"{path}.day_1_schedule[{index}]")

    for index, item in enumerate(payload["day_2_schedule"]):
        _validate_scheduled_action_row(item, f"{path}.day_2_schedule[{index}]")

    for index, item in enumerate(payload["day_3_schedule"]):
        _validate_scheduled_action_row(item, f"{path}.day_3_schedule[{index}]")

    for index, item in enumerate(payload["backlog"]):
        _validate_unscheduled_action_row(item, f"{path}.backlog[{index}]")

    for field_name in ("warnings", "risks"):
        for index, row in enumerate(payload[field_name]):
            if not isinstance(row, dict):
                _fail(f"{path}.{field_name}[{index}] must be dict")
            if "type" not in row or not isinstance(row.get("type"), str):
                _fail(f"{path}.{field_name}[{index}].type must be str")

    internal = payload["_internal"]
    if not isinstance(internal, dict):
        _fail(f"{path}._internal must be dict")

    required_internal = {
        "schedule_score",
        "baseline_schedule_score",
        "optimization_applied",
        "daily_load_balancer",
    }
    allowed_internal = set(required_internal) | {"decision_trace"}
    _require_keys(internal, required=required_internal, allowed=allowed_internal, path=f"{path}._internal")

    if not isinstance(internal["schedule_score"], (int, float)):
        _fail(f"{path}._internal.schedule_score must be int or float")
    if not isinstance(internal["baseline_schedule_score"], (int, float)):
        _fail(f"{path}._internal.baseline_schedule_score must be int or float")
    if not isinstance(internal["optimization_applied"], bool):
        _fail(f"{path}._internal.optimization_applied must be bool")

    _validate_daily_load_balancer(internal["daily_load_balancer"], f"{path}._internal.daily_load_balancer")

    if "decision_trace" in internal:
        _validate_decision_trace(internal["decision_trace"], f"{path}._internal.decision_trace")

    return payload


def _validate_brief_action_item(item: Any, path: str) -> None:
    if not isinstance(item, dict):
        _fail(f"{path} must be dict")

    required = {
        "proposal_id",
        "title",
        "description",
        "source_module",
        "decision_type",
        "reason",
        "confidence",
        "normalized_priority",
        "ordering_position",
        "time_bucket",
        "score",
        "duration_units",
        "duration",
        "start_time",
        "end_time",
    }
    _require_keys(item, required=required, allowed=set(required), path=path)

    if not isinstance(item["confidence"], (int, float)):
        _fail(f"{path}.confidence must be int or float")
    if not isinstance(item["score"], (int, float)):
        _fail(f"{path}.score must be int or float")
    if not isinstance(item["duration_units"], int):
        _fail(f"{path}.duration_units must be int")
    if not isinstance(item["duration"], int):
        _fail(f"{path}.duration must be int")
    if item["duration_units"] < 1 or item["duration"] < 1:
        _fail(f"{path}.duration and duration_units must be >= 1")

    if item["start_time"] is not None:
        _validate_iso_datetime(item["start_time"], f"{path}.start_time")
    if item["end_time"] is not None:
        _validate_iso_datetime(item["end_time"], f"{path}.end_time")


def _validate_brief_priority_row(item: Any, path: str) -> None:
    if not isinstance(item, dict):
        _fail(f"{path} must be dict")
    required = {
        "rank",
        "proposal_id",
        "title",
        "source_module",
        "normalized_priority",
        "score",
        "urgency_score",
        "context_score",
    }
    _require_keys(item, required=required, allowed=set(required), path=path)


def validate_brief_output_contract(payload: Any, *, path: str = "brief_output") -> dict[str, Any]:
    if not isinstance(payload, dict):
        _fail(f"{path} must be dict")

    required = {
        "household_id",
        "date",
        "schedule",
        "personal_agendas",
        "suggestions",
        "suggested_actions",
        "priorities",
        "warnings",
        "risks",
        "summary_text",
        "time_based_schedule",
        "financial",
        "meals",
        "interrupts",
        "meta",
    }
    _require_keys(payload, required=required, allowed=set(required), path=path)

    if not isinstance(payload["household_id"], str):
        _fail(f"{path}.household_id must be str")
    if not isinstance(payload["date"], str):
        _fail(f"{path}.date must be str")
    if not isinstance(payload["summary_text"], str):
        _fail(f"{path}.summary_text must be str")

    for field_name in ("schedule", "suggestions", "suggested_actions", "priorities", "warnings", "risks", "interrupts"):
        if not isinstance(payload[field_name], list):
            _fail(f"{path}.{field_name} must be list")

    personal_agendas = payload["personal_agendas"]
    if not isinstance(personal_agendas, dict):
        _fail(f"{path}.personal_agendas must be dict")
    _require_keys(personal_agendas, required={"tasks", "notifications"}, allowed={"tasks", "notifications"}, path=f"{path}.personal_agendas")
    if not isinstance(personal_agendas["tasks"], list):
        _fail(f"{path}.personal_agendas.tasks must be list")
    if not isinstance(personal_agendas["notifications"], list):
        _fail(f"{path}.personal_agendas.notifications must be list")

    time_based_schedule = payload["time_based_schedule"]
    if not isinstance(time_based_schedule, dict):
        _fail(f"{path}.time_based_schedule must be dict")
    _require_keys(
        time_based_schedule,
        required={"morning", "afternoon", "evening"},
        allowed={"morning", "afternoon", "evening"},
        path=f"{path}.time_based_schedule",
    )
    for bucket in ("morning", "afternoon", "evening"):
        if not isinstance(time_based_schedule[bucket], list):
            _fail(f"{path}.time_based_schedule.{bucket} must be list")

    for index, row in enumerate(payload["schedule"]):
        _validate_brief_action_item(row, f"{path}.schedule[{index}]")

    for index, row in enumerate(payload["suggestions"]):
        _validate_brief_action_item(row, f"{path}.suggestions[{index}]")

    for index, row in enumerate(payload["suggested_actions"]):
        _validate_brief_action_item(row, f"{path}.suggested_actions[{index}]")

    for list_name in ("tasks", "notifications"):
        for index, row in enumerate(personal_agendas[list_name]):
            _validate_brief_action_item(row, f"{path}.personal_agendas.{list_name}[{index}]")

    for bucket in ("morning", "afternoon", "evening"):
        for index, row in enumerate(time_based_schedule[bucket]):
            _validate_brief_action_item(row, f"{path}.time_based_schedule.{bucket}[{index}]")

    for index, row in enumerate(payload["priorities"]):
        _validate_brief_priority_row(row, f"{path}.priorities[{index}]")

    for field_name in ("warnings", "risks"):
        for index, row in enumerate(payload[field_name]):
            if not isinstance(row, dict):
                _fail(f"{path}.{field_name}[{index}] must be dict")

    for obj_name in ("financial", "meals"):
        obj = payload[obj_name]
        if not isinstance(obj, dict):
            _fail(f"{path}.{obj_name} must be dict")
        _require_keys(obj, required={"items"}, allowed={"items"}, path=f"{path}.{obj_name}")
        if not isinstance(obj["items"], list):
            _fail(f"{path}.{obj_name}.items must be list")
        for index, row in enumerate(obj["items"]):
            _validate_brief_action_item(row, f"{path}.{obj_name}.items[{index}]")

    meta = payload["meta"]
    if not isinstance(meta, dict):
        _fail(f"{path}.meta must be dict")
    required_meta = {"decision_count", "scheduled_count", "deferred_count"}
    _require_keys(meta, required=required_meta, allowed=set(required_meta), path=f"{path}.meta")
    for key in sorted(required_meta):
        if not isinstance(meta[key], int):
            _fail(f"{path}.meta.{key} must be int")

    return payload