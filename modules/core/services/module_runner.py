from __future__ import annotations

import re

from modules.calendar.services.calendar_module import calendar_module
from modules.core.models.module_output import ModuleOutput, validate_module_output as validate_typed_module_output
from modules.core.services.contract_registry import validate_module_output_contract
from modules.meals.services.meal_module import meal_module
from modules.tasks.services.task_module import task_module


def _extract_time_window(description: str) -> dict | None:
    match = re.search(r"time_window=([^;]+)", description)
    if not match:
        return None

    value = match.group(1).strip()
    if value == "none":
        return None

    if "->" not in value:
        return None

    start, end = value.split("->", 1)
    return {
        "start": start.strip(),
        "end": end.strip(),
    }


def _signal_confidence_from_severity(severity: str) -> float:
    mapping = {
        "high": 0.9,
        "medium": 0.7,
        "low": 0.5,
    }
    return mapping.get(severity.lower(), 0.5)


def _to_validation_contract(output: ModuleOutput) -> dict:
    proposals = []
    for proposal in output.proposals:
        row = {
            "type": proposal.type,
            "reference": proposal.id,
            "priority_hint": float(proposal.priority),
            "description": proposal.description,
        }
        time_window = _extract_time_window(proposal.description)
        if time_window is not None:
            row["time_window"] = time_window
        proposals.append(row)

    signals = []
    for signal in output.signals:
        signals.append(
            {
                "type": signal.type,
                "value": signal.message,
                "confidence": _signal_confidence_from_severity(signal.severity),
            }
        )

    return {
        "proposals": proposals,
        "signals": signals,
        "metadata": dict(output.metadata),
    }


def run_all_modules(household_id: str) -> list[ModuleOutput]:
    raw_outputs = [
        task_module(household_id),
        calendar_module(household_id),
        meal_module(household_id),
    ]

    validated: list[ModuleOutput] = []
    for output in raw_outputs:
        typed_output = validate_typed_module_output(output)
        validate_module_output_contract(typed_output.to_dict())
        validated.append(typed_output)

    return validated


def run_all_modules_as_dict(household_id: str) -> list[dict]:
    return [output.to_dict() for output in run_all_modules(household_id)]

