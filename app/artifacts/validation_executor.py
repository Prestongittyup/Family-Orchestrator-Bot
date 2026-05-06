from __future__ import annotations

import time
from typing import Any, Mapping

import pytest


ValidationExecutionResult = dict[str, Any]


def _normalized_steps(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_steps = plan.get("steps")
    if not isinstance(raw_steps, list):
        raise ValueError("Validation plan must provide a steps list")

    normalized: list[dict[str, Any]] = []
    for row in raw_steps:
        if not isinstance(row, Mapping):
            raise ValueError("Validation step entries must be mapping rows")

        order = int(row.get("order") or 0)
        name = str(row.get("name") or "").strip()
        raw_args = row.get("args")
        stop_on_failure = bool(row.get("stop_on_failure", True))
        required = bool(row.get("required", True))

        if order < 1:
            raise ValueError("Validation step order must be >= 1")
        if not name:
            raise ValueError("Validation step name is required")
        if not isinstance(raw_args, list):
            raise ValueError("Validation step args must be a list")

        args = [str(arg).strip() for arg in raw_args if str(arg).strip()]
        if not args:
            raise ValueError("Validation step args cannot be empty")

        normalized.append(
            {
                "order": order,
                "name": name,
                "args": args,
                "stop_on_failure": stop_on_failure,
                "required": required,
            }
        )

    return sorted(
        normalized,
        key=lambda step: (
            int(step["order"]),
            str(step["name"]),
            "\x1f".join(str(arg) for arg in step["args"]),
        ),
    )


def run_validation_plan(plan: Mapping[str, Any]) -> ValidationExecutionResult:
    step_results: list[dict[str, Any]] = []
    failed_step: str | None = None

    for step in _normalized_steps(plan):
        started_at = time.perf_counter()
        exit_code = pytest.main([str(arg) for arg in step["args"]])
        duration = time.perf_counter() - started_at

        passed = exit_code == pytest.ExitCode.OK
        step_result: dict[str, Any] = {
            "name": str(step["name"]),
            "status": "passed" if passed else "failed",
            "duration": duration,
            "failure": None if passed else f"pytest exit code {int(exit_code)}",
        }
        step_results.append(step_result)

        is_required = bool(step["required"])
        if not passed and is_required and failed_step is None:
            failed_step = str(step["name"])

        if not passed and bool(step["stop_on_failure"]):
            break

    overall_status = "failed" if failed_step is not None else "passed"
    return {
        "step_results": step_results,
        "overall_status": overall_status,
        "failed_step": failed_step,
    }


def run_default_plan(plan: Mapping[str, Any]) -> ValidationExecutionResult:
    return run_validation_plan(plan)
