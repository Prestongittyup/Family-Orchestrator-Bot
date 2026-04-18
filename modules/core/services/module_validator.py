from __future__ import annotations

from typing import Any


def _fail(reason: str) -> None:
    raise ValueError(reason)


def _validate_time_window(value: Any, path: str) -> None:
    if not isinstance(value, dict):
        _fail(f"{path} must be a dict when provided")

    allowed_keys = {"start", "end"}
    actual_keys = set(value.keys())
    if not actual_keys.issubset(allowed_keys):
        _fail(f"{path} contains unexpected keys: {sorted(actual_keys - allowed_keys)}")

    for key in ("start", "end"):
        if key in value and not isinstance(value[key], str):
            _fail(f"{path}.{key} must be str when provided")


def _validate_proposal(item: Any, index: int) -> None:
    path = f"proposals[{index}]"
    if not isinstance(item, dict):
        _fail(f"{path} must be dict")

    allowed_keys = {"type", "reference", "priority_hint", "description", "time_window"}
    required_keys = {"type", "reference", "priority_hint", "description"}

    keys = set(item.keys())
    missing = required_keys - keys
    if missing:
        _fail(f"{path} missing required keys: {sorted(missing)}")

    extra = keys - allowed_keys
    if extra:
        _fail(f"{path} has unexpected keys: {sorted(extra)}")

    if not isinstance(item["type"], str):
        _fail(f"{path}.type must be str")
    if not isinstance(item["reference"], str):
        _fail(f"{path}.reference must be str")
    if not isinstance(item["priority_hint"], (int, float)):
        _fail(f"{path}.priority_hint must be int or float")
    if not isinstance(item["description"], str):
        _fail(f"{path}.description must be str")

    if "time_window" in item:
        _validate_time_window(item["time_window"], f"{path}.time_window")


def _validate_signal(item: Any, index: int) -> None:
    path = f"signals[{index}]"
    if not isinstance(item, dict):
        _fail(f"{path} must be dict")

    allowed_keys = {"type", "value", "confidence"}
    required_keys = {"type", "value", "confidence"}

    keys = set(item.keys())
    missing = required_keys - keys
    if missing:
        _fail(f"{path} missing required keys: {sorted(missing)}")

    extra = keys - allowed_keys
    if extra:
        _fail(f"{path} has unexpected keys: {sorted(extra)}")

    if not isinstance(item["type"], str):
        _fail(f"{path}.type must be str")

    confidence = item["confidence"]
    if not isinstance(confidence, (int, float)):
        _fail(f"{path}.confidence must be float")
    confidence_value = float(confidence)
    if not (0.0 <= confidence_value <= 1.0):
        _fail(f"{path}.confidence must be between 0 and 1")

    value = item["value"]
    if isinstance(value, (dict, list, tuple, set)):
        _fail(f"{path}.value must not contain nested structures")


def validate_module_output(output: dict) -> bool:
    if not isinstance(output, dict):
        _fail("output must be dict")

    required_top_level = {"proposals", "signals", "metadata"}
    keys = set(output.keys())

    missing = required_top_level - keys
    if missing:
        _fail(f"output missing required keys: {sorted(missing)}")

    extra = keys - required_top_level
    if extra:
        _fail(f"output has unexpected keys: {sorted(extra)}")

    if not isinstance(output["proposals"], list):
        _fail("proposals must be list")
    if not isinstance(output["signals"], list):
        _fail("signals must be list")
    if not isinstance(output["metadata"], dict):
        _fail("metadata must be dict")

    for index, proposal in enumerate(output["proposals"]):
        _validate_proposal(proposal, index)

    for index, signal in enumerate(output["signals"]):
        _validate_signal(signal, index)

    return True
