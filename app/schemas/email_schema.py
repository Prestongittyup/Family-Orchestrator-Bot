from __future__ import annotations

from typing import Any

from jsonschema import Draft7Validator


EMAIL_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "priority": {
            "type": "string",
            "enum": ["high", "medium", "low"],
        },
        "needs_attention": {"type": "boolean"},
        "actions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["reply", "task"],
                    },
                    "title": {"type": "string"},
                    "due": {"type": ["string", "null"]},
                },
                "required": ["type", "title"],
            },
        },
        "state_summary": {"type": "string"},
        "reason": {"type": "string"},
        "upgrade_available": {"type": "boolean"},
        "metadata": {"type": "object"},
    },
    "required": ["priority", "needs_attention", "actions", "state_summary", "reason"],
}


_EMAIL_VALIDATOR = Draft7Validator(EMAIL_SCHEMA)


def fallback_email_response(*, reason: str = "tier_limit_reached") -> dict[str, Any]:
    return {
        "priority": "medium",
        "needs_attention": False,
        "actions": [],
        "state_summary": "Upgrade required for AI features",
        "reason": reason,
        "upgrade_available": True,
    }


def validate_email_payload(payload: dict[str, Any]) -> tuple[bool, list[str]]:
    errors = sorted(_EMAIL_VALIDATOR.iter_errors(payload), key=lambda item: list(item.path))
    if not errors:
        return True, []

    messages: list[str] = []
    for item in errors:
        location = ".".join(str(part) for part in item.path)
        if location:
            messages.append(f"{location}: {item.message}")
        else:
            messages.append(item.message)
    return False, messages
