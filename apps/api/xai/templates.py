"""
XAI Layer — Template Engine
==============================

Deterministic, parameterised explanation text renderer.

Design rules
------------
1. Every ``ReasonCode`` maps to exactly one template string.
   ``MissingTemplateError`` fires at **import time** if any entry is absent.

2. Templates use ``{named}`` placeholders only.
   No positional args. No f-strings at call sites. No dynamic concatenation.

3. ``render()`` is a pure function: same inputs → same output string. Always.

4. Missing required placeholders raise ``ValueError`` (fail-loud; no silent fallback).

5. No synonyms, no phrasing variants, no randomness.

Template placeholder universe
------------------------------
``entity_name``       — name of the entity that changed (always required)
``trigger_name``      — generic name of the triggering entity
``plan_name``         — name of the related plan (optional; defaults to "this plan")
``old_time``          — previous scheduled time (for reschedule / time-change codes)
``new_time``          — new scheduled time (for reschedule / time-change codes)
``trigger_reason``    — human-readable reason string provided by caller
``conflicting_event`` — name of the overlapping event (for conflict codes)
``dependency_name``   — name of the blocking dependency (for dependency codes)
``policy_name``       — name of the policy that blocked the action (for guardrail codes)
"""
from __future__ import annotations

from apps.api.xai.schema import ReasonCode

# ---------------------------------------------------------------------------
# Template registry — one entry per ReasonCode (enforced below)
# ---------------------------------------------------------------------------

_TEMPLATES: dict[ReasonCode, str] = {
    # ==================================================================
    # TASK
    # ==================================================================
    ReasonCode.TASK_CREATED: (
        "Task '{entity_name}' was created as part of plan '{plan_name}'."
    ),
    ReasonCode.TASK_COMPLETED: (
        "Task '{entity_name}' was marked as completed."
    ),
    ReasonCode.TASK_RESCHEDULED: (
        "Task '{entity_name}' was moved from {old_time} to {new_time} due to {trigger_reason}."
    ),
    ReasonCode.TASK_BLOCKED: (
        "Task '{entity_name}' is blocked because '{dependency_name}' has not yet completed."
    ),
    ReasonCode.TASK_UNBLOCKED: (
        "Task '{entity_name}' is no longer blocked. '{dependency_name}' has completed."
    ),
    ReasonCode.TASK_DELETED: (
        "Task '{entity_name}' was deleted."
    ),

    # ==================================================================
    # EVENT
    # ==================================================================
    ReasonCode.EVENT_CREATED: (
        "Event '{entity_name}' was added to the family calendar."
    ),
    ReasonCode.EVENT_UPDATED: (
        "Event '{entity_name}' was updated."
    ),
    ReasonCode.EVENT_CANCELLED: (
        "Event '{entity_name}' was cancelled."
    ),
    ReasonCode.EVENT_TIME_CHANGED: (
        "Event '{entity_name}' was moved from {old_time} to {new_time}."
    ),

    # ==================================================================
    # PLAN
    # ==================================================================
    ReasonCode.PLAN_CREATED: (
        "Plan '{entity_name}' was created."
    ),
    ReasonCode.PLAN_RECOMPUTED: (
        "Plan '{entity_name}' was recomputed to reflect the latest schedule changes."
    ),
    ReasonCode.PLAN_OPTIMIZED: (
        "Plan '{entity_name}' was adjusted to reduce conflicts and improve scheduling."
    ),

    # ==================================================================
    # CONFLICTS
    # ==================================================================
    ReasonCode.EVENT_TIME_CONFLICT: (
        "Event '{entity_name}' overlaps with '{conflicting_event}', causing a scheduling conflict."
    ),
    ReasonCode.TASK_TIME_CONFLICT: (
        "Task '{entity_name}' conflicts with '{conflicting_event}' in the schedule."
    ),
    ReasonCode.RESOURCE_CONFLICT: (
        "'{entity_name}' could not be scheduled because '{conflicting_event}' "
        "requires the same resource or participant."
    ),

    # ==================================================================
    # DEPENDENCIES
    # ==================================================================
    ReasonCode.DEPENDENCY_DELAY: (
        "Task '{entity_name}' was delayed because dependent task '{dependency_name}' "
        "is not yet complete."
    ),
    ReasonCode.DEPENDENCY_COMPLETED: (
        "Task '{entity_name}' is now unblocked. "
        "'{dependency_name}' completed successfully."
    ),
    ReasonCode.DEPENDENCY_BLOCKED: (
        "Task '{entity_name}' could not proceed because '{dependency_name}' is blocked."
    ),

    # ==================================================================
    # SYSTEM
    # ==================================================================
    ReasonCode.SYSTEM_RECOMPUTE: (
        "The system recomputed '{entity_name}' in response to a schedule change."
    ),
    ReasonCode.SYSTEM_OPTIMIZATION: (
        "Your schedule was adjusted to better fit available time and reduce conflicts."
    ),
    ReasonCode.SYSTEM_RECOVERY: (
        "'{entity_name}' was restored after a system recovery event."
    ),

    # ==================================================================
    # USER
    # ==================================================================
    ReasonCode.USER_CREATED: (
        "'{entity_name}' was created directly by a family member."
    ),
    ReasonCode.USER_UPDATED: (
        "'{entity_name}' was updated directly by a family member."
    ),
    ReasonCode.USER_OVERRIDE: (
        "Change applied manually by user."
    ),

    # ==================================================================
    # GUARDRAILS
    # ==================================================================
    ReasonCode.ACTION_BLOCKED_POLICY: (
        "Action on '{entity_name}' was blocked by policy '{policy_name}'."
    ),
    ReasonCode.ACTION_REQUIRES_CONFIRMATION: (
        "Action on '{entity_name}' requires confirmation before it can proceed."
    ),
}

# ---------------------------------------------------------------------------
# Completeness guard — fires at import time if any ReasonCode lacks a template
# ---------------------------------------------------------------------------


class MissingTemplateError(RuntimeError):
    """Raised when a ReasonCode has no registered template."""


_missing = [code for code in ReasonCode if code not in _TEMPLATES]
if _missing:
    raise MissingTemplateError(
        f"XAI TemplateEngine: missing templates for reason codes: "
        f"{[c.value for c in _missing]}"
    )

# ---------------------------------------------------------------------------
# Declared placeholder universe — all known substitution keys
# ---------------------------------------------------------------------------

_ALL_PLACEHOLDERS: frozenset[str] = frozenset(
    {
        "entity_name",       # always required
        "trigger_name",      # generic trigger entity name
        "plan_name",         # parent plan name
        "old_time",          # previous time (reschedule / time-change)
        "new_time",          # new time (reschedule / time-change)
        "trigger_reason",    # human-readable reason string
        "conflicting_event", # overlapping event name (conflict codes)
        "dependency_name",   # blocking dependency name (dependency codes)
        "policy_name",       # policy that blocked (guardrail codes)
    }
)


# ---------------------------------------------------------------------------
# TemplateEngine — pure, stateless renderer
# ---------------------------------------------------------------------------


class TemplateEngine:
    """
    Deterministic explanation text renderer.

    All arguments are keyword-only. ``entity_name`` is always required.
    All other parameters are optional; a ``ValueError`` is raised if the
    chosen template requires a placeholder that was not supplied.

    Example::

        engine = TemplateEngine()

        engine.render(
            reason_code=ReasonCode.TASK_RESCHEDULED,
            entity_name="Prepare dinner",
            old_time="18:00",
            new_time="19:30",
            trigger_reason="a schedule conflict",
        )
        # "Task 'Prepare dinner' was moved from 18:00 to 19:30 due to a schedule conflict."

        engine.render(
            reason_code=ReasonCode.EVENT_TIME_CONFLICT,
            entity_name="School pickup",
            conflicting_event="Dentist appointment",
        )
        # "Event 'School pickup' overlaps with 'Dentist appointment', causing a scheduling conflict."

        engine.render(
            reason_code=ReasonCode.DEPENDENCY_DELAY,
            entity_name="Cook dinner",
            dependency_name="Buy groceries",
        )
        # "Task 'Cook dinner' was delayed because dependent task 'Buy groceries' is not yet complete."
    """

    def render(
        self,
        *,
        reason_code: ReasonCode,
        entity_name: str,
        trigger_name: str | None = None,
        plan_name: str | None = None,
        old_time: str | None = None,
        new_time: str | None = None,
        trigger_reason: str | None = None,
        conflicting_event: str | None = None,
        dependency_name: str | None = None,
        policy_name: str | None = None,
    ) -> str:
        """
        Render the explanation text for the given reason code.

        Parameters
        ----------
        reason_code : ReasonCode
            The change reason (strict closed enum).
        entity_name : str
            Human-readable name of the entity that changed (always required).
        trigger_name : str | None
            Generic name of the triggering entity.
        plan_name : str | None
            Name of the related plan. Defaults to "this plan" when the
            template requires it but the value is not supplied.
        old_time : str | None
            Previous scheduled time (TASK_RESCHEDULED, EVENT_TIME_CHANGED).
        new_time : str | None
            New scheduled time (TASK_RESCHEDULED, EVENT_TIME_CHANGED).
        trigger_reason : str | None
            Human-readable reason string (TASK_RESCHEDULED).
        conflicting_event : str | None
            Name of the overlapping event (conflict codes).
        dependency_name : str | None
            Name of the blocking or completing dependency.
        policy_name : str | None
            Name of the policy that blocked or gated the action.

        Returns
        -------
        str
            Filled-in explanation text. Deterministic: same args → same output.

        Raises
        ------
        ValueError
            If a placeholder required by the template has no value.
        """
        template = _TEMPLATES[reason_code]

        subs: dict[str, str] = {"entity_name": entity_name}
        # plan_name falls back to "this plan" so templates that require it always render
        subs["plan_name"] = plan_name if plan_name is not None else "this plan"
        if trigger_name is not None:
            subs["trigger_name"] = trigger_name
        if old_time is not None:
            subs["old_time"] = old_time
        if new_time is not None:
            subs["new_time"] = new_time
        if trigger_reason is not None:
            subs["trigger_reason"] = trigger_reason
        if conflicting_event is not None:
            subs["conflicting_event"] = conflicting_event
        if dependency_name is not None:
            subs["dependency_name"] = dependency_name
        if policy_name is not None:
            subs["policy_name"] = policy_name

        try:
            text = template.format_map(_StrictFormatMap(subs))
        except KeyError as exc:
            raise ValueError(
                f"Template for {reason_code.value!r} requires placeholder "
                f"{exc} but it was not supplied."
            ) from exc

        return text


class _StrictFormatMap(dict):  # type: ignore[type-arg]
    """
    Raises ``KeyError`` (never silences) when a required placeholder is absent.
    Prevents silent partial-substitution bugs.
    """

    def __missing__(self, key: str) -> str:  # pragma: no cover
        raise KeyError(key)
