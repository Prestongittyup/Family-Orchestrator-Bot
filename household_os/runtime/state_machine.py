from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
import inspect
from typing import Any, Literal

from household_os.security.trust_boundary_enforcer import SecurityViolation, enforce_import_boundary


enforce_import_boundary("household_os.runtime.state_machine")

ALLOWED_FSM_CALLERS = {
    "household_os.runtime.action_pipeline",
    "household_os.runtime.orchestrator",
    "household_os.runtime.state_reducer",
    "household_os.runtime.state_firewall",
}


def _resolve_fsm_caller_module() -> str:
    for frame_info in inspect.stack()[2:]:
        module_name = str(frame_info.frame.f_globals.get("__name__", ""))
        if not module_name:
            continue
        if module_name == "household_os.runtime.state_machine":
            continue
        if module_name.startswith("importlib"):
            continue
        return module_name
    return ""


def _enforce_fsm_caller() -> None:
    caller = _resolve_fsm_caller_module()
    if caller.startswith("tests."):
        return
    if any(caller == allowed or caller.startswith(f"{allowed}.") for allowed in ALLOWED_FSM_CALLERS):
        return
    raise SecurityViolation(f"FSM transition blocked for unauthorized caller: {caller or 'unknown'}")


class ActionState(str, Enum):
    PROPOSED = "proposed"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    COMMITTED = "committed"
    REJECTED = "rejected"
    FAILED = "failed"


class ActionMutabilityLevel(str, Enum):
    READ = "read"
    PROPOSE = "propose"
    COMMIT = "commit"
    DESTRUCTIVE = "destructive"


class TransitionError(Exception):
    pass


class RetryableError(TransitionError):
    pass


class NonRetryableError(TransitionError):
    pass


ALLOWED_TRANSITIONS: dict[ActionState, frozenset[ActionState]] = {
    ActionState.PROPOSED: frozenset({
        ActionState.PENDING_APPROVAL,
        ActionState.APPROVED,
        ActionState.REJECTED,
        ActionState.FAILED,
    }),
    ActionState.PENDING_APPROVAL: frozenset({
        ActionState.APPROVED,
        ActionState.REJECTED,
        ActionState.FAILED,
    }),
    ActionState.APPROVED: frozenset({
        ActionState.COMMITTED,
        ActionState.FAILED,
    }),
    ActionState.COMMITTED: frozenset(),
    ActionState.REJECTED: frozenset(),
    ActionState.FAILED: frozenset({
        ActionState.PROPOSED,
    }),
}

INVALID_TRANSITIONS: list[tuple[ActionState, ActionState]] = [
    (ActionState.APPROVED, ActionState.PROPOSED),
    (ActionState.APPROVED, ActionState.PENDING_APPROVAL),
    (ActionState.PENDING_APPROVAL, ActionState.PROPOSED),
    (ActionState.COMMITTED, ActionState.APPROVED),
    (ActionState.COMMITTED, ActionState.PENDING_APPROVAL),
    (ActionState.COMMITTED, ActionState.PROPOSED),
    (ActionState.REJECTED, ActionState.PROPOSED),
    (ActionState.REJECTED, ActionState.PENDING_APPROVAL),
    (ActionState.REJECTED, ActionState.APPROVED),
]

STATE_TIMEOUTS: dict[ActionState, int | None] = {
    ActionState.PROPOSED: 600,
    ActionState.PENDING_APPROVAL: 1800,
    ActionState.APPROVED: 3600,
    ActionState.COMMITTED: None,
    ActionState.REJECTED: None,
    ActionState.FAILED: None,
}

RETRY_POLICY = {
    "max_retries": 3,
    "backoff_schedule": [
        {"attempt": 1, "backoff_seconds": 1, "jitter_seconds": 0.5},
        {"attempt": 2, "backoff_seconds": 4, "jitter_seconds": 1},
        {"attempt": 3, "backoff_seconds": 16, "jitter_seconds": 2},
    ],
}

RETRYABLE_ERRORS = frozenset({
    "database_connection_error",
    "temporary_service_unavailable",
    "network_timeout",
    "deadlock_detected",
    "partial_write_failure",
    "resource_exhausted",
    "internal_server_error",
})

NON_RETRYABLE_ERRORS = frozenset({
    "validation_error",
    "authorization_denied",
    "duplicate_key_violation",
    "precondition_failed",
    "malformed_payload",
    "resource_not_found",
    "not_implemented",
})


def validate_state_before_persist(state: Any) -> ActionState:
    if isinstance(state, ActionState):
        return state

    raw_value = getattr(state, "value", state)
    try:
        return ActionState(str(raw_value))
    except ValueError as exc:
        raise TransitionError(f"Invalid lifecycle state for persistence: {state!r}") from exc


class FSMRetryPolicy:
    @staticmethod
    def should_retry(*, state: ActionState, retry_count: int) -> bool:
        return state == ActionState.FAILED and retry_count < RETRY_POLICY["max_retries"]

    @staticmethod
    def get_retry_delay_seconds(*, retry_count: int) -> float:
        attempt = max(1, retry_count)
        schedule = RETRY_POLICY["backoff_schedule"]
        bounded_attempt = min(attempt, len(schedule))
        backoff_entry = schedule[bounded_attempt - 1]
        return float(backoff_entry["backoff_seconds"]) + float(backoff_entry["jitter_seconds"])


class FSMTimeoutPolicy:
    @staticmethod
    def get_timeout_seconds(*, state: ActionState, override_seconds: int | None = None) -> int | None:
        if override_seconds is not None:
            return override_seconds
        return STATE_TIMEOUTS.get(state)

    @staticmethod
    def has_timed_out(
        *,
        state: ActionState,
        updated_at: datetime,
        reference_time: datetime | None = None,
        override_seconds: int | None = None,
    ) -> bool:
        timeout_seconds = FSMTimeoutPolicy.get_timeout_seconds(
            state=state,
            override_seconds=override_seconds,
        )
        if timeout_seconds is None:
            return False

        now = reference_time or datetime.now(UTC)
        elapsed = (now - updated_at).total_seconds()
        return elapsed > timeout_seconds


def can_transition(from_state: ActionState, to_state: ActionState) -> bool:
    return to_state in ALLOWED_TRANSITIONS.get(from_state, frozenset())


def validate_transition(
    from_state: ActionState,
    to_state: ActionState,
    context: dict[str, Any] | None = None,
) -> None:
    context = context or {}

    if from_state == to_state:
        raise TransitionError(
            f"No-op transition not allowed: {from_state.value} -> {to_state.value}"
        )

    if (from_state, to_state) in INVALID_TRANSITIONS:
        allowed = sorted(s.value for s in ALLOWED_TRANSITIONS[from_state])
        raise TransitionError(
            f"Invalid transition: {from_state.value} -> {to_state.value}. "
            f"Allowed targets: {allowed}"
        )

    if not can_transition(from_state, to_state):
        allowed = sorted(s.value for s in ALLOWED_TRANSITIONS[from_state])
        raise TransitionError(
            f"Transition not allowed: {from_state.value} -> {to_state.value}. "
            f"Allowed targets: {allowed}"
        )

    if to_state == ActionState.APPROVED and context.get("actor_type") == "assistant":
        raise TransitionError("Assistant cannot approve actions (suggest-only capability)")

    if (
        from_state == ActionState.PROPOSED
        and to_state == ActionState.APPROVED
        and context.get("requires_approval") is True
    ):
        raise TransitionError(
            "Action requires approval; must transition through pending_approval state"
        )


def classify_error(error_code: str) -> Literal["retryable", "non_retryable"]:
    if error_code in RETRYABLE_ERRORS:
        return "retryable"
    if error_code in NON_RETRYABLE_ERRORS:
        return "non_retryable"
    return "non_retryable"


@dataclass(frozen=True)
class StateTransitionEvent:
    action_id: str
    from_state: ActionState
    to_state: ActionState
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    reason: str = ""
    correlation_id: str = ""
    retry_attempt: int = 0
    error_code: str | None = None
    error_classification: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_id": self.action_id,
            "from_state": self.from_state.value,
            "to_state": self.to_state.value,
            "timestamp": self.timestamp.isoformat(),
            "reason": self.reason,
            "correlation_id": self.correlation_id,
            "retry_attempt": self.retry_attempt,
            "error_code": self.error_code,
            "error_classification": self.error_classification,
            "metadata": self.metadata,
        }


@dataclass
class StateMachine:
    action_id: str
    state: ActionState = ActionState.PROPOSED
    retry_count: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    transitions: list[StateTransitionEvent] = field(default_factory=list)

    def transition_to(
        self,
        target_state: ActionState,
        *,
        reason: str = "",
        correlation_id: str = "",
        context: dict[str, Any] | None = None,
        error_code: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StateTransitionEvent:
        _enforce_fsm_caller()
        validate_state_before_persist(self.state)
        validate_transition(self.state, target_state, context=context)

        error_classification = None
        if target_state == ActionState.FAILED and error_code:
            error_classification = classify_error(error_code)

        event = StateTransitionEvent(
            action_id=self.action_id,
            from_state=self.state,
            to_state=target_state,
            timestamp=datetime.now(UTC),
            reason=reason,
            correlation_id=correlation_id,
            retry_attempt=self.retry_count,
            error_code=error_code,
            error_classification=error_classification,
            metadata=metadata or {},
        )

        self.state = target_state
        self.updated_at = event.timestamp
        self.transitions.append(event)

        if target_state == ActionState.FAILED:
            self.retry_count += 1

        return event

    def is_terminal(self) -> bool:
        if self.state in {ActionState.COMMITTED, ActionState.REJECTED}:
            return True
        if self.state == ActionState.FAILED and self.retry_count >= RETRY_POLICY["max_retries"]:
            return True
        return False

    def can_retry(self) -> bool:
        return FSMRetryPolicy.should_retry(state=self.state, retry_count=self.retry_count)

    def get_retry_delay(self) -> timedelta:
        if not self.can_retry():
            return timedelta(0)
        return timedelta(seconds=FSMRetryPolicy.get_retry_delay_seconds(retry_count=self.retry_count))

    def get_timeout_seconds(self) -> int | None:
        return STATE_TIMEOUTS.get(self.state)

    def has_timed_out(self, reference_time: datetime | None = None) -> bool:
        return FSMTimeoutPolicy.has_timed_out(
            state=self.state,
            updated_at=self.updated_at,
            reference_time=reference_time,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "action_id": self.action_id,
            "state": self.state.value,
            "retry_count": self.retry_count,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "is_terminal": self.is_terminal(),
            "can_retry": self.can_retry(),
            "transitions": [t.to_dict() for t in self.transitions],
        }
