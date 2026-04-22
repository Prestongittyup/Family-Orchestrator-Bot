from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from apps.api.core.state_machine import ActionState


class StateMutationViolation(Exception):
    pass


@dataclass
class MutationAttempt:
    object_id: str
    field: str
    from_value: Any
    to_value: Any
    source: str


class StateMutationFirewall:
    """
    Central enforcement layer that prevents any direct mutation
    of lifecycle state outside StateMachine.
    """

    def __init__(self) -> None:
        self._enabled = True
        self._authorized_object_ids: ContextVar[set[str]] = ContextVar(
            "authorized_state_mutations",
            default=set(),
        )
        self._violation_log: list[MutationAttempt] = []

    def transition(
        self,
        *,
        state_machine,
        obj,
        to_state: str,
        reason: str,
        context: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        correlation_id: str = "",
    ):
        return state_machine.transition_to(
            ActionState(to_state),
            reason=reason,
            correlation_id=correlation_id,
            context=context,
            metadata=metadata or {},
        )

    @contextmanager
    def authorize_mutation(self, object_id: str):
        current = self._authorized_object_ids.get()
        next_ids = set(current)
        next_ids.add(object_id)
        token = self._authorized_object_ids.set(next_ids)
        try:
            yield
        finally:
            self._authorized_object_ids.reset(token)

    def can_mutate(self, object_id: str) -> bool:
        return object_id in self._authorized_object_ids.get()

    def block_direct_mutation(self, obj, field: str, value: Any, *, source: str = "runtime") -> None:
        if not self._enabled:
            return

        attempt = MutationAttempt(
            object_id=getattr(obj, "action_id", "unknown"),
            field=field,
            from_value=getattr(obj, field, None),
            to_value=value,
            source=source,
        )
        self.observe_attempt(attempt)
        raise StateMutationViolation(
            f"Direct mutation blocked: {attempt.object_id} -> {field} = {value}. "
            "Use StateMachine.transition_to()."
        )

    def observe_attempt(self, attempt: MutationAttempt) -> None:
        self._violation_log.append(attempt)


FIREWALL = StateMutationFirewall()
