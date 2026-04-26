from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Optional


@dataclass(frozen=True)
class ActorContext:
    actor_type: str  # user | assistant | system_worker | scheduler
    actor_id: str
    household_id: str
    auth_scope: str


@dataclass
class ExecutionContext:
    actor_type: str  # 'api_user' | 'assistant' | 'system_worker'
    user_id: Optional[str] = None
    household_id: str = ""
    request_id: str = ""
    trace_id: str = ""
    initiated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)

    def _canonical_actor_type(self) -> str:
        raw = str(self.actor_type or "").strip().lower()
        if raw == "api_user":
            return "user"
        if raw in {"user", "assistant", "system_worker", "scheduler"}:
            return raw
        raise ValueError(f"Unknown actor_type: {self.actor_type!r}")

    def to_actor_context(self) -> ActorContext:
        canonical_actor_type = self._canonical_actor_type()
        actor_id = str(self.user_id or "system")
        auth_scope = "system" if canonical_actor_type in {"system_worker", "scheduler"} else "household"
        return ActorContext(
            actor_type=canonical_actor_type,
            actor_id=actor_id,
            household_id=self.household_id,
            auth_scope=auth_scope,
        )

    def to_fsm_context(self) -> dict[str, Any]:
        actor_ctx = self.to_actor_context()
        return {
            "actor_type": actor_ctx.actor_type,
            "user_id": self.user_id,
            "household_id": actor_ctx.household_id,
            "auth_scope": actor_ctx.auth_scope,
        }

    def to_event_metadata(self) -> dict[str, Any]:
        actor_ctx = self.to_actor_context()
        return {
            "actor_type": actor_ctx.actor_type,
            "user_id": self.user_id,
            "subject_id": actor_ctx.actor_id,
            "household_id": actor_ctx.household_id,
            "auth_scope": actor_ctx.auth_scope,
            "actor_context": {
                "actor_type": actor_ctx.actor_type,
                "actor_id": actor_ctx.actor_id,
                "household_id": actor_ctx.household_id,
                "auth_scope": actor_ctx.auth_scope,
            },
            "request_id": self.request_id,
            "initiated_at": self.initiated_at.isoformat(),
            **self.metadata,
        }

    @classmethod
    def from_api_request(
        cls,
        household_id: str,
        actor_type: str,
        user_id: str | None = None,
        request_id: str = "",
    ) -> "ExecutionContext":
        return cls(
            actor_type=actor_type,
            household_id=household_id,
            user_id=user_id,
            request_id=request_id,
        )

    @classmethod
    def system_context(cls, household_id: str, trigger_type: str = "scheduled") -> "ExecutionContext":
        return cls(
            actor_type="system_worker",
            household_id=household_id,
            metadata={"trigger_type": trigger_type},
        )