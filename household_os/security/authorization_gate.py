from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable


class ActorType(str, Enum):
    USER = "user"
    API_USER = "api_user"
    ASSISTANT = "assistant"
    SYSTEM_WORKER = "system_worker"
    SCHEDULER = "scheduler"


@dataclass(frozen=True)
class ActorIdentity:
    actor_type: ActorType
    subject_id: str
    session_id: str | None
    verified: bool


@dataclass(frozen=True)
class AuthorizationResult:
    allowed: bool
    reason: str = ""


class AuthorizationGate:
    """Single canonical trust-boundary authorization gate."""

    def __init__(
        self,
        *,
        verify_household_owner: Callable[[str, str], bool],
        require_system_worker_proof: bool = True,
    ) -> None:
        self._verify_household_owner = verify_household_owner
        self._require_system_worker_proof = require_system_worker_proof

    def normalize_actor_identity(self, raw_actor: Any) -> ActorIdentity:
        """
        Normalize and validate actor identity.

        No silent defaults are permitted. Unknown actor types hard-fail.
        """
        if isinstance(raw_actor, ActorIdentity):
            actor = raw_actor
        elif isinstance(raw_actor, dict):
            raw_type_original = str(raw_actor.get("actor_type", ""))
            raw_type = raw_type_original.strip().lower()
            if raw_type_original != raw_type:
                raise PermissionError("actor_type must be canonical lowercase without surrounding whitespace")
            try:
                actor_type = ActorType(raw_type)
            except ValueError as exc:
                raise PermissionError(f"Unknown actor_type: {raw_type!r}") from exc

            subject_id = str(raw_actor.get("subject_id") or raw_actor.get("user_id") or "").strip()
            session_id = raw_actor.get("session_id")
            if session_id is not None:
                session_id = str(session_id)
            verified = bool(raw_actor.get("verified", False))
            actor = ActorIdentity(
                actor_type=actor_type,
                subject_id=subject_id,
                session_id=session_id,
                verified=verified,
            )
        else:
            raise PermissionError("Actor identity missing or malformed")

        if not actor.verified:
            raise PermissionError("Actor identity is not verified")

        if actor.actor_type in {ActorType.USER, ActorType.API_USER, ActorType.ASSISTANT} and not actor.subject_id:
            raise PermissionError("Verified actor is missing subject_id")

        return actor

    def authorize_read(self, actor: ActorIdentity, household_id: str, resource_type: str) -> AuthorizationResult:
        if actor.actor_type in {ActorType.SYSTEM_WORKER, ActorType.SCHEDULER}:
            return AuthorizationResult(True, "system_worker read allowed")

        if not self._verify_household_owner(household_id, actor.subject_id):
            return AuthorizationResult(False, f"{actor.subject_id} does not own household {household_id}")

        return AuthorizationResult(True, f"{actor.actor_type.value} read allowed for {resource_type}")

    def authorize_write(self, actor: ActorIdentity, household_id: str, action_type: str) -> AuthorizationResult:
        if actor.actor_type in {ActorType.SYSTEM_WORKER, ActorType.SCHEDULER}:
            return AuthorizationResult(True, "system_worker write allowed")

        if not self._verify_household_owner(household_id, actor.subject_id):
            return AuthorizationResult(False, f"{actor.subject_id} does not own household {household_id}")

        if actor.actor_type == ActorType.ASSISTANT:
            # Assistant is suggest-only for write operations.
            return AuthorizationResult(False, f"assistant cannot perform write action {action_type}")

        return AuthorizationResult(True, f"{actor.actor_type.value} write allowed for {action_type}")

    def authorize_action(
        self,
        actor: ActorIdentity,
        action: str,
        household_id: str,
        context: dict[str, Any] | None = None,
    ) -> AuthorizationResult:
        context = context or {}

        if action == "LEGACY_EXECUTION" and not bool(context.get("legacy_execution", False)):
            return AuthorizationResult(False, "legacy execution requires explicit authorization flag")

        if actor.actor_type in {ActorType.SYSTEM_WORKER, ActorType.SCHEDULER}:
            if self._require_system_worker_proof:
                if not bool(context.get("system_worker_verified", False)):
                    return AuthorizationResult(False, "system_worker cryptographic proof missing")
            return AuthorizationResult(True, f"system_worker action allowed: {action}")

        # Household ownership is mandatory for non-system actors.
        if not self._verify_household_owner(household_id, actor.subject_id):
            return AuthorizationResult(False, f"{actor.subject_id} does not own household {household_id}")

        if actor.actor_type == ActorType.ASSISTANT and action in {"APPROVE", "REJECT", "EXECUTE"}:
            action_lower = action.lower()
            if action == "APPROVE":
                return AuthorizationResult(False, "assistant cannot approve actions")
            return AuthorizationResult(False, f"assistant cannot {action_lower} actions")

        return AuthorizationResult(True, f"{actor.actor_type.value} action allowed: {action}")
