from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field

from apps.api.product_surface.contracts import ActionCard, ChatResponse, UIBootstrapState, UIPatch


SyncStatus = Literal["synced", "lagging", "desynced"]


class ChatSessionState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    session_id: str
    message_history: list[str] = Field(default_factory=list)
    pending_action_cards: list[ActionCard] = Field(default_factory=list)
    last_ui_patch: list[UIPatch] = Field(default_factory=list)
    awaiting_confirmation: bool = False
    last_response_fingerprint: str | None = None


class FrontendState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    snapshot: UIBootstrapState
    applied_patches: list[UIPatch] = Field(default_factory=list)
    pending_actions: list[ActionCard] = Field(default_factory=list)
    chat_sessions: dict[str, ChatSessionState] = Field(default_factory=dict)
    last_sync_watermark: str
    sync_status: SyncStatus = "synced"
    materialized_index: dict[str, dict[str, Any]] = Field(default_factory=dict)


class FrontendRuntimeEngine:
    """Deterministic frontend runtime contract implementation."""

    def initialize(self, *, snapshot: UIBootstrapState) -> FrontendState:
        return FrontendState(
            snapshot=snapshot,
            applied_patches=[],
            pending_actions=[],
            chat_sessions={},
            last_sync_watermark=snapshot.source_watermark,
            sync_status="synced",
            materialized_index=self.reconstruct_materialized(snapshot=snapshot, patches=[]),
        )

    def apply_backend_snapshot(self, *, state: FrontendState, snapshot: UIBootstrapState) -> FrontendState:
        # Backend always wins. Local patch history is discarded on authoritative snapshot.
        return FrontendState(
            snapshot=snapshot,
            applied_patches=[],
            pending_actions=state.pending_actions,
            chat_sessions=state.chat_sessions,
            last_sync_watermark=snapshot.source_watermark,
            sync_status="synced",
            materialized_index=self.reconstruct_materialized(snapshot=snapshot, patches=[]),
        )

    def apply_patches(self, *, state: FrontendState, patches: list[UIPatch]) -> FrontendState:
        ordered = self._ordered_unique_patches(patches)
        if not ordered:
            return state

        existing_keys = {
            self._patch_key(patch)
            for patch in state.applied_patches
        }
        to_apply = [patch for patch in ordered if self._patch_key(patch) not in existing_keys]
        if not to_apply:
            # Duplicate replay; idempotent no-op.
            return state

        base_version = max((patch.version for patch in state.applied_patches), default=state.snapshot.snapshot_version)
        expected_next = base_version + 1
        for patch in to_apply:
            if patch.version != expected_next:
                return self.on_missing_patch_version(state=state)
            expected_next += 1

        applied = [*state.applied_patches, *to_apply]
        materialized = self.reconstruct_materialized(snapshot=state.snapshot, patches=applied)
        last_patch = to_apply[-1]
        return FrontendState(
            snapshot=state.snapshot,
            applied_patches=applied,
            pending_actions=state.pending_actions,
            chat_sessions=state.chat_sessions,
            last_sync_watermark=f"{state.last_sync_watermark}:{last_patch.version}",
            sync_status="synced",
            materialized_index=materialized,
        )

    def reconstruct_materialized(self, *, snapshot: UIBootstrapState, patches: list[UIPatch]) -> dict[str, dict[str, Any]]:
        index = self._snapshot_index(snapshot)
        ordered = self._ordered_unique_patches(patches)

        for patch in ordered:
            key = self._entity_key(patch.entity_type, patch.entity_id)
            if patch.change_type == "delete":
                index.pop(key, None)
            else:
                index[key] = dict(patch.payload)

        return dict(sorted(index.items(), key=lambda item: item[0]))

    def apply_chat_response(
        self,
        *,
        state: FrontendState,
        session_id: str,
        response: ChatResponse,
    ) -> FrontendState:
        response_fingerprint = self._chat_fingerprint(response)
        current_session = state.chat_sessions.get(session_id)
        if current_session is None:
            current_session = ChatSessionState(session_id=session_id)

        message_history = list(current_session.message_history)
        if current_session.last_response_fingerprint != response_fingerprint:
            message_history.append(response.assistant_message)

        next_session = ChatSessionState(
            session_id=session_id,
            message_history=message_history,
            pending_action_cards=response.action_cards,
            last_ui_patch=response.ui_patch,
            awaiting_confirmation=response.requires_confirmation,
            last_response_fingerprint=response_fingerprint,
        )

        chat_sessions = dict(state.chat_sessions)
        chat_sessions[session_id] = next_session
        pending_actions = sorted(response.action_cards, key=lambda row: row.id)

        interim = FrontendState(
            snapshot=state.snapshot,
            applied_patches=state.applied_patches,
            pending_actions=pending_actions,
            chat_sessions=chat_sessions,
            last_sync_watermark=state.last_sync_watermark,
            sync_status=state.sync_status,
            materialized_index=state.materialized_index,
        )
        return self.apply_patches(state=interim, patches=response.ui_patch)

    def on_stale_snapshot(self, *, state: FrontendState) -> FrontendState:
        return FrontendState(
            snapshot=state.snapshot,
            applied_patches=state.applied_patches,
            pending_actions=state.pending_actions,
            chat_sessions=state.chat_sessions,
            last_sync_watermark=state.last_sync_watermark,
            sync_status="lagging",
            materialized_index=state.materialized_index,
        )

    def on_missing_patch_version(self, *, state: FrontendState) -> FrontendState:
        return FrontendState(
            snapshot=state.snapshot,
            applied_patches=state.applied_patches,
            pending_actions=state.pending_actions,
            chat_sessions=state.chat_sessions,
            last_sync_watermark=state.last_sync_watermark,
            sync_status="desynced",
            materialized_index=state.materialized_index,
        )

    def on_chat_desync(self, *, state: FrontendState, session_id: str) -> FrontendState:
        chat_sessions = dict(state.chat_sessions)
        current = chat_sessions.get(session_id)
        if current is None:
            return self.on_missing_patch_version(state=state)
        chat_sessions[session_id] = ChatSessionState(
            session_id=session_id,
            message_history=current.message_history,
            pending_action_cards=[],
            last_ui_patch=[],
            awaiting_confirmation=False,
            last_response_fingerprint=current.last_response_fingerprint,
        )
        return FrontendState(
            snapshot=state.snapshot,
            applied_patches=state.applied_patches,
            pending_actions=state.pending_actions,
            chat_sessions=chat_sessions,
            last_sync_watermark=state.last_sync_watermark,
            sync_status="desynced",
            materialized_index=state.materialized_index,
        )

    @staticmethod
    def _snapshot_index(snapshot: UIBootstrapState) -> dict[str, dict[str, Any]]:
        index: dict[str, dict[str, Any]] = {}
        index[FrontendRuntimeEngine._entity_key("family", snapshot.family.family_id)] = snapshot.family.model_dump()
        for row in snapshot.active_plans:
            index[FrontendRuntimeEngine._entity_key("plan", row.plan_id)] = row.model_dump()
        for section in (
            snapshot.task_board.pending,
            snapshot.task_board.in_progress,
            snapshot.task_board.completed,
            snapshot.task_board.failed,
        ):
            for row in section:
                index[FrontendRuntimeEngine._entity_key("task", row.task_id)] = row.model_dump()
        for row in snapshot.calendar.events:
            index[FrontendRuntimeEngine._entity_key("event", row.event_id)] = row.model_dump()
        for row in snapshot.notifications:
            index[FrontendRuntimeEngine._entity_key("notification", row.notification_id)] = row.model_dump()
        return dict(sorted(index.items(), key=lambda item: item[0]))

    @staticmethod
    def _ordered_unique_patches(patches: list[UIPatch]) -> list[UIPatch]:
        ordered = sorted(
            patches,
            key=lambda row: (
                row.version,
                row.source_timestamp,
                row.entity_type,
                row.entity_id,
                row.change_type,
            ),
        )
        seen = set()
        unique: list[UIPatch] = []
        for row in ordered:
            dedupe_key = (row.entity_id, row.version)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            unique.append(row)
        return unique

    @staticmethod
    def _entity_key(entity_type: str, entity_id: str) -> str:
        return f"{entity_type}:{entity_id}"

    @staticmethod
    def _patch_key(patch: UIPatch) -> str:
        return f"{patch.entity_type}:{patch.entity_id}:{patch.version}:{patch.change_type}"

    @staticmethod
    def _chat_fingerprint(response: ChatResponse) -> str:
        canonical = json.dumps(
            {
                "assistant_message": response.assistant_message,
                "requires_confirmation": response.requires_confirmation,
                "action_cards": [row.model_dump() for row in response.action_cards],
                "ui_patch": [
                    {
                        "entity_type": row.entity_type,
                        "entity_id": row.entity_id,
                        "change_type": row.change_type,
                        "payload": row.payload,
                        "version": row.version,
                        "source_timestamp": row.source_timestamp.isoformat(),
                    }
                    for row in response.ui_patch
                ],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SyncStrategySpec:
    poll_ms_synced: int = 30000
    poll_ms_lagging: int = 10000
    poll_ms_desynced: int = 3000

    def polling_interval_ms(self, *, sync_status: SyncStatus) -> int:
        if sync_status == "synced":
            return self.poll_ms_synced
        if sync_status == "lagging":
            return self.poll_ms_lagging
        return self.poll_ms_desynced

    def reconcile(self, *, runtime: FrontendRuntimeEngine, state: FrontendState, backend_snapshot: UIBootstrapState) -> FrontendState:
        # Conflict resolution: backend authoritative state always wins.
        return runtime.apply_backend_snapshot(state=state, snapshot=backend_snapshot)


class ActionExecutionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    family_id: str
    session_id: str
    action_card_id: str
    endpoint: str
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str
    retry_count: int = 0


class ActionExecutionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal["succeeded", "failed"]
    response_payload: dict[str, Any] = Field(default_factory=dict)


class ActionExecutionBinder:
    """ActionCard -> click -> API call -> UI update contract with idempotent execution."""

    def __init__(self) -> None:
        self._execution_log: dict[str, ActionExecutionResult] = {}

    def build_request(
        self,
        *,
        family_id: str,
        session_id: str,
        action_card: ActionCard,
        endpoint: str,
        payload_override: dict[str, Any] | None = None,
    ) -> ActionExecutionRequest:
        payload = payload_override if payload_override is not None else action_card.required_action_payload
        canonical = json.dumps(
            {
                "family_id": family_id,
                "session_id": session_id,
                "action_card_id": action_card.id,
                "endpoint": endpoint,
                "payload": payload,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return ActionExecutionRequest(
            family_id=family_id,
            session_id=session_id,
            action_card_id=action_card.id,
            endpoint=endpoint,
            payload=dict(payload),
            idempotency_key=f"ui-action-{digest[:24]}",
            retry_count=0,
        )

    def execute(
        self,
        *,
        request: ActionExecutionRequest,
        call_api: Callable[[ActionExecutionRequest], ActionExecutionResult],
    ) -> ActionExecutionResult:
        existing = self._execution_log.get(request.idempotency_key)
        if existing is not None:
            return existing

        result = call_api(request)
        self._execution_log[request.idempotency_key] = result
        return result

    def optimistic_patch(self, *, action_card: ActionCard, version: int) -> UIPatch:
        """Optional optimistic update; must be reversible by backend snapshot reconciliation."""
        return UIPatch(
            entity_type="notification",
            entity_id=f"optimistic:{action_card.id}",
            change_type="create",
            payload={
                "notification_id": f"optimistic:{action_card.id}",
                "title": "Applying action",
                "message": f"Applying {action_card.title}...",
                "level": "info",
                "related_entity": action_card.related_entity,
            },
            version=version,
            source_timestamp=datetime.now(tz=UTC),
        )
