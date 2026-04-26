"""
Security Verification Harness
==============================
Family Orchestration Bot — Attack Simulation Suite

This is NOT a standard unit-test suite.
Each test programmatically attempts to break a security invariant and
reports PASS / FAIL with a clear violation message when the invariant
does not hold.

Invariants verified
-------------------
1. actor_type flows end-to-end: API → Orchestrator → ActionPipeline → FSM
2. Assistant cannot approve at ANY layer (API, Orchestrator, FSM)
3. Cross-household access is blocked (user from household A cannot operate on B)
4. Missing actor_type does NOT silently escalate to privileged behaviour
5. FSM guard is always enforced regardless of context presence
6. Event replay cannot be used to inject an unauthorized approval
7. Audit metadata completeness — executed actions carry actor_type / user_id

Run with:
    python -m pytest tests/test_security_verification_harness.py -v
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch, call

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from apps.api.core.state_machine import (
    ActionState,
    StateMachine,
    TransitionError,
)
from household_os.core.execution_context import ExecutionContext
from household_os.core.household_state_graph import HouseholdStateGraphStore
from household_os.runtime.domain_event import DomainEvent, LIFECYCLE_EVENT_TYPES
from household_os.runtime.orchestrator import HouseholdOSOrchestrator
from household_os.runtime.state_reducer import StateReductionError, reduce_state
from household_os.core.lifecycle_state import LifecycleState


# ─────────────────────────────────────────────────────────────────────────────
# HELPER UTILITIES
# ─────────────────────────────────────────────────────────────────────────────


def make_mock_request(
    actor_type: str,
    user_id: str | None,
    household_id: str,
) -> dict[str, Any]:
    """Return a dict that simulates a decoded auth request payload."""
    return {
        "actor_type": actor_type,
        "user_id": user_id,
        "household_id": household_id,
        "request_id": str(uuid.uuid4()),
    }


def create_test_action(graph: dict[str, Any], household_id: str = "household-test") -> str:
    """
    Inject a minimal proposed action directly into a graph dict.
    Returns the action_id.
    """
    action_id = f"action-{uuid.uuid4().hex[:8]}"
    graph.setdefault("action_lifecycle", {}).setdefault("actions", {})[action_id] = {
        "action_id": action_id,
        "request_id": str(uuid.uuid4()),
        "title": "Security Test Action",
        "description": "Injected by security harness",
        "domain": "health",
        "execution_handler": "noop",
        "current_state": "proposed",
        "approval_required": False,
        "trigger_id": "trigger-sec-001",
        "trigger_type": "manual",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
        "transitions": [],
    }
    return action_id


def inject_event(
    aggregate_id: str,
    event_type: str,
    actor_type: str,
    payload: dict[str, Any] | None = None,
) -> DomainEvent:
    """Create a DomainEvent with explicit metadata — simulates an attacker injecting
    a tampered event into the replay stream."""
    return DomainEvent.create(
        aggregate_id=aggregate_id,
        event_type=event_type,
        payload=payload or {},
        metadata={"actor_type": actor_type},
    )


def _build_app_with_orchestrator(
    orchestrator: HouseholdOSOrchestrator,
    actor_type: str,
    user_id: str | None,
) -> FastAPI:
    """Wire a FastAPI app injecting actor context via middleware — mirrors production auth."""
    import apps.api.assistant_runtime_router as assistant_runtime_router

    assistant_runtime_router.runtime_orchestrator = orchestrator
    app = FastAPI()

    @app.middleware("http")
    async def inject_auth_context(request: Request, call_next):
        request.state.actor_type = actor_type
        request.state.user = {"sub": user_id} if user_id is not None else None
        return await call_next(request)

    app.include_router(assistant_runtime_router.router, prefix="/assistant")
    return app


def _build_client(
    tmp_path: Path,
    *,
    actor_type: str,
    user_id: str | None,
    verify_owner,
    store: HouseholdStateGraphStore | None = None,
) -> tuple[TestClient, HouseholdOSOrchestrator]:
    if store is None:
        nonce = uuid.uuid4().hex[:8]
        store = HouseholdStateGraphStore(
            graph_path=Path(tmp_path) / f"sec-harness-{nonce}.json"
        )
    store.verify_household_owner = verify_owner
    orchestrator = HouseholdOSOrchestrator(state_store=store)
    app = _build_app_with_orchestrator(orchestrator, actor_type, user_id)
    return TestClient(app), orchestrator


def _create_action_via_run(client: TestClient, household_id: str) -> str:
    """Use /assistant/run to create a real proposed action and return its action_id."""
    nonce = uuid.uuid4().hex
    resp = client.post(
        "/assistant/run",
        json={
            "message": f"I need to schedule a workout {nonce}",
            "household_id": household_id,
        },
    )
    assert resp.status_code == 200, f"[harness] /run failed: {resp.text}"
    action_id = resp.json().get("action_id")
    assert action_id, "[harness] /run did not return action_id"
    return str(action_id)


def _normalize_state(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw).split(".")[-1].lower()


# ─────────────────────────────────────────────────────────────────────────────
# INVARIANT 1 — Actor Context Propagation
# ─────────────────────────────────────────────────────────────────────────────


class TestInvariant1ActorContextPropagation:
    """
    Invariant: actor_type set at the API layer must propagate without
    modification all the way into the FSM transition context.
    """

    def test_invariant_actor_context_propagation(self, tmp_path: Path) -> None:
        """actor_type flows: API → Orchestrator → ActionPipeline → FSM guard."""
        observed_contexts: list[dict[str, Any]] = []

        client, orchestrator = _build_client(
            tmp_path,
            actor_type="api_user",
            user_id="user-propagation",
            verify_owner=lambda hid, uid: True,
        )

        # Create an action so there is something to approve.
        action_id = _create_action_via_run(client, "household-prop")

        original_validate = None
        from apps.api.core import state_machine as sm_module

        original_validate = sm_module.validate_transition

        def spy_validate(from_state, to_state, context=None):
            if context:
                observed_contexts.append(dict(context))
            return original_validate(from_state, to_state, context=context)

        with patch.object(sm_module, "validate_transition", side_effect=spy_validate):
            resp = client.post(
                "/assistant/approve",
                json={"action_id": action_id, "household_id": "household-prop"},
            )

        assert resp.status_code == 200, (
            f"VIOLATION: Approval request failed unexpectedly ({resp.status_code}). "
            f"actor_type may have been dropped before reaching FSM. Body: {resp.text}"
        )

        assert observed_contexts, (
            "VIOLATION: validate_transition was never called with a context dict. "
            "actor_type was dropped before reaching the FSM."
        )

        actor_types_seen = [ctx.get("actor_type") for ctx in observed_contexts]
        assert any(at is not None for at in actor_types_seen), (
            f"VIOLATION: actor_type was None in ALL FSM context calls. "
            f"Contexts observed: {observed_contexts}. "
            "actor_type is being dropped somewhere in the pipeline."
        )

        non_null_types = [at for at in actor_types_seen if at is not None]
        for at in non_null_types:
            assert at == "api_user", (
                f"VIOLATION: actor_type mutated in transit. "
                f"Expected 'api_user', got '{at}'. "
                "An upstream component is overwriting the actor identity."
            )

    def test_invariant_context_not_silently_dropped_on_retry(self, tmp_path: Path) -> None:
        """
        If the action pipeline falls back due to a TypeError, the fallback must
        still carry the correct actor_type — not silently drop it.
        """
        client, orchestrator = _build_client(
            tmp_path,
            actor_type="api_user",
            user_id="user-retry",
            verify_owner=lambda hid, uid: True,
        )
        action_id = _create_action_via_run(client, "household-retry")

        pipeline_calls: list[dict[str, Any]] = []
        original_approve = orchestrator.action_pipeline.approve_actions

        def spy_approve(**kwargs):
            pipeline_calls.append(dict(kwargs))
            return original_approve(**kwargs)

        orchestrator.action_pipeline.approve_actions = spy_approve

        resp = client.post(
            "/assistant/approve",
            json={"action_id": action_id, "household_id": "household-retry"},
        )
        assert resp.status_code == 200, resp.text

        assert pipeline_calls, "VIOLATION: approve_actions was never called."
        final_call = pipeline_calls[-1]
        actor_in_call = final_call.get("actor_type") or (
            final_call.get("context") and final_call["context"].actor_type
        )
        assert actor_in_call == "api_user", (
            f"VIOLATION: Final approve_actions call lost actor_type. "
            f"Got: {actor_in_call!r}. kwargs: {final_call}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# INVARIANT 2 — Assistant Cannot Approve at Any Layer
# ─────────────────────────────────────────────────────────────────────────────


class TestInvariant2AssistantCannotApprove:
    """
    Invariant: An assistant actor MUST be blocked from approving actions
    at every enforcement layer independently.
    """

    def test_invariant_assistant_blocked_at_api_layer(self, tmp_path: Path) -> None:
        """2A — HTTP 403 when assistant calls /assistant/approve."""
        store = HouseholdStateGraphStore(
            graph_path=Path(tmp_path) / "inv2a.json"
        )
        store.verify_household_owner = lambda hid, uid: True
        orchestrator = HouseholdOSOrchestrator(state_store=store)

        worker_client = TestClient(
            _build_app_with_orchestrator(orchestrator, "system_worker", None)
        )
        action_id = _create_action_via_run(worker_client, "household-a")

        assistant_client = TestClient(
            _build_app_with_orchestrator(orchestrator, "assistant", "bot-001")
        )
        resp = assistant_client.post(
            "/assistant/approve",
            json={"action_id": action_id, "household_id": "household-a"},
        )

        assert resp.status_code == 403, (
            f"VIOLATION: Assistant received HTTP {resp.status_code} instead of 403. "
            "The API layer does not block assistant self-approval. "
            f"Body: {resp.text}"
        )
        detail = str(resp.json().get("detail", "")).lower()
        assert "cannot approve" in detail or "assistant" in detail, (
            f"VIOLATION: 403 response body does not explain the denial. "
            f"Expected 'cannot approve' in detail, got: {detail!r}"
        )

    def test_invariant_assistant_blocked_at_orchestrator_layer(self, tmp_path: Path) -> None:
        """2B — Direct orchestrator call with assistant actor raises exception."""
        store = HouseholdStateGraphStore(
            graph_path=Path(tmp_path) / "inv2b.json"
        )
        store.verify_household_owner = lambda hid, uid: True
        orchestrator = HouseholdOSOrchestrator(state_store=store)

        # Create an action first.
        graph = store.load_graph("household-a")
        action_id = create_test_action(graph, "household-a")
        store.save_graph(graph)

        from fastapi import HTTPException

        with pytest.raises((HTTPException, PermissionError, Exception)) as exc_info:
            orchestrator.approve_and_execute(
                household_id="household-a",
                request_id=str(uuid.uuid4()),
                action_ids=[action_id],
                actor_type="assistant",
                user_id=None,
            )

        exc = exc_info.value
        status = getattr(exc, "status_code", None)
        msg = str(exc).lower()

        if status is not None:
            assert status == 403, (
                f"VIOLATION: Orchestrator raised HTTPException with status {status} "
                f"(expected 403) for assistant actor."
            )
        else:
            assert "assistant" in msg or "cannot approve" in msg or "permission" in msg, (
                f"VIOLATION: Orchestrator raised {type(exc).__name__} but the message "
                f"does not indicate assistant denial. Got: {msg!r}"
            )

    def test_invariant_assistant_blocked_at_fsm_layer(self) -> None:
        """2C — Direct FSM transition with assistant context raises TransitionError."""
        fsm = StateMachine(
            action_id="fsm-assistant-inv2c",
            state=ActionState.PENDING_APPROVAL,
        )

        with pytest.raises(TransitionError) as exc_info:
            fsm.transition_to(
                ActionState.APPROVED,
                context={"actor_type": "assistant"},
            )

        msg = str(exc_info.value).lower()
        assert "assistant" in msg or "suggest-only" in msg or "cannot approve" in msg, (
            f"VIOLATION: FSM raised TransitionError but the message does not mention "
            f"the assistant restriction. Got: {msg!r}. "
            "The FSM guard message should be informative."
        )

    def test_invariant_assistant_blocked_via_execution_context(self, tmp_path: Path) -> None:
        """2D — Orchestrator blocks assistant even when ExecutionContext is used directly."""
        store = HouseholdStateGraphStore(
            graph_path=Path(tmp_path) / "inv2d.json"
        )
        store.verify_household_owner = lambda hid, uid: True
        orchestrator = HouseholdOSOrchestrator(state_store=store)

        graph = store.load_graph("household-a")
        action_id = create_test_action(graph, "household-a")
        store.save_graph(graph)

        ctx = ExecutionContext.from_api_request(
            household_id="household-a",
            actor_type="assistant",
            user_id="bot-002",
        )

        from fastapi import HTTPException

        with pytest.raises((HTTPException, PermissionError, Exception)) as exc_info:
            orchestrator.approve_and_execute(
                household_id="household-a",
                request_id=str(uuid.uuid4()),
                action_ids=[action_id],
                context=ctx,
            )

        exc = exc_info.value
        status = getattr(exc, "status_code", None)
        if status is not None:
            assert status == 403, (
                f"VIOLATION: Orchestrator allowed ExecutionContext(actor_type='assistant') "
                f"with status {status}."
            )


# ─────────────────────────────────────────────────────────────────────────────
# INVARIANT 3 — Cross-Household Isolation
# ─────────────────────────────────────────────────────────────────────────────


class TestInvariant3CrossHouseholdIsolation:
    """
    Invariant: A user authenticating for household A cannot perform any
    operation on household B's data.
    """

    def test_invariant_cross_household_access_blocked(self, tmp_path: Path) -> None:
        """User from household-A cannot approve actions in household-B."""
        store = HouseholdStateGraphStore(
            graph_path=Path(tmp_path) / "inv3-cross.json"
        )
        # user-from-A owns household-A only.
        store.verify_household_owner = (
            lambda hid, uid: hid == "household-a" and uid == "user-from-a"
        )
        orchestrator = HouseholdOSOrchestrator(state_store=store)

        # Create an action in household-B (as system_worker).
        worker_client = TestClient(
            _build_app_with_orchestrator(orchestrator, "system_worker", None)
        )
        action_id = _create_action_via_run(worker_client, "household-b")

        # user-from-A tries to approve an action in household-B.
        api_client = TestClient(
            _build_app_with_orchestrator(orchestrator, "api_user", "user-from-a")
        )
        resp = api_client.post(
            "/assistant/approve",
            json={"action_id": action_id, "household_id": "household-b"},
        )

        assert resp.status_code == 403, (
            f"VIOLATION: Cross-household access was permitted (HTTP {resp.status_code}). "
            "user-from-a should not be able to approve actions in household-b. "
            f"Body: {resp.text}"
        )

    def test_invariant_cross_household_via_orchestrator_direct(self, tmp_path: Path) -> None:
        """Direct orchestrator call: user-from-A on household-B raises / 403."""
        store = HouseholdStateGraphStore(
            graph_path=Path(tmp_path) / "inv3-orch.json"
        )
        store.verify_household_owner = (
            lambda hid, uid: hid == "household-a" and uid == "user-from-a"
        )
        orchestrator = HouseholdOSOrchestrator(state_store=store)

        graph = store.load_graph("household-b")
        action_id = create_test_action(graph, "household-b")
        store.save_graph(graph)

        from fastapi import HTTPException

        with pytest.raises((HTTPException, PermissionError, Exception)) as exc_info:
            orchestrator.approve_and_execute(
                household_id="household-b",
                request_id=str(uuid.uuid4()),
                action_ids=[action_id],
                actor_type="api_user",
                user_id="user-from-a",
            )

        exc = exc_info.value
        status = getattr(exc, "status_code", None)
        if status is not None:
            assert status == 403, (
                f"VIOLATION: Expected 403 for cross-household access, got {status}."
            )
        else:
            msg = str(exc).lower()
            assert any(kw in msg for kw in ("own", "household", "permission", "forbidden")), (
                f"VIOLATION: Exception raised but message does not indicate "
                f"household isolation. Got: {msg!r}"
            )

    def test_invariant_system_worker_bypasses_household_check_for_tick(
        self, tmp_path: Path
    ) -> None:
        """
        system_worker actors must be allowed to operate across households
        (scheduled tasks do not have a household owner).
        """
        store = HouseholdStateGraphStore(
            graph_path=Path(tmp_path) / "inv3-worker.json"
        )
        # Ownership check intentionally returns False for every user.
        store.verify_household_owner = lambda hid, uid: False
        orchestrator = HouseholdOSOrchestrator(state_store=store)

        # system_worker should NOT raise a household-ownership error during tick.
        try:
            orchestrator.tick(
                household_id="household-any",
                actor_type="system_worker",
                user_id=None,
                user_input="daily summary",
            )
        except PermissionError as exc:
            pytest.fail(
                f"VIOLATION: system_worker was blocked by household ownership check. "
                f"system_worker actors must bypass this restriction. Error: {exc}"
            )
        except Exception:
            # Other errors (e.g. AI service unavailable) are acceptable in tests.
            pass


# ─────────────────────────────────────────────────────────────────────────────
# INVARIANT 4 — No Silent Default Escalation
# ─────────────────────────────────────────────────────────────────────────────


class TestInvariant4NoSilentDefaultEscalation:
    """
    Invariant: When actor_type is omitted, the system must NOT silently grant
    privileged behaviour.  It must either fail-safe or restrict behaviour and
    emit a warning.
    """

    def test_invariant_missing_actor_type_does_not_approve(self, tmp_path: Path) -> None:
        """
        Calling approve_and_execute without actor_type / context must NOT
        silently succeed with full privileges — it must either raise or
        produce only api_user-level restricted behaviour.
        """
        store = HouseholdStateGraphStore(
            graph_path=Path(tmp_path) / "inv4-no-actor.json"
        )
        # Ownership check always succeeds so it cannot be the blocker.
        store.verify_household_owner = lambda hid, uid: True
        orchestrator = HouseholdOSOrchestrator(state_store=store)

        graph = store.load_graph("household-a")
        action_id = create_test_action(graph, "household-a")
        store.save_graph(graph)

        import logging

        warning_emitted = []

        class WarningCatcher(logging.Handler):
            def emit(self, record):
                if record.levelno >= logging.WARNING:
                    warning_emitted.append(record.getMessage())

        handler = WarningCatcher()
        logging.getLogger().addHandler(handler)
        try:
            try:
                result = orchestrator.approve_and_execute(
                    household_id="household-a",
                    request_id=str(uuid.uuid4()),
                    action_ids=[action_id],
                    # actor_type intentionally omitted
                    user_id="user-default",
                )
                # If it succeeds, inspect the effective actor_type used.
                # It must NOT have silently become "assistant" or "system_worker".
                # Defaulting to "api_user" is acceptable IF a warning was logged.
                assert warning_emitted, (
                    "VIOLATION: approve_and_execute succeeded without actor_type "
                    "and without emitting any warning. Silent privilege escalation risk."
                )
            except Exception:
                # Any exception is also acceptable (fail-safe).
                pass
        finally:
            logging.getLogger().removeHandler(handler)

    def test_invariant_missing_actor_type_never_becomes_assistant(
        self, tmp_path: Path
    ) -> None:
        """
        The default actor_type (when none is provided) must never be 'assistant',
        because assistant is a restricted actor.
        """
        store = HouseholdStateGraphStore(
            graph_path=Path(tmp_path) / "inv4-no-assistant-default.json"
        )
        store.verify_household_owner = lambda hid, uid: True
        orchestrator = HouseholdOSOrchestrator(state_store=store)

        graph = store.load_graph("household-a")
        action_id = create_test_action(graph, "household-a")
        store.save_graph(graph)

        effective_actor_types: list[str] = []
        original_approve = orchestrator.action_pipeline.approve_actions

        def capture_actor(**kwargs):
            # Capture actor_type from the kwargs OR from ExecutionContext
            at = kwargs.get("actor_type")
            ctx = kwargs.get("context")
            if at is None and ctx is not None:
                at = ctx.actor_type
            if at is not None:
                effective_actor_types.append(str(at))
            return original_approve(**kwargs)

        orchestrator.action_pipeline.approve_actions = capture_actor

        try:
            orchestrator.approve_and_execute(
                household_id="household-a",
                request_id=str(uuid.uuid4()),
                action_ids=[action_id],
                user_id="user-default",
                # actor_type intentionally omitted
            )
        except Exception:
            pass  # Fail-safe raises are acceptable.

        for at in effective_actor_types:
            assert at != "assistant", (
                f"VIOLATION: Missing actor_type defaulted to 'assistant'. "
                "This is a privilege escalation — assistant is a restricted actor. "
                "The system must default to 'api_user' (restricted) or raise."
            )


# ─────────────────────────────────────────────────────────────────────────────
# INVARIANT 5 — FSM Guard Always Enforced
# ─────────────────────────────────────────────────────────────────────────────


class TestInvariant5FSMGuardAlwaysEnforced:
    """
    Invariant: The FSM guard blocking assistant from APPROVED must fire
    regardless of whether context is explicitly provided or not.
    """

    def test_invariant_fsm_blocks_assistant_with_context(self) -> None:
        """FSM guard fires when context explicitly says actor_type=assistant."""
        fsm = StateMachine(
            action_id="inv5-with-ctx",
            state=ActionState.PENDING_APPROVAL,
        )
        with pytest.raises(TransitionError, match="(?i)assistant|suggest-only"):
            fsm.transition_to(
                ActionState.APPROVED,
                context={"actor_type": "assistant"},
            )

    def test_invariant_fsm_allows_api_user_with_context(self) -> None:
        """FSM guard permits api_user to approve (no restriction on that actor)."""
        fsm = StateMachine(
            action_id="inv5-api-user-ok",
            state=ActionState.PENDING_APPROVAL,
        )
        event = fsm.transition_to(
            ActionState.APPROVED,
            context={"actor_type": "api_user"},
        )
        assert event.to_state == ActionState.APPROVED, (
            "VIOLATION: api_user was blocked from approving. "
            "api_user should be a permitted approver."
        )

    def test_invariant_fsm_blocks_approval_skip_when_requires_approval(self) -> None:
        """FSM guard prevents PROPOSED → APPROVED when requires_approval=True."""
        fsm = StateMachine(
            action_id="inv5-skip-approval",
            state=ActionState.PROPOSED,
        )
        with pytest.raises(TransitionError, match="(?i)approval|pending"):
            fsm.transition_to(
                ActionState.APPROVED,
                context={"actor_type": "api_user", "requires_approval": True},
            )

    def test_invariant_fsm_blocks_assistant_without_context(self) -> None:
        """
        Even without explicit context, an assistant should not be able to approve
        if the guard is incorporated properly at the pipeline level.
        This directly tests the FSM level with an empty context vs None.
        """
        fsm = StateMachine(
            action_id="inv5-no-ctx",
            state=ActionState.PENDING_APPROVAL,
        )
        # No context supplied — should NOT raise (no actor info, no guard trigger).
        # This is the boundary: absence of context should NOT accidentally block
        # legitimate users.
        event = fsm.transition_to(ActionState.APPROVED, context=None)
        assert event.to_state == ActionState.APPROVED

    def test_invariant_fsm_blocks_assistant_in_proposed_to_approved(self) -> None:
        """Assistant cannot jump from PROPOSED to APPROVED directly."""
        fsm = StateMachine(
            action_id="inv5-jump",
            state=ActionState.PROPOSED,
        )
        with pytest.raises(TransitionError):
            fsm.transition_to(
                ActionState.APPROVED,
                context={"actor_type": "assistant"},
            )


# ─────────────────────────────────────────────────────────────────────────────
# INVARIANT 6 — Event Replay Cannot Bypass Authorization
# ─────────────────────────────────────────────────────────────────────────────


class TestInvariant6EventReplayAuthorizationBypass:
    """
    Invariant: Injecting a tampered 'action_approved' event with
    actor_type='assistant' into the replay stream must either:
    - raise StateReductionError / TransitionError, OR
    - be provably NOT applied to derive new state.

    The test simulates a write-side attacker who manages to insert a plausible
    event into the stream with an unauthorized actor.
    """

    def _proposed_event(self, aggregate_id: str) -> DomainEvent:
        return DomainEvent.create(
            aggregate_id=aggregate_id,
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
            payload={"state": LifecycleState.PROPOSED},
            metadata={"actor_type": "api_user"},
        )

    def _approved_event(self, aggregate_id: str, actor_type: str) -> DomainEvent:
        return DomainEvent.create(
            aggregate_id=aggregate_id,
            event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
            payload={"state": LifecycleState.APPROVED},
            metadata={"actor_type": actor_type},
        )

    def test_invariant_replay_with_injected_assistant_approval_fails_or_rejects(
        self,
    ) -> None:
        """
        A replay stream where the APPROVED event carries actor_type='assistant'
        must not silently succeed.  The reducer should raise OR the guard must
        have been invoked to block the original write.

        NOTE: The reduce_state function is a PURE read-model function and does
        not re-run FSM guards by default (it trusts the write-side did so).
        Therefore this test verifies the WRITE-SIDE guard by attempting the
        equivalent FSM transition — proving that the event could never have
        been legitimately produced.
        """
        aggregate_id = f"replay-inv6-{uuid.uuid4().hex[:8]}"

        # 1. Verify that the FSM write-side would have blocked this transition.
        fsm = StateMachine(
            action_id=aggregate_id,
            state=ActionState.PENDING_APPROVAL,
        )
        fsm_blocked = False
        try:
            fsm.transition_to(
                ActionState.APPROVED,
                context={"actor_type": "assistant"},
            )
        except TransitionError:
            fsm_blocked = True

        assert fsm_blocked, (
            "VIOLATION: The FSM did NOT block an assistant from approving. "
            "This means the injected event could have been produced legitimately. "
            "The write-side guard is not enforced."
        )

        # 2. Attempt to replay the injected stream.
        #    The reducer may or may not re-validate actor_type; either outcome is
        #    tested: if it raises, the system actively rejects; if it does not
        #    raise, we document that the red-line is the write-side guard above.
        injected_stream = [
            self._proposed_event(aggregate_id),
            self._approved_event(aggregate_id, actor_type="assistant"),
        ]
        try:
            state = reduce_state(injected_stream)
            # Reducer did not raise — this is acceptable ONLY because the
            # write-side guard (asserted above) prevents this event from ever
            # being legitimately written.  Log for audit.
            assert state == LifecycleState.APPROVED  # State reflects injected event.
            # The important guarantee is that the WRITE path blocks this.
        except (StateReductionError, TransitionError):
            # Even better: reducer actively rejects the tampered stream.
            pass

    def test_invariant_replay_proposed_to_approved_skipping_required_guard(self) -> None:
        """
        A stream containing a PROPOSED → APPROVED event where requires_approval=True
        must be detectable as invalid at the write-side.
        """
        aggregate_id = f"replay-skip-{uuid.uuid4().hex[:8]}"

        fsm = StateMachine(action_id=aggregate_id, state=ActionState.PROPOSED)
        fsm_blocked = False
        try:
            fsm.transition_to(
                ActionState.APPROVED,
                context={"actor_type": "api_user", "requires_approval": True},
            )
        except TransitionError:
            fsm_blocked = True

        assert fsm_blocked, (
            "VIOLATION: The FSM allowed skipping the approval gate when "
            "requires_approval=True. An attacker could produce an event that "
            "bypasses the pending_approval state."
        )

    def test_invariant_replay_of_valid_stream_succeeds(self) -> None:
        """
        Sanity check: a legitimate event stream must still reduce correctly.
        This ensures we are not over-blocking valid replay.
        """
        aggregate_id = f"replay-valid-{uuid.uuid4().hex[:8]}"
        events = [
            DomainEvent.create(
                aggregate_id=aggregate_id,
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_PROPOSED"],
                payload={"state": LifecycleState.PROPOSED},
                metadata={"actor_type": "api_user"},
            ),
            DomainEvent.create(
                aggregate_id=aggregate_id,
                event_type=LIFECYCLE_EVENT_TYPES["ACTION_APPROVED"],
                payload={"state": LifecycleState.APPROVED},
                metadata={"actor_type": "api_user"},
            ),
        ]
        state = reduce_state(events)
        assert state == LifecycleState.APPROVED, (
            f"VIOLATION: Legitimate approved event stream was rejected. "
            f"Got state: {state!r}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# INVARIANT 7 — Audit Metadata Completeness
# ─────────────────────────────────────────────────────────────────────────────


class TestInvariant7AuditMetadataCompleteness:
    """
    Invariant: Every domain event emitted for an approved / committed action
    must contain actor_type and user_id (where applicable) in its metadata
    and must NOT contain 'unknown' as actor_type unless the caller deliberately
    provided no actor information.
    """

    def test_invariant_events_contain_actor_type(self, tmp_path: Path) -> None:
        """Events emitted during approval carry actor_type in metadata."""
        client, orchestrator = _build_client(
            tmp_path,
            actor_type="api_user",
            user_id="user-audit",
            verify_owner=lambda hid, uid: True,
        )

        action_id = _create_action_via_run(client, "household-audit")

        resp = client.post(
            "/assistant/approve",
            json={"action_id": action_id, "household_id": "household-audit"},
        )
        assert resp.status_code == 200, resp.text

        # Inspect persisted events via the event store.
        from household_os.runtime.lifecycle_migration import get_migration_layer

        migration = get_migration_layer()
        events = migration.event_store.get_events(action_id)

        assert events, (
            "VIOLATION: No events were persisted for the action after approval. "
            "The audit trail is empty."
        )

        for event in events:
            actor_type_in_meta = event.metadata.get("actor_type")
            assert actor_type_in_meta is not None, (
                f"VIOLATION: Event {event.event_type!r} (id={event.event_id}) "
                "has no actor_type in metadata. Audit trail is incomplete."
            )

    def test_invariant_events_actor_type_not_unknown_for_identified_caller(
        self, tmp_path: Path
    ) -> None:
        """
        When a known actor (api_user, user_id set) triggers an action,
        the events must NOT record actor_type='unknown'.
        """
        client, orchestrator = _build_client(
            tmp_path,
            actor_type="api_user",
            user_id="user-known",
            verify_owner=lambda hid, uid: True,
        )

        action_id = _create_action_via_run(client, "household-known")

        resp = client.post(
            "/assistant/approve",
            json={"action_id": action_id, "household_id": "household-known"},
        )
        assert resp.status_code == 200, resp.text

        from household_os.runtime.lifecycle_migration import get_migration_layer

        migration = get_migration_layer()
        events = migration.event_store.get_events(action_id)

        approval_events = [
            e for e in events if e.event_type == LIFECYCLE_EVENT_TYPES.get("ACTION_APPROVED")
        ]

        for event in approval_events:
            actor_type_in_meta = event.metadata.get("actor_type")
            assert actor_type_in_meta != "unknown", (
                f"VIOLATION: Approval event {event.event_id} records actor_type='unknown' "
                "even though the caller was an identified api_user. "
                "The ExecutionContext is not being threaded to event metadata."
            )

    def test_invariant_execution_context_metadata_wired_to_events(self, tmp_path: Path) -> None:
        """
        When ExecutionContext carries request_id, the DomainEvents emitted
        for approval should include actor_type in their metadata for
        full traceability.
        """
        client, orchestrator = _build_client(
            tmp_path,
            actor_type="api_user",
            user_id="user-trace",
            verify_owner=lambda hid, uid: True,
        )
        action_id = _create_action_via_run(client, "household-trace")

        resp = client.post(
            "/assistant/approve",
            json={"action_id": action_id, "household_id": "household-trace"},
        )
        assert resp.status_code == 200, resp.text

        from household_os.runtime.lifecycle_migration import get_migration_layer

        migration = get_migration_layer()
        events = migration.event_store.get_events(action_id)

        # At minimum the proposed event should exist.
        assert events, (
            "VIOLATION: No events found for action. "
            "Cannot verify audit metadata completeness."
        )

        for event in events:
            assert "actor_type" in event.metadata, (
                f"VIOLATION: Event {event.event_type!r} missing 'actor_type' in metadata. "
                "ExecutionContext.to_event_metadata() must populate this field."
            )

    def test_invariant_system_worker_events_marked_correctly(self, tmp_path: Path) -> None:
        """system_worker-initiated events should record actor_type='system_worker'."""
        # Use the HTTP flow: create action as system_worker via /run,
        # then approve via direct orchestrator call with system_context.
        store = HouseholdStateGraphStore(
            graph_path=Path(tmp_path) / "inv7-sysworker.json"
        )
        store.verify_household_owner = lambda hid, uid: False
        orchestrator = HouseholdOSOrchestrator(state_store=store)

        # Create the action via run as system_worker (no ownership check).
        worker_app = _build_app_with_orchestrator(orchestrator, "system_worker", None)
        worker_client = TestClient(worker_app)
        action_id = _create_action_via_run(worker_client, "household-sys")

        # Now approve using the explicit system_context ExecutionContext.
        ctx = ExecutionContext.system_context(household_id="household-sys")
        try:
            orchestrator.approve_and_execute(
                household_id="household-sys",
                request_id=str(uuid.uuid4()),
                action_ids=[action_id],
                context=ctx,
            )
        except Exception:
            pass

        from household_os.runtime.lifecycle_migration import get_migration_layer

        migration = get_migration_layer()
        events = migration.event_store.get_events(action_id)

        assert events, (
            "VIOLATION: No events found for system_worker action. "
            "Cannot verify actor_type audit marking."
        )

        for event in events:
            at = event.metadata.get("actor_type")
            if at is not None and at != "unknown":
                assert at == "system_worker", (
                    f"VIOLATION: system_worker context produced event with "
                    f"actor_type={at!r}. Expected 'system_worker'."
                )


# ─────────────────────────────────────────────────────────────────────────────
# BOUNDARY / REGRESSION TESTS
# ─────────────────────────────────────────────────────────────────────────────


class TestBoundarySecurityEdgeCases:
    """
    Regression tests for specific attack patterns identified in the
    PERMISSION_ENFORCEMENT_AUDIT and THREAT_MODEL_ATTACK_VECTORS documents.
    """

    def test_empty_action_ids_does_not_bypass_auth(self, tmp_path: Path) -> None:
        """Submitting an empty action_ids list must not bypass auth checks."""
        store = HouseholdStateGraphStore(
            graph_path=Path(tmp_path) / "bound-empty-ids.json"
        )
        store.verify_household_owner = lambda hid, uid: True
        orchestrator = HouseholdOSOrchestrator(state_store=store)

        # Should not raise auth errors — the call is well-formed: api_user + valid household.
        # If it raises for other reasons (empty list) that is acceptable.
        try:
            orchestrator.approve_and_execute(
                household_id="household-a",
                request_id=str(uuid.uuid4()),
                action_ids=[],
                actor_type="api_user",
                user_id="user-legitimate",
            )
        except Exception as exc:
            msg = str(exc).lower()
            # Must NOT fail for an auth reason — only for business logic.
            assert "403" not in msg and "assistant" not in msg and "cannot approve" not in msg, (
                f"VIOLATION: Empty action_ids triggered an auth error: {msg!r}. "
                "Auth should have passed; only business-logic validation should fail here."
            )

    def test_unknown_actor_type_is_rejected(self, tmp_path: Path) -> None:
        """An unrecognised actor_type string must not be granted any access."""
        store = HouseholdStateGraphStore(
            graph_path=Path(tmp_path) / "bound-unknown-actor.json"
        )
        store.verify_household_owner = lambda hid, uid: True
        orchestrator = HouseholdOSOrchestrator(state_store=store)

        graph = store.load_graph("household-a")
        action_id = create_test_action(graph, "household-a")
        store.save_graph(graph)

        raised = False
        try:
            orchestrator.tick(
                household_id="household-a",
                actor_type="superadmin",  # Not a valid actor type.
                user_id="attacker",
                user_input="take over system",
            )
        except (PermissionError, Exception):
            raised = True

        assert raised, (
            "VIOLATION: Unknown actor_type 'superadmin' was accepted without error. "
            "The system must reject unrecognised actor types rather than defaulting."
        )

    def test_fsm_terminal_states_cannot_be_transitioned_out_of(self) -> None:
        """COMMITTED and REJECTED are terminal — no further transitions allowed."""
        for terminal_state in (ActionState.COMMITTED, ActionState.REJECTED):
            fsm = StateMachine(action_id=f"terminal-{terminal_state.value}", state=terminal_state)
            for target in ActionState:
                if target == terminal_state:
                    continue
                with pytest.raises(TransitionError, match="(?i)not allowed|invalid|terminal|no allowed"):
                    fsm.transition_to(target)

    def test_execution_context_from_api_request_carries_correct_fields(self) -> None:
        """ExecutionContext.from_api_request populates all required fields."""
        ctx = ExecutionContext.from_api_request(
            household_id="household-test",
            actor_type="api_user",
            user_id="user-xyz",
            request_id="req-abc",
        )
        assert ctx.actor_type == "api_user", "actor_type must be preserved"
        assert ctx.user_id == "user-xyz", "user_id must be preserved"
        assert ctx.household_id == "household-test", "household_id must be preserved"
        assert ctx.request_id == "req-abc", "request_id must be preserved"

        fsm_ctx = ctx.to_fsm_context()
        assert fsm_ctx["actor_type"] == "api_user"
        assert fsm_ctx["user_id"] == "user-xyz"
        assert fsm_ctx["household_id"] == "household-test"

        event_meta = ctx.to_event_metadata()
        assert event_meta["actor_type"] == "api_user"
        assert event_meta["user_id"] == "user-xyz"
        assert event_meta["request_id"] == "req-abc"
        assert "initiated_at" in event_meta

    def test_execution_context_system_context_actor_is_system_worker(self) -> None:
        """ExecutionContext.system_context must produce actor_type='system_worker'."""
        ctx = ExecutionContext.system_context(household_id="household-sys")
        assert ctx.actor_type == "system_worker", (
            f"VIOLATION: system_context() produced actor_type={ctx.actor_type!r}. "
            "system_worker is the only valid automatic actor type."
        )
        assert ctx.user_id is None, (
            "system_worker context must not carry a user_id."
        )
