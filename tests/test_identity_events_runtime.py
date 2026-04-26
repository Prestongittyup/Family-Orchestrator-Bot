from __future__ import annotations

import pytest

from apps.api.identity.sqlalchemy_repository import SQLAlchemyIdentityRepository
from apps.api.schemas.event import SystemEvent


class _CaptureRouter:
    def __init__(self) -> None:
        self.calls = 0
        self.events: list[SystemEvent] = []

    def emit(self, event: SystemEvent) -> None:
        self.calls += 1
        self.events.append(event)


class _SuccessSession:
    def add(self, _obj) -> None:
        return None

    def flush(self) -> None:
        return None

    def commit(self) -> None:
        return None

    def refresh(self, _obj) -> None:
        return None


class _FailingCommitSession(_SuccessSession):
    def commit(self) -> None:
        raise RuntimeError("commit failed")


def test_create_user_success_emits_user_created(monkeypatch: pytest.MonkeyPatch) -> None:
    from apps.api.identity import sqlalchemy_repository as repo_mod

    capture = _CaptureRouter()
    monkeypatch.setattr(repo_mod, "router", capture)

    repo = SQLAlchemyIdentityRepository(session=_SuccessSession())
    user = repo.create_user(
        user_id="user-1",
        household_id="hh-1",
        name="Alice",
        role="admin",
        email="alice@example.com",
    )

    assert user.user_id == "user-1"
    assert capture.calls == 1
    assert capture.events[-1].type == "user_created"


def test_create_user_failure_emits_user_creation_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    from apps.api.identity import sqlalchemy_repository as repo_mod

    capture = _CaptureRouter()
    monkeypatch.setattr(repo_mod, "router", capture)

    repo = SQLAlchemyIdentityRepository(session=_FailingCommitSession())
    with pytest.raises(RuntimeError):
        repo.create_user(
            user_id="user-2",
            household_id="hh-1",
            name="Bob",
            role="member",
            email="bob@example.com",
        )

    assert capture.calls == 1
    assert capture.events[-1].type == "user_creation_failed"
