from __future__ import annotations

from datetime import UTC, datetime, timedelta
import sqlite3

import pytest
from sqlalchemy.exc import OperationalError

from archive.apps.api.identity.sqlalchemy_repository import SQLAlchemyIdentityRepository
from archive.apps.api.schemas.event import SystemEvent

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



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


class _RetryableLockSession(_SuccessSession):
    def __init__(self) -> None:
        self.commit_attempts = 0
        self.rollback_calls = 0

    def commit(self) -> None:
        self.commit_attempts += 1
        if self.commit_attempts == 1:
            raise OperationalError(
                statement="INSERT INTO session_tokens (...) VALUES (...)",
                params={},
                orig=sqlite3.OperationalError("database is locked"),
            )

    def rollback(self) -> None:
        self.rollback_calls += 1


@pytest.mark.integration
@pytest.mark.legacy
def test_create_user_success_emits_user_created(monkeypatch: pytest.MonkeyPatch) -> None:
    from archive.apps.api.identity import sqlalchemy_repository as repo_mod

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


@pytest.mark.integration
@pytest.mark.legacy
def test_create_user_failure_emits_user_creation_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    from archive.apps.api.identity import sqlalchemy_repository as repo_mod

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


@pytest.mark.integration
@pytest.mark.legacy
@pytest.mark.flaky
def test_create_session_token_retries_on_sqlite_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_calls: list[float] = []

    def _fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    from archive.apps.api.identity import sqlalchemy_repository as repo_mod

    monkeypatch.setattr(repo_mod.time, "sleep", _fake_sleep)

    session = _RetryableLockSession()
    repo = SQLAlchemyIdentityRepository(session=session)

    token = repo.create_session_token(
        token_id="tok-1",
        household_id="hh-1",
        user_id="user-1",
        device_id="dev-1",
        role="ADULT",
        session_claims='{"typ":"access"}',
        expires_at=datetime.now(UTC) + timedelta(minutes=15),
    )

    assert token.token_id == "tok-1"
    assert session.commit_attempts == 2
    assert session.rollback_calls == 1
    assert len(sleep_calls) == 1