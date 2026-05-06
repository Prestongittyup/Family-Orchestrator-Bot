from __future__ import annotations
import pytest

import json

import archive.apps.api.ingestion.email_priority_llm as email_priority_llm

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



def _base_kwargs() -> dict[str, object]:
    return {
        "sender": "updates@service.test",
        "subject": "Reminder about account updates",
        "body": "Please review the account details and confirm by tomorrow.",
        "score": 12,
        "to_me": True,
        "cc_me": False,
        "thread_id": "thread-1",
        "latest_message_id": "msg-1",
        "thread_context": "Please review the account details and confirm by tomorrow.",
    }


def _gemini_payload(payload: dict[str, object]) -> dict[str, object]:
    return {
        "candidates": [
            {
                "content": {
                    "parts": [
                        {
                            "text": json.dumps(payload),
                        }
                    ]
                }
            }
        ]
    }


def _reset_llm_runtime_state() -> None:
    email_priority_llm._requests_last_minute.clear()
    email_priority_llm._requests_last_day.clear()
    email_priority_llm._cache.clear()


@pytest.mark.integration
@pytest.mark.legacy
def test_email_priority_llm_disabled_skips_remote_call(monkeypatch) -> None:
    _reset_llm_runtime_state()
    monkeypatch.setenv("EMAIL_PRIORITY_LLM_ENABLED", "0")

    call_count = {"value": 0}

    def _fake_request(**_: object) -> dict[str, object]:
        call_count["value"] += 1
        return {}

    monkeypatch.setattr(email_priority_llm, "_request_gemini_priority", _fake_request)

    result = email_priority_llm.maybe_refine_email_priority(**_base_kwargs())

    assert result is None
    assert call_count["value"] == 0


@pytest.mark.integration
@pytest.mark.legacy
def test_email_priority_llm_enabled_requires_credentials(monkeypatch) -> None:
    _reset_llm_runtime_state()
    monkeypatch.setenv("EMAIL_PRIORITY_LLM_ENABLED", "1")
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("EMAIL_PRIORITY_LLM_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("EMAIL_PRIORITY_LLM_ACCESS_TOKEN", raising=False)

    call_count = {"value": 0}

    def _fake_request(**_: object) -> dict[str, object]:
        call_count["value"] += 1
        return {}

    monkeypatch.setattr(email_priority_llm, "_request_gemini_priority", _fake_request)

    result = email_priority_llm.maybe_refine_email_priority(**_base_kwargs())

    assert result is None
    assert call_count["value"] == 0


@pytest.mark.integration
@pytest.mark.legacy
def test_email_priority_llm_applies_valid_high_confidence_refinement(monkeypatch) -> None:
    _reset_llm_runtime_state()
    monkeypatch.setenv("EMAIL_PRIORITY_LLM_ENABLED", "1")
    monkeypatch.setenv("GOOGLE_API_KEY", "test-key")
    monkeypatch.setenv("EMAIL_PRIORITY_LLM_MIN_CONFIDENCE", "0.6")

    def _fake_request(**_: object) -> dict[str, object]:
        return _gemini_payload(
            {
                "priority": "high",
                "needs_attention": True,
                "actions": [
                    {
                        "type": "reply",
                        "title": "Reply re: account update",
                        "urgency": "high",
                        "due": None,
                    }
                ],
                "state_summary": "A direct response is requested before tomorrow.",
                "reason": "Contains direct request and near-term deadline language.",
                "confidence": 0.92,
            }
        )

    monkeypatch.setattr(email_priority_llm, "_request_gemini_priority", _fake_request)

    result = email_priority_llm.maybe_refine_email_priority(**_base_kwargs())

    assert result is not None
    assert result["priority"] == "high"
    assert result["needs_attention"] is True
    assert result["triage_decision"] == "task"
    assert result["actions"][0]["type"] == "reply"


@pytest.mark.integration
@pytest.mark.legacy
def test_email_priority_llm_skips_low_score_cases(monkeypatch) -> None:
    _reset_llm_runtime_state()
    monkeypatch.setenv("EMAIL_PRIORITY_LLM_ENABLED", "1")
    monkeypatch.setenv("GOOGLE_API_KEY", "test-key")

    call_count = {"value": 0}

    def _fake_request(**_: object) -> dict[str, object]:
        call_count["value"] += 1
        return _gemini_payload(
            {
                "priority": "medium",
                "needs_attention": True,
                "actions": [],
                "state_summary": "Should not be used.",
                "reason": "Should not be used.",
                "confidence": 0.99,
            }
        )

    monkeypatch.setattr(email_priority_llm, "_request_gemini_priority", _fake_request)

    result = email_priority_llm.maybe_refine_email_priority(
        sender="news@promo.example",
        subject="Urgent looking promo",
        body="Limited-time offer!",
        score=5,
    )

    assert result is None
    assert call_count["value"] == 0


@pytest.mark.integration
@pytest.mark.legacy
def test_email_priority_llm_middle_band_requires_ambiguity(monkeypatch) -> None:
    _reset_llm_runtime_state()
    monkeypatch.setenv("EMAIL_PRIORITY_LLM_ENABLED", "1")
    monkeypatch.setenv("GOOGLE_API_KEY", "test-key")

    call_count = {"value": 0}

    def _fake_request(**_: object) -> dict[str, object]:
        call_count["value"] += 1
        return _gemini_payload(
            {
                "priority": "medium",
                "needs_attention": True,
                "actions": [
                    {
                        "type": "task",
                        "title": "Review approval request",
                        "urgency": "normal",
                        "due": None,
                    }
                ],
                "state_summary": "Approval requested.",
                "reason": "Ambiguous approval language.",
                "confidence": 0.85,
            }
        )

    monkeypatch.setattr(email_priority_llm, "_request_gemini_priority", _fake_request)

    result = email_priority_llm.maybe_refine_email_priority(
        sender="ops@example.com",
        subject="Can you review this approval?",
        body="Please review when you can.",
        score=8,
    )

    assert result is not None
    assert call_count["value"] == 1


@pytest.mark.integration
@pytest.mark.legacy
def test_email_priority_llm_respects_per_minute_budget(monkeypatch) -> None:
    _reset_llm_runtime_state()
    monkeypatch.setenv("EMAIL_PRIORITY_LLM_ENABLED", "1")
    monkeypatch.setenv("GOOGLE_API_KEY", "test-key")
    monkeypatch.setenv("EMAIL_PRIORITY_LLM_MAX_REQUESTS_PER_MIN", "1")

    call_count = {"value": 0}

    def _fake_request(**_: object) -> dict[str, object]:
        call_count["value"] += 1
        return _gemini_payload(
            {
                "priority": "medium",
                "needs_attention": True,
                "actions": [],
                "state_summary": "Follow-up required.",
                "reason": "Request language detected.",
                "confidence": 0.91,
            }
        )

    monkeypatch.setattr(email_priority_llm, "_request_gemini_priority", _fake_request)

    first = email_priority_llm.maybe_refine_email_priority(**_base_kwargs())
    second = email_priority_llm.maybe_refine_email_priority(
        sender="updates@service.test",
        subject="Approval needed?",
        body="Can you confirm this by tomorrow?",
        score=8,
        thread_id="thread-2",
        latest_message_id="msg-2",
    )

    assert first is not None
    assert second is None
    assert call_count["value"] == 1


@pytest.mark.integration
@pytest.mark.legacy
def test_email_priority_llm_uses_thread_cache_key(monkeypatch) -> None:
    _reset_llm_runtime_state()
    monkeypatch.setenv("EMAIL_PRIORITY_LLM_ENABLED", "1")
    monkeypatch.setenv("GOOGLE_API_KEY", "test-key")

    call_count = {"value": 0}

    def _fake_request(**_: object) -> dict[str, object]:
        call_count["value"] += 1
        return _gemini_payload(
            {
                "priority": "medium",
                "needs_attention": True,
                "actions": [],
                "state_summary": "Needs follow up.",
                "reason": "Review request.",
                "confidence": 0.9,
            }
        )

    monkeypatch.setattr(email_priority_llm, "_request_gemini_priority", _fake_request)

    first = email_priority_llm.maybe_refine_email_priority(**_base_kwargs())
    second = email_priority_llm.maybe_refine_email_priority(**_base_kwargs())

    assert first is not None
    assert second is not None
    assert call_count["value"] == 1