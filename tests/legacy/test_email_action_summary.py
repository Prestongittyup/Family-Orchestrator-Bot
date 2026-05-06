from __future__ import annotations
import pytest

from datetime import UTC, datetime

import archive.apps.api.ingestion.email_action_summary as email_action_summary_module
from archive.apps.api.ingestion.email_action_summary import summarize_email_to_actions

_existing_pytestmark = globals().get("pytestmark", [])
if not isinstance(_existing_pytestmark, list):
    _existing_pytestmark = [_existing_pytestmark]
pytestmark = [*_existing_pytestmark, pytest.mark.migration]



@pytest.mark.integration
@pytest.mark.legacy
def test_email_action_summary_extracts_prioritized_actions_and_calendar_candidates() -> None:
    result = summarize_email_to_actions(
        sender="teacher@school.edu",
        subject="Urgent: Parent meeting tomorrow",
        body="Please confirm attendance tomorrow. Bring signed form. Meeting at 2026-04-28 15:00.",
        received_at=datetime(2026, 4, 26, 18, 0, tzinfo=UTC),
    )

    assert result["importance_score"] >= 0.58
    assert result["importance_bucket"] in {"high", "critical"}
    assert result["priority_label"] == "high"
    assert result["rule_score"] >= 15
    assert result["action_items"]
    assert result["action_items"][0]["due_hint_local"] is not None
    assert result["calendar_candidates"]
    assert result["calendar_candidates"][0]["time_hint"] is not None
    assert result["calendar_candidates"][0]["time_hint_local"] is not None
    assert result["priority"] == "high"
    assert result["needs_attention"] is True
    assert isinstance(result["actions"], list)
    assert result["state_summary"]
    assert result["reason"]


@pytest.mark.integration
@pytest.mark.legacy
def test_email_action_summary_is_deterministic_for_identical_input() -> None:
    kwargs = {
        "sender": "billing@bank.test",
        "subject": "Invoice due tomorrow",
        "body": "Please pay invoice INV-4451 tomorrow and confirm receipt.",
        "received_at": datetime(2026, 4, 26, 12, 30, tzinfo=UTC),
    }

    first = summarize_email_to_actions(**kwargs)
    second = summarize_email_to_actions(**kwargs)

    assert first == second


@pytest.mark.integration
@pytest.mark.legacy
def test_email_action_summary_classifies_promotional_mail_as_junk() -> None:
    result = summarize_email_to_actions(
        sender="no-reply@newsletter.offers.test",
        subject="Limited time deal!!!",
        body="Unsubscribe anytime. Manage preferences. Big promotion with coupons and sale prices.",
        received_at=datetime(2026, 4, 26, 12, 30, tzinfo=UTC),
    )

    assert result["is_junk"] is True
    assert result["triage_decision"] == "junk"
    assert result["action_items"] == []
    assert result["calendar_candidates"] == []
    assert result["actions"] == []
    assert result["needs_attention"] is False


@pytest.mark.integration
@pytest.mark.legacy
def test_email_action_summary_marks_non_action_updates_as_informational() -> None:
    result = summarize_email_to_actions(
        sender="updates@service.test",
        subject="Monthly account statement available",
        body="Your monthly statement is now available. Details are provided for your records.",
        received_at=datetime(2026, 4, 26, 9, 0, tzinfo=UTC),
    )

    assert result["is_junk"] is False
    assert result["priority_label"] == "low"
    assert result["triage_decision"] == "informational"
    assert result["action_items"] == []
    assert result["informational_items"]
    assert result["needs_attention"] is False


@pytest.mark.integration
@pytest.mark.legacy
def test_email_action_summary_ignores_quoted_lines_for_action_extraction() -> None:
    result = summarize_email_to_actions(
        sender="teacher@school.edu",
        subject="Re: Parent meeting follow-up",
        body=(
            "Please confirm attendance by Friday.\n"
            "On Tue, Apr 23, 2026 at 9:15 AM, Admin wrote:\n"
            "> Urgent update from old thread\n"
            "> Subject: Old context\n"
        ),
        received_at=datetime(2026, 4, 26, 9, 0, tzinfo=UTC),
    )

    assert result["action_items"]
    assert all("wrote:" not in row["title"].lower() for row in result["action_items"])
    assert all(not row["title"].startswith(">") for row in result["action_items"])


@pytest.mark.integration
@pytest.mark.legacy
def test_email_action_summary_does_not_apply_sensitive_sender_bonus_to_junk_domains() -> None:
    common_kwargs = {
        "subject": "Important account review",
        "body": "Urgent update. Click here to manage preferences and unsubscribe.",
        "received_at": datetime(2026, 4, 26, 12, 0, tzinfo=UTC),
    }

    trusted = summarize_email_to_actions(sender="advisor@bank.com", **common_kwargs)
    junk_like = summarize_email_to_actions(sender="noreply@bank-alerts.test", **common_kwargs)

    assert trusted["rule_score"] > junk_like["rule_score"]


@pytest.mark.integration
@pytest.mark.legacy
def test_email_action_summary_applies_llm_refinement_when_available(monkeypatch) -> None:
    def _fake_refine(**_: object) -> dict[str, object]:
        return {
            "priority": "high",
            "needs_attention": True,
            "actions": [
                {
                    "type": "task",
                    "title": "Confirm account changes",
                    "urgency": "high",
                    "due": None,
                }
            ],
            "state_summary": "Account update needs confirmation.",
            "reason": "Explicit confirmation request detected.",
            "importance_bucket": "high",
            "importance_score": 0.74,
            "triage_decision": "task",
            "is_junk": False,
        }

    monkeypatch.setattr(email_action_summary_module, "maybe_refine_email_priority", _fake_refine)

    result = summarize_email_to_actions(
        sender="updates@service.test",
        subject="Reminder about your account",
        body="Here is a general update for your records.",
        received_at=datetime(2026, 4, 26, 9, 0, tzinfo=UTC),
    )

    assert result["priority_label"] == "high"
    assert result["importance_bucket"] == "high"
    assert result["triage_decision"] == "task"
    assert result["called_llm"] is True
    assert result["actions"]


@pytest.mark.integration
@pytest.mark.legacy
def test_email_action_summary_does_not_force_informational_when_actions_exist(monkeypatch) -> None:
    def _fake_refine(**_: object) -> dict[str, object]:
        return {
            "priority": "medium",
            "needs_attention": True,
            "actions": [
                {
                    "type": "reply",
                    "title": "Reply with availability",
                    "urgency": "normal",
                    "due": None,
                }
            ],
            "state_summary": "Follow-up response requested.",
            "reason": "Sender asked for confirmation.",
            "importance_bucket": "medium",
            "importance_score": 0.53,
            "triage_decision": "informational",
            "is_junk": False,
        }

    monkeypatch.setattr(email_action_summary_module, "maybe_refine_email_priority", _fake_refine)

    result = summarize_email_to_actions(
        sender="teacher@school.edu",
        subject="Parent meeting tomorrow",
        body="Please confirm attendance tomorrow and reply with your availability.",
        received_at=datetime(2026, 4, 26, 9, 0, tzinfo=UTC),
    )

    assert result["action_items"]
    assert result["triage_decision"] == "task"