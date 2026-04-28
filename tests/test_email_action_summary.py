from __future__ import annotations

from datetime import UTC, datetime

from archive.apps.api.ingestion.email_action_summary import summarize_email_to_actions


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


def test_email_action_summary_does_not_apply_sensitive_sender_bonus_to_junk_domains() -> None:
    common_kwargs = {
        "subject": "Important account review",
        "body": "Urgent update. Click here to manage preferences and unsubscribe.",
        "received_at": datetime(2026, 4, 26, 12, 0, tzinfo=UTC),
    }

    trusted = summarize_email_to_actions(sender="advisor@bank.com", **common_kwargs)
    junk_like = summarize_email_to_actions(sender="noreply@bank-alerts.test", **common_kwargs)

    assert trusted["rule_score"] > junk_like["rule_score"]
