from __future__ import annotations

from dataclasses import dataclass

from apps.api.core.feature_flags import set_household_feature_flags
from apps.api.ingestion.adapters.execution_runner import run_email_ingestion_cycle
from apps.api.ingestion.adapters.imap_email_adapter import ImapEmailAdapter


@dataclass
class _FakeImapClient:
    messages: dict[str, bytes]
    internal_dates: dict[str, str]

    def login(self, username: str, password: str):
        return "OK", [b"Logged in"]

    def select(self, mailbox: str = "INBOX", readonly: bool = False):
        return "OK", [b"1"]

    def search(self, charset, criterion: str):
        ids = " ".join(sorted(self.messages.keys(), key=lambda v: int(v)))
        return "OK", [ids.encode("utf-8")]

    def fetch(self, message_set: str, message_parts: str):
        payload = self.messages[message_set]
        internaldate = self.internal_dates.get(message_set, "15-Apr-2026 09:00:00 +0000")
        meta = f'{message_set} (RFC822 INTERNALDATE "{internaldate}")'.encode("utf-8")
        return "OK", [(meta, payload)]

    def logout(self):
        return "OK", [b"Logged out"]



def _build_rfc822_message(*, subject: str, sender: str, recipient: str, body: str, date: str) -> bytes:
    text = (
        f"From: {sender}\r\n"
        f"To: {recipient}\r\n"
        f"Subject: {subject}\r\n"
        f"Date: {date}\r\n"
        "MIME-Version: 1.0\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        f"{body}\r\n"
    )
    return text.encode("utf-8")



def test_imap_adapter_pipeline_to_brief_via_runner(test_client):
    fake_client = _FakeImapClient(
        messages={
            "1": _build_rfc822_message(
                subject="IMAP adapter pipeline",
                sender="alerts@example.com",
                recipient="home@example.com",
                body="This should flow through ingestion",
                date="Wed, 15 Apr 2026 09:00:00 +0000",
            )
        },
        internal_dates={"1": "15-Apr-2026 09:00:00 +0000"},
    )

    adapter = ImapEmailAdapter(
        host="imap.example.com",
        username="sandbox-user",
        password="sandbox-pass",
        imap_client_factory=lambda host, port: fake_client,
    )

    run = run_email_ingestion_cycle(adapter, mode="poll")
    assert run["status"] == "ok"
    assert run["summary"]["processed"] == 1
    result = run["cycle_result"]["results"][0]["outcome"]["result"]
    assert result["status"] in {"success", "duplicate_ignored"}

    brief_response = test_client.get("/brief/hh-001?include_observability=true")
    assert brief_response.status_code == 200
    payload = brief_response.json()
    assert payload["status"] == "success"
    assert "brief" in payload
    assert "observability" in payload



def test_imap_adapter_quarantine_on_malformed_external_payload():
    adapter = ImapEmailAdapter(
        sandbox_mode=True,
        sandbox_messages=[
            {
                "uid": "bad-1",
                "envelope": {
                    "from": "alerts@example.com",
                    "to": "home@example.com",
                    "subject": "Bad timestamp",
                },
                "body": "Malformed payload",
                "internaldate": "not-a-timestamp",
            }
        ],
    )

    run = run_email_ingestion_cycle(adapter, mode="poll")
    assert run["status"] == "ok"
    assert run["summary"]["failed"] == 1

    outcome = run["cycle_result"]["results"][0]["outcome"]
    assert outcome["status"] == "failed"
    detail = outcome["error"]["detail"]
    assert detail["status"] == "quarantined"



def test_imap_adapter_respects_feature_flag_gate():
    set_household_feature_flags("hh-001", {"ingestion_enabled": False})

    adapter = ImapEmailAdapter(
        sandbox_mode=True,
        sandbox_messages=[
            {
                "uid": "flag-1",
                "envelope": {
                    "from": "alerts@example.com",
                    "to": "home@example.com",
                    "subject": "Gate test",
                },
                "body": "Should be gated",
                "internaldate": "2026-04-15T09:00:00Z",
            }
        ],
    )

    run = run_email_ingestion_cycle(adapter, mode="poll")
    assert run["status"] == "ok"
    assert run["summary"]["processed"] == 1
    outcome = run["cycle_result"]["results"][0]["outcome"]
    assert outcome["status"] == "processed"
    assert outcome["result"]["status"] == "disabled"



def test_imap_adapter_deterministic_replay_with_mocked_api_responses(test_client):
    fake_client = _FakeImapClient(
        messages={
            "1": _build_rfc822_message(
                subject="Deterministic replay",
                sender="alerts@example.com",
                recipient="home@example.com",
                body="Replay test body",
                date="Wed, 15 Apr 2026 09:30:00 +0000",
            )
        },
        internal_dates={"1": "15-Apr-2026 09:30:00 +0000"},
    )

    adapter = ImapEmailAdapter(
        host="imap.example.com",
        username="sandbox-user",
        password="sandbox-pass",
        imap_client_factory=lambda host, port: fake_client,
    )

    first = run_email_ingestion_cycle(adapter, mode="poll")
    second = run_email_ingestion_cycle(adapter, mode="poll")

    first_result = first["cycle_result"]["results"][0]["outcome"]["result"]
    second_result = second["cycle_result"]["results"][0]["outcome"]["result"]

    assert first_result["status"] == "success"
    assert second_result["status"] == "duplicate_ignored"

    brief_1 = test_client.get("/brief/hh-001").json()
    brief_2 = test_client.get("/brief/hh-001").json()
    assert brief_1["brief"] == brief_2["brief"]
