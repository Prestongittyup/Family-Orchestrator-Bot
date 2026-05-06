from __future__ import annotations

from copy import deepcopy
from typing import Any

from archive.apps.api.ingestion.adapters.email_provider_adapter import ParsedEmailMessage


class MockEmailProviderAdapter:
    """
    Deterministic mock adapter simulating external email providers.

    Supported payload styles:
    - API style:
      {
        "id": "msg-1",
        "from": "sender@example.com",
        "to": "recipient@example.com",
        "subject": "Hello",
        "body": "World",
        "received_at": "2026-04-15T10:30:00Z"
      }

    - IMAP style:
      {
        "uid": "42",
        "envelope": {
          "from": "sender@example.com",
          "to": "recipient@example.com",
          "subject": "Hello"
        },
        "body": "World",
        "internaldate": "2026-04-15T10:30:00Z"
      }
    """

    def __init__(
        self,
        *,
        provider_name: str,
        fixed_poll_dataset: list[dict[str, Any]] | None = None,
    ) -> None:
        self.provider_name = provider_name
        self._fixed_poll_dataset = [deepcopy(item) for item in (fixed_poll_dataset or [])]
        self._push_queue: list[dict[str, Any]] = []

    def poll_messages(self) -> list[dict[str, Any]]:
        return [deepcopy(item) for item in self._fixed_poll_dataset]

    def queue_push_message(self, raw_message: dict[str, Any]) -> None:
        self._push_queue.append(deepcopy(raw_message))

    def drain_push_messages(self) -> list[dict[str, Any]]:
        items = [deepcopy(item) for item in self._push_queue]
        self._push_queue = []
        return items

    def parse_message(self, raw_message: dict[str, Any]) -> ParsedEmailMessage:
        if "envelope" in raw_message:
            envelope = raw_message.get("envelope") or {}
            return ParsedEmailMessage(
                email_id=str(raw_message.get("uid", "")),
                sender=str(envelope.get("from", "")),
                recipient=str(envelope.get("to", "")),
                subject=str(envelope.get("subject", "")),
                body=str(raw_message.get("body", "")),
                received_at=str(raw_message.get("internaldate", "")),
                provider=self.provider_name,
              thread_id=str(raw_message.get("thread_id") or "").strip() or None,
              latest_message_id=str(raw_message.get("latest_message_id") or raw_message.get("uid") or "").strip() or None,
              thread_messages=list(raw_message.get("thread_messages") or []),
            )

        return ParsedEmailMessage(
            email_id=str(raw_message.get("id", "")),
            sender=str(raw_message.get("from", "")),
            recipient=str(raw_message.get("to", "")),
            subject=str(raw_message.get("subject", "")),
            body=str(raw_message.get("body", "")),
            received_at=str(raw_message.get("received_at", "")),
            provider=self.provider_name,
          thread_id=str(raw_message.get("thread_id") or raw_message.get("threadId") or "").strip() or None,
          latest_message_id=str(raw_message.get("latest_message_id") or raw_message.get("id") or "").strip() or None,
          thread_messages=list(raw_message.get("thread_messages") or raw_message.get("threadMessages") or []),
        )
