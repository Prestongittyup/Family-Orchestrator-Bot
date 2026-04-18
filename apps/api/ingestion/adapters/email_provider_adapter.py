from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class ParsedEmailMessage:
    email_id: str
    sender: str
    recipient: str
    subject: str
    body: str
    received_at: str
    provider: str


class EmailProviderAdapter(Protocol):
    """Pluggable provider contract for external email integrations."""

    provider_name: str

    def poll_messages(self) -> list[dict[str, Any]]:
        """Return provider-native payloads in deterministic order."""

    def parse_message(self, raw_message: dict[str, Any]) -> ParsedEmailMessage:
        """Convert provider-native payload to ingestion-ready shape."""
