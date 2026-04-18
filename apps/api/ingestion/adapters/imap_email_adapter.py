from __future__ import annotations

import imaplib
import re
from copy import deepcopy
from datetime import UTC
from email import policy
from email.header import decode_header, make_header
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Protocol

from apps.api.ingestion.adapters.email_provider_adapter import ParsedEmailMessage


class _ImapClientProtocol(Protocol):
    def login(self, username: str, password: str) -> tuple[str, list[bytes]]: ...

    def select(self, mailbox: str = "INBOX", readonly: bool = False) -> tuple[str, list[bytes]]: ...

    def search(self, charset: str | None, criterion: str) -> tuple[str, list[bytes]]: ...

    def fetch(self, message_set: str, message_parts: str) -> tuple[str, list[Any]]: ...

    def logout(self) -> tuple[str, list[bytes]]: ...


def _decode_mime_header(value: str | None) -> str:
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value))).strip()
    except Exception:
        return str(value).strip()


def _normalize_to_iso_utc(value: str | None) -> str:
    if not value:
        return ""

    text = value.strip()
    if text == "":
        return ""

    # Already ISO-like with UTC suffix.
    if text.endswith("Z") and "T" in text:
        return text

    try:
        parsed = parsedate_to_datetime(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        else:
            parsed = parsed.astimezone(UTC)
        return parsed.isoformat().replace("+00:00", "Z")
    except Exception:
        return text


def _extract_internaldate_from_fetch_meta(meta: bytes | str) -> str:
    text = meta.decode("utf-8", errors="ignore") if isinstance(meta, bytes) else str(meta)
    match = re.search(r'INTERNALDATE\s+"([^"]+)"', text)
    if not match:
        return ""
    return _normalize_to_iso_utc(match.group(1))


class ImapEmailAdapter:
    """
    Production-safe IMAP adapter with readonly polling.

    - Uses IMAP readonly select to avoid mutating mailbox state.
    - Supports sandbox mode for deterministic test execution.
    - Supports injectable IMAP client factory for mocked API responses.
    """

    provider_name = "imap"

    def __init__(
        self,
        *,
        host: str = "",
        username: str = "",
        password: str = "",
        mailbox: str = "INBOX",
        port: int = 993,
        search_criterion: str = "ALL",
        max_messages: int = 25,
        sandbox_mode: bool = False,
        sandbox_messages: list[dict[str, Any]] | None = None,
        imap_client_factory: Callable[[str, int], _ImapClientProtocol] | None = None,
    ) -> None:
        self.host = host
        self.username = username
        self.password = password
        self.mailbox = mailbox
        self.port = port
        self.search_criterion = search_criterion
        self.max_messages = max(1, int(max_messages))
        self.sandbox_mode = sandbox_mode
        self._sandbox_messages = [deepcopy(item) for item in (sandbox_messages or [])]
        self._imap_client_factory = imap_client_factory or (lambda host, port: imaplib.IMAP4_SSL(host, port))

    def poll_messages(self) -> list[dict[str, Any]]:
        if self.sandbox_mode:
            return [deepcopy(item) for item in self._sandbox_messages]

        if not self.host or not self.username or not self.password:
            raise ValueError("IMAP host/username/password are required outside sandbox mode")

        client = self._imap_client_factory(self.host, self.port)
        try:
            status, _ = client.login(self.username, self.password)
            if status.upper() != "OK":
                raise RuntimeError("IMAP login failed")

            status, _ = client.select(self.mailbox, readonly=True)
            if status.upper() != "OK":
                raise RuntimeError(f"IMAP select failed for mailbox '{self.mailbox}'")

            status, ids = client.search(None, self.search_criterion)
            if status.upper() != "OK":
                raise RuntimeError("IMAP search failed")

            message_ids = self._parse_message_ids(ids)
            message_ids = message_ids[-self.max_messages :]

            rows: list[dict[str, Any]] = []
            for message_id in message_ids:
                status, fetched = client.fetch(message_id, "(RFC822 INTERNALDATE)")
                if status.upper() != "OK":
                    continue

                raw = self._build_raw_imap_message(message_id=message_id, fetched=fetched)
                if raw is not None:
                    rows.append(raw)

            return rows
        finally:
            try:
                client.logout()
            except Exception:
                pass

    @staticmethod
    def _parse_message_ids(ids: list[bytes]) -> list[str]:
        tokens: list[str] = []
        for block in ids:
            text = block.decode("utf-8", errors="ignore").strip()
            if text:
                tokens.extend(part for part in text.split(" ") if part)

        # Stable numeric ordering when IDs are numeric strings.
        def _sort_key(value: str) -> tuple[int, str]:
            return (int(value), value) if value.isdigit() else (10**9, value)

        return sorted(tokens, key=_sort_key)

    @staticmethod
    def _build_raw_imap_message(*, message_id: str, fetched: list[Any]) -> dict[str, Any] | None:
        message_bytes: bytes | None = None
        internaldate = ""

        for item in fetched:
            if isinstance(item, tuple) and len(item) >= 2:
                meta, payload = item[0], item[1]
                if isinstance(payload, (bytes, bytearray)):
                    message_bytes = bytes(payload)
                if isinstance(meta, (bytes, str)):
                    internaldate = _extract_internaldate_from_fetch_meta(meta)

        if message_bytes is None:
            return None

        parsed = BytesParser(policy=policy.default).parsebytes(message_bytes)
        subject = _decode_mime_header(parsed.get("Subject"))
        sender = _decode_mime_header(parsed.get("From"))
        recipient = _decode_mime_header(parsed.get("To"))
        header_date = _decode_mime_header(parsed.get("Date"))

        text_body = ""
        if parsed.is_multipart():
            for part in parsed.walk():
                if part.get_content_type() == "text/plain":
                    text_body = part.get_content().strip()
                    if text_body:
                        break
        else:
            try:
                text_body = parsed.get_content().strip()
            except Exception:
                text_body = ""

        if internaldate == "":
            internaldate = _normalize_to_iso_utc(header_date)

        return {
            "uid": message_id,
            "envelope": {
                "from": sender,
                "to": recipient,
                "subject": subject,
            },
            "body": text_body,
            "internaldate": internaldate,
        }

    def parse_message(self, raw_message: dict[str, Any]) -> ParsedEmailMessage:
        envelope = raw_message.get("envelope") or {}
        received_at = _normalize_to_iso_utc(str(raw_message.get("internaldate", "")))

        return ParsedEmailMessage(
            email_id=str(raw_message.get("uid", "")),
            sender=str(envelope.get("from", "")),
            recipient=str(envelope.get("to", "")),
            subject=str(envelope.get("subject", "")),
            body=str(raw_message.get("body", "")),
            received_at=received_at,
            provider=self.provider_name,
        )
