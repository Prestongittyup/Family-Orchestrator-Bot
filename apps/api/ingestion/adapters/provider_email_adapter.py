from __future__ import annotations

import base64
import html
import re
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

from archive.apps.api.ingestion.adapters.email_provider_adapter import ParsedEmailMessage


_PROVIDER_ALIASES: dict[str, str] = {
    "google": "google",
    "gmail": "google",
    "googlemail": "google",
    "outlook": "outlook",
    "microsoft": "outlook",
    "office365": "outlook",
    "msgraph": "outlook",
    "yahoo": "yahoo",
    "ymail": "yahoo",
    "imap": "imap",
    "generic": "generic",
}


_HTML_BLOCK_BREAK_RE = re.compile(r"<(?:br|/p|/div|/li|/tr|/h[1-6]|/section|/article)\b[^>]*>", re.IGNORECASE)
_HTML_SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)
_HTML_TAG_RE = re.compile(r"<[^>]+>", re.IGNORECASE)


def _looks_like_html(value: str) -> bool:
    return bool(re.search(r"<\s*[a-zA-Z][^>]*>", value))


def _html_to_text(value: str) -> str:
    normalized = value.replace("\r\n", "\n").replace("\r", "\n")
    without_scripts = _HTML_SCRIPT_STYLE_RE.sub(" ", normalized)
    with_breaks = _HTML_BLOCK_BREAK_RE.sub("\n", without_scripts)
    without_tags = _HTML_TAG_RE.sub(" ", with_breaks)
    unescaped = html.unescape(without_tags)
    compact = re.sub(r"[ \t\f\v]+", " ", unescaped)
    compact = re.sub(r"\n{3,}", "\n\n", compact)
    return compact.strip()


def _normalize_email_body(value: str) -> str:
    text = str(value or "").strip()
    if text == "":
        return ""

    if _looks_like_html(text):
        return _html_to_text(text)

    return html.unescape(text).replace("\r\n", "\n").replace("\r", "\n").strip()


def normalize_provider_name(provider_name: str | None) -> str:
    text = str(provider_name or "generic").strip().lower()
    if not text:
        return "generic"
    return _PROVIDER_ALIASES.get(text, text)


def _coerce_iso(value: Any) -> str:
    if value is None:
        return ""

    text = str(value).strip()
    if text == "":
        return ""

    if text.isdigit():
        epoch = int(text)
        if epoch > 10_000_000_000:
            epoch = epoch // 1000
        return datetime.fromtimestamp(epoch, tz=UTC).isoformat().replace("+00:00", "Z")

    if text.endswith("Z"):
        return text

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        else:
            parsed = parsed.astimezone(UTC)
        return parsed.isoformat().replace("+00:00", "Z")
    except ValueError:
        return text


def _header_value(headers: list[dict[str, Any]], name: str) -> str:
    wanted = name.strip().lower()
    for row in headers:
        if not isinstance(row, dict):
            continue
        key = str(row.get("name", "")).strip().lower()
        if key != wanted:
            continue
        return str(row.get("value", "")).strip()
    return ""


def _decode_base64_url(data: str) -> str:
    raw = str(data or "").strip()
    if raw == "":
        return ""

    padding = "=" * (-len(raw) % 4)
    try:
        decoded = base64.urlsafe_b64decode(raw + padding)
        return decoded.decode("utf-8", errors="ignore").strip()
    except Exception:
        return ""


def _extract_gmail_body(raw_message: dict[str, Any]) -> str:
    payload = raw_message.get("payload")
    if not isinstance(payload, dict):
        return str(raw_message.get("snippet", ""))

    stack: list[dict[str, Any]] = [payload]
    while stack:
        part = stack.pop()
        if not isinstance(part, dict):
            continue

        mime_type = str(part.get("mimeType", "")).lower()
        body = part.get("body") or {}
        if isinstance(body, dict) and body.get("data"):
            decoded = _decode_base64_url(str(body.get("data", "")))
            if decoded:
                if mime_type in {"text/plain", ""}:
                    return decoded
                if mime_type.startswith("text/"):
                    return decoded

        parts = part.get("parts")
        if isinstance(parts, list):
            for child in reversed(parts):
                if isinstance(child, dict):
                    stack.append(child)

    return str(raw_message.get("snippet", ""))


def _parse_google_message(raw_message: dict[str, Any]) -> dict[str, str]:
    payload = raw_message.get("payload")
    headers = payload.get("headers") if isinstance(payload, dict) else []
    if not isinstance(headers, list):
        headers = []

    return {
        "email_id": str(raw_message.get("id") or raw_message.get("message_id") or ""),
        "sender": (
            str(raw_message.get("from", "")).strip()
            or _header_value(headers, "From")
        ),
        "recipient": (
            str(raw_message.get("to", "")).strip()
            or str(raw_message.get("recipient", "")).strip()
            or _header_value(headers, "To")
        ),
        "subject": (
            str(raw_message.get("subject", "")).strip()
            or _header_value(headers, "Subject")
        ),
        "body": str(raw_message.get("body", "")).strip() or _extract_gmail_body(raw_message),
        "received_at": _coerce_iso(
            raw_message.get("received_at")
            or raw_message.get("internalDate")
            or raw_message.get("internal_date")
        ),
    }


def _extract_graph_email_address(value: Any) -> str:
    if isinstance(value, dict):
        email_obj = value.get("emailAddress")
        if isinstance(email_obj, dict):
            return str(email_obj.get("address", "")).strip()
        return str(value.get("address", "")).strip()
    return str(value or "").strip()


def _parse_outlook_message(raw_message: dict[str, Any]) -> dict[str, str]:
    recipients = raw_message.get("toRecipients")
    first_recipient = recipients[0] if isinstance(recipients, list) and recipients else None

    body_obj = raw_message.get("body")
    body = ""
    if isinstance(body_obj, dict):
        body = str(body_obj.get("content", "")).strip()
    elif isinstance(body_obj, str):
        body = body_obj.strip()

    return {
        "email_id": str(raw_message.get("id") or raw_message.get("internetMessageId") or ""),
        "sender": (
            _extract_graph_email_address(raw_message.get("from"))
            or _extract_graph_email_address(raw_message.get("sender"))
        ),
        "recipient": (
            _extract_graph_email_address(first_recipient)
            or str(raw_message.get("to", "")).strip()
            or str(raw_message.get("recipient", "")).strip()
        ),
        "subject": str(raw_message.get("subject", "")).strip(),
        "body": body or str(raw_message.get("bodyPreview", "")).strip(),
        "received_at": _coerce_iso(
            raw_message.get("receivedDateTime")
            or raw_message.get("received_at")
        ),
    }


def _parse_yahoo_message(raw_message: dict[str, Any]) -> dict[str, str]:
    headers = raw_message.get("headers")
    if not isinstance(headers, dict):
        headers = {}

    envelope = raw_message.get("envelope")
    if not isinstance(envelope, dict):
        envelope = {}

    return {
        "email_id": str(raw_message.get("id") or raw_message.get("mid") or raw_message.get("uid") or ""),
        "sender": (
            str(raw_message.get("from", "")).strip()
            or str(envelope.get("from", "")).strip()
            or str(headers.get("from", "")).strip()
        ),
        "recipient": (
            str(raw_message.get("to", "")).strip()
            or str(raw_message.get("recipient", "")).strip()
            or str(envelope.get("to", "")).strip()
            or str(headers.get("to", "")).strip()
        ),
        "subject": (
            str(raw_message.get("subject", "")).strip()
            or str(envelope.get("subject", "")).strip()
            or str(headers.get("subject", "")).strip()
        ),
        "body": (
            str(raw_message.get("body", "")).strip()
            or str(raw_message.get("content", "")).strip()
        ),
        "received_at": _coerce_iso(
            raw_message.get("receivedDate")
            or raw_message.get("internaldate")
            or raw_message.get("received_at")
        ),
    }


def _parse_generic_message(raw_message: dict[str, Any]) -> dict[str, str]:
    if "envelope" in raw_message:
        envelope = raw_message.get("envelope") or {}
        return {
            "email_id": str(raw_message.get("uid", "")),
            "sender": str(envelope.get("from", "")).strip(),
            "recipient": str(envelope.get("to", "")).strip(),
            "subject": str(envelope.get("subject", "")).strip(),
            "body": str(raw_message.get("body", "")).strip(),
            "received_at": _coerce_iso(raw_message.get("internaldate")),
        }

    return {
        "email_id": str(raw_message.get("id") or raw_message.get("email_id") or ""),
        "sender": str(raw_message.get("from") or raw_message.get("sender") or "").strip(),
        "recipient": str(raw_message.get("to") or raw_message.get("recipient") or "").strip(),
        "subject": str(raw_message.get("subject", "")).strip(),
        "body": str(raw_message.get("body", "")).strip(),
        "received_at": _coerce_iso(raw_message.get("received_at") or raw_message.get("date")),
    }


def _extract_thread_messages(raw_message: dict[str, Any]) -> list[Any]:
    candidates = (
        raw_message.get("thread_messages"),
        raw_message.get("threadMessages"),
        raw_message.get("conversation_messages"),
    )
    for candidate in candidates:
        if isinstance(candidate, list):
            return [item for item in candidate]
    return []


def _resolve_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


class ProviderEmailAdapter:
    """
    Provider-native email adapter for Google, Outlook, Yahoo, and generic inputs.

    Unknown provider names are accepted and parsed using the generic schema.
    """

    def __init__(
        self,
        *,
        provider_name: str,
        fixed_poll_dataset: list[dict[str, Any]] | None = None,
    ) -> None:
        self.provider_name = normalize_provider_name(provider_name)
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
        if not isinstance(raw_message, dict):
            raise ValueError("raw_message must be a dict")

        parser = _parse_generic_message
        if self.provider_name == "google":
            parser = _parse_google_message
        elif self.provider_name == "outlook":
            parser = _parse_outlook_message
        elif self.provider_name == "yahoo":
            parser = _parse_yahoo_message
        elif self.provider_name == "imap":
            parser = _parse_generic_message

        parsed = parser(raw_message)

        return ParsedEmailMessage(
            email_id=str(parsed.get("email_id", "")),
            sender=str(parsed.get("sender", "")),
            recipient=str(parsed.get("recipient", "")),
            subject=str(parsed.get("subject", "")),
            body=_normalize_email_body(str(parsed.get("body", ""))),
            received_at=str(parsed.get("received_at", "")),
            provider=self.provider_name,
            thread_id=(
                str(raw_message.get("thread_id") or raw_message.get("threadId") or raw_message.get("conversationId") or "").strip()
                or None
            ),
            latest_message_id=(
                str(raw_message.get("latest_message_id") or raw_message.get("latestMessageId") or parsed.get("email_id") or "").strip()
                or None
            ),
            thread_messages=_extract_thread_messages(raw_message),
            to_me=_resolve_bool(raw_message.get("to_me")),
            cc_me=_resolve_bool(raw_message.get("cc_me") or raw_message.get("is_cc")),
        )
