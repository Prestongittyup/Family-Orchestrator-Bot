from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any


_URGENT_TERMS = {
    "urgent",
    "asap",
    "immediately",
    "deadline",
    "overdue",
    "critical",
    "final notice",
}

_IMPORTANT_TERMS = {
    "today",
    "tomorrow",
    "confirm",
    "approval",
    "payment",
    "invoice",
    "appointment",
    "meeting",
    "school",
    "doctor",
}

_ACTION_TERMS = {
    "please",
    "review",
    "submit",
    "send",
    "call",
    "schedule",
    "book",
    "pay",
    "reply",
    "complete",
    "follow up",
}

_CALENDAR_TERMS = {
    "meeting",
    "appointment",
    "call",
    "pickup",
    "drop-off",
    "drop off",
    "interview",
    "event",
    "visit",
}

_SENSITIVE_SENDERS = {
    "school",
    "clinic",
    "hospital",
    "bank",
    "billing",
    "teacher",
}

_ACTION_PATTERNS = [
    re.compile(r"\bplease\s+review\b", re.IGNORECASE),
    re.compile(r"\bcan\s+you\b", re.IGNORECASE),
    re.compile(r"\bneed\s+you\s+to\b", re.IGNORECASE),
    re.compile(r"\baction\s+required\b", re.IGNORECASE),
    re.compile(r"\blet\s+me\s+know\b", re.IGNORECASE),
]

_DEADLINE_PATTERNS = [
    re.compile(r"\bby\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", re.IGNORECASE),
    re.compile(r"\bby\s+\d{1,2}/\d{1,2}(?:/\d{2,4})?\b", re.IGNORECASE),
    re.compile(r"\bdue\s+(tomorrow|today|tonight)\b", re.IGNORECASE),
]

_QUESTION_PATTERNS = [
    re.compile(r"\bcan\s+you\b", re.IGNORECASE),
    re.compile(r"\bcould\s+you\b", re.IGNORECASE),
    re.compile(r"\blet\s+me\s+know\b", re.IGNORECASE),
]

_ATTACHMENT_TERMS = {
    "attached",
    "attachment",
    "attachments",
    "see attached",
    "pdf",
    "spreadsheet",
}

_THREAD_MARKER_RE = re.compile(r"^\s*>", re.IGNORECASE)
_REPLY_PREFIX_RE = re.compile(r"^\s*(re|fwd?)\s*:", re.IGNORECASE)

_JUNK_TERMS = {
    "unsubscribe",
    "newsletter",
    "special offer",
    "limited time",
    "deal",
    "promotion",
    "promotional",
    "coupon",
    "sale",
    "clearance",
    "sponsored",
    "view in browser",
    "manage preferences",
    "marketing",
}

_JUNK_SENDER_MARKERS = {
    "noreply",
    "no-reply",
    "mailer-daemon",
    "marketing",
    "promotions",
    "news",
}

_INFORMATIONAL_TERMS = {
    "reminder",
    "update",
    "details",
    "notice",
    "announcement",
    "confirmation",
}

_BUSINESS_SIGNAL_TERMS = {
    "invoice",
    "appointment",
    "meeting",
    "school",
    "doctor",
    "payment",
    "deadline",
}

_WEEKDAY_TO_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

_ISO_DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2}(?:[T\s]\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:\d{2})?)?)\b", re.IGNORECASE)
_TIME_PHRASE_RE = re.compile(r"\b(today|tomorrow|tonight|monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", re.IGNORECASE)
_MULTI_SPLIT_RE = re.compile(r"[\n\r]+")
_SENTENCE_SPLIT_RE = re.compile(r"[.!?]+")
_QUOTED_LINE_RE = re.compile(r"^\s*(?:>|On\s.+wrote:|From:\s|Sent:\s|To:\s|Subject:\s|Date:\s)", re.IGNORECASE)
_SUBJECT_PREFIX_RE = re.compile(r"^\s*(re|fwd?)\s*:\s*", re.IGNORECASE)


def _sender_policy_set(env_key: str) -> set[str]:
    raw = str(os.getenv(env_key, "")).strip()
    if raw == "":
        return set()
    return {
        " ".join(str(item).strip().lower().split())
        for item in re.split(r"[,;\n]", raw)
        if " ".join(str(item).strip().lower().split())
    }


_ALWAYS_HIGH_SENDERS = _sender_policy_set("EMAIL_RULE_ALWAYS_HIGH_SENDERS")
_ALWAYS_LOW_SENDERS = _sender_policy_set("EMAIL_RULE_ALWAYS_LOW_SENDERS")


@dataclass(frozen=True)
class EmailFeatures:
    sender: str
    subject: str
    body: str
    normalized_sender: str
    normalized_subject: str
    normalized_body: str
    is_direct: bool
    is_cc: bool
    has_attachments: bool
    thread_length: int
    contains_question: bool
    contains_deadline: bool
    contains_action: bool


def _normalize_text(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _trim_title(value: str, *, max_len: int = 96) -> str:
    compact = re.sub(r"^[\s\-\*\d\.)]+", "", value).strip()
    compact = " ".join(compact.split())
    if len(compact) <= max_len:
        return compact
    return f"{compact[: max_len - 3].rstrip()}..."


def _term_hits(text: str, terms: set[str]) -> int:
    hits = 0
    for term in terms:
        if term in text:
            hits += 1
    return hits


def _strip_quoted_lines(body: str) -> str:
    return "\n".join(
        line for line in body.splitlines()
        if not _QUOTED_LINE_RE.match(line)
    )


def _clean_action_title(raw: str, *, max_len: int = 120) -> str:
    if _QUOTED_LINE_RE.match(raw):
        return ""

    text = raw.strip().lstrip(">").strip()
    text = " ".join(text.split())
    if not text:
        return ""

    text = text[0].upper() + text[1:]
    if len(text) <= max_len:
        return text
    return f"{text[: max_len - 3].rstrip()}..."


def _detect_action(text: str) -> bool:
    normalized = _normalize_text(text)
    if _term_hits(normalized, _ACTION_TERMS) > 0:
        return True
    return any(pattern.search(normalized) is not None for pattern in _ACTION_PATTERNS)


def _detect_deadline(text: str, *, received_at: datetime) -> bool:
    normalized = _normalize_text(text)
    if any(pattern.search(normalized) is not None for pattern in _DEADLINE_PATTERNS):
        return True
    return _extract_due_hint(normalized, received_at=received_at) is not None


def _detect_question(text: str) -> bool:
    if "?" in text:
        return True
    normalized = _normalize_text(text)
    return any(pattern.search(normalized) is not None for pattern in _QUESTION_PATTERNS)


def _detect_attachments(*, body: str, subject: str, attachment_count: int | None) -> bool:
    if attachment_count is not None:
        return attachment_count > 0

    combined = _normalize_text(f"{subject} {body}")
    return _term_hits(combined, _ATTACHMENT_TERMS) > 0


def _estimate_thread_length(*, subject: str, body: str, explicit_thread_length: int | None) -> int:
    if explicit_thread_length is not None:
        return max(1, int(explicit_thread_length))

    count = 1
    if _REPLY_PREFIX_RE.search(subject) is not None:
        count += 1

    quoted_lines = sum(1 for line in body.splitlines() if _THREAD_MARKER_RE.search(line) is not None)
    if quoted_lines > 0:
        count += min(12, quoted_lines)

    return max(1, min(20, count))


def _sender_matches_policy(sender: str, policy_set: set[str]) -> bool:
    if not policy_set:
        return False

    normalized_sender = _normalize_text(sender)
    if normalized_sender in policy_set:
        return True

    if "@" in normalized_sender:
        local, domain = normalized_sender.split("@", 1)
    else:
        local, domain = normalized_sender, ""

    if local in policy_set or domain in policy_set:
        return True

    return any(marker in normalized_sender for marker in policy_set)


def _sender_is_junk_domain(sender: str) -> bool:
    normalized = _normalize_text(sender)
    return any(marker in normalized for marker in _JUNK_SENDER_MARKERS)


def _resolve_recipient_flags(*, recipient: str | None, to_me: bool | None, cc_me: bool | None) -> tuple[bool, bool]:
    if to_me is not None or cc_me is not None:
        resolved_direct = bool(to_me) if to_me is not None else not bool(cc_me)
        resolved_cc = bool(cc_me) if cc_me is not None else not resolved_direct
        return resolved_direct, resolved_cc

    normalized_recipient = _normalize_text(recipient or "")
    if normalized_recipient == "":
        return True, False

    if "cc:" in normalized_recipient:
        return False, True

    if "," in normalized_recipient or ";" in normalized_recipient:
        return False, True

    return True, False


def _build_features(
    *,
    sender: str,
    subject: str,
    body: str,
    received_at: datetime,
    recipient: str | None,
    to_me: bool | None,
    cc_me: bool | None,
    attachment_count: int | None,
    thread_length: int | None,
) -> EmailFeatures:
    normalized_sender = _normalize_text(sender)
    normalized_subject = _normalize_text(subject)
    normalized_body = _normalize_text(body)
    combined = f"{normalized_subject} {normalized_body}".strip()
    is_direct, is_cc = _resolve_recipient_flags(recipient=recipient, to_me=to_me, cc_me=cc_me)

    return EmailFeatures(
        sender=sender,
        subject=subject,
        body=body,
        normalized_sender=normalized_sender,
        normalized_subject=normalized_subject,
        normalized_body=normalized_body,
        is_direct=is_direct,
        is_cc=is_cc,
        has_attachments=_detect_attachments(body=body, subject=subject, attachment_count=attachment_count),
        thread_length=_estimate_thread_length(subject=subject, body=body, explicit_thread_length=thread_length),
        contains_question=_detect_question(combined),
        contains_deadline=_detect_deadline(combined, received_at=received_at),
        contains_action=_detect_action(combined),
    )


def _score_email_features(features: EmailFeatures) -> int:
    score = 0

    if _sender_matches_policy(features.normalized_sender, _ALWAYS_HIGH_SENDERS):
        score += 20
    elif _sender_matches_policy(features.normalized_sender, _ALWAYS_LOW_SENDERS):
        score -= 12
    elif _term_hits(features.normalized_sender, _SENSITIVE_SENDERS) > 0 and not _sender_is_junk_domain(features.normalized_sender):
        score += 10

    if features.is_direct:
        score += 4
    elif features.is_cc:
        score += 1

    if features.contains_action:
        score += 5

    if features.contains_deadline:
        score += 6

    if features.contains_question:
        score += 2

    if features.has_attachments:
        score += 2

    if features.thread_length > 4:
        score += 3

    if "unsubscribe" in features.normalized_body:
        score -= 10

    if "newsletter" in features.normalized_subject:
        score -= 5

    return score


def _classify_priority(score: int) -> str:
    if score >= 15:
        return "HIGH"
    if score >= 8:
        return "MEDIUM"
    return "LOW"


def _importance_from_rule_score(score: int) -> tuple[float, str]:
    bounded = max(-10, min(30, score))
    importance_score = round((bounded + 10) / 40, 3)
    if score >= 22:
        return importance_score, "critical"
    if score >= 15:
        return importance_score, "high"
    if score >= 8:
        return importance_score, "medium"
    return importance_score, "low"


def _extract_key_sentence(body: str) -> str:
    clean_body = _strip_quoted_lines(body)
    text = clean_body.replace("\r", " ").replace("\n", " ").strip()
    if text == "":
        return ""

    sentences = [segment.strip() for segment in _SENTENCE_SPLIT_RE.split(text) if segment.strip()]
    if not sentences:
        return ""

    for sentence in sentences:
        normalized = _normalize_text(sentence)
        if _detect_action(normalized) or _detect_question(normalized):
            return _trim_title(sentence, max_len=160)
        if _term_hits(normalized, _URGENT_TERMS) > 0 or _term_hits(normalized, _IMPORTANT_TERMS) > 0:
            return _trim_title(sentence, max_len=160)

    return _trim_title(sentences[0], max_len=160)


def _display_sender(sender: str) -> str:
    match = re.match(r"^(.+?)\s*<[^>]+>", sender.strip())
    if match:
        return match.group(1).strip()
    if "@" in sender:
        return sender.split("@", 1)[0].strip()
    return sender.strip() or "unknown sender"


def _clean_subject(subject: str) -> str:
    cleaned = subject.strip()
    while True:
        next_cleaned = _SUBJECT_PREFIX_RE.sub("", cleaned).strip()
        if next_cleaned == cleaned:
            break
        cleaned = next_cleaned
    return cleaned or "no subject"


def _build_summary(
    *,
    features: EmailFeatures,
    priority_label: str,
    action_items: list[dict[str, Any]],
    calendar_candidates: list[dict[str, Any]],
    is_junk: bool,
) -> str:
    if is_junk:
        return "Promotional or automated email - no action needed."

    sender_name = _display_sender(features.sender)
    clean_subject = _clean_subject(features.subject)
    priority_word = {
        "high": "urgent",
        "medium": "important",
        "low": "low-priority",
    }.get(priority_label.lower(), "new")

    best_action: str | None = None
    if action_items:
        top = action_items[0]
        raw_title = str(top.get("title", "")).strip()
        cleaned_title = _clean_action_title(raw_title)
        if cleaned_title and len(cleaned_title) <= 100:
            best_action = cleaned_title[0].lower() + cleaned_title[1:]

    deadline_hint: str | None = None
    if action_items:
        for item in action_items:
            local_due = item.get("due_hint_local") or item.get("due_hint")
            if local_due:
                deadline_hint = str(local_due)
                break

    if deadline_hint is None and calendar_candidates:
        for candidate in calendar_candidates:
            local_hint = candidate.get("time_hint_local") or candidate.get("time_hint")
            if local_hint:
                deadline_hint = str(local_hint)
                break

    if best_action and deadline_hint:
        return f"{sender_name} is asking you to {best_action} - due {deadline_hint}."
    if best_action:
        return f"{sender_name} is asking you to {best_action}."
    if features.contains_deadline and deadline_hint:
        return f"{priority_word.capitalize()} email from {sender_name} re: \"{clean_subject}\" - {deadline_hint}."
    if features.contains_question:
        return f"{sender_name} is asking a question about \"{clean_subject}\"."
    if features.has_attachments:
        return f"{sender_name} sent an attachment re: \"{clean_subject}\"."
    if calendar_candidates:
        return f"{sender_name} may be scheduling something re: \"{clean_subject}\"."

    return f"{priority_word.capitalize()} email from {sender_name} re: \"{clean_subject}\"."


def _local_timezone():
    return datetime.now().astimezone().tzinfo


def _to_local_datetime(value: datetime) -> datetime:
    local_tz = _local_timezone()
    if value.tzinfo is None:
        if local_tz is None:
            return value
        return value.replace(tzinfo=local_tz)
    if local_tz is None:
        return value
    return value.astimezone(local_tz)


def _format_local_hint(value: str | None) -> str | None:
    if value is None:
        return None

    text = value.strip()
    if text == "":
        return None

    if "T" not in text:
        try:
            parsed_date = datetime.fromisoformat(f"{text}T00:00:00")
            return parsed_date.strftime("%a %b %d")
        except ValueError:
            return text

    candidate = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return text

    local_value = _to_local_datetime(parsed)
    tz_name = ""
    if local_value.tzinfo is not None:
        tz_name = local_value.tzname() or ""
    suffix = f" {tz_name}" if tz_name else ""
    return f"{local_value.strftime('%a %b %d %I:%M %p')}{suffix}"


def _extract_due_hint(text: str, *, received_at: datetime) -> str | None:
    normalized = _normalize_text(text)

    iso_match = _ISO_DATE_RE.search(normalized)
    if iso_match:
        raw_iso = iso_match.group(1).replace(" ", "T").replace("t", "T")
        has_time_component = "T" in raw_iso
        if not has_time_component:
            return raw_iso

        candidate = raw_iso.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return raw_iso
        local_value = _to_local_datetime(parsed)
        return local_value.replace(microsecond=0).isoformat()

    phrase_match = _TIME_PHRASE_RE.search(normalized)
    if phrase_match is None:
        return None

    token = phrase_match.group(1).lower()
    anchor = _to_local_datetime(received_at).date()

    if token in {"today", "tonight"}:
        return anchor.isoformat()
    if token == "tomorrow":
        return (anchor + timedelta(days=1)).isoformat()

    target_weekday = _WEEKDAY_TO_INDEX.get(token)
    if target_weekday is None:
        return None

    day_offset = (target_weekday - anchor.weekday()) % 7
    if day_offset == 0:
        day_offset = 7
    return (anchor + timedelta(days=day_offset)).isoformat()


def _candidate_lines(subject: str, body: str) -> list[tuple[str, bool, int]]:
    rows: list[tuple[str, bool, int]] = []

    cleaned_subject = _trim_title(subject)
    if cleaned_subject:
        rows.append((cleaned_subject, True, 0))

    clean_body = _strip_quoted_lines(body)
    raw_lines = _MULTI_SPLIT_RE.split(clean_body)
    line_index = 1
    for raw in raw_lines:
        for sentence in _SENTENCE_SPLIT_RE.split(raw):
            candidate = _trim_title(sentence)
            if not candidate:
                continue
            rows.append((candidate, False, line_index))
            line_index += 1

    deduped: list[tuple[str, bool, int]] = []
    seen: set[str] = set()
    for line, is_subject, idx in rows:
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append((line, is_subject, idx))
    return deduped


def _is_action_line(line: str) -> bool:
    normalized = _normalize_text(line)
    if _detect_action(normalized):
        return True
    if _detect_question(normalized):
        return True
    if any(pattern.search(normalized) is not None for pattern in _DEADLINE_PATTERNS):
        return True
    return _term_hits(normalized, _URGENT_TERMS) > 0


def _is_action_line_with_context(line: str, *, received_at: datetime) -> bool:
    normalized = _normalize_text(line)
    if _detect_action(normalized):
        return True
    if _detect_deadline(normalized, received_at=received_at):
        return True
    if _detect_question(normalized):
        return True
    return _term_hits(normalized, _URGENT_TERMS) > 0


def _action_score(*, line: str, is_subject: bool, due_hint: str | None, received_at: datetime) -> float:
    normalized = _normalize_text(line)
    score = 0.22
    if _detect_action(normalized):
        score += 0.28
    if _detect_deadline(normalized, received_at=received_at):
        score += 0.22
    if _detect_question(normalized):
        score += 0.1
    score += min(0.22, _term_hits(normalized, _URGENT_TERMS) * 0.09)
    score += min(0.12, _term_hits(normalized, _IMPORTANT_TERMS) * 0.05)
    if due_hint:
        score += 0.16
    if is_subject:
        score += 0.05
    return round(min(1.0, score), 3)


def _importance_bucket(score: float) -> str:
    if score >= 0.78:
        return "critical"
    if score >= 0.58:
        return "high"
    if score >= 0.38:
        return "medium"
    return "low"


def _junk_score(*, sender: str, subject: str, body: str) -> float:
    sender_text = _normalize_text(sender)
    subject_text = _normalize_text(subject)
    body_text = _normalize_text(body)
    combined_text = f"{subject_text} {body_text}".strip()

    score = 0.0
    score += min(0.42, _term_hits(combined_text, _JUNK_TERMS) * 0.08)
    score += min(0.28, _term_hits(sender_text, _JUNK_SENDER_MARKERS) * 0.14)

    if "@" in sender_text:
        domain = sender_text.split("@", 1)[1]
        if any(marker in domain for marker in ("newsletter", "marketing", "promo", "offers")):
            score += 0.18

    exclamation_count = subject.count("!") + body.count("!")
    if exclamation_count >= 4:
        score += 0.12
    elif exclamation_count >= 2:
        score += 0.06

    # Strong business/action signals lower junk confidence.
    score -= min(0.22, _term_hits(combined_text, _BUSINESS_SIGNAL_TERMS) * 0.08)
    score -= min(0.14, _term_hits(combined_text, _URGENT_TERMS) * 0.06)

    return round(max(0.0, min(1.0, score)), 3)


def _is_likely_junk(*, importance_score: float, junk_score: float) -> bool:
    if junk_score >= 0.72:
        return True
    if junk_score >= 0.62 and importance_score < 0.7:
        return True
    return False


def _triage_decision(*, is_junk: bool, importance_score: float, action_count: int, calendar_count: int) -> str:
    if importance_score >= 0.7:
        priority_label = "HIGH"
    elif importance_score >= 0.45:
        priority_label = "MEDIUM"
    else:
        priority_label = "LOW"
    return _triage_decision_with_priority(
        is_junk=is_junk,
        priority_label=priority_label,
        action_count=action_count,
        calendar_count=calendar_count,
    )


def _triage_decision_with_priority(*, is_junk: bool, priority_label: str, action_count: int, calendar_count: int) -> str:
    if is_junk:
        return "junk"
    if action_count > 0 or calendar_count > 0:
        return "task"
    if priority_label == "HIGH":
        return "task"
    return "informational"


def summarize_email_to_actions(
    *,
    sender: str,
    subject: str,
    body: str,
    received_at: datetime,
    recipient: str | None = None,
    to_me: bool | None = None,
    cc_me: bool | None = None,
    attachment_count: int | None = None,
    thread_length: int | None = None,
) -> dict[str, Any]:
    features = _build_features(
        sender=sender,
        subject=subject,
        body=body,
        received_at=received_at,
        recipient=recipient,
        to_me=to_me,
        cc_me=cc_me,
        attachment_count=attachment_count,
        thread_length=thread_length,
    )

    normalized_sender = features.normalized_sender

    rule_score = _score_email_features(features)
    priority_label = _classify_priority(rule_score)
    importance_score, importance_bucket = _importance_from_rule_score(rule_score)

    candidate_lines = _candidate_lines(subject=subject, body=body)

    action_rows: list[dict[str, Any]] = []
    for line, is_subject, idx in candidate_lines:
        if not _is_action_line_with_context(line, received_at=received_at):
            continue

        clean_title = _clean_action_title(line)
        if not clean_title:
            continue

        due_hint = _extract_due_hint(line, received_at=received_at)
        score = _action_score(line=line, is_subject=is_subject, due_hint=due_hint, received_at=received_at)
        action_rows.append(
            {
                "title": clean_title,
                "details": "email_subject" if is_subject else "email_body",
                "importance_score": score,
                "importance_bucket": _importance_bucket(score),
                "due_hint": due_hint,
                "due_hint_local": _format_local_hint(due_hint),
                "source_line": idx,
            }
        )

    action_rows.sort(
        key=lambda row: (
            -float(row["importance_score"]),
            str(row["title"]).lower(),
            int(row["source_line"]),
        )
    )
    action_items = action_rows[:5]

    calendar_rows: list[dict[str, Any]] = []
    for line, is_subject, idx in candidate_lines:
        normalized_line = _normalize_text(line)
        due_hint = _extract_due_hint(line, received_at=received_at)
        term_hits = _term_hits(normalized_line, _CALENDAR_TERMS)
        if term_hits == 0 and due_hint is None and not _detect_deadline(normalized_line, received_at=received_at):
            continue
        confidence = 0.42 + min(0.24, term_hits * 0.08)
        if due_hint is not None:
            confidence += 0.24
        if is_subject:
            confidence += 0.06
        calendar_rows.append(
            {
                "title": line,
                "time_hint": due_hint,
                "time_hint_local": _format_local_hint(due_hint),
                "confidence": round(min(1.0, confidence), 3),
                "source_line": idx,
            }
        )

    deduped_calendar: list[dict[str, Any]] = []
    seen_calendar: set[tuple[str, str | None]] = set()
    for row in sorted(
        calendar_rows,
        key=lambda item: (-float(item["confidence"]), str(item["title"]).lower(), int(item["source_line"])),
    ):
        key = (str(row["title"]).lower(), row.get("time_hint"))
        if key in seen_calendar:
            continue
        seen_calendar.add(key)
        deduped_calendar.append(row)
    calendar_candidates = deduped_calendar[:3]

    informational_items: list[dict[str, Any]] = []
    action_titles = {str(row.get("title", "")).strip().lower() for row in action_items}
    for line, is_subject, idx in candidate_lines:
        normalized_line = _normalize_text(line)
        if not normalized_line:
            continue
        if normalized_line in action_titles:
            continue

        if _is_action_line_with_context(line, received_at=received_at):
            continue

        due_hint = _extract_due_hint(line, received_at=received_at)
        informational_signal = _term_hits(normalized_line, _INFORMATIONAL_TERMS) + _term_hits(normalized_line, _IMPORTANT_TERMS)
        if due_hint is None and informational_signal == 0:
            continue

        informational_items.append(
            {
                "title": line,
                "details": "email_subject" if is_subject else "email_body",
                "due_hint": due_hint,
                "due_hint_local": _format_local_hint(due_hint),
                "source_line": idx,
            }
        )

    informational_items = informational_items[:3]

    junk_score = _junk_score(sender=sender, subject=subject, body=body)
    is_junk = _is_likely_junk(importance_score=importance_score, junk_score=junk_score)

    if _sender_matches_policy(normalized_sender, _ALWAYS_HIGH_SENDERS):
        is_junk = False
    if _sender_matches_policy(normalized_sender, _ALWAYS_LOW_SENDERS) and priority_label == "LOW":
        is_junk = True

    triage_decision = _triage_decision_with_priority(
        is_junk=is_junk,
        priority_label=priority_label,
        action_count=len(action_items),
        calendar_count=len(calendar_candidates),
    )

    if is_junk:
        action_items = []
        calendar_candidates = []
        informational_items = []

    summary = _build_summary(
        features=features,
        priority_label=priority_label.lower(),
        action_items=action_items,
        calendar_candidates=calendar_candidates,
        is_junk=is_junk,
    )

    return {
        "summary": summary,
        "importance_score": importance_score,
        "importance_bucket": importance_bucket,
        "priority_label": priority_label.lower(),
        "rule_score": rule_score,
        "junk_score": junk_score,
        "is_junk": is_junk,
        "triage_decision": triage_decision,
        "action_items": action_items,
        "calendar_candidates": calendar_candidates,
        "informational_items": informational_items,
    }