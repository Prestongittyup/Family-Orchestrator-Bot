from __future__ import annotations

import hashlib
import re
from datetime import UTC, date as date_value, datetime
from typing import Any, Mapping, Sequence


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _as_mapping_list(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return [row for row in value if isinstance(row, Mapping)]
    return []


def _utc_iso(raw_value: Any) -> str | None:
    if isinstance(raw_value, datetime):
        parsed = raw_value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")

    text = str(raw_value or "").strip()
    if not text:
        return None

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _iso_date(raw_value: Any) -> str | None:
    iso = _utc_iso(raw_value)
    if not iso:
        return None
    return iso[:10]


def _stable_id(*parts: str) -> str:
    token = "|".join(part.strip() for part in parts)
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]


def _parse_iso_datetime(raw_value: Any) -> datetime | None:
    iso = _utc_iso(raw_value)
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return None


_PRIORITY_RANK = {"high": 0, "medium": 1, "low": 2}
_ACTION_VERBS = {
    "confirm",
    "reply",
    "call",
    "schedule",
    "send",
    "review",
    "book",
    "pay",
    "check",
    "update",
    "share",
    "complete",
    "submit",
    "approve",
}
_FILLER_WORDS = {
    "please",
    "kindly",
    "just",
    "the",
    "a",
    "an",
    "that",
    "this",
    "can",
    "could",
    "would",
    "should",
}
_SAME_DAY_HINTS = (
    "today",
    "tonight",
    "this morning",
    "this afternoon",
    "this evening",
    "eod",
    "end of day",
    "asap",
    "urgent",
)
HOME_V0_ROOT_KEYS = ("needs_decision", "actions", "calendar", "summary")


def _pluralize(word: str, count: int) -> str:
    if count == 1:
        return word
    return f"{word}s"


def _trim_phrase(text: str, *, max_words: int) -> str:
    words = [part for part in text.split() if part]
    return " ".join(words[:max_words])


def _content_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9']+", str(text or "").lower())
        if len(token) >= 4 and token not in _FILLER_WORDS
    }


def _has_same_day_hint(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(token in lowered for token in _SAME_DAY_HINTS)


def _verb_first_action_title(action_text: str, fallback: str) -> str:
    source = re.sub(r"^\s*action\s*:\s*", "", action_text or "", flags=re.IGNORECASE).strip()
    if not source:
        source = fallback

    raw_words = re.findall(r"[A-Za-z0-9']+", source.lower())
    words = [word for word in raw_words if word not in _FILLER_WORDS]
    if not words:
        return "Review action item"

    verb_index = next((index for index, word in enumerate(words) if word in _ACTION_VERBS), None)
    if verb_index is not None:
        candidate = words[verb_index : verb_index + 8]
    else:
        candidate = ["review", *words[:7]]

    title = " ".join(candidate[:8]).strip()
    if not title:
        return "Review action item"
    return title.capitalize()


def _action_priority(*, title: str, summary: str) -> str:
    combined = f"{title} {summary}".lower()
    if any(token in combined for token in _SAME_DAY_HINTS):
        return "high"
    if title:
        return "medium"
    return "low"


def _summary_narrative(
    *,
    conflict_count: int,
    action_count: int,
    event_count: int,
) -> str:
    if conflict_count > 0:
        conflict_verb = "requires" if conflict_count == 1 else "require"
        conflict_sentence = (
            f"Conflicts: {conflict_count} {_pluralize('conflict', conflict_count)} {conflict_verb} decisions."
        )
    else:
        conflict_sentence = "Conflicts: none."

    action_sentence = f"Actions: {action_count} email {_pluralize('action', action_count)} pending."
    schedule_sentence = f"Schedule: {event_count} {_pluralize('event', event_count)} planned."
    return f"{conflict_sentence} {action_sentence} {schedule_sentence}"


def freeze_home_v0_contract(payload: Mapping[str, Any]) -> dict[str, Any]:
    needs_decision_raw = payload.get("needs_decision")
    actions_raw = payload.get("actions")
    calendar_raw = payload.get("calendar")

    needs_decision = [
        dict(item)
        for item in needs_decision_raw
        if isinstance(item, Mapping)
    ] if isinstance(needs_decision_raw, list) else []

    actions = [
        dict(item)
        for item in actions_raw
        if isinstance(item, Mapping)
    ] if isinstance(actions_raw, list) else []

    calendar = [
        dict(item)
        for item in calendar_raw
        if isinstance(item, Mapping)
    ] if isinstance(calendar_raw, list) else []

    return {
        "needs_decision": needs_decision,
        "actions": actions,
        "calendar": calendar,
        "summary": str(payload.get("summary") or ""),
    }


def email_agent(raw_emails: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Build normalized email items with basic action/noise classification."""
    parser = EmailAgent()
    noise_tokens = ("newsletter", "unsubscribe", "promotion", "promo", "sale", "digest")

    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(raw_emails, start=1):
        subject = str(row.get("subject") or "").strip()
        body = str(row.get("body") or row.get("snippet") or "").strip()
        summary = subject or body or f"email-{index}"

        extracted_actions = parser.extract_action_items(row)
        action_required = None
        if extracted_actions:
            action_required = str(extracted_actions[0].get("title") or "").strip() or None

        lowered_summary = f"{subject} {body}".strip().lower()
        classification = "fyi"
        if lowered_summary and any(token in lowered_summary for token in noise_tokens):
            classification = "noise"
        elif action_required:
            classification = "action"

        normalized.append(
            {
                "id": str(row.get("email_id") or row.get("id") or f"email-{_stable_id(summary, str(index))}"),
                "summary": summary,
                "action_required": action_required,
                "classification": classification,
            }
        )

    normalized.sort(key=lambda item: (str(item.get("id") or ""), str(item.get("summary") or "")))
    return normalized


def calendar_agent(calendar_events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Normalize calendar events and mark whether each event is in conflict."""
    parser = CalendarAgent()
    normalized_events = parser.normalize_events({"events": list(calendar_events)})
    conflicts = parser.detect_conflicts(normalized_events)

    conflict_event_ids: set[str] = set()
    for item in conflicts:
        left_event_id = str(item.get("left_event_id") or "").strip()
        right_event_id = str(item.get("right_event_id") or "").strip()
        if left_event_id:
            conflict_event_ids.add(left_event_id)
        if right_event_id:
            conflict_event_ids.add(right_event_id)

    rows: list[dict[str, Any]] = []
    for item in normalized_events:
        event_id = str(item.get("event_id") or "").strip()
        rows.append(
            {
                "id": event_id,
                "title": str(item.get("title") or "").strip(),
                "start": str(item.get("start_at") or "").strip(),
                "end": str(item.get("end_at") or "").strip(),
                "is_conflict": event_id in conflict_event_ids,
            }
        )

    rows.sort(key=lambda item: (str(item.get("start") or ""), str(item.get("end") or ""), str(item.get("id") or "")))
    return rows


def orchestrator(
    email_items: Sequence[Mapping[str, Any]],
    calendar_items: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Combine email and calendar agent output into the canonical /home contract."""
    action_candidates: list[dict[str, Any]] = []
    for row in email_items:
        if str(row.get("classification") or "").strip().lower() != "action":
            continue

        action_id = str(row.get("id") or "").strip()
        summary = str(row.get("summary") or "").strip()
        raw_action_text = str(row.get("action_required") or "").strip() or summary
        action_title = _verb_first_action_title(raw_action_text, summary)
        priority = _action_priority(title=raw_action_text, summary=summary)

        action_candidates.append(
            {
                "id": action_id or f"email-action-{_stable_id(action_title, summary)}",
                "title": action_title,
                "source": "email",
                "priority": priority,
                "_summary": summary,
                "_time_sensitive": _has_same_day_hint(f"{summary} {raw_action_text}"),
            }
        )

    deduped_actions: dict[str, dict[str, Any]] = {}
    for action in action_candidates:
        action_id = str(action.get("id") or "").strip()
        action_title = str(action.get("title") or "").strip()
        dedupe_key = action_id or f"title-{_stable_id(action_title)}"
        existing = deduped_actions.get(dedupe_key)
        if existing is None:
            deduped_actions[dedupe_key] = action
            continue

        existing_rank = _PRIORITY_RANK.get(str(existing.get("priority") or "low"), 3)
        candidate_rank = _PRIORITY_RANK.get(str(action.get("priority") or "low"), 3)
        existing_key = (
            existing_rank,
            str(existing.get("title") or ""),
            str(existing.get("id") or ""),
        )
        candidate_key = (
            candidate_rank,
            str(action.get("title") or ""),
            str(action.get("id") or ""),
        )
        if candidate_key < existing_key:
            deduped_actions[dedupe_key] = action

    unresolved_actions = sorted(
        deduped_actions.values(),
        key=lambda item: (
            _PRIORITY_RANK.get(str(item.get("priority") or "low"), 3),
            str(item.get("title") or ""),
            str(item.get("id") or ""),
        ),
    )

    calendar: list[dict[str, Any]] = []
    parsed_calendar: list[tuple[datetime, datetime, dict[str, Any]]] = []
    for row in calendar_items:
        start_text = str(row.get("start") or "").strip()
        end_text = str(row.get("end") or "").strip()
        start_dt = _parse_iso_datetime(start_text)
        end_dt = _parse_iso_datetime(end_text)
        if start_dt is None or end_dt is None or end_dt <= start_dt:
            continue

        normalized = {
            "id": str(row.get("id") or "").strip(),
            "title": str(row.get("title") or "").strip(),
            "start": _utc_iso(start_dt) or start_text,
            "end": _utc_iso(end_dt) or end_text,
        }
        calendar.append(normalized)
        parsed_calendar.append((start_dt, end_dt, normalized))

    calendar.sort(key=lambda item: (str(item.get("start") or ""), str(item.get("end") or ""), str(item.get("id") or "")))
    parsed_calendar.sort(key=lambda item: (item[0], item[1], str(item[2].get("id") or "")))

    needs_decision: list[dict[str, Any]] = []
    for left_index in range(len(parsed_calendar)):
        left_start, left_end, left_item = parsed_calendar[left_index]
        for right_index in range(left_index + 1, len(parsed_calendar)):
            right_start, right_end, right_item = parsed_calendar[right_index]
            if right_start >= left_end:
                break
            if left_start < right_end and right_start < left_end:
                left_title = str(left_item.get("title") or "event A")
                right_title = str(right_item.get("title") or "event B")
                needs_decision.append(
                    {
                        "id": f"decision-{_stable_id(str(left_item.get('id')), str(right_item.get('id')), 'calendar')}",
                        "type": "calendar_conflict",
                        "priority": "high",
                        "question": f"Which event should keep this time slot: {left_title} or {right_title}?",
                        "options": [
                            f"Keep {left_title}",
                            f"Keep {right_title}",
                            "Reschedule one of them",
                        ],
                    }
                )

    calendar_title_tokens = [_content_tokens(str(item.get("title") or "")) for item in calendar]
    if not needs_decision:
        for action in unresolved_actions:
            if not bool(action.get("_time_sensitive")):
                continue
            action_text = f"{action.get('title') or ''} {action.get('_summary') or ''}"
            action_tokens = _content_tokens(action_text)
            has_calendar_match = any(action_tokens.intersection(tokens) for tokens in calendar_title_tokens)
            if has_calendar_match:
                continue

            action_title = str(action.get("title") or "this action")
            needs_decision.append(
                {
                    "id": f"decision-{_stable_id(str(action.get('id')), 'time_constraint')}",
                    "type": "time_constraint_violation",
                    "priority": "high",
                    "question": f"When should '{action_title}' be scheduled today?",
                    "options": [
                        "Schedule it this morning",
                        "Schedule it this afternoon",
                        "Defer it to tomorrow",
                    ],
                }
            )

    needs_decision.sort(
        key=lambda item: (
            _PRIORITY_RANK.get(str(item.get("priority") or "low"), 3),
            str(item.get("type") or ""),
            str(item.get("id") or ""),
        )
    )

    actions = [
        {
            "id": str(item.get("id") or ""),
            "title": str(item.get("title") or ""),
            "source": "email",
            "priority": str(item.get("priority") or "low"),
        }
        for item in unresolved_actions
    ]

    summary = _summary_narrative(
        conflict_count=sum(1 for item in needs_decision if str(item.get("type") or "") == "calendar_conflict"),
        action_count=len(actions),
        event_count=len(calendar),
    )

    return freeze_home_v0_contract(
        {
            "needs_decision": needs_decision,
            "actions": actions,
            "calendar": calendar,
            "summary": summary,
        }
    )


class EmailAgent:
    """Extract actionable items from normalized email payloads without side effects."""

    _ACTION_HINT_TOKENS = (
        "action",
        "todo",
        "follow up",
        "follow-up",
        "reply",
        "schedule",
        "confirm",
    )

    def extract_action_items(self, email_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
        explicit_items = self._extract_explicit_items(email_payload)
        if explicit_items:
            return explicit_items

        body = str(email_payload.get("body") or email_payload.get("snippet") or "").strip()
        lines = [line.strip(" -\t") for line in body.splitlines() if line.strip()]

        extracted: list[dict[str, Any]] = []
        for line in lines:
            lowered = line.lower()
            if not any(token in lowered for token in self._ACTION_HINT_TOKENS):
                continue
            if len(line) > 180:
                line = line[:177].rstrip() + "..."
            extracted.append(
                {
                    "action_id": f"email-action-{_stable_id(line)}",
                    "title": line,
                    "due_date": self._extract_due_date(line),
                    "source": "email_body",
                }
            )

        return extracted

    def _extract_explicit_items(self, email_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
        raw_items = email_payload.get("action_items")
        if not isinstance(raw_items, list):
            return []

        normalized: list[dict[str, Any]] = []
        for index, item in enumerate(raw_items, start=1):
            if isinstance(item, Mapping):
                title = str(item.get("title") or item.get("text") or "").strip()
                if not title:
                    continue
                normalized.append(
                    {
                        "action_id": str(item.get("action_id") or f"email-action-{_stable_id(title, str(index))}"),
                        "title": title,
                        "due_date": self._extract_due_date(item.get("due_date") or item.get("due_at") or ""),
                        "source": str(item.get("source") or "email_payload"),
                    }
                )
                continue

            title = str(item or "").strip()
            if not title:
                continue
            normalized.append(
                {
                    "action_id": f"email-action-{_stable_id(title, str(index))}",
                    "title": title,
                    "due_date": self._extract_due_date(title),
                    "source": "email_payload",
                }
            )
        return normalized

    def _extract_due_date(self, text: Any) -> str | None:
        value = str(text or "").strip()
        if not value:
            return None

        try:
            return date_value.fromisoformat(value).isoformat()
        except ValueError:
            return None


class CalendarAgent:
    """Detect deterministic conflicts from normalized calendar event windows."""

    def normalize_events(self, payload: Mapping[str, Any]) -> list[dict[str, Any]]:
        raw_events = payload.get("events")
        rows = _as_mapping_list(raw_events)

        normalized: list[dict[str, Any]] = []
        for index, row in enumerate(rows, start=1):
            title = str(row.get("title") or row.get("summary") or "").strip() or f"event-{index}"
            start_at = _utc_iso(row.get("start_at") or row.get("start") or row.get("start_time"))
            end_at = _utc_iso(row.get("end_at") or row.get("end") or row.get("end_time"))
            if not start_at or not end_at:
                continue
            if end_at <= start_at:
                continue

            normalized.append(
                {
                    "event_id": str(row.get("event_id") or row.get("id") or f"calendar-{_stable_id(title, start_at, end_at)}"),
                    "title": title,
                    "start_at": start_at,
                    "end_at": end_at,
                    "source": str(row.get("source") or "calendar_payload"),
                }
            )

        normalized.sort(key=lambda row: (row["start_at"], row["end_at"], row["event_id"]))
        return normalized

    def detect_conflicts(self, events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        conflicts: list[dict[str, Any]] = []
        for left_index in range(len(events)):
            left = events[left_index]
            for right_index in range(left_index + 1, len(events)):
                right = events[right_index]
                right_start = str(right.get("start_at") or "")
                left_end = str(left.get("end_at") or "")
                if right_start >= left_end:
                    break

                left_start = str(left.get("start_at") or "")
                right_end = str(right.get("end_at") or "")
                if left_start < right_end and right_start < left_end:
                    conflicts.append(
                        {
                            "conflict_id": f"conflict-{_stable_id(str(left.get('event_id')), str(right.get('event_id')))}",
                            "left_event_id": str(left.get("event_id") or ""),
                            "right_event_id": str(right.get("event_id") or ""),
                            "left_title": str(left.get("title") or ""),
                            "right_title": str(right.get("title") or ""),
                            "start_at": max(left_start, right_start),
                            "end_at": min(left_end, right_end),
                        }
                    )
        return conflicts


class HeadAgent:
    """Coordinate domain agents without owning any state or persistence."""

    def __init__(
        self,
        *,
        email_agent: EmailAgent | None = None,
        calendar_agent: CalendarAgent | None = None,
    ) -> None:
        self._email_agent = email_agent or EmailAgent()
        self._calendar_agent = calendar_agent or CalendarAgent()

    def process_email_ingest(self, payload: Mapping[str, Any], *, request_id: str) -> dict[str, Any]:
        email = _as_mapping(payload.get("email") or payload)
        email_id = str(email.get("email_id") or email.get("id") or f"email-{request_id}")
        subject = str(email.get("subject") or "")

        action_items = self._email_agent.extract_action_items(email)
        linked_actions: list[dict[str, Any]] = []
        for item in action_items:
            linked = dict(item)
            linked["email_id"] = email_id
            linked_actions.append(linked)

        return {
            "email": {
                "email_id": email_id,
                "from": str(email.get("from") or email.get("sender") or ""),
                "subject": subject,
                "received_at": _utc_iso(email.get("received_at") or email.get("date")) or _utc_iso(datetime.now(UTC)),
            },
            "action_items": linked_actions,
        }

    def process_calendar_ingest(self, payload: Mapping[str, Any], *, request_id: str) -> dict[str, Any]:
        normalized_events = self._calendar_agent.normalize_events(payload)
        conflicts = self._calendar_agent.detect_conflicts(normalized_events)

        return {
            "calendar_id": str(payload.get("calendar_id") or f"calendar-{request_id}"),
            "events": normalized_events,
            "conflicts": conflicts,
        }
