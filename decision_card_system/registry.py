from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import Any, Iterable, Mapping


DECISION_CARD_CONTRACT_VERSION = "v1"
DECISION_CARD_CANONICAL_ORIGIN_API = "core.replay.decision_card_registry.createDecisionCard"

DECISION_CARD_GENERATED_EVENT_CANONICAL = "DecisionCardGenerated"
DECISION_CARD_SURFACED_EVENT_CANONICAL = "DecisionCardSurfaced"
DECISION_CARD_ACKNOWLEDGED_EVENT_CANONICAL = "DecisionCardAcknowledged"
DECISION_CARD_RESOLVED_EVENT_CANONICAL = "DecisionCardResolved"
DECISION_CARD_APPLIED_EVENT_CANONICAL = "DecisionCardApplied"

DECISION_CARD_STATE_GENERATED = "generated"
DECISION_CARD_STATE_SURFACED = "surfaced"
DECISION_CARD_STATE_ACKNOWLEDGED = "acknowledged"
DECISION_CARD_STATE_RESOLVED = "resolved"
DECISION_CARD_STATE_APPLIED = "applied"

_DECISION_ACTION_RESOLUTION_EVENT_TYPES = frozenset({"DecisionDeferred", "DecisionIgnored"})
_DECISION_ACTION_APPLIED_EVENT_TYPES = frozenset({"DecisionCompleted"})

_DECISION_CARD_STATE_BY_EVENT = {
    DECISION_CARD_GENERATED_EVENT_CANONICAL: DECISION_CARD_STATE_GENERATED,
    DECISION_CARD_SURFACED_EVENT_CANONICAL: DECISION_CARD_STATE_SURFACED,
    DECISION_CARD_ACKNOWLEDGED_EVENT_CANONICAL: DECISION_CARD_STATE_ACKNOWLEDGED,
    DECISION_CARD_RESOLVED_EVENT_CANONICAL: DECISION_CARD_STATE_RESOLVED,
    DECISION_CARD_APPLIED_EVENT_CANONICAL: DECISION_CARD_STATE_APPLIED,
    "DecisionDeferred": DECISION_CARD_STATE_RESOLVED,
    "DecisionIgnored": DECISION_CARD_STATE_RESOLVED,
    "DecisionCompleted": DECISION_CARD_STATE_APPLIED,
}

_DECISION_CARD_ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    DECISION_CARD_STATE_GENERATED: frozenset({DECISION_CARD_STATE_SURFACED}),
    DECISION_CARD_STATE_SURFACED: frozenset({DECISION_CARD_STATE_ACKNOWLEDGED}),
    DECISION_CARD_STATE_ACKNOWLEDGED: frozenset({DECISION_CARD_STATE_RESOLVED}),
    DECISION_CARD_STATE_RESOLVED: frozenset({DECISION_CARD_STATE_APPLIED}),
    DECISION_CARD_STATE_APPLIED: frozenset(),
}

_DECISION_CARD_CLOSED_STATES = frozenset({
    DECISION_CARD_STATE_RESOLVED,
    DECISION_CARD_STATE_APPLIED,
})


class DecisionCardInvariantError(ValueError):
    """Raised when decision-card lifecycle invariants are violated."""


def normalize_root_cause_key(value: str) -> str:
    normalized = " ".join(str(value or "").strip().lower().split())
    normalized = re.sub(r"[^a-z0-9._\-]+", "-", normalized)
    normalized = re.sub(r"\-+", "-", normalized)
    return normalized.strip("-._")


def build_decision_card_dedupe_key(root_cause_key: str, contract_version: str) -> str:
    normalized_root = normalize_root_cause_key(root_cause_key)
    normalized_version = str(contract_version or "").strip() or DECISION_CARD_CONTRACT_VERSION
    if not normalized_root:
        raise DecisionCardInvariantError("root_cause_key is required")
    return f"{normalized_root}:{normalized_version}"


def _stable_decision_card_id(*, household_id: str, dedupe_key: str) -> str:
    fingerprint = hashlib.sha256(f"{household_id}:{dedupe_key}".encode("utf-8")).hexdigest()
    return f"dc-{fingerprint[:24]}"


def _coerce_timestamp(value: datetime | str | None) -> str:
    if isinstance(value, datetime):
        resolved = value
        if resolved.tzinfo is None:
            resolved = resolved.replace(tzinfo=UTC)
        return resolved.astimezone(UTC).isoformat().replace("+00:00", "Z")

    raw = str(value or "").strip()
    if not raw:
        return datetime.now(UTC).isoformat().replace("+00:00", "Z")

    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return raw
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def createDecisionCard(
    *,
    household_id: str,
    root_cause_key: str,
    title: str,
    actor_id: str,
    timestamp: datetime | str | None = None,
    contract_version: str = DECISION_CARD_CONTRACT_VERSION,
    metadata: Mapping[str, Any] | None = None,
    decision_card_id: str | None = None,
) -> dict[str, Any]:
    resolved_household_id = str(household_id or "").strip()
    resolved_actor_id = str(actor_id or "").strip()
    resolved_title = str(title or "").strip()

    if not resolved_household_id:
        raise DecisionCardInvariantError("household_id is required")
    if not resolved_actor_id:
        raise DecisionCardInvariantError("actor_id is required")
    if not resolved_title:
        raise DecisionCardInvariantError("title is required")

    dedupe_key = build_decision_card_dedupe_key(root_cause_key, contract_version)
    resolved_contract_version = str(contract_version or "").strip() or DECISION_CARD_CONTRACT_VERSION
    resolved_card_id = str(decision_card_id or "").strip() or _stable_decision_card_id(
        household_id=resolved_household_id,
        dedupe_key=dedupe_key,
    )

    event_payload: dict[str, Any] = {
        "event_type": DECISION_CARD_GENERATED_EVENT_CANONICAL,
        "decision_card_id": resolved_card_id,
        "household_id": resolved_household_id,
        "actor_id": resolved_actor_id,
        "title": resolved_title,
        "root_cause_key": normalize_root_cause_key(root_cause_key),
        "contract_version": resolved_contract_version,
        "origin_api": DECISION_CARD_CANONICAL_ORIGIN_API,
        "dedupe_key": dedupe_key,
        "timestamp": _coerce_timestamp(timestamp),
    }

    if metadata is not None:
        event_payload["metadata"] = dict(metadata)

    return event_payload


def create_decision_card(**kwargs: Any) -> dict[str, Any]:
    return createDecisionCard(**kwargs)


def _normalize_decision_cards(decision_cards: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for decision_card_id, row in decision_cards.items():
        if not isinstance(row, Mapping):
            continue
        normalized[str(decision_card_id)] = dict(row)
    return normalized


def _active_card_id_for_dedupe_key(
    *,
    decision_cards: Mapping[str, Mapping[str, Any]],
    dedupe_key: str,
) -> str | None:
    for decision_card_id, row in decision_cards.items():
        if not isinstance(row, Mapping):
            continue
        if str(row.get("dedupe_key") or "").strip() != dedupe_key:
            continue
        state = str(row.get("state") or "").strip().lower()
        if state not in _DECISION_CARD_CLOSED_STATES:
            return str(decision_card_id)
    return None


def _extract_decision_card_id(
    *,
    event_type: str,
    payload: Mapping[str, Any],
    decision_cards: Mapping[str, Mapping[str, Any]],
) -> str:
    explicit = str(payload.get("decision_card_id") or payload.get("card_id") or "").strip()
    if explicit:
        return explicit

    if event_type in _DECISION_ACTION_RESOLUTION_EVENT_TYPES or event_type in _DECISION_ACTION_APPLIED_EVENT_TYPES:
        decision_id = str(payload.get("decision_id") or "").strip()
        if decision_id and decision_id in decision_cards:
            return decision_id

    return ""


def _transition_allowed(*, from_state: str, to_state: str) -> bool:
    allowed = _DECISION_CARD_ALLOWED_TRANSITIONS.get(from_state, frozenset())
    return to_state in allowed


def reduce_decision_card_projection(
    *,
    event_type: str,
    payload: Mapping[str, Any],
    recorded_at: str,
    decision_cards: Mapping[str, Mapping[str, Any]],
    strict: bool = True,
) -> dict[str, dict[str, Any]]:
    target_state = _DECISION_CARD_STATE_BY_EVENT.get(str(event_type or "").strip())
    current_cards = _normalize_decision_cards(decision_cards)

    if target_state is None:
        return current_cards

    if event_type == DECISION_CARD_GENERATED_EVENT_CANONICAL:
        decision_card_id = _extract_decision_card_id(
            event_type=event_type,
            payload=payload,
            decision_cards=current_cards,
        )
        if not decision_card_id:
            if strict:
                raise DecisionCardInvariantError("DecisionCardGenerated missing decision_card_id")
            return current_cards

        if decision_card_id in current_cards:
            raise DecisionCardInvariantError(
                f"duplicate DecisionCardGenerated for decision_card_id={decision_card_id}"
            )

        root_cause_key = normalize_root_cause_key(str(payload.get("root_cause_key") or ""))
        if not root_cause_key:
            raise DecisionCardInvariantError("DecisionCardGenerated missing root_cause_key")

        contract_version = str(payload.get("contract_version") or "").strip() or DECISION_CARD_CONTRACT_VERSION
        origin_api = str(payload.get("origin_api") or "").strip()
        if origin_api != DECISION_CARD_CANONICAL_ORIGIN_API:
            raise DecisionCardInvariantError(
                "DecisionCardGenerated origin_api must match canonical createDecisionCard API"
            )
        expected_dedupe_key = build_decision_card_dedupe_key(root_cause_key, contract_version)
        dedupe_key = str(payload.get("dedupe_key") or "").strip() or expected_dedupe_key

        if dedupe_key != expected_dedupe_key:
            raise DecisionCardInvariantError(
                "DecisionCardGenerated dedupe_key does not match root_cause_key + contract_version"
            )

        active_card_id = _active_card_id_for_dedupe_key(
            decision_cards=current_cards,
            dedupe_key=dedupe_key,
        )
        if active_card_id is not None:
            raise DecisionCardInvariantError(
                "duplicate unresolved decision card for "
                f"dedupe_key={dedupe_key} (existing decision_card_id={active_card_id})"
            )

        timestamp = _coerce_timestamp(payload.get("timestamp") or recorded_at)
        created_row: dict[str, Any] = {
            "decision_card_id": decision_card_id,
            "household_id": str(payload.get("household_id") or "").strip(),
            "title": str(payload.get("title") or "").strip(),
            "root_cause_key": root_cause_key,
            "contract_version": contract_version,
            "origin_api": origin_api,
            "dedupe_key": dedupe_key,
            "state": DECISION_CARD_STATE_GENERATED,
            "created_at": timestamp,
            "updated_at": timestamp,
            "actor_id": str(payload.get("actor_id") or "").strip(),
            "last_event_type": event_type,
        }
        if isinstance(payload.get("metadata"), Mapping):
            created_row["metadata"] = dict(payload.get("metadata") or {})

        return {**current_cards, decision_card_id: created_row}

    decision_card_id = _extract_decision_card_id(
        event_type=event_type,
        payload=payload,
        decision_cards=current_cards,
    )
    if not decision_card_id:
        if not strict and (
            event_type in _DECISION_ACTION_RESOLUTION_EVENT_TYPES
            or event_type in _DECISION_ACTION_APPLIED_EVENT_TYPES
        ):
            return current_cards
        if strict:
            raise DecisionCardInvariantError(
                f"{event_type} missing decision_card_id"
            )
        return current_cards

    existing = current_cards.get(decision_card_id)
    if not isinstance(existing, Mapping):
        if not strict and (
            event_type in _DECISION_ACTION_RESOLUTION_EVENT_TYPES
            or event_type in _DECISION_ACTION_APPLIED_EVENT_TYPES
        ):
            return current_cards
        raise DecisionCardInvariantError(
            f"{event_type} references unknown decision_card_id={decision_card_id}"
        )

    current_state = str(existing.get("state") or "").strip().lower()
    if current_state == target_state:
        # Runtime may emit explicit card lifecycle events and legacy Decision* events
        # in the same command path. Treat alias Decision* transitions as idempotent
        # when the card is already at the target state.
        if (
            target_state == DECISION_CARD_STATE_RESOLVED
            and event_type in _DECISION_ACTION_RESOLUTION_EVENT_TYPES
        ):
            return current_cards
        if (
            target_state == DECISION_CARD_STATE_APPLIED
            and event_type in _DECISION_ACTION_APPLIED_EVENT_TYPES
        ):
            return current_cards
        raise DecisionCardInvariantError(
            f"duplicate {event_type} for decision_card_id={decision_card_id}"
        )

    if not _transition_allowed(from_state=current_state, to_state=target_state):
        allowed = sorted(_DECISION_CARD_ALLOWED_TRANSITIONS.get(current_state, frozenset()))
        raise DecisionCardInvariantError(
            f"invalid lifecycle transition for decision_card_id={decision_card_id}: "
            f"{current_state} -> {target_state}; allowed={allowed}"
        )

    timestamp = _coerce_timestamp(payload.get("timestamp") or recorded_at)
    updated = dict(existing)
    updated["state"] = target_state
    updated["updated_at"] = timestamp
    updated["last_event_type"] = event_type

    actor_id = str(payload.get("actor_id") or "").strip()
    if actor_id:
        updated["actor_id"] = actor_id

    if target_state == DECISION_CARD_STATE_RESOLVED:
        updated["resolved_at"] = timestamp
        defer_to_date = str(payload.get("defer_to_date") or "").strip()
        if defer_to_date:
            updated["defer_to_date"] = defer_to_date

    if target_state == DECISION_CARD_STATE_APPLIED:
        updated["applied_at"] = timestamp

    return {**current_cards, decision_card_id: updated}


def project_decision_card_registry(
    events: Iterable[Mapping[str, Any] | Any],
    *,
    strict: bool = True,
) -> dict[str, dict[str, Any]]:
    projected: dict[str, dict[str, Any]] = {}

    for event in events:
        if isinstance(event, Mapping):
            event_type = str(event.get("event_type") or event.get("type") or "").strip()
            payload = event.get("payload")
            timestamp = event.get("timestamp")
        else:
            event_type = str(getattr(event, "event_type", getattr(event, "type", "")) or "").strip()
            payload = getattr(event, "payload", None)
            timestamp = getattr(event, "timestamp", None)

        if not isinstance(payload, Mapping):
            continue

        projected = reduce_decision_card_projection(
            event_type=event_type,
            payload=payload,
            recorded_at=_coerce_timestamp(timestamp),
            decision_cards=projected,
            strict=strict,
        )

    return projected
