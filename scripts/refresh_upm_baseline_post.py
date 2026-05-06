from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

from app.main import app

THRESHOLD_SUPPRESSION_REASONS = {
    "upm_actionability_below_threshold",
    "upm_confidence_below_threshold",
    "upm_no_decision_required",
    "low_impact_actionable_noise",
}
COMPRESSION_SUPPRESSION_REASONS = {
    "decision_cap_reached",
    "decision_density_cap_reached",
    "collapsed_into_low_priority_decision",
}
DEDUP_SUPPRESSION_REASONS = {"merged_into_existing"}


def _seed_schedule(client: TestClient, *, household_id: str, schedule_id: str, title: str, start_at: str, end_at: str) -> None:
    response = client.post(
        "/command",
        json={
            "command_type": "schedule.create",
            "household_id": household_id,
            "payload": {
                "schedule_id": schedule_id,
                "title": title,
                "start_at": start_at,
                "end_at": end_at,
            },
        },
    )
    response.raise_for_status()


def _ingest_message(client: TestClient, *, household_id: str, raw_content: str, created_at: str, member_id: str) -> dict:
    response = client.post(
        "/ingest/message",
        json={
            "household_id": household_id,
            "raw_content": raw_content,
            "source": "manual",
            "created_at": created_at,
            "member_id": member_id,
        },
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        return {}
    response_payload = payload.get("response")
    if isinstance(response_payload, dict):
        return response_payload
    return {}


def _counter_to_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter) if counter[key] > 0}


def _compute_metrics(responses: list[dict]) -> dict:
    total = len(responses) or 1
    priority_distribution = Counter(str(item.get("upm_priority_class") or "noise") for item in responses)

    threshold_reasons = Counter()
    compression_reasons = Counter()
    dedup_reasons = Counter()
    other_reasons = Counter()

    decision_count = 0
    unnecessary_decisions = 0
    missed_decisions = 0
    conflicts_missed = 0
    false_negative_suppressions = 0
    high_conf_actionable_missed = 0
    valid_noise_suppressions = 0
    conflict_events = 0
    borderline_count = 0

    for row in responses:
        generated = bool(row.get("decision_generated"))
        blocked = bool(row.get("decision_blocked"))
        suppression_reason = str(row.get("suppression_reason") or "").strip()
        priority_class = str(row.get("upm_priority_class") or "noise")
        effective_requires_decision = bool(row.get("effective_upm_requires_decision", row.get("upm_requires_decision")))
        conflict_risk = bool(row.get("upm_conflict_risk")) or bool(str(row.get("conflict_schedule_id") or "").strip())
        if conflict_risk:
            conflict_events += 1
        if bool(row.get("upm_borderline_event")):
            borderline_count += 1

        if generated:
            decision_count += 1
            if priority_class in {"low", "noise"} and not conflict_risk:
                unnecessary_decisions += 1

        if not generated and suppression_reason:
            if suppression_reason in THRESHOLD_SUPPRESSION_REASONS:
                threshold_reasons[suppression_reason] += 1
            elif suppression_reason in COMPRESSION_SUPPRESSION_REASONS:
                compression_reasons[suppression_reason] += 1
            elif suppression_reason in DEDUP_SUPPRESSION_REASONS:
                dedup_reasons[suppression_reason] += 1
            else:
                other_reasons[suppression_reason] += 1

        if not generated and priority_class == "noise" and not effective_requires_decision:
            valid_noise_suppressions += 1

        if not generated and (effective_requires_decision or blocked):
            missed_decisions += 1
            if conflict_risk:
                conflicts_missed += 1
            if suppression_reason and suppression_reason not in DEDUP_SUPPRESSION_REASONS:
                false_negative_suppressions += 1
            actionability_score = float(row.get("upm_actionability_score") or 0.0)
            confidence_score = float(row.get("upm_confidence_score") or 0.0)
            actionability_threshold = float(row.get("upm_actionability_threshold") or 0.0)
            confidence_min = float(row.get("upm_confidence_min") or 0.0)
            if actionability_score >= actionability_threshold and confidence_score >= confidence_min:
                high_conf_actionable_missed += 1

    return {
        "borderline_event_rate": round(borderline_count / total, 2),
        "compression_suppression_reasons": _counter_to_dict(compression_reasons),
        "conflict_events": conflict_events,
        "conflicts_missed": conflicts_missed,
        "decision_count": decision_count,
        "dedup_suppression_reasons": _counter_to_dict(dedup_reasons),
        "false_negative_suppressions": false_negative_suppressions,
        "high_conf_actionable_missed": high_conf_actionable_missed,
        "missed_decisions": missed_decisions,
        "other_suppression_reasons": _counter_to_dict(other_reasons),
        "priority_distribution": _counter_to_dict(priority_distribution),
        "threshold_suppression_reasons": _counter_to_dict(threshold_reasons),
        "unnecessary_decisions": unnecessary_decisions,
        "valid_noise_suppressions": valid_noise_suppressions,
    }


def main() -> None:
    household_id = f"upm-baseline-refresh-{uuid4().hex[:10]}"
    schedules = [
        ("sched-baseline-pickup", "School pickup", "2026-05-05T15:00:00Z", "2026-05-05T16:00:00Z"),
        ("sched-baseline-practice", "Practice", "2026-05-05T18:00:00Z", "2026-05-05T19:00:00Z"),
        ("sched-baseline-dinner", "Dinner prep", "2026-05-05T17:45:00Z", "2026-05-05T18:45:00Z"),
        ("sched-baseline-soccer", "Soccer practice Tuesday", "2026-05-12T17:00:00Z", "2026-05-12T18:00:00Z"),
        ("sched-baseline-conference", "Teacher conference", "2026-05-08T09:00:00Z", "2026-05-08T10:00:00Z"),
    ]
    templates = [
        "Work meeting at 6pm",
        "Practice overlaps with dinner time",
        "Teacher conference rescheduled to Friday morning",
        "Soccer practice cancelled Tuesday",
        "Can we rethink tomorrow afternoon plans?",
    ]

    responses: list[dict] = []
    with TestClient(app) as client:
        for schedule_id, title, start_at, end_at in schedules:
            _seed_schedule(
                client,
                household_id=household_id,
                schedule_id=schedule_id,
                title=title,
                start_at=start_at,
                end_at=end_at,
            )

        for idx in range(25):
            raw_content = templates[idx % len(templates)]
            minute = idx * 3
            hour = 8 + (minute // 60)
            minute_part = minute % 60
            created_at = f"2026-05-05T{hour:02d}:{minute_part:02d}:00Z"
            response = _ingest_message(
                client,
                household_id=household_id,
                raw_content=raw_content,
                created_at=created_at,
                member_id="member-baseline-refresh",
            )
            responses.append(response)

    metrics = _compute_metrics(responses)
    output_path = Path("artifacts/upm_recall_baseline_post.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"household_id": household_id, "output_path": str(output_path), "metrics": metrics}, indent=2))


if __name__ == "__main__":
    main()
