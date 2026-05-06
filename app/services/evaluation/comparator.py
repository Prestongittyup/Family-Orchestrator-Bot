from __future__ import annotations

from typing import Any


def normalize_email_output(payload: dict[str, Any]) -> dict[str, Any]:
    priority = str(payload.get("priority") or "medium").strip().lower()
    if priority not in {"high", "medium", "low"}:
        priority = "medium"

    actions: list[dict[str, Any]] = []
    rows = payload.get("actions")
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            action_type = str(row.get("type") or "").strip().lower()
            if action_type not in {"reply", "task"}:
                continue
            title = str(row.get("title") or "").strip()
            if not title:
                continue
            due_raw = row.get("due")
            due = str(due_raw).strip() if isinstance(due_raw, str) else None
            actions.append(
                {
                    "type": action_type,
                    "title": title,
                    "due": due or None,
                }
            )

    return {
        "priority": priority,
        "needs_attention": bool(payload.get("needs_attention", False)),
        "actions": actions[:6],
        "state_summary": str(payload.get("state_summary") or "").strip() or "No summary",
        "reason": str(payload.get("reason") or "").strip() or "unspecified",
    }


def compare_email_outputs(rule_output: dict[str, Any], llm_output: dict[str, Any]) -> dict[str, Any]:
    rule_actions = _action_fingerprint_set(rule_output)
    llm_actions = _action_fingerprint_set(llm_output)

    union_count = len(rule_actions | llm_actions)
    intersection_count = len(rule_actions & llm_actions)
    action_agreement = 1.0 if union_count == 0 else round(intersection_count / union_count, 4)

    llm_only_actions = sorted(list(llm_actions - rule_actions))
    rules_only_actions = sorted(list(rule_actions - llm_actions))

    metrics = {
        "action_agreement": action_agreement,
        "action_match": action_agreement == 1.0,
        "priority_delta": str(rule_output.get("priority")) != str(llm_output.get("priority")),
        "missed_intent_detection": bool(llm_only_actions),
        "false_positive_rate": 1.0 if bool(rules_only_actions) and not bool(llm_only_actions) else 0.0,
        "llm_only_actions": llm_only_actions,
        "rules_only_actions": rules_only_actions,
    }
    return metrics


def compute_dls(rule_output: dict[str, Any], llm_output: dict[str, Any]) -> float:
    score = 0.0

    if len(llm_output.get("actions", [])) > len(rule_output.get("actions", [])):
        score += 2.0

    if str(rule_output.get("priority")) != str(llm_output.get("priority")):
        score += 1.5

    if bool(llm_output.get("needs_attention")) and not bool(rule_output.get("needs_attention")):
        score += 2.0

    if bool(rule_output.get("needs_attention")) and not bool(llm_output.get("needs_attention")):
        score -= 1.5

    return round(score, 4)


def compute_value_delta(rule_output: dict[str, Any], llm_output: dict[str, Any]) -> float:
    return compute_dls(rule_output, llm_output)


def outputs_disagree(rule_output: dict[str, Any], llm_output: dict[str, Any]) -> bool:
    if str(rule_output.get("priority")) != str(llm_output.get("priority")):
        return True
    if bool(rule_output.get("needs_attention")) != bool(llm_output.get("needs_attention")):
        return True
    return _action_fingerprint_set(rule_output) != _action_fingerprint_set(llm_output)


def _action_fingerprint_set(payload: dict[str, Any]) -> set[str]:
    rows = payload.get("actions")
    if not isinstance(rows, list):
        return set()

    output: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        action_type = str(row.get("type") or "").strip().lower()
        title = str(row.get("title") or "").strip().lower()
        if action_type in {"reply", "task"} and title:
            output.add(f"{action_type}:{title}")

    return output
