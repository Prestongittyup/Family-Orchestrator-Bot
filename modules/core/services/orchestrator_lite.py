from __future__ import annotations

import hashlib
import re
from typing import Any

from modules.core.models.module_output import ModuleOutput, Proposal, Signal
from modules.core.services.module_runner import run_all_modules

LEGACY_ISOLATED = True


class DuplicateModuleNameError(ValueError):
    """Raised when two ModuleOutput items share the same module name."""


_MODULE_WEIGHT = {
    "task_module": 1.0,
    "calendar_module": 1.2,
    "meal_module": 0.9,
}

_MODULE_TIE_BREAK = {
    "task_module": 0,
    "calendar_module": 1,
    "meal_module": 2,
}


def _normalize_title(value: str) -> str:
    lowered = value.lower().strip()
    alnum_space = re.sub(r"[^a-z0-9\s]+", " ", lowered)
    return " ".join(alnum_space.split())


def _proposal_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _extract_time_window(message: str) -> str | None:
    # Matches values such as 4 PM, 4:00 PM, 16:00.
    match = re.search(
        r"\b(?:[01]?\d|2[0-3])(?::[0-5]\d)?\s?(?:[AaPp][Mm])?\b",
        message,
    )
    if not match:
        return None
    return " ".join(match.group(0).upper().split())


def _proposal_time_window_start(proposal: Proposal) -> str:
    match = re.search(r"time_window=([^;]+)", proposal.description)
    if not match:
        return ""

    raw_value = match.group(1).strip()
    if raw_value == "none":
        return ""

    if "->" not in raw_value:
        return raw_value

    start, _ = raw_value.split("->", 1)
    return start.strip()


def _sort_deterministic_proposals(proposals: list[Proposal]) -> list[Proposal]:
    # Stable sort with explicit deterministic keys.
    return sorted(
        proposals,
        key=lambda item: (
            -item.priority,
            1 if _proposal_time_window_start(item) == "" else 0,
            _proposal_time_window_start(item),
            item.type,
            item.id,
        ),
    )


def _sort_deterministic_signals(signals: list[Signal]) -> list[Signal]:
    # Stable sort by type first, with deterministic tie-breakers.
    return sorted(
        signals,
        key=lambda item: (item.type, item.id, item.source_module),
    )


def _signal_domain(signal: Signal) -> str:
    token = f"{signal.type} {signal.message}".lower()
    if any(word in token for word in ("schedule", "overdue", "deadline", "conflict", "time")):
        return "time_conflict"
    if "meal" in token:
        return "meal"
    return "general"


def _build_duplicate_clusters(proposals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}

    for proposal in proposals:
        key = (_normalize_title(proposal["title"]), proposal["type"])
        grouped.setdefault(key, []).append(proposal)

    clusters: list[dict[str, Any]] = []
    cluster_index = 1

    for (normalized_title, intent_type), items in grouped.items():
        modules = {item["source_module"] for item in items}
        if len(items) >= 2 and len(modules) >= 2:
            clusters.append(
                {
                    "cluster_id": f"duplicate_cluster_{cluster_index}",
                    "normalized_title": normalized_title,
                    "intent_type": intent_type,
                    "proposals": [item["id"] for item in items],
                    "proposal_ids": [item["id"] for item in items],
                    "modules": sorted(modules),
                    "size": len(items),
                }
            )
            cluster_index += 1

    return clusters


def _build_signal_correlations(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}

    for signal in signals:
        domain = _signal_domain(
            Signal(
                id=signal["id"],
                type=signal["type"],
                message=signal["message"],
                severity=signal["severity"],
                source_module=signal["source_module"],
            )
        )
        time_window = _extract_time_window(signal["message"]) or "any"
        grouped.setdefault((domain, time_window), []).append(signal)

    correlations: list[dict[str, Any]] = []
    index = 1

    for (domain, time_window), items in grouped.items():
        modules = {item["source_module"] for item in items}
        if len(items) >= 2 and len(modules) >= 2:
            correlation_id = f"correlation_{index}"
            correlations.append(
                {
                    "correlation_id": correlation_id,
                    "domain": domain,
                    "time_window": time_window,
                    "signal_ids": [item["id"] for item in items],
                    "modules": sorted(modules),
                    "size": len(items),
                }
            )
            index += 1

    return correlations


def apply_semantic_layer(merged: dict[str, Any]) -> dict[str, Any]:
    # Build detached records so original ModuleOutput objects remain untouched.
    proposal_records: list[dict[str, Any]] = []
    for proposal in merged["proposals"]:
        module_name = proposal.source_module
        weight = _MODULE_WEIGHT.get(module_name, 1.0)
        normalized_priority = proposal.priority * weight
        proposal_records.append(
            {
                **proposal.to_dict(),
                "normalized_priority": normalized_priority,
                "_module_rank": _MODULE_TIE_BREAK.get(module_name, 99),
                "_stable_hash": _proposal_hash(proposal.id),
            }
        )

    proposal_records.sort(
        key=lambda item: (
            -item["normalized_priority"],
            item["_module_rank"],
            item["_stable_hash"],
        )
    )

    ordering_index = [
        {
            "position": idx,
            "id": item["id"],
            "proposal_id": item["id"],
            "source_module": item["source_module"],
            "normalized_priority": item["normalized_priority"],
            "stable_hash": item["_stable_hash"],
        }
        for idx, item in enumerate(proposal_records)
    ]

    public_proposals = [
        {
            "id": item["id"],
            "type": item["type"],
            "title": item["title"],
            "description": item["description"],
            "priority": item["priority"],
            "source_module": item["source_module"],
            "duration": int(item.get("duration", 1)),
            "effort": str(item.get("effort", "medium")),
            "category": str(item.get("category", "other")),
            "normalized_priority": item["normalized_priority"],
        }
        for item in proposal_records
    ]

    signal_records = [signal.to_dict() for signal in merged["signals"]]
    signal_correlations = _build_signal_correlations(signal_records)

    correlation_lookup: dict[str, str] = {}
    for correlation in signal_correlations:
        for signal_id in correlation["signal_ids"]:
            correlation_lookup[signal_id] = correlation["correlation_id"]

    public_signals = []
    for signal in signal_records:
        signal_copy = dict(signal)
        if signal_copy["id"] in correlation_lookup:
            signal_copy["correlation_id"] = correlation_lookup[signal_copy["id"]]
        public_signals.append(signal_copy)

    duplicate_clusters = _build_duplicate_clusters(public_proposals)

    return {
        "proposals": public_proposals,
        "signals": public_signals,
        "by_module": merged["by_module"],
        "metadata": dict(merged["metadata"]),
        "semantic_layer": {
            "duplicate_clusters": duplicate_clusters,
            "signal_correlations": signal_correlations,
            "ordering_index": ordering_index,
        },
    }


def merge_module_outputs(outputs: list[ModuleOutput]) -> dict[str, Any]:
    if not isinstance(outputs, list):
        raise TypeError("outputs must be a list[ModuleOutput]")

    proposals: list[Proposal] = []
    signals: list[Signal] = []
    by_module: dict[str, ModuleOutput] = {}

    for output in outputs:
        if not isinstance(output, ModuleOutput):
            raise TypeError("All items in outputs must be ModuleOutput")

        module_name = output.module
        if module_name in by_module:
            raise DuplicateModuleNameError(
                f"Duplicate module name detected: {module_name}"
            )

        # Pure merge behavior: preserve all records, no filtering/prioritization.
        proposals.extend(output.proposals)
        signals.extend(output.signals)
        by_module[module_name] = output

    sorted_proposals = _sort_deterministic_proposals(proposals)
    sorted_signals = _sort_deterministic_signals(signals)

    return {
        "proposals": sorted_proposals,
        "signals": sorted_signals,
        "by_module": by_module,
        "metadata": {
            "module_count": len(outputs),
        },
    }


def run_orchestrator(household_id: str) -> dict[str, Any]:
    outputs = run_all_modules(household_id)
    merged = merge_module_outputs(outputs)
    return apply_semantic_layer(merged)


def run_orchestrator_as_dict(household_id: str) -> dict[str, Any]:
    merged = run_orchestrator(household_id)

    proposals = []
    for proposal in merged["proposals"]:
        if hasattr(proposal, "to_dict"):
            proposals.append(proposal.to_dict())
        else:
            proposals.append(dict(proposal))

    signals = []
    for signal in merged["signals"]:
        if hasattr(signal, "to_dict"):
            signals.append(signal.to_dict())
        else:
            signals.append(dict(signal))

    return {
        "proposals": proposals,
        "signals": signals,
        "by_module": {
            module_name: output.to_dict()
            for module_name, output in merged["by_module"].items()
        },
        "metadata": dict(merged["metadata"]),
        "semantic_layer": dict(merged["semantic_layer"]),
    }
